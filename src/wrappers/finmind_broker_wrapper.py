import datetime as dt
import os
import sys
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from FinMind.data import DataLoader

from .finmind_rate_limit import FinMindRateLimiter, batch_size

TAIPEI = dt.timezone(dt.timedelta(hours=8))

# taiwan_stock_market_value_weight()'s own columns (rank, stock_name, type
# dropped): weight_per is % of the index's market value, date is the
# publication's own as-of date (a string, "YYYY-MM-DD").
WEIGHT_COLUMNS = ["stock_id", "weight_per", "date"]

# Column order of the archive; a window is read as one multi-file scan, so a
# written day must not reshuffle it.
BROKER_COLUMNS = [
    "securities_trader",
    "price",
    "buy",
    "sell",
    "securities_trader_id",
    "stock_id",
    "date",
]

# Every stock that did not come back is checked individually (a check costs
# the same one request as a fetch). Exception: when most of the universe is
# missing, a small sample decides and the result is flagged inferred.
PROBE_SAMPLE = 8
THIN_FRACTION = 0.4

# Guards against certifying a day against a truncated universe.
MIN_EXPECTED = 1200

_INDEX_INDUSTRIES = {"大盤", "Index", "所有證券"}
_INDEX_IDS = {"TAIEX", "TPEx"}


class BrokerFetchResult:
    """Outcome of fetching a set of ``(stock_id, day)`` cells.

    ``absent``: shown empty upstream. ``unresolved``: returned nothing but not
    shown empty (likely dropped). ``inferred``: absence concluded from a
    sample rather than per stock.
    """

    def __init__(self, frame, received, absent, unresolved, requests_spent,
                 inferred=False):
        self.frame = frame
        self.received = received
        self.absent = absent
        self.unresolved = unresolved
        self.requests_spent = requests_spent
        self.inferred = inferred

    @property
    def complete(self):
        return not self.unresolved and not self.inferred


class FinMindWrapper:
    _api = None
    _ref_count = 0
    _limiter = None
    _stock_info = None

    def __init__(self):
        if FinMindWrapper._api is None:
            token = os.environ.get("FINMIND_API_TOKEN")
            if not token:
                print("Warning: FINMIND_API_TOKEN not set. Download may fail.", file=sys.stderr)

            FinMindWrapper._api = DataLoader()
            if token:
                FinMindWrapper._api.login_by_token(api_token=token)
        FinMindWrapper._ref_count += 1

    def __del__(self):
        FinMindWrapper._ref_count -= 1
        if FinMindWrapper._ref_count == 0:
            FinMindWrapper._api = None

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @staticmethod
    def broker_dir():
        output_dir = os.environ.get("DATA_SDK_FINMIND_BROKER_PATH")
        if not output_dir:
            print(
                "Warning: DATA_SDK_FINMIND_BROKER_PATH not set. Using current directory.",
                file=sys.stderr,
            )
            output_dir = "."
        return output_dir

    @staticmethod
    def broker_day_path(day, output_dir=None):
        return os.path.join(output_dir or FinMindWrapper.broker_dir(), f"{day}.parquet")

    # ------------------------------------------------------------------
    # Read surface -- pure. Never downloads, never writes.
    # ------------------------------------------------------------------

    def get_broker(self, day, sid):
        """Broker rows for ``(day, sid)`` from the archive.

        Reads never download: the archive is filled by the scheduled writer,
        and a missing day raises instead of costing a whole-day fetch here.
        """
        path = self.broker_day_path(day)
        self._require_day_file(day, path)
        out = pd.read_parquet(path, filters=[("stock_id", "==", str(sid))])
        return out.reset_index(drop=True)

    def get_broker_day(self, day, sids=None, columns=None):
        """Whole-day broker rows, optionally projected and filtered."""
        path = self.broker_day_path(day)
        self._require_day_file(day, path)
        filters = None
        if sids is not None:
            filters = [("stock_id", "in", [str(s) for s in sids])]
        out = pd.read_parquet(
            path, columns=list(columns) if columns else None, filters=filters
        )
        return out.reset_index(drop=True)

    def archived_stock_ids(self, day, output_dir=None):
        """Distinct stock ids already stored for ``day``; empty set if absent."""
        path = self.broker_day_path(day, output_dir)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return set()
        frame = pd.read_parquet(path, columns=["stock_id"])
        return set(frame["stock_id"].astype(str))

    @staticmethod
    def _require_day_file(day, path):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"FinMind broker file not found: {path} (day {day})")
        if os.path.getsize(path) == 0:
            raise FileNotFoundError(f"FinMind broker file is empty: {path} (day {day})")

    def get_trading_dates(self, start_date, end_date):
        """Taiwan trading calendar between the two dates, inclusive.

        Thin accessor over FinMind's ``taiwan_stock_trading_date`` dataset so
        consumers never reach the underlying DataLoader directly. Returns the
        DataLoader's DataFrame (a ``date`` column of ``YYYY-MM-DD`` strings).

        A consumer counting remaining trading days to a futures 最後結算日
        must not pick a fixed window: the furthest listed delivery month can
        be nine months out. Use ``get_trading_dates_covering`` with the
        contracts' delivery dates so the range ends past the furthest one.
        """
        self.limiter().acquire(1)
        return FinMindWrapper._api.taiwan_stock_trading_date(
            start_date=start_date, end_date=end_date
        )

    @staticmethod
    def calendar_range_covering(start_date, delivery_dates, margin_days=7):
        """``(start, end)`` for a calendar that covers every delivery date.

        ``delivery_dates`` are ``YYYY/MM/DD`` / ``YYYY-MM-DD`` strings or
        dates (``ShioajiWrapper.get_futures_contracts()['delivery_date']``);
        the end is the furthest of them plus ``margin_days``, and never
        earlier than ``start_date``. Raises ValueError on an empty input, so
        an empty contract list can never shrink the range to one day.
        """
        parsed = pd.to_datetime(pd.Series(list(delivery_dates)), errors="coerce")
        parsed = parsed.dropna()
        if parsed.empty:
            raise ValueError("no delivery dates to cover")
        start = pd.Timestamp(start_date).normalize()
        end = max(parsed.max().normalize() + pd.Timedelta(days=margin_days), start)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def get_trading_dates_covering(self, start_date, delivery_dates, margin_days=7):
        """Trading calendar from ``start_date`` past the furthest delivery date.

        The §8.5 rule for 剩餘交易日: the range is derived from the listed
        contracts, so a nine-month-out delivery month is always inside it.
        """
        start, end = self.calendar_range_covering(start_date, delivery_dates, margin_days)
        return self.get_trading_dates(start, end)

    # ------------------------------------------------------------------
    # 加權指數成分股權重 (TaiwanStockMarketValueWeight) -- cached per day,
    # one row per TWSE-listed constituent (TPEx rows dropped: the 臺指期
    # page only weights TAIEX, the TWSE index).
    # ------------------------------------------------------------------

    @staticmethod
    def weight_dir():
        return os.environ.get(
            "DATA_SDK_FINMIND_WEIGHT_PATH", "/mnt/nfs/backup/finmind_cache/twii_weight"
        )

    @staticmethod
    def weight_cache_path(day, output_dir=None):
        return os.path.join(output_dir or FinMindWrapper.weight_dir(), f"{day}.parquet")

    @staticmethod
    def _taipei_today_str():
        return dt.datetime.now(TAIPEI).date().isoformat()

    def get_twii_weights(self, day=None) -> pd.DataFrame:
        """TAIEX constituent weights for ``day`` (default: today, Taipei).

        Calls ``taiwan_stock_market_value_weight(start_date=day,
        end_date=day)`` and keeps ``type == "twse"`` (TPEx's own index has
        its own constituents; the 臺指期 page only needs TAIEX's). Returns
        ``stock_id, weight_per, date`` indexed by ``stock_id`` (str).

        This table changes once a day, so unlike the broker archive there is
        no per-stock missing-cell detection: a cache hit for ``day`` is
        served as is, and a miss does one fetch for the whole table. Cached
        to ``DATA_SDK_FINMIND_WEIGHT_PATH`` (default
        ``/mnt/nfs/backup/finmind_cache/twii_weight/<day>.parquet``); a warm
        day needs no token, a cold one needs ``FINMIND_API_TOKEN`` and draws
        from the shared ``limiter()``.
        """
        day = day or self._taipei_today_str()
        path = self.weight_cache_path(day)
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return pd.read_parquet(path)

        self.limiter().acquire(1)
        raw = FinMindWrapper._api.taiwan_stock_market_value_weight(
            start_date=day, end_date=day
        )
        frame = raw[raw["type"] == "twse"][WEIGHT_COLUMNS].copy()
        frame["stock_id"] = frame["stock_id"].astype(str)
        frame = frame.set_index("stock_id")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".twii-weight-", suffix=".tmp")
        os.close(fd)
        try:
            frame.to_parquet(tmp_path)
            os.chmod(tmp_path, 0o644)
            os.replace(tmp_path, path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        return frame

    # ------------------------------------------------------------------
    # Fetch surface -- used by the scheduled writer and the archive repair.
    # ------------------------------------------------------------------

    @classmethod
    def limiter(cls):
        if cls._limiter is None:
            ledger = os.environ.get("DATA_SDK_FINMIND_RATE_LEDGER") or os.path.join(
                cls.broker_dir(), ".finmind_rate_ledger.json"
            )
            cls._limiter = FinMindRateLimiter(ledger, capacity=cls._account_limit())
        return cls._limiter

    @classmethod
    def _account_limit(cls):
        """The account's hourly quota, or None when it cannot be read.

        ``api_usage`` is not a trustworthy live counter, so only the ceiling is
        taken from here; pacing comes from the local ledger.
        """
        try:
            return int(FinMindWrapper._api.api_request_limit)
        except Exception:
            return None

    def get_traded_stock_ids(self, day):
        """Stock ids that actually traded on ``day``.

        One request. Uses FinMind's sync branch, which raises on a quota
        rejection instead of returning a short (falsely complete) universe.
        """
        self.limiter().acquire(1)
        prices = FinMindWrapper._api.taiwan_stock_daily(
            start_date=day, end_date=day
        )
        if prices is None or prices.empty:
            return set()
        traded = set(
            prices[prices["Trading_Volume"] > 0]["stock_id"].astype(str)
        )
        return traded & self._listed_stock_ids()

    def _listed_stock_ids(self):
        """Real twse/tpex equities, memoised for the process."""
        if FinMindWrapper._stock_info is None:
            self.limiter().acquire(1)
            info = FinMindWrapper._api.taiwan_stock_info()
            listed = info[
                info["type"].isin(["twse", "tpex"])
                & ~info["industry_category"].isin(_INDEX_INDUSTRIES)
                & ~info["stock_id"].isin(_INDEX_IDS)
            ]
            FinMindWrapper._stock_info = set(listed["stock_id"].astype(str))
        return FinMindWrapper._stock_info

    def fetch_broker_cells(self, day, stock_ids, verify=True, universe_size=0):
        """Fetch exactly these ``(stock_id, day)`` cells.

        FinMind's batch path is asynchronous and discards failed responses,
        so anything not returned is re-checked on the single-stock sync path,
        which raises on a quota rejection. ``universe_size`` (the day's
        traded-stock count) only serves to recognise a nearly-empty date.
        """
        wanted = [str(s) for s in stock_ids]
        if not wanted:
            return BrokerFetchResult(self._empty_frame(), set(), set(), set(), 0)

        limiter = self.limiter()
        # A batch is reserved in one go, so it can never exceed an hour's worth.
        chunk = max(1, min(batch_size(), limiter.capacity))
        frames = []
        spent = 0
        for start in range(0, len(wanted), chunk):
            batch = wanted[start : start + chunk]
            limiter.acquire(len(batch))
            spent += len(batch)
            frame = FinMindWrapper._api.taiwan_stock_trading_daily_report(
                date=day, stock_id_list=batch
            )
            if frame is not None and not frame.empty:
                frames.append(frame)

        merged = (
            pd.concat(frames, ignore_index=True) if frames else self._empty_frame()
        )
        received = (
            set(merged["stock_id"].astype(str)) if not merged.empty else set()
        )
        missing = [s for s in wanted if s not in received]
        if not missing or not verify:
            return BrokerFetchResult(
                merged, received, set(), set(missing), spent
            )

        absent, unresolved, probe_cost, inferred = self._classify_missing(
            day, missing, universe_size
        )
        return BrokerFetchResult(
            merged, received, absent, unresolved, spent + probe_cost, inferred
        )

    def _classify_missing(self, day, missing, universe_size=0):
        """Split ids that returned nothing into empty-upstream and lost."""
        if universe_size and len(missing) > universe_size * THIN_FRACTION:
            # Most of the market absent: sample rather than walk it.
            ordered = sorted(missing)
            stride = max(1, len(ordered) // PROBE_SAMPLE)
            spent = 0
            for sid in ordered[::stride][:PROBE_SAMPLE]:
                rows, cost = self._probe(day, sid)
                spent += cost
                if rows:
                    # Upstream has data we did not receive: the batch lost rows.
                    return set(), set(missing), spent, False
            return set(missing), set(), spent, True

        absent, unresolved, spent = set(), set(), 0
        for sid in missing:
            rows, cost = self._probe(day, sid)
            spent += cost
            (absent if rows == 0 else unresolved).add(sid)
        return absent, unresolved, spent, False

    def _probe(self, day, sid):
        """Single-stock sync request: raises on quota, returns a row count."""
        self.limiter().acquire(1)
        frame = FinMindWrapper._api.taiwan_stock_trading_daily_report(
            date=day, stock_id=str(sid)
        )
        return (0 if frame is None or frame.empty else len(frame)), 1

    @staticmethod
    def _empty_frame():
        return pd.DataFrame({c: pd.Series(dtype="object") for c in BROKER_COLUMNS})

    # ------------------------------------------------------------------
    # Write surface -- the only writer of the archive.
    # ------------------------------------------------------------------

    def write_broker_day(self, day, df, output_dir=None):
        """Deduplicate and atomically publish one day of broker rows.

        Upstream can serve duplicated rows; a net position sums buy - sell
        per branch, so duplicates are dropped once here, not by every reader.
        """
        output_dir = output_dir or self.broker_dir()
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, f"{day}.parquet")

        frame = df.copy()
        for column in BROKER_COLUMNS:
            if column not in frame.columns:
                raise ValueError(f"broker frame for {day} is missing column {column}")
        frame = frame[BROKER_COLUMNS]
        frame["stock_id"] = frame["stock_id"].astype(str)
        frame = frame.drop_duplicates().reset_index(drop=True)

        table = pa.Table.from_pandas(frame, preserve_index=False)
        # Pin the text columns so every written day keeps one schema.
        table = table.cast(
            pa.schema(
                [
                    pa.field(f.name, pa.large_string())
                    if pa.types.is_string(f.type) or pa.types.is_large_string(f.type)
                    else f
                    for f in table.schema
                ]
            )
        )

        fd, tmp_path = tempfile.mkstemp(dir=output_dir, prefix=".broker-", suffix=".tmp")
        os.close(fd)
        try:
            pq.write_table(table, tmp_path)
            # mkstemp creates 0600, which os.replace would keep.
            os.chmod(tmp_path, 0o644)
            os.replace(tmp_path, path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        return path
