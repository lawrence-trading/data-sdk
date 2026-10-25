import os
import sys
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from FinMind.data import DataLoader

from .finmind_rate_limit import FinMindRateLimiter, batch_size
from .paths import env_path

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

MIN_EXPECTED_WARRANTS = 5000
MAX_MISSING_WARRANT_FRACTION = 0.01

# Warrant code ranges, from TWSE's warrant FAQ and TPEx's 上櫃權證簡介:
# calls are 6 digits, every other kind is 5 digits plus a letter
# (P/U/T domestic put, F foreign call, Q foreign put, C/X bull, B/Y bear).
_WARRANT_LETTERS = frozenset("PUTFQCBXY")
_CALL_RANGES = (
    (30001, 89999),      # TWSE 030001-089999
    (700000, 739999),    # TPEx 700000-739999
)
_LETTERED_RANGES = (
    (3001, 8999),        # TWSE 03001x-08999x
    (70000, 73999),      # TPEx 70000x-73999x
)


def is_warrant_code(code):
    """Whether ``code`` lies in a TWSE or TPEx warrant code range."""
    code = str(code)
    if len(code) != 6 or not code[:5].isdigit():
        return False
    last = code[5]
    if last.isdigit():
        number = int(code)
        ranges = _CALL_RANGES
    elif last in _WARRANT_LETTERS:
        number = int(code[:5])
        ranges = _LETTERED_RANGES
    else:
        return False
    return any(low <= number <= high for low, high in ranges)

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
        return str(env_path("DATA_SDK_FINMIND_BROKER_PATH"))

    @staticmethod
    def warrant_broker_dir():
        return str(env_path("DATA_SDK_FINMIND_BROKER_WARRANT_PATH"))

    @staticmethod
    def broker_day_path(day, output_dir=None, warrant=False):
        """One day's archive file, ``{dir}/{day}.parquet``, in the stock archive
        (``DATA_SDK_FINMIND_BROKER_PATH``) or the warrant archive
        (``DATA_SDK_FINMIND_BROKER_WARRANT_PATH``)."""
        if output_dir is None:
            output_dir = FinMindWrapper.warrant_broker_dir() if warrant else FinMindWrapper.broker_dir()
        return os.path.join(output_dir, f"{day}.parquet")

    # ------------------------------------------------------------------
    # Read surface -- pure. Never downloads, never writes.
    # ------------------------------------------------------------------

    def get_broker(self, day, sid=None, trader_id=None, warrant=False):
        """Broker rows for ``day`` from the archive, filtered by ``sid`` (one
        instrument, every branch) and/or ``trader_id`` (one branch, every
        instrument); neither filter returns the whole day.

        ``warrant=True`` reads the warrant archive
        (``DATA_SDK_FINMIND_BROKER_WARRANT_PATH``), whose rows have the same
        columns with warrant codes in ``stock_id``.

        Reads never download: the archive is filled by the scheduled writer,
        and a missing day raises instead of costing a whole-day fetch here.
        """
        path = self.broker_day_path(day, warrant=warrant)
        self._require_day_file(day, path)
        filters = []
        if sid is not None:
            filters.append(("stock_id", "==", str(sid)))
        if trader_id is not None:
            filters.append(("securities_trader_id", "==", str(trader_id)))
        out = pd.read_parquet(path, filters=filters or None)
        return out.reset_index(drop=True)

    def get_broker_day(self, day, sids=None, columns=None, warrant=False):
        """Whole-day broker rows, optionally projected and filtered."""
        path = self.broker_day_path(day, warrant=warrant)
        self._require_day_file(day, path)
        filters = None
        if sids is not None:
            filters = [("stock_id", "in", [str(s) for s in sids])]
        out = pd.read_parquet(
            path, columns=list(columns) if columns else None, filters=filters
        )
        return out.reset_index(drop=True)

    def archived_stock_ids(self, day, output_dir=None, warrant=False):
        """Distinct stock (or warrant) ids already stored for ``day``; empty set
        if absent."""
        path = self.broker_day_path(day, output_dir, warrant=warrant)
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
        """
        self.limiter().acquire(1)
        return FinMindWrapper._api.taiwan_stock_trading_date(
            start_date=start_date, end_date=end_date
        )

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

    def get_traded_warrant_ids(self, day):
        """Warrant codes that traded on ``day`` (volume > 0). One request, on the
        same daily-price dataset as :meth:`get_traded_stock_ids`; warrants are
        recognised by the exchanges' code ranges, see :func:`is_warrant_code`."""
        self.limiter().acquire(1)
        prices = FinMindWrapper._api.taiwan_stock_daily(
            start_date=day, end_date=day
        )
        if prices is None or prices.empty:
            return set()
        traded = prices[prices["Trading_Volume"] > 0]["stock_id"].astype(str)
        return {code for code in traded if is_warrant_code(code)}

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

    def fetch_warrant_day(self, day, verify=True):
        """Fetch the whole market's warrant branch report for ``day`` in one
        request (sponsor-tier storage object, FinMind >= 2.0.10).

        Complete only if dated ``day``, holding at least ``MIN_EXPECTED_WARRANTS``
        warrants and, with ``verify``, covering the day's traded warrants.
        """
        self.limiter().acquire(1)
        frame = FinMindWrapper._api.taiwan_stock_warrant_trading_daily_report(
            date=day, use_object=True
        )
        spent = 1
        if frame is None or frame.empty:
            return BrokerFetchResult(self._empty_frame(), set(), set(), {day}, spent)
        frame = frame.copy()
        frame["stock_id"] = frame["stock_id"].astype(str)
        received = set(frame["stock_id"])

        dated = frame["date"].astype(str) == day
        too_few = len(received) < MIN_EXPECTED_WARRANTS
        if not dated.all() or too_few:
            return BrokerFetchResult(frame, received, set(), {day}, spent)
        if not verify:
            return BrokerFetchResult(frame, received, set(), set(), spent)

        traded = self.get_traded_warrant_ids(day)
        spent += 1
        missing = traded - received
        tolerated = len(traded) * MAX_MISSING_WARRANT_FRACTION
        if len(missing) > tolerated:
            return BrokerFetchResult(frame, received, set(), missing, spent)
        # Within tolerance: the few traded-but-absent codes are odd-lot-only days.
        return BrokerFetchResult(frame, received, missing, set(), spent)

    @staticmethod
    def _empty_frame():
        return pd.DataFrame({c: pd.Series(dtype="object") for c in BROKER_COLUMNS})

    # ------------------------------------------------------------------
    # Write surface -- the only writer of the archive.
    # ------------------------------------------------------------------

    def write_broker_day(self, day, df, output_dir=None, warrant=False):
        """Deduplicate and atomically publish one day of broker rows
        (``warrant=True``: into the warrant archive).

        Upstream can serve duplicated rows; a net position sums buy - sell
        per branch, so duplicates are dropped once here, not by every reader.
        """
        if output_dir is None:
            output_dir = self.warrant_broker_dir() if warrant else self.broker_dir()
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, f"{day}.parquet")

        frame = df.copy()
        for column in BROKER_COLUMNS:
            if column not in frame.columns:
                raise ValueError(f"broker frame for {day} is missing column {column}")
        frame = frame[BROKER_COLUMNS]
        frame["stock_id"] = frame["stock_id"].astype(str)
        # Pin the share counts too: the storage object serves int32, the per-stock
        # endpoint int64, and a multi-day scan needs one type.
        for column in ("buy", "sell"):
            frame[column] = frame[column].astype("int64")
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
