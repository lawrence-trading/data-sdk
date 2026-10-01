"""TAIFEX public reference endpoints for the SSF page, cached per as-of day.

    DailyMarketReportFut   https://openapi.taifex.com.tw/v1/DailyMarketReportFut
                           settlement price, open interest, volume per contract
                           month; the ``一般`` session only
    SSFAdjustedInfo        https://openapi.taifex.com.tw/v1/SSFAdjustedInfo
                           調整型 contracts and their adjusted share counts
    sSFTradePauseInfoDown  https://www.taifex.com.tw/cht/4/sSFTradePauseInfoDown
                           CP950 CSV of contracts with trading paused; the first
                           line is the as-of date; ``商品類別 == '股票期貨'`` kept

No credential is involved. Each endpoint serves only its latest publication,
so a fetch is stored under the day the payload itself carries
(``DATA_SDK_TAIFEX_CACHE_PATH``, default ``/mnt/nfs/backup/taifex_cache``)
and a read for a specific day is answered from that store or not at all.
"""

import csv
import datetime as dt
import io
import json
import os
import tempfile
from typing import Callable, Optional, Tuple

import pandas as pd

OPENAPI_BASE = "https://openapi.taifex.com.tw/v1"
DAILY_MARKET_REPORT_FUT_URL = f"{OPENAPI_BASE}/DailyMarketReportFut"
SSF_ADJUSTED_INFO_URL = f"{OPENAPI_BASE}/SSFAdjustedInfo"
SSF_TRADE_PAUSE_URL = "https://www.taifex.com.tw/cht/4/sSFTradePauseInfoDown"

REQUEST_TIMEOUT_S = 30

# Contract codes the daily report spells differently from the PROD-ID root.
_REPORT_CONTRACT_ALIASES = {
    "TX": "TXF",   # 臺股期貨
    "MTX": "MXF",  # 小型臺指
    "TE": "EXF",   # 電子期貨
    "TF": "FXF",   # 金融期貨
}
_MONTH_CODES = "ABCDEFGHIJKL"


def _normalize_day(day) -> str:
    if isinstance(day, (dt.date, dt.datetime)):
        return day.strftime("%Y%m%d")
    return str(day).replace("-", "").replace("/", "")


def taifex_prod_id(contract: str, contract_month: str) -> Optional[str]:
    """PROD-ID for one daily-report row, or None for a calendar spread.

    ``contract_month`` is the report's ``ContractMonth(Week)``: ``YYYYMM``
    for a monthly contract, ``YYYYMMWn`` for week ``n``, and ``A/B`` for a
    spread (out of scope, None). Monthly: root + month letter (A=Jan ..
    L=Dec) + last digit of the year, e.g. ``CDF, 202609 -> CDFI6``. Weekly:
    the root's first two letters + week digit, e.g. ``MTX, 202609W4 ->
    MX4I6``. Report spellings TX/MTX/TE/TF map to TXF/MXF/EXF/FXF.
    """
    if "/" in contract_month:
        return None
    root = _REPORT_CONTRACT_ALIASES.get(contract, contract)
    if "W" in contract_month:
        year_month, week = contract_month.split("W", 1)
        root = root[:2] + week
    else:
        year_month = contract_month
    month = int(year_month[4:6])
    return f"{root}{_MONTH_CODES[month - 1]}{year_month[3]}"


class TaifexResponseError(ValueError):
    """An OpenAPI endpoint answered with a body ``json.loads`` cannot parse --
    an error page, an empty body, or anything else that is not the endpoint's
    normal JSON array. Carries the URL, the response's Content-Type and a
    preview of its first bytes so the caller (``refresh_ssf_reference.py``'s
    ``errors`` dict, surfaced to ``GET /api/ssf/health``) names what actually
    came back instead of just "Expecting value: line 1 column 1" (ISSUES.md
    I-98)."""


class TaifexWrapper:
    _DEFAULT_CACHE_DIR = "/mnt/nfs/backup/taifex_cache"

    def __init__(
        self,
        fetch_json: Optional[Callable[[str], list]] = None,
        fetch_bytes: Optional[Callable[[str], bytes]] = None,
    ):
        # Both fetchers are injectable so the parsing and caching paths are
        # testable without the network.
        self._fetch_json = fetch_json or self._http_json
        self._fetch_bytes = fetch_bytes or self._http_bytes

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    @staticmethod
    def _http_bytes(url: str) -> bytes:
        import requests

        response = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        response.raise_for_status()
        return response.content

    @classmethod
    def _http_json(cls, url: str) -> list:
        import requests

        response = requests.get(url, timeout=REQUEST_TIMEOUT_S)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "<no Content-Type header>")
        try:
            return json.loads(response.content.decode("utf-8", errors="replace"))
        except (UnicodeDecodeError, ValueError) as exc:
            preview = response.content[:200]
            raise TaifexResponseError(
                f"TAIFEX endpoint {url} did not return JSON "
                f"(content-type={content_type!r}, first bytes={preview!r}): {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    @staticmethod
    def cache_dir() -> str:
        return os.environ.get("DATA_SDK_TAIFEX_CACHE_PATH", TaifexWrapper._DEFAULT_CACHE_DIR)

    @classmethod
    def cache_path(cls, endpoint: str, day: str, suffix: str) -> str:
        return os.path.join(cls.cache_dir(), endpoint, f"{_normalize_day(day)}.{suffix}")

    @staticmethod
    def _write_atomic(payload: bytes, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(payload)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    @staticmethod
    def _read_cached(path: str) -> Optional[bytes]:
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, "rb") as fh:
                return fh.read()
        return None

    def _latest_cached_rows(self, endpoint: str) -> Optional[Tuple[str, list]]:
        """The most recently cached day for *endpoint* (by the as-of date in
        its filename), or None when nothing is cached at all. The fallback
        ``_fetch_rows_with_retry`` uses once a live fetch has failed twice in
        a row -- a stale-but-real last-good day beats no data at all for a
        page nothing on the dashboard actually reads from this endpoint yet
        (ISSUES.md I-98)."""
        directory = os.path.join(self.cache_dir(), endpoint)
        if not os.path.isdir(directory):
            return None
        days = sorted(name[:-5] for name in os.listdir(directory) if name.endswith(".json") and name[:-5].isdigit())
        if not days:
            return None
        latest = days[-1]
        cached = self._read_cached(os.path.join(directory, f"{latest}.json"))
        if cached is None:
            return None
        try:
            return latest, json.loads(cached.decode("utf-8"))
        except ValueError:
            return None

    def _fetch_rows_with_retry(self, endpoint: str, url: str) -> list:
        """One fetch, a single retry on any failure (including a non-JSON or
        empty response -- ``_http_json``/an injected ``fetch_json`` raising,
        or a response that parses to an empty list), and only then a fall
        back to the latest cached day for *endpoint* if one exists. Raises
        the retry's own error (already naming the endpoint, content-type and
        a body preview when it came from ``_http_json``) when there is
        nothing cached to fall back to (ISSUES.md I-98)."""
        last_exc: Exception
        for _ in range(2):
            try:
                rows = self._fetch_json(url)
            except Exception as exc:  # noqa: BLE001 - re-raised below if nothing can rescue this
                last_exc = exc
                continue
            if not rows:
                last_exc = RuntimeError(f"TAIFEX {endpoint} returned an empty response ({url})")
                continue
            return rows
        fallback = self._latest_cached_rows(endpoint)
        if fallback is not None:
            return fallback[1]
        raise RuntimeError(f"TAIFEX {endpoint} fetch failed after a retry ({url}): {last_exc}") from last_exc

    def _openapi_rows(self, endpoint: str, url: str, day) -> Tuple[str, list]:
        """Rows of an OpenAPI endpoint for ``day`` (None: whatever is live).

        The endpoint publishes one day at a time, keyed by its own ``Date``
        field; a fetch is always cached under that date. Asking for a day the
        endpoint no longer serves and the cache does not hold raises
        FileNotFoundError rather than returning another day's rows. A live
        fetch (``day`` is None, or the day is not yet cached) that errors or
        comes back empty is retried once, then served from the latest cached
        day if one exists (ISSUES.md I-98); only when neither recovers it
        does the clear, endpoint-naming error from the retry reach the
        caller.
        """
        if day is not None:
            path = self.cache_path(endpoint, day, "json")
            cached = self._read_cached(path)
            if cached is not None:
                return _normalize_day(day), json.loads(cached.decode("utf-8"))
        rows = self._fetch_rows_with_retry(endpoint, url)
        live_day = _normalize_day(rows[0]["Date"])
        self._write_atomic(
            json.dumps(rows, ensure_ascii=False).encode("utf-8"),
            self.cache_path(endpoint, live_day, "json"),
        )
        if day is not None and live_day != _normalize_day(day):
            raise FileNotFoundError(
                f"TAIFEX {endpoint} for {_normalize_day(day)} is not cached and the "
                f"endpoint currently serves {live_day}"
            )
        return live_day, rows

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    def get_daily_market_report_fut(self, day=None) -> pd.DataFrame:
        """``DailyMarketReportFut`` rows with ``TradingSession == '一般'``.

        Columns are the endpoint's own (``Date``, ``Contract``,
        ``ContractMonth(Week)``, ``Open``, ``High``, ``Low``, ``Last``,
        ``Change``, ``%``, ``Volume``, ``SettlementPrice``, ``OpenInterest``,
        ``BestBid``, ``BestAsk``, ``HistoricalHigh``, ``HistoricalLow``,
        ``TradingHalt``, ``TradingSession``, ...) as strings, exactly as
        published (``-`` and ``NULL`` included), plus ``prod_id`` from
        ``taifex_prod_id`` (None for calendar spreads). Prices stay strings
        so no consumer inherits a float on a money path by accident.

        ``day``: ``YYYYMMDD`` / ``YYYY-MM-DD`` / date. None returns the
        endpoint's current day. A day that is neither cached nor live raises
        FileNotFoundError.
        """
        _, rows = self._openapi_rows("daily_market_report_fut", DAILY_MARKET_REPORT_FUT_URL, day)
        frame = pd.DataFrame(rows, dtype=str)
        frame = frame[frame["TradingSession"] == "一般"].reset_index(drop=True)
        # object dtype keeps None for spreads (a str column would turn it into NaN)
        frame["prod_id"] = pd.Series(
            [taifex_prod_id(c, m) for c, m in zip(frame["Contract"], frame["ContractMonth(Week)"])],
            index=frame.index,
            dtype=object,
        )
        return frame

    def get_ssf_adjusted_info(self, day=None, futures_only: bool = True) -> pd.DataFrame:
        """``SSFAdjustedInfo`` rows: the 調整型 contracts and their terms.

        Columns as published (``Date``, ``Contract``, ``StockCode``,
        ``ContractName``, ``TWSEStock/TPExStock/TWSEETF``, ``Futures/Options``,
        ``UnderlyingSecurityShares``, the two adjustment amounts,
        ``DeliveryMonths``), all strings. ``futures_only`` keeps
        ``Futures/Options == '股票期貨'``; the share count is the contract's
        own and is not 2,000 for these products.
        """
        _, rows = self._openapi_rows("ssf_adjusted_info", SSF_ADJUSTED_INFO_URL, day)
        frame = pd.DataFrame(rows, dtype=str)
        if futures_only:
            frame = frame[frame["Futures/Options"] == "股票期貨"]
        return frame.reset_index(drop=True)

    @staticmethod
    def parse_trade_pause_csv(payload: bytes) -> Tuple[dt.date, pd.DataFrame]:
        """Decode the CP950 pause download into ``(as_of_date, frame)``.

        Line 1 is ``YYYY/MM/DD,``; line 2 the header; the rest one paused
        contract per line. Only ``商品類別 == '股票期貨'`` rows are kept.
        An empty table (no contract paused) is a frame with the header's
        columns and no rows.
        """
        text = payload.decode("cp950")
        lines = [line for line in text.splitlines() if line.strip()]
        if len(lines) < 2:
            raise ValueError("TAIFEX trade-pause download is missing its date or header line")
        as_of = dt.datetime.strptime(lines[0].split(",")[0].strip(), "%Y/%m/%d").date()
        reader = csv.reader(io.StringIO("\n".join(lines[1:])))
        header = [h.strip() for h in next(reader)]
        while header and header[-1] == "":
            header.pop()
        records = []
        for row in reader:
            cells = [c.strip() for c in row][: len(header)]
            cells += [""] * (len(header) - len(cells))
            records.append(cells)
        frame = pd.DataFrame(records, columns=header, dtype=str)
        if "商品類別" in frame.columns:
            frame = frame[frame["商品類別"] == "股票期貨"]
        return as_of, frame.reset_index(drop=True)

    def get_ssf_trade_pause(self, day=None) -> Tuple[dt.date, pd.DataFrame]:
        """Contracts with trading paused, as ``(as_of_date, frame)``.

        The raw CP950 download is cached under its own as-of date; ``day``
        selects a cached day (None: fetch the live one). A day that is
        neither cached nor live raises FileNotFoundError.
        """
        endpoint = "ssf_trade_pause"
        if day is not None:
            cached = self._read_cached(self.cache_path(endpoint, day, "csv"))
            if cached is not None:
                return self.parse_trade_pause_csv(cached)
        payload = self._fetch_bytes(SSF_TRADE_PAUSE_URL)
        as_of, frame = self.parse_trade_pause_csv(payload)
        self._write_atomic(payload, self.cache_path(endpoint, as_of, "csv"))
        if day is not None and _normalize_day(day) != _normalize_day(as_of):
            raise FileNotFoundError(
                f"TAIFEX {endpoint} for {_normalize_day(day)} is not cached and the "
                f"download currently carries {as_of.strftime('%Y%m%d')}"
            )
        return as_of, frame
