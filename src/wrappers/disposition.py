"""TWSE's 公布處置有價證券 and TPEx's 處置有價證券 open data: today's 處置股.

TWSE  https://www.twse.com.tw/rwd/zh/announcement/punish?response=json
       the same "fields"-indexed shape as holidaySchedule; the columns that
       matter here are 證券代號, 證券名稱 and 處置起迄時間 (ROC, e.g.
       "115/09/18～115/09/30").
TPEx   https://www.tpex.org.tw/openapi/v1/tpex_disposal_information
       a plain JSON array of objects: SecuritiesCompanyCode, CompanyName and
       DispositionPeriod (ROC, e.g. "1150924~1151006", no slashes).

No credential is involved. Fetched at most once per calendar date and cached
under DATA_SDK_DISPOSITION_CACHE_PATH (default
``$XDG_CACHE_HOME/data_sdk/disposition``, ``~/.cache/...`` when unset) the
same way MarketCalendar caches a year: the raw response from each exchange,
untouched, so a second call for the same date never touches the network.
Either exchange's fetch failing raises -- this module never returns a partial
list silently. Callers (backend.app.preopen) decide how to degrade and say
so; DispositionList itself makes no such decision.
"""

import datetime as dt
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

TWSE_PUNISH_URL = "https://www.twse.com.tw/rwd/zh/announcement/punish?response=json"
TPEX_DISPOSAL_URL = "https://www.tpex.org.tw/openapi/v1/tpex_disposal_information"
REQUEST_TIMEOUT_S = 30

_ROC_DATE_RE = re.compile(r"^(\d{3})(\d{2})(\d{2})$")
_PERIOD_SEP_RE = re.compile(r"[~～]")  # half-width ~ (TPEx) or full-width ～ (TWSE)


@dataclass(frozen=True)
class DispositionEntry:
    """One 處置股 row: the announced period is inclusive on both ends."""

    code: str
    name: str
    start_date: str  # YYYY-MM-DD
    end_date: str     # YYYY-MM-DD


def _roc_to_iso(text: str) -> Optional[str]:
    """``"115/09/18"`` or ``"1150918"`` (ROC year, no separators once
    stripped) -> ``"2026-09-18"``; None when *text* is not a valid ROC date."""
    digits = re.sub(r"[^0-9]", "", text)
    match = _ROC_DATE_RE.match(digits)
    if match is None:
        return None
    roc_year, month, day = (int(g) for g in match.groups())
    try:
        return dt.date(roc_year + 1911, month, day).isoformat()
    except ValueError:
        return None


def _roc_period(text: str) -> Optional[Tuple[str, str]]:
    """``"115/09/18～115/09/30"`` or ``"1150924~1151006"`` -> ``(start, end)``
    in ISO; None when either side does not parse as a ROC date."""
    parts = _PERIOD_SEP_RE.split(text.strip())
    if len(parts) != 2:
        return None
    start, end = _roc_to_iso(parts[0]), _roc_to_iso(parts[1])
    if start is None or end is None:
        return None
    return start, end


class DispositionList:
    """Today's 處置股 from TWSE + TPEx, fetched at most once a day.

    ``fetch_json`` is injectable so parsing and caching are testable without
    the network; production code leaves it as the real HTTP call.
    """

    def __init__(self, fetch_json: Optional[Callable[[str], object]] = None) -> None:
        self._fetch_json = fetch_json or self._http_json
        self._day_cache: Dict[str, List[DispositionEntry]] = {}

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    @staticmethod
    def _http_json(url: str) -> object:
        import requests

        response = requests.get(url, timeout=REQUEST_TIMEOUT_S, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Cache: today's raw pair of responses, exactly as MarketCalendar caches
    # a year -- unparsed, so a change to the parsing rules below never needs
    # a re-fetch of an already-cached day.
    # ------------------------------------------------------------------

    @staticmethod
    def cache_dir() -> str:
        configured = os.environ.get("DATA_SDK_DISPOSITION_CACHE_PATH")
        if configured:
            return configured
        base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
        return os.path.join(base, "data_sdk", "disposition")

    @classmethod
    def cache_path(cls, date_iso: str) -> str:
        return os.path.join(cls.cache_dir(), f"{date_iso}.json")

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
    def _read_cached(path: str) -> Optional[Dict[str, object]]:
        """The cached date's raw pair; None when absent or unreadable (a bad file is removed)."""
        if not (os.path.isfile(path) and os.path.getsize(path) > 0):
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            if not isinstance(payload, dict) or "twse" not in payload or "otc" not in payload:
                raise ValueError("not a cached disposition payload")
            return payload
        except (OSError, ValueError) as exc:
            logger.warning("disposition cache %s unreadable, fetching again: %s", path, exc)
            try:
                os.unlink(path)
            except OSError:
                pass
            return None

    def _raw(self, date_iso: str) -> Dict[str, object]:
        cached = self._read_cached(self.cache_path(date_iso))
        if cached is not None:
            return cached
        raw = {"twse": self._fetch_json(TWSE_PUNISH_URL), "otc": self._fetch_json(TPEX_DISPOSAL_URL)}
        try:
            self._write_atomic(json.dumps(raw, ensure_ascii=False).encode("utf-8"), self.cache_path(date_iso))
        except OSError as exc:
            logger.warning("disposition %s not cached at %s: %s", date_iso, self.cache_path(date_iso), exc)
        return raw

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_twse(payload: object) -> List[DispositionEntry]:
        out: List[DispositionEntry] = []
        if not isinstance(payload, dict):
            return out
        fields, rows = payload.get("fields"), payload.get("data")
        if not isinstance(fields, list) or not isinstance(rows, list):
            return out
        try:
            i_code, i_name, i_period = fields.index("證券代號"), fields.index("證券名稱"), fields.index("處置起迄時間")
        except ValueError:
            logger.warning("TWSE punish response missing an expected column; fields=%r", fields)
            return out
        needed = max(i_code, i_name, i_period)
        for row in rows:
            if not isinstance(row, list) or len(row) <= needed:
                continue
            code = str(row[i_code]).strip()
            period = _roc_period(str(row[i_period]))
            if not code or period is None:
                continue
            out.append(DispositionEntry(code=code, name=str(row[i_name]).strip(),
                                        start_date=period[0], end_date=period[1]))
        return out

    @staticmethod
    def _parse_tpex(payload: object) -> List[DispositionEntry]:
        out: List[DispositionEntry] = []
        if not isinstance(payload, list):
            return out
        for row in payload:
            if not isinstance(row, dict):
                continue
            code = str(row.get("SecuritiesCompanyCode") or "").strip()
            period = _roc_period(str(row.get("DispositionPeriod") or ""))
            if not code or period is None:
                continue
            out.append(DispositionEntry(code=code, name=str(row.get("CompanyName") or "").strip(),
                                        start_date=period[0], end_date=period[1]))
        return out

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    def entries(self, date: Optional[dt.date] = None) -> List[DispositionEntry]:
        """Every 處置股 row TWSE + TPEx have announced as of *date* (default
        today), both markets together, deduplication left to the caller.

        Raises whatever the transport raises on the first call of a date --
        never returns a partial list when one side fails.
        """
        d = date or dt.date.today()
        date_iso = d.isoformat()
        if date_iso in self._day_cache:
            return self._day_cache[date_iso]
        raw = self._raw(date_iso)
        out = self._parse_twse(raw.get("twse")) + self._parse_tpex(raw.get("otc"))
        self._day_cache[date_iso] = out
        return out

    def effective_on(self, date: Optional[dt.date] = None) -> List[DispositionEntry]:
        """``entries()`` whose announced ``[start_date, end_date]`` covers *date*."""
        d = date or dt.date.today()
        date_iso = d.isoformat()
        return [e for e in self.entries(d) if e.start_date <= date_iso <= e.end_date]
