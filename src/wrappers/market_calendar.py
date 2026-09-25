"""TWSE's published market-calendar (開休市日期) endpoint, cached per year.

holidaySchedule   https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule
                   the announced exceptions to the Mon-Fri trading week for one
                   calendar year; ``?date=YYYYMMDD`` picks the year that date
                   falls in and ``response=json`` returns
                   ``{"data": [[date, name, description], ...], "total": n, ...}``

No credential is involved. Each year's schedule is fetched once and cached
under ``DATA_SDK_MARKET_CALENDAR_CACHE_PATH`` (default
``$XDG_CACHE_HOME/data_sdk/market_calendar``, ``~/.cache/...`` when unset --
a few kilobytes a year, so it lives with the user rather than on the shared
backup volume); a read for a cached year never touches the network. A cache
that cannot be written is logged and skipped: the fetched year is still
served, and the next process fetches it again. A year TWSE has not published yet comes back with no
rows (``total: 0``); that response is never cached, so every call keeps
asking the network and raises ``UnknownCalendarYear`` until TWSE publishes
it -- never silently falls back to "every weekday is open". Callers that
need to keep running on an unreachable or unpublished calendar apply the
weekday rule themselves and say so.
"""

import datetime as dt
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

HOLIDAY_SCHEDULE_URL = "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
REQUEST_TIMEOUT_S = 30

# An entry whose 名稱 contains either marker is informational only: TWSE
# still trades that day (a compensation trading day can land on what would
# otherwise be a weekend), it is just calling out the first/last trading day
# of a closed stretch. Every other entry actually closes the market.
_TRADING_DAY_MARKERS = ("開始交易", "最後交易")

DateLike = Union[str, dt.date, dt.datetime]


def _normalize_date(date: DateLike) -> dt.date:
    if isinstance(date, dt.datetime):
        return date.date()
    if isinstance(date, dt.date):
        return date
    return dt.datetime.strptime(str(date).replace("/", "-"), "%Y-%m-%d").date()


def _iso(date: dt.date) -> str:
    return date.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class HolidayEntry:
    """One row TWSE published for a date: its name, note, and whether it trades."""

    date: str  # YYYY-MM-DD
    name: str
    description: str
    is_trading_day: bool


class UnknownCalendarYear(Exception):
    """TWSE has not published a holiday schedule for this year yet.

    Callers must not treat this as "every weekday is open"; they fall back
    to the weekday rule themselves and say so (see ``docs/preopen.md`` in
    the taiwan_dashboard monorepo).
    """

    def __init__(self, year: int):
        super().__init__(f"TWSE has not published a holiday schedule for {year}")
        self.year = year


class MarketCalendar:
    """TWSE's Mon-Fri trading week plus its published exceptions.

    ``fetch_json`` is injectable so parsing and caching are testable without
    the network; production code leaves it as the real HTTP call.
    """


    def __init__(self, fetch_json: Optional[Callable[[str], dict]] = None):
        self._fetch_json = fetch_json or self._http_json
        self._year_cache: Dict[int, Dict[str, HolidayEntry]] = {}

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    @staticmethod
    def _http_json(url: str) -> dict:
        import requests

        response = requests.get(
            url, timeout=REQUEST_TIMEOUT_S, headers={"User-Agent": "Mozilla/5.0"}
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    @staticmethod
    def cache_dir() -> str:
        configured = os.environ.get("DATA_SDK_MARKET_CALENDAR_CACHE_PATH")
        if configured:
            return configured
        base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
        return os.path.join(base, "data_sdk", "market_calendar")

    @classmethod
    def cache_path(cls, year: int) -> str:
        return os.path.join(cls.cache_dir(), f"{year}.json")

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
    def _read_cached(path: str) -> Optional[dict]:
        """The cached year's payload; None when absent or unreadable (a bad file is removed)."""
        if not (os.path.isfile(path) and os.path.getsize(path) > 0):
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            if not isinstance(payload, dict):
                raise ValueError("not a JSON object")
            return payload
        except (OSError, ValueError) as exc:
            logger.warning("market calendar cache %s unreadable, fetching again: %s", path, exc)
            try:
                os.unlink(path)
            except OSError:
                pass
            return None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_entries(payload: dict) -> Dict[str, HolidayEntry]:
        entries: Dict[str, HolidayEntry] = {}
        for row in payload.get("data", []):
            date_str, name = row[0], row[1]
            description = row[2] if len(row) > 2 else ""
            is_trading = any(marker in name for marker in _TRADING_DAY_MARKERS)
            entries[date_str] = HolidayEntry(
                date=date_str, name=name, description=description, is_trading_day=is_trading
            )
        return entries

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    def year_entries(self, year: int) -> Dict[str, HolidayEntry]:
        """TWSE's announced exceptions for ``year``, keyed by ``YYYY-MM-DD``.

        Raises ``UnknownCalendarYear`` when TWSE's endpoint answers with no
        rows for the year (not published yet); that response is never
        cached, so the next call asks the network again.
        """
        if year in self._year_cache:
            return self._year_cache[year]
        cached = self._read_cached(self.cache_path(year))
        if cached is not None:
            entries = self._parse_entries(cached)
            self._year_cache[year] = entries
            return entries
        url = f"{HOLIDAY_SCHEDULE_URL}?date={year}0101&response=json"
        payload = self._fetch_json(url)
        if not payload.get("data"):
            raise UnknownCalendarYear(year)
        try:
            self._write_atomic(
                json.dumps(payload, ensure_ascii=False).encode("utf-8"), self.cache_path(year)
            )
        except OSError as exc:
            logger.warning("market calendar %d not cached at %s: %s", year, self.cache_path(year), exc)
        entries = self._parse_entries(payload)
        self._year_cache[year] = entries
        return entries

    def entry(self, date: DateLike) -> Optional[HolidayEntry]:
        """The published entry for ``date``, or None if TWSE announced none."""
        d = _normalize_date(date)
        return self.year_entries(d.year).get(_iso(d))

    def is_trading_day(self, date: DateLike) -> bool:
        """True if TWSE trades on ``date``.

        An announced entry wins over the weekday rule in both directions:
        an informational 開始交易／最後交易 entry is a trading day even on a
        weekend compensation day, and any other announced entry (including
        a ``市場無交易…`` settlement-only day) closes the market even on a
        weekday. With no entry for the date, Mon-Fri trades and Sat/Sun
        does not.
        """
        d = _normalize_date(date)
        entry = self.entry(d)
        if entry is not None:
            return entry.is_trading_day
        return d.weekday() < 5

    def closure(self, date: DateLike) -> Optional[str]:
        """The reason ``date`` is closed, or None if it trades.

        The announced holiday name (e.g. 中秋節) when TWSE published an
        entry for this date; the generic 週末 for an unannounced Saturday
        or Sunday.
        """
        d = _normalize_date(date)
        entry = self.entry(d)
        if entry is not None:
            return None if entry.is_trading_day else entry.name
        return None if d.weekday() < 5 else "週末"

    def next_trading_day(self, date: DateLike, max_days: int = 30) -> str:
        """The first trading day strictly after ``date``, as ``YYYY-MM-DD``."""
        d = _normalize_date(date)
        for _ in range(max_days):
            d += dt.timedelta(days=1)
            if self.is_trading_day(d):
                return _iso(d)
        raise RuntimeError(f"no trading day found within {max_days} days after {date}")

    def previous_trading_day(self, date: DateLike, max_days: int = 30) -> str:
        """The last trading day strictly before ``date``, as ``YYYY-MM-DD``."""
        d = _normalize_date(date)
        for _ in range(max_days):
            d -= dt.timedelta(days=1)
            if self.is_trading_day(d):
                return _iso(d)
        raise RuntimeError(f"no trading day found within {max_days} days before {date}")

    def export_range(self, start_date: DateLike, end_date: DateLike) -> List[dict]:
        """JSON-able rows for every date in ``[start_date, end_date]``.

        Each row is ``{"date", "trading_day", "closure"}``; this is the
        payload the pre-open job writes to ``market-calendar.json`` next to
        its manifest so td-web never needs the internet.
        """
        start, end = _normalize_date(start_date), _normalize_date(end_date)
        if end < start:
            raise ValueError(f"export_range end {end} is before start {start}")
        rows = []
        d = start
        while d <= end:
            rows.append(
                {
                    "date": _iso(d),
                    "trading_day": self.is_trading_day(d),
                    "closure": self.closure(d),
                }
            )
            d += dt.timedelta(days=1)
        return rows
