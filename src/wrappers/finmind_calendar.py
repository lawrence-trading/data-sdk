"""FinMind's ``taiwan_stock_trading_date`` dataset as a trading-day calendar,
cached per year.

Owner decision (taiwan_dashboard ISSUES.md I-255, 2026-10-10): FinMind is the
single source of truth for "is today a trading day" across the whole
monorepo -- every place that used to ask ``MarketCalendar`` (TWSE's own
``holidaySchedule``) that question now asks this class instead, directly or
by reading ``market-calendar.json`` (which the pre-open job now exports from
here). TWSE's calendar is demoted to a display-label lookup only: it may
NAME a day this class already says is closed (its announced holiday name,
e.g. 中秋節), but it never decides trading/not -- a weekday TWSE calls open
that is absent from FinMind's own trading-date list is still closed here.

``taiwan_stock_trading_date`` lists every actual trading day of a calendar
year -- past and future, since the government's holiday schedule is
published for the whole year in advance -- but FinMind only reaches the end
of the *current* year; asking for a year beyond that comes back with no
rows at all. This class treats that exactly like ``MarketCalendar``'s own
unpublished year: raise ``FinMindUnknownCalendarYear`` rather than silently
falling back to "every weekday is open", and never cache that empty
response so the next call asks the network again. A year that does have
rows is cached once, as a sorted JSON array of ``YYYY-MM-DD`` strings,
under ``DATA_SDK_FINMIND_CALENDAR_CACHE_PATH`` (default
``$XDG_CACHE_HOME/data_sdk/finmind_calendar``, ``~/.cache/...`` when unset
-- the same convention, and a few kilobytes a year, as ``MarketCalendar``'s
own cache). A cache that cannot be written is logged and skipped, not
fatal; a corrupt cache file is refetched and replaced.

Callers that need to keep running on an unreachable or unpublished calendar
apply the weekday rule themselves and say so, exactly as for
``MarketCalendar``.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import tempfile
from typing import Callable, Dict, List, Optional, Union

from .market_calendar import MarketCalendar

logger = logging.getLogger(__name__)

DateLike = Union[str, dt.date, dt.datetime]

# A closed weekday with no usable TWSE label (TWSE has no entry, disagrees,
# or cannot be reached at all) falls back to this. A weekend closure is
# always WEEKEND_CLOSURE, never this -- TWSE's label is only ever borrowed
# for a closed *weekday*.
GENERIC_WEEKDAY_CLOSURE = "休市"
WEEKEND_CLOSURE = "週末"


class FinMindUnknownCalendarYear(Exception):
    """FinMind has not published a trading-date calendar for this year yet.

    Callers must not treat this as "every weekday is open"; they fall back
    to the weekday rule themselves and say so (see ``docs/preopen.md`` in
    the taiwan_dashboard monorepo) -- the same contract
    ``data_sdk.wrappers.market_calendar.UnknownCalendarYear`` gives for
    TWSE's own (now display-label-only) calendar. Kept as a distinct class
    because the two sources' "not published yet" can land on different
    years and the message should name the right one.
    """

    def __init__(self, year: int):
        super().__init__(f"FinMind has not published a trading-date calendar for {year}")
        self.year = year


def _normalize_date(date: DateLike) -> dt.date:
    if isinstance(date, dt.datetime):
        return date.date()
    if isinstance(date, dt.date):
        return date
    return dt.datetime.strptime(str(date).replace("/", "-"), "%Y-%m-%d").date()


def _iso(date: dt.date) -> str:
    return date.strftime("%Y-%m-%d")


def _dates_from_frame(frame: object) -> List[str]:
    """The ``date`` column of a ``get_trading_dates``-shaped frame, as plain
    strings; ``[]`` for ``None`` or anything with no rows or no such column
    -- never raises, so a thin/empty response reads as "no trading days"
    rather than crashing the caller."""
    if frame is None:
        return []
    try:
        if len(frame) == 0:
            return []
        return [str(d) for d in frame["date"]]
    except (TypeError, KeyError):
        return []


class FinMindCalendar:
    """Taiwan's trading calendar resolved from FinMind's published trading
    dates alone (ISSUES.md I-255's single source of truth for trading/not).

    Offers the same read surface ``MarketCalendar`` gives its callers --
    ``is_trading_day``, ``closure``, ``next_trading_day``,
    ``previous_trading_day``, ``export_range`` -- so a caller that switches
    from one to the other changes nothing but the construction.

    ``trading_dates_fetcher(start_date, end_date)`` is injectable for tests
    (a ``get_trading_dates``-shaped callable); production code leaves it
    unset and lazily builds ``FinMindWrapper().get_trading_dates`` on first
    use, so constructing a ``FinMindCalendar`` never itself needs
    ``FINMIND_API_TOKEN`` or touches the network. ``holiday_calendar`` is a
    ``MarketCalendar``-shaped object (default: a real one, also built
    lazily) consulted ONLY for a closed weekday's display name; it never
    decides whether a day trades, and any failure reading it -- including
    its own ``UnknownCalendarYear`` -- is caught and logged, falling back
    to the generic label rather than propagating.
    """

    def __init__(
        self,
        trading_dates_fetcher: Optional[Callable[[str, str], object]] = None,
        holiday_calendar: Optional[object] = None,
    ):
        self._fetch = trading_dates_fetcher
        self._holiday_calendar = holiday_calendar
        self._year_cache: Dict[int, frozenset] = {}

    # ------------------------------------------------------------------
    # Lazy collaborators
    # ------------------------------------------------------------------

    def _fetcher(self) -> Callable[[str, str], object]:
        if self._fetch is None:
            from .finmind_broker_wrapper import FinMindWrapper

            self._fetch = FinMindWrapper().get_trading_dates
        return self._fetch

    def _display_calendar(self):
        if self._holiday_calendar is None:
            self._holiday_calendar = MarketCalendar()
        return self._holiday_calendar

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    @staticmethod
    def cache_dir() -> str:
        configured = os.environ.get("DATA_SDK_FINMIND_CALENDAR_CACHE_PATH")
        if configured:
            return configured
        base = os.environ.get("XDG_CACHE_HOME") or os.path.join(os.path.expanduser("~"), ".cache")
        return os.path.join(base, "data_sdk", "finmind_calendar")

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
    def _read_cached(path: str) -> Optional[List[str]]:
        """The cached year's trading dates; ``None`` when absent or
        unreadable (a bad file is removed)."""
        if not (os.path.isfile(path) and os.path.getsize(path) > 0):
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            if not isinstance(payload, list):
                raise ValueError("not a JSON array")
            return payload
        except (OSError, ValueError) as exc:
            logger.warning("finmind calendar cache %s unreadable, fetching again: %s", path, exc)
            try:
                os.unlink(path)
            except OSError:
                pass
            return None

    # ------------------------------------------------------------------
    # Year data
    # ------------------------------------------------------------------

    def _trading_dates_of_year(self, year: int) -> frozenset:
        """FinMind's trading days for ``year``, as a frozenset of
        ``YYYY-MM-DD`` strings.

        Raises ``FinMindUnknownCalendarYear`` when FinMind answers with no
        rows at all for the year (not published yet); that response is
        never cached, so the next call asks the network again.
        """
        if year in self._year_cache:
            return self._year_cache[year]
        cached = self._read_cached(self.cache_path(year))
        if cached is not None:
            dates = frozenset(cached)
            self._year_cache[year] = dates
            return dates
        frame = self._fetcher()(f"{year}-01-01", f"{year}-12-31")
        dates_list = sorted(set(_dates_from_frame(frame)))
        if not dates_list:
            raise FinMindUnknownCalendarYear(year)
        try:
            self._write_atomic(
                json.dumps(dates_list, ensure_ascii=False).encode("utf-8"), self.cache_path(year)
            )
        except OSError as exc:
            logger.warning(
                "finmind calendar %d not cached at %s: %s", year, self.cache_path(year), exc
            )
        dates = frozenset(dates_list)
        self._year_cache[year] = dates
        return dates

    # ------------------------------------------------------------------
    # Read surface -- same shape as MarketCalendar
    # ------------------------------------------------------------------

    def is_trading_day(self, date: DateLike) -> bool:
        """True if FinMind lists *date* as a Taiwan trading day. This is the
        only verdict that matters -- TWSE's calendar is never asked."""
        d = _normalize_date(date)
        return _iso(d) in self._trading_dates_of_year(d.year)

    def closure(self, date: DateLike) -> Optional[str]:
        """The reason *date* is closed, or ``None`` when FinMind says it
        trades.

        A closed weekend is always "週末". A closed weekday borrows TWSE's
        announced name (``MarketCalendar``) only when TWSE also calls that
        day closed and has a name for it; otherwise it is the generic
        "休市" -- TWSE is consulted for the label alone, never for whether
        the day trades.
        """
        d = _normalize_date(date)
        if self.is_trading_day(d):
            return None
        if d.weekday() >= 5:
            return WEEKEND_CLOSURE
        return self._weekday_label(d)

    def _weekday_label(self, d: dt.date) -> str:
        try:
            calendar = self._display_calendar()
            if calendar.is_trading_day(d):
                # TWSE disagrees and calls this day open; it never decides
                # trading/not here, so there is no closure name to borrow.
                return GENERIC_WEEKDAY_CLOSURE
            name = calendar.closure(d)
            return name or GENERIC_WEEKDAY_CLOSURE
        except Exception as exc:  # noqa: BLE001 -- display label only, never fatal
            logger.info("finmind calendar: TWSE label unavailable for %s: %s", _iso(d), exc)
            return GENERIC_WEEKDAY_CLOSURE

    def next_trading_day(self, date: DateLike, max_days: int = 30) -> str:
        """The first trading day strictly after *date*, as ``YYYY-MM-DD``."""
        d = _normalize_date(date)
        for _ in range(max_days):
            d += dt.timedelta(days=1)
            if self.is_trading_day(d):
                return _iso(d)
        raise RuntimeError(f"no trading day found within {max_days} days after {date}")

    def previous_trading_day(self, date: DateLike, max_days: int = 30) -> str:
        """The last trading day strictly before *date*, as ``YYYY-MM-DD``."""
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
        its manifest so td-web never needs the network.
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
