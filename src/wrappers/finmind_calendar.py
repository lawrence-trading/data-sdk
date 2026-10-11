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
fatal; a corrupt cache file is refetched and replaced. Clear a year's cache
by deleting its file; the whole cache is just that one directory.

ISSUES.md I-268: a *past* year's cache, once written, is never touched again
-- FinMind does not retroactively edit a year that is over. The *current*
year is different: FinMind corrects it during the year itself (a makeup
workday or a typhoon day off announced after the year's calendar was first
cached), so its cache is revalidated at most once a day
(``CURRENT_YEAR_REVALIDATE_INTERVAL``) -- this instance's *first* call for
the current year, if made 24+ hours after the cache file's own mtime,
refetches it; every call after that is served from this instance's own
in-memory copy without checking the file's age again, so a long-lived
instance only ever revalidates once. Each of the two production call sites
(``backend/app/preopen/job.py``) constructs a fresh ``FinMindCalendar`` per
invocation, so in practice every invocation does see the staleness check.
A refetch that fails (unreachable network) or comes back with no rows
(FinMind answering empty for a year it has already published, which never
happens for a legitimate reason) keeps serving the existing cached copy and
logs why; it never raises and never discards a cache that is still the best
information available. Only the current year's own cache file's mtime is
touched this way -- a past year is cached forever, exactly as before.

ISSUES.md I-274: a *failed* revalidation attempt (unreachable network, or an
empty response) is recorded too, not just a successful one -- otherwise
every freshly constructed ``FinMindCalendar`` during a FinMind outage sees
the same stale cache file and dials out again, so a multi-process pre-open
run or a string of deploys each cost their own attempt instead of at most
one per ``CURRENT_YEAR_REVALIDATE_INTERVAL``. The attempt time (success or
failure alike) is written to a sibling marker file next to the year's own
cache (``<year>.json.revalidated_at``), atomically, and the staleness check
treats the cache as fresh until that long after whichever of the cache
file's mtime or the marker is more recent. A missing or unparsable marker
reads as "no attempt yet" and falls back to the cache file's mtime alone,
exactly as before I-274.

ISSUES.md I-280: the marker above must not suppress a retry that still
needs to happen. "A revalidation attempt was made" only means something
once it is either a *failure* (nothing to lose -- nothing new was fetched)
or a *persisted success* (the fetch succeeded AND ``_write_cache`` actually
wrote it). A fetch that succeeds but whose cache write fails is neither: a
correction was fetched (the fetching instance's own in-memory answer is
already correct) but never reached disk, so the marker is left untouched
-- the next freshly constructed instance still sees the old cache file's
mtime alone and retries, instead of serving the stale answer for a full
``CURRENT_YEAR_REVALIDATE_INTERVAL`` because of an attempt nothing came of.

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
    to the generic label rather than propagating. ``now`` is the clock used
    to decide which year is "current" (for ISSUES.md I-268's daily cache
    revalidation) and how old the current year's cache file is; injectable
    for tests, defaults to ``datetime.datetime.now``.
    """

    # ISSUES.md I-268: how long the current year's cache is trusted before
    # the next call refetches it. Chosen so the calendar can self-correct
    # for a same-year announcement (a makeup workday, a typhoon day off)
    # within one day, without refetching on every single call.
    CURRENT_YEAR_REVALIDATE_INTERVAL = dt.timedelta(hours=24)

    def __init__(
        self,
        trading_dates_fetcher: Optional[Callable[[str, str], object]] = None,
        holiday_calendar: Optional[object] = None,
        *,
        now: Optional[Callable[[], dt.datetime]] = None,
    ):
        self._fetch = trading_dates_fetcher
        self._holiday_calendar = holiday_calendar
        self._year_cache: Dict[int, frozenset] = {}
        self._now = now or dt.datetime.now

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

    @classmethod
    def _revalidation_attempt_path(cls, year: int) -> str:
        """ISSUES.md I-274: the sibling marker that records *year*'s last
        revalidation attempt, success or failure, next to its own cache
        file."""
        return cls.cache_path(year) + ".revalidated_at"

    @classmethod
    def _record_revalidation_attempt(cls, year: int, when: dt.datetime) -> None:
        """Marks *when* as the latest revalidation attempt for *year* --
        called on every attempt, success or failure, so a failed attempt
        backs off the next one exactly like a successful attempt does
        (ISSUES.md I-274). Written atomically; a write failure is logged
        and skipped, never fatal, same contract as ``_write_cache``."""
        path = cls._revalidation_attempt_path(year)
        try:
            cls._write_atomic(str(when.timestamp()).encode("ascii"), path)
        except OSError as exc:
            logger.warning(
                "finmind calendar %d revalidation-attempt marker not written at %s: %s",
                year,
                path,
                exc,
            )

    @classmethod
    def _last_revalidation_attempt(cls, year: int) -> Optional[float]:
        """Epoch seconds of the last revalidation attempt recorded for
        *year*, or ``None`` when there has never been one -- a missing
        file, an unreadable one, or content that is not a plain float all
        read as "no attempt yet" rather than raising (ISSUES.md I-274)."""
        path = cls._revalidation_attempt_path(year)
        try:
            with open(path, "r", encoding="ascii") as fh:
                return float(fh.read().strip())
        except (OSError, ValueError):
            return None

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

        ISSUES.md I-268: the in-memory cache below is checked first, so this
        only runs the staleness check on this instance's *first* call for
        ``year`` -- a long-lived instance never re-checks after that. When
        ``year`` is the current year (per ``now``) and its cache file is
        older than ``CURRENT_YEAR_REVALIDATE_INTERVAL`` at that first call,
        this refetches before serving it -- a refetch failure (network, or
        an empty response) keeps the existing cached copy and logs why,
        rather than raising or discarding it. A past year's cache is never
        revalidated: once written, it is read back as-is forever.
        """
        if year in self._year_cache:
            return self._year_cache[year]
        path = self.cache_path(year)
        cached = self._read_cached(path)
        if cached is not None:
            if year == self._now().year and self._current_year_cache_is_stale(year, path):
                refetched = self._revalidate(year, path)
                if refetched is not None:
                    self._year_cache[year] = refetched
                    return refetched
            dates = frozenset(cached)
            self._year_cache[year] = dates
            return dates
        dates_list = self._fetch_year(year)
        if not dates_list:
            raise FinMindUnknownCalendarYear(year)
        self._write_cache(year, dates_list, path)
        dates = frozenset(dates_list)
        self._year_cache[year] = dates
        return dates

    def _fetch_year(self, year: int) -> List[str]:
        frame = self._fetcher()(f"{year}-01-01", f"{year}-12-31")
        return sorted(set(_dates_from_frame(frame)))

    def _write_cache(self, year: int, dates_list: List[str], path: str) -> bool:
        """Persists *dates_list* for *year*; ``True`` once it is actually on
        disk, ``False`` on a write failure (logged, never fatal). ISSUES.md
        I-280: callers that gate a "verified" marker on persistence, not
        merely on having fetched, need this to tell the two apart."""
        try:
            self._write_atomic(json.dumps(dates_list, ensure_ascii=False).encode("utf-8"), path)
            return True
        except OSError as exc:
            logger.warning("finmind calendar %d not cached at %s: %s", year, path, exc)
            return False

    def _current_year_cache_is_stale(self, year: int, path: str) -> bool:
        """True once ``CURRENT_YEAR_REVALIDATE_INTERVAL`` has passed since
        whichever is more recent: the cache file's own mtime, or the last
        revalidation attempt recorded for *year* -- success or failure
        alike (``_last_revalidation_attempt``, ISSUES.md I-274). A failed
        attempt therefore backs off the next one for the same interval a
        successful attempt would, instead of every freshly constructed
        instance retrying the network while the outage lasts. A missing or
        garbled marker (``None``) falls back to the cache file's mtime
        alone, exactly as before I-274."""
        try:
            reference = os.path.getmtime(path)
        except OSError:
            return True
        attempted_at = self._last_revalidation_attempt(year)
        if attempted_at is not None and attempted_at > reference:
            reference = attempted_at
        age = self._now().timestamp() - reference
        return age >= self.CURRENT_YEAR_REVALIDATE_INTERVAL.total_seconds()

    def _revalidate(self, year: int, path: str) -> Optional[frozenset]:
        """Refetches the current year for ISSUES.md I-268; ``None`` (keep the
        existing cache) on any failure, logged either way.

        ISSUES.md I-274/I-280: the revalidation-attempt marker is recorded
        only once this attempt is something the next staleness check may
        safely treat as "settled" -- a fetch that raises, or one that comes
        back with no rows, counts immediately (there is nothing new to
        lose by waiting another interval). A fetch that succeeds is only
        "settled" once ``_write_cache`` actually persists it; if the write
        fails, the marker is left untouched on purpose, so the next freshly
        constructed instance still sees the old cache file's mtime alone
        and retries, rather than serving the stale answer for a full
        ``CURRENT_YEAR_REVALIDATE_INTERVAL`` because of an attempt that
        never reached disk. Either way this call returns the freshly
        fetched dates to its own caller -- a write failure only costs the
        *next* instance a retry, never this instance's own in-memory
        answer.
        """
        try:
            dates_list = self._fetch_year(year)
        except Exception as exc:  # noqa: BLE001 -- keep the cached copy on ANY failure
            logger.warning(
                "finmind calendar %d revalidation failed, keeping cached copy: %s", year, exc
            )
            self._record_revalidation_attempt(year, self._now())
            return None
        if not dates_list:
            logger.warning(
                "finmind calendar %d revalidation returned no rows, keeping cached copy", year
            )
            self._record_revalidation_attempt(year, self._now())
            return None
        if self._write_cache(year, dates_list, path):
            self._record_revalidation_attempt(year, self._now())
        else:
            logger.warning(
                "finmind calendar %d revalidation fetched but not cached, "
                "next call will retry instead of waiting out the interval",
                year,
            )
        return frozenset(dates_list)

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
