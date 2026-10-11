"""FinMindCalendar against recorded fixtures: no network.

ISSUES.md I-255's owner decision: FinMind is the single source of truth for
trading/not; TWSE's ``MarketCalendar`` is consulted only for a closed
weekday's display name. These tests cover both the ordinary case where the
two sources agree (built off the same real 2026 TWSE response the sibling
``test_market_calendar.py`` uses) and the cases where they must not: FinMind
deciding a day TWSE calls open, and TWSE's label being unavailable.
"""

import datetime as dt
import fcntl
import json
import os
import threading
import time
from pathlib import Path

import pandas as pd
import pytest

from data_sdk import FinMindUnknownCalendarYear, MarketCalendar
from data_sdk.wrappers.finmind_calendar import FinMindCalendar

FIXTURES = Path(__file__).parent / "fixtures" / "market_calendar"
REAL_2026 = json.loads((FIXTURES / "holiday_schedule_2026.json").read_text("utf-8"))


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_FINMIND_CALENDAR_CACHE_PATH", str(tmp_path))
    return tmp_path


def _refusing_fetcher(start_date, end_date):
    raise AssertionError("network touched on a cached year")


def _frame(dates):
    return pd.DataFrame({"date": list(dates)})


def _twse_2026():
    """The real TWSE calendar, fixture-backed -- used only to derive an
    AGREEING FinMind fixture for the ordinary-case tests below."""
    return MarketCalendar(fetch_json=lambda url: REAL_2026)


def _agreeing_year_dates(year, twse):
    d, end = dt.date(year, 1, 1), dt.date(year, 12, 31)
    out = []
    while d <= end:
        if twse.is_trading_day(d):
            out.append(d.isoformat())
        d += dt.timedelta(days=1)
    return out


def _agreeing_fetcher(start_date, end_date):
    """FinMind trading dates for the year of *start_date* that happen to
    agree with the real 2026 TWSE fixture -- the ordinary case."""
    year = int(start_date[:4])
    return _frame(_agreeing_year_dates(year, _twse_2026()))


def _write_atomic_failing_for_the_cache_file_only():
    """A replacement for ``FinMindCalendar._write_atomic`` that raises
    ``OSError`` only for the year's own cache file, while letting a sibling
    ``.revalidated_at`` marker write through to the real implementation --
    isolates "the fetch succeeded but persisting it failed" (ISSUES.md
    I-280) from "the attempt marker itself failed to write" (already
    covered by ``test_an_unwritable_cache_still_serves_the_fetched_year``
    and friends), which a single always-fail patch cannot distinguish
    since both go through the same low-level atomic writer."""
    real_write_atomic = FinMindCalendar._write_atomic

    def selective(payload, path):
        if str(path).endswith(".revalidated_at"):
            return real_write_atomic(payload, path)
        raise OSError("disk full")

    return staticmethod(selective)


def _counting(fetcher):
    calls = []

    def wrapped(start_date, end_date):
        calls.append((start_date, end_date))
        return fetcher(start_date, end_date)

    return wrapped, calls


# ----------------------------------------------------------------------
# Ordinary case: FinMind and TWSE agree -- the label is borrowed
# ----------------------------------------------------------------------


def test_named_holiday_closes_the_market_with_twses_name(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day("2026-09-25") is False
    assert calendar.closure("2026-09-25") == "中秋節"


def test_unannounced_weekend_is_closed_with_the_generic_reason(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day("2026-09-26") is False  # Saturday
    assert calendar.closure("2026-09-26") == "週末"
    assert calendar.is_trading_day("2026-09-27") is False  # Sunday
    assert calendar.closure("2026-09-27") == "週末"


def test_ordinary_weekday_with_no_closure_trades(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day("2026-09-24") is True
    assert calendar.closure("2026-09-24") is None


def test_date_input_accepts_date_object_and_slash_string(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day(dt.date(2026, 9, 25)) is False
    assert calendar.is_trading_day("2026/09/25") is False


def test_next_trading_day_skips_the_mid_autumn_stretch(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.next_trading_day("2026-09-24") == "2026-09-29"
    assert calendar.next_trading_day("2026-09-25") == "2026-09-29"


def test_previous_trading_day_skips_the_mid_autumn_stretch(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.previous_trading_day("2026-09-29") == "2026-09-24"


def test_next_trading_day_gives_up_after_max_days(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    with pytest.raises(RuntimeError):
        calendar.next_trading_day("2026-09-25", max_days=1)


def test_export_range_matches_the_mid_autumn_scenario(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    rows = calendar.export_range("2026-09-25", "2026-09-29")
    assert rows == [
        {"date": "2026-09-25", "trading_day": False, "closure": "中秋節"},
        {"date": "2026-09-26", "trading_day": False, "closure": "週末"},
        {"date": "2026-09-27", "trading_day": False, "closure": "週末"},
        {"date": "2026-09-28", "trading_day": False, "closure": "孔子誕辰紀念日/ 教師節"},
        {"date": "2026-09-29", "trading_day": True, "closure": None},
    ]


def test_export_range_end_before_start_raises(cache_dir):
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    with pytest.raises(ValueError):
        calendar.export_range("2026-09-29", "2026-09-25")


# ----------------------------------------------------------------------
# FinMind is the single source of truth: it decides even when TWSE
# disagrees, in both directions.
# ----------------------------------------------------------------------


def test_finmind_closes_a_day_twse_calls_an_ordinary_trading_weekday(cache_dir):
    # 2026-03-02 is a Monday with no TWSE entry at all (an ordinary trading
    # day by the ground-truth fixture); FinMind's own data says otherwise
    # (e.g. an unannounced closure TWSE's published schedule never covers).
    def fetcher(start_date, end_date):
        dates = [d for d in _agreeing_year_dates(2026, _twse_2026()) if d != "2026-03-02"]
        return _frame(dates)

    calendar = FinMindCalendar(fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day("2026-03-02") is False
    # TWSE never decides trading/not, so there is no borrowed name for a
    # closure TWSE itself does not recognise -- the generic label stands.
    assert calendar.closure("2026-03-02") == "休市"


def test_finmind_trades_a_day_twse_calendar_calls_closed(cache_dir):
    # The opposite disagreement: FinMind lists 2026-09-25 (中秋節 per TWSE)
    # as an ordinary trading day (e.g. a correction FinMind's dataset
    # applied that TWSE's cached schedule has not caught up with yet).
    def fetcher(start_date, end_date):
        dates = set(_agreeing_year_dates(2026, _twse_2026()))
        dates.add("2026-09-25")
        return _frame(sorted(dates))

    calendar = FinMindCalendar(fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day("2026-09-25") is True
    assert calendar.closure("2026-09-25") is None


def test_twse_label_unavailable_falls_back_to_the_generic_reason(cache_dir):
    # The display-name lookup itself fails (e.g. TWSE's holidaySchedule is
    # unreachable, or raises its own UnknownCalendarYear) -- the closure
    # verdict (already decided by FinMind) must not be affected, only the
    # label text degrades.
    def fetcher(start_date, end_date):
        return _frame(d for d in _agreeing_year_dates(2026, _twse_2026()) if d != "2026-09-25")

    def broken_holiday_calendar():
        class _Unreachable:
            def is_trading_day(self, date):
                raise RuntimeError("TWSE endpoint unreachable")

        return _Unreachable()

    calendar = FinMindCalendar(fetcher, holiday_calendar=broken_holiday_calendar())
    assert calendar.is_trading_day("2026-09-25") is False
    assert calendar.closure("2026-09-25") == "休市"


# ----------------------------------------------------------------------
# A year FinMind has not published yet is never silently "all weekdays open"
# ----------------------------------------------------------------------


def test_unpublished_year_raises(cache_dir):
    calendar = FinMindCalendar(lambda start, end: _frame([]))
    with pytest.raises(FinMindUnknownCalendarYear) as excinfo:
        calendar.is_trading_day("2027-01-04")
    assert excinfo.value.year == 2027


def test_unpublished_year_response_is_never_cached(cache_dir):
    calendar = FinMindCalendar(lambda start, end: _frame([]))
    with pytest.raises(FinMindUnknownCalendarYear):
        calendar.is_trading_day("2027-01-04")
    assert not (cache_dir / "2027.json").exists()


def test_unpublished_year_retries_the_network_on_every_call(cache_dir):
    fetcher, calls = _counting(lambda start, end: _frame([]))
    calendar = FinMindCalendar(fetcher)
    for _ in range(3):
        with pytest.raises(FinMindUnknownCalendarYear):
            calendar.is_trading_day("2027-01-04")
    assert len(calls) == 3


def test_a_none_frame_is_also_treated_as_unpublished(cache_dir):
    calendar = FinMindCalendar(lambda start, end: None)
    with pytest.raises(FinMindUnknownCalendarYear):
        calendar.is_trading_day("2027-01-04")


# ----------------------------------------------------------------------
# Cache: a covered year never touches the network again
# ----------------------------------------------------------------------


def test_year_is_cached_to_disk(cache_dir):
    FinMindCalendar(_agreeing_fetcher).is_trading_day("2026-09-25")
    assert (cache_dir / "2026.json").is_file()


def test_cached_payload_is_the_sorted_trading_dates(cache_dir):
    FinMindCalendar(_agreeing_fetcher).is_trading_day("2026-09-25")
    stored = json.loads((cache_dir / "2026.json").read_text("utf-8"))
    assert stored == sorted(stored)
    assert "2026-09-25" not in stored  # 中秋節 -- not a trading day
    assert "2026-09-24" in stored


def test_cached_year_is_served_offline(cache_dir):
    FinMindCalendar(_agreeing_fetcher).is_trading_day("2026-09-25")

    calendar = FinMindCalendar(_refusing_fetcher, holiday_calendar=_twse_2026())
    assert calendar.is_trading_day("2026-09-25") is False
    assert calendar.closure("2026-12-25") == "行憲紀念日"


def test_same_instance_never_refetches_a_year(cache_dir):
    fetcher, calls = _counting(_agreeing_fetcher)
    calendar = FinMindCalendar(fetcher, holiday_calendar=_twse_2026())
    calendar.is_trading_day("2026-01-02")
    calendar.is_trading_day("2026-09-25")
    calendar.closure("2026-12-25")
    assert len(calls) == 1


def test_default_cache_lives_under_the_users_cache_home(monkeypatch, tmp_path):
    monkeypatch.delenv("DATA_SDK_FINMIND_CALENDAR_CACHE_PATH", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert FinMindCalendar.cache_dir() == str(tmp_path / "data_sdk" / "finmind_calendar")
    monkeypatch.delenv("XDG_CACHE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert FinMindCalendar.cache_dir() == str(
        tmp_path / "home" / ".cache" / "data_sdk" / "finmind_calendar"
    )


def test_an_unwritable_cache_still_serves_the_fetched_year(monkeypatch, tmp_path, caplog):
    blocked = tmp_path / "blocked"
    blocked.write_text("a file where the cache directory should be")
    monkeypatch.setenv("DATA_SDK_FINMIND_CALENDAR_CACHE_PATH", str(blocked / "finmind_calendar"))
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    with caplog.at_level("WARNING"):
        assert calendar.is_trading_day("2026-09-25") is False
        assert calendar.closure("2026-09-25") == "中秋節"
    assert "not cached" in caplog.text


def test_a_corrupt_cache_file_is_fetched_again_and_replaced(cache_dir, caplog):
    (cache_dir / "2026.json").write_text("{not json", encoding="utf-8")
    calendar = FinMindCalendar(_agreeing_fetcher, holiday_calendar=_twse_2026())
    with caplog.at_level("WARNING"):
        assert calendar.closure("2026-09-25") == "中秋節"
    assert "unreadable" in caplog.text
    stored = json.loads((cache_dir / "2026.json").read_text("utf-8"))
    assert "2026-09-24" in stored and "2026-09-25" not in stored


# ----------------------------------------------------------------------
# Construction never touches the network or needs a token
# ----------------------------------------------------------------------


def test_constructing_a_calendar_never_fetches(cache_dir):
    FinMindCalendar(_refusing_fetcher)  # no call made yet -- would raise if it fetched eagerly


# ----------------------------------------------------------------------
# ISSUES.md I-268: the current year's cache is revalidated once a day;
# past years are cached forever regardless of age.
# ----------------------------------------------------------------------


def _backdate(path: Path, now: dt.datetime, hours_ago: float) -> None:
    """Sets *path*'s mtime to *hours_ago* hours before *now* -- standing in
    for time actually passing, so these tests never sleep."""
    stamp = now.timestamp() - hours_ago * 3600
    os.utime(path, (stamp, stamp))


def test_current_year_cache_is_revalidated_once_the_interval_has_passed(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)  # older than CURRENT_YEAR_REVALIDATE_INTERVAL

    # FinMind corrected the day between the two calls (a makeup workday
    # cancelled, say) -- the revalidating call must see the correction.
    fetcher, calls = _counting(lambda s, e: _frame(["2026-06-02"]))
    second = FinMindCalendar(fetcher, now=lambda: now)
    assert second.is_trading_day("2026-06-01") is False
    assert second.is_trading_day("2026-06-02") is True
    assert len(calls) == 1
    assert json.loads(path.read_text("utf-8")) == ["2026-06-02"]


def test_current_year_cache_is_not_revalidated_before_the_interval(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=23)  # under the interval

    second = FinMindCalendar(_refusing_fetcher, now=lambda: now)
    assert second.is_trading_day("2026-06-01") is True  # still the cached answer, unrevalidated


def test_a_past_years_cache_is_never_revalidated_however_old(cache_dir):
    cached_at = dt.datetime(2024, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2024-06-01"]), now=lambda: cached_at)
    assert first.is_trading_day("2024-06-01") is True
    path = cache_dir / "2024.json"
    now = dt.datetime(2026, 6, 1, 9, 0, 0)  # "now" is two years later -- 2024 is a past year
    _backdate(path, now, hours_ago=400 * 24)  # far older than the daily interval

    second = FinMindCalendar(_refusing_fetcher, now=lambda: now)
    assert second.is_trading_day("2024-06-01") is True  # would have raised if it refetched


def test_revalidation_failure_keeps_the_cached_copy_and_says_so(cache_dir, caplog):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    def _unreachable(start_date, end_date):
        raise RuntimeError("network down")

    second = FinMindCalendar(_unreachable, now=lambda: now)
    with caplog.at_level("WARNING"):
        assert second.is_trading_day("2026-06-01") is True  # the stale-but-only copy still serves
    assert "revalidation failed" in caplog.text
    assert json.loads(path.read_text("utf-8")) == ["2026-06-01"]  # never overwritten


def test_revalidation_with_no_rows_keeps_the_cached_copy_and_says_so(cache_dir, caplog):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    second = FinMindCalendar(lambda s, e: _frame([]), now=lambda: now)
    with caplog.at_level("WARNING"):
        assert second.is_trading_day("2026-06-01") is True
    assert "revalidation returned no rows" in caplog.text
    assert json.loads(path.read_text("utf-8")) == ["2026-06-01"]  # never overwritten


def test_now_defaults_to_the_real_clock(cache_dir):
    calendar = FinMindCalendar(_refusing_fetcher)
    assert abs((calendar._now() - dt.datetime.now()).total_seconds()) < 5


# ----------------------------------------------------------------------
# ISSUES.md I-274: a failed revalidation backs off for the same interval
# as a successful one, across fresh instances (and processes, since the
# marker is a file) sharing the same cache directory.
# ----------------------------------------------------------------------


def test_fresh_instances_after_a_failed_attempt_make_exactly_one_network_call(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)  # older than CURRENT_YEAR_REVALIDATE_INTERVAL

    def _unreachable(start_date, end_date):
        raise RuntimeError("network down")

    fetcher, calls = _counting(_unreachable)
    for _ in range(5):
        calendar = FinMindCalendar(fetcher, now=lambda: now)
        assert calendar.is_trading_day("2026-06-01") is True  # the stale-but-only copy
    assert len(calls) == 1  # only the first instance's attempt actually touched the network


def test_a_new_attempt_is_made_once_the_interval_has_passed_again(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    def _unreachable(start_date, end_date):
        raise RuntimeError("network down")

    fetcher, calls = _counting(_unreachable)
    FinMindCalendar(fetcher, now=lambda: now).is_trading_day("2026-06-01")
    assert len(calls) == 1  # the one attempt this interval allows

    later = now + FinMindCalendar.CURRENT_YEAR_REVALIDATE_INTERVAL
    FinMindCalendar(fetcher, now=lambda: later).is_trading_day("2026-06-01")
    assert len(calls) == 2  # a full interval has passed since the failed attempt


def test_a_success_after_an_outage_still_refreshes_the_cache(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    def _unreachable(start_date, end_date):
        raise RuntimeError("network down")

    FinMindCalendar(_unreachable, now=lambda: now).is_trading_day("2026-06-01")  # the outage

    later = now + FinMindCalendar.CURRENT_YEAR_REVALIDATE_INTERVAL
    recovered = FinMindCalendar(lambda s, e: _frame(["2026-06-02"]), now=lambda: later)
    assert recovered.is_trading_day("2026-06-01") is False
    assert recovered.is_trading_day("2026-06-02") is True
    assert json.loads(path.read_text("utf-8")) == ["2026-06-02"]


def test_revalidation_attempt_marker_is_written_next_to_the_cache_file(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    def _unreachable(start_date, end_date):
        raise RuntimeError("network down")

    FinMindCalendar(_unreachable, now=lambda: now).is_trading_day("2026-06-01")
    marker = cache_dir / "2026.json.revalidated_at"
    assert marker.is_file()
    assert float(marker.read_text("ascii")) == now.timestamp()


def test_a_garbled_attempt_marker_is_treated_as_no_attempt_yet(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)
    (cache_dir / "2026.json.revalidated_at").write_text("not-a-timestamp", encoding="ascii")

    fetcher, calls = _counting(lambda s, e: _frame(["2026-06-01"]))
    FinMindCalendar(fetcher, now=lambda: now).is_trading_day("2026-06-01")
    assert len(calls) == 1  # the garbled marker did not suppress the attempt


# ----------------------------------------------------------------------
# ISSUES.md I-280: a fetch that succeeds but whose cache write fails must
# not let the I-274 marker suppress the next instance's retry.
# ----------------------------------------------------------------------


def test_fetch_success_with_write_failure_lets_the_next_instance_retry(cache_dir, monkeypatch):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    # The fetch itself succeeds with FinMind's correction (2026-06-01 is no
    # longer a trading day), but persisting the cache file fails (e.g. a
    # full disk, a permissions race); the marker write is left real.
    # Scoped to its own context so undoing it does not also undo the
    # cache_dir fixture's own env var patch (sharing the outer
    # `monkeypatch` for that would redirect the second instance below at
    # the user's real cache directory instead of this test's tmp_path).
    with monkeypatch.context() as scoped:
        scoped.setattr(
            FinMindCalendar, "_write_atomic", _write_atomic_failing_for_the_cache_file_only()
        )
        # FinMind's correction: 2026-06-01 is dropped, 2026-06-02 is now
        # listed instead -- a non-empty, successful fetch (not "no rows").
        corrected_fetcher, first_calls = _counting(lambda s, e: _frame(["2026-06-02"]))
        revalidating = FinMindCalendar(corrected_fetcher, now=lambda: now)
        # The fetching instance's own in-memory answer is still correct
        # even though persistence failed.
        assert revalidating.is_trading_day("2026-06-01") is False
        assert len(first_calls) == 1
        # Nothing reached disk -- the stale cache file is untouched.
        assert json.loads(path.read_text("utf-8")) == ["2026-06-01"]

    # A second, freshly constructed instance a minute later (real
    # _write_atomic restored, still this test's tmp_path cache_dir) must
    # still see the cache as needing a retry -- not suppressed by a marker
    # that a fetch was "attempted" -- since the write never persisted.
    fetcher, second_calls = _counting(lambda s, e: _frame(["2026-06-02"]))
    second = FinMindCalendar(fetcher, now=lambda: now)
    assert second.is_trading_day("2026-06-01") is False
    assert len(second_calls) == 1  # retried the network instead of serving the stale cache
    assert json.loads(path.read_text("utf-8")) == ["2026-06-02"]  # this time it persisted


def test_fetch_success_with_write_failure_does_not_write_the_attempt_marker(
    cache_dir, monkeypatch
):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    monkeypatch.setattr(
        FinMindCalendar, "_write_atomic", _write_atomic_failing_for_the_cache_file_only()
    )
    FinMindCalendar(lambda s, e: _frame(["2026-06-02"]), now=lambda: now).is_trading_day(
        "2026-06-01"
    )
    assert not (cache_dir / "2026.json.revalidated_at").exists()


def test_fetch_success_with_write_failure_is_logged(cache_dir, monkeypatch, caplog):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    monkeypatch.setattr(
        FinMindCalendar, "_write_atomic", _write_atomic_failing_for_the_cache_file_only()
    )
    with caplog.at_level("WARNING"):
        FinMindCalendar(lambda s, e: _frame(["2026-06-02"]), now=lambda: now).is_trading_day(
            "2026-06-01"
        )
    assert "not cached" in caplog.text
    assert "next call will retry" in caplog.text


# ----------------------------------------------------------------------
# ISSUES.md I-283: the stale-check, marker-write and fetch are serialised
# with an advisory lock across processes/threads sharing one cache
# directory, keeping the at-most-one-attempt-per-interval guarantee.
# ----------------------------------------------------------------------


def test_a_lock_held_by_another_process_is_served_the_cache_without_fetching(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    # Simulate another process already revalidating this year: hold the
    # lock file externally before constructing this instance.
    lock_path = cache_dir / "2026.json.lock"
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    try:
        fetcher, calls = _counting(lambda s, e: _frame(["2026-06-02"]))
        calendar = FinMindCalendar(fetcher, now=lambda: now)
        # Loses the race for the lock -- serves the stale-but-only cache
        # rather than also dialing out to FinMind.
        assert calendar.is_trading_day("2026-06-01") is True
        assert len(calls) == 0
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


def test_concurrent_constructions_against_the_same_cache_make_exactly_one_fetch(cache_dir):
    now = dt.datetime(2026, 6, 1, 9, 0, 0)
    first = FinMindCalendar(lambda s, e: _frame(["2026-06-01"]), now=lambda: now)
    assert first.is_trading_day("2026-06-01") is True
    path = cache_dir / "2026.json"
    _backdate(path, now, hours_ago=25)

    calls = []
    calls_lock = threading.Lock()
    started = threading.Barrier(2)

    def slow_fetcher(start_date, end_date):
        with calls_lock:
            calls.append((start_date, end_date))
        time.sleep(0.3)  # widens the window for the other thread to lose the lock race
        return _frame(["2026-06-02"])

    results = []
    results_lock = threading.Lock()

    def worker():
        started.wait()
        calendar = FinMindCalendar(slow_fetcher, now=lambda: now)
        answer = calendar.is_trading_day("2026-06-01")
        with results_lock:
            results.append(answer)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert len(calls) == 1  # exactly one of the two instances touched the network
    assert len(results) == 2
