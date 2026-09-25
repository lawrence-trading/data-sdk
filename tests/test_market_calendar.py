"""MarketCalendar against the real 2026 TWSE response and injected payloads: no network."""

import json
from pathlib import Path

import pytest

from data_sdk import HolidayEntry, MarketCalendar, UnknownCalendarYear

FIXTURES = Path(__file__).parent / "fixtures" / "market_calendar"


def _load(name):
    return json.loads((FIXTURES / name).read_text("utf-8"))


REAL_2026 = _load("holiday_schedule_2026.json")


def _unpublished(year):
    """Shape TWSE actually returns for a year it has not published yet (verified live for 2027)."""
    return {
        "stat": "ok",
        "date": f"{year}0101",
        "title": f"{year - 1911} 年市場開休市日期",
        "fields": ["日期", "名稱", "說明"],
        "data": [],
        "queryYear": year,
        "total": 0,
    }


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_MARKET_CALENDAR_CACHE_PATH", str(tmp_path))
    return tmp_path


def _refusing_fetch(url):
    raise AssertionError("network touched on a cached year")


# ----------------------------------------------------------------------
# Classification against the real 2026 response
# ----------------------------------------------------------------------


def test_named_holiday_closes_the_market(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day("2026-09-25") is False
    assert calendar.closure("2026-09-25") == "中秋節"


def test_teachers_day_is_named_by_its_own_entry(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day("2026-09-28") is False
    assert calendar.closure("2026-09-28") == "孔子誕辰紀念日/ 教師節"


def test_market_no_trade_entry_closes_an_ordinary_weekday(cache_dir):
    # 2026-02-12/13 are a Thursday and Friday, not a weekend; TWSE calls them
    # out as settlement-only, which closes the market.
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day("2026-02-12") is False
    assert calendar.closure("2026-02-12") == "市場無交易，僅辦理結算交割作業"
    assert calendar.is_trading_day("2026-02-13") is False


def test_informational_entry_on_an_ordinary_weekday_still_trades(cache_dir):
    # 2026-01-02 (國曆新年開始交易日) is a Friday; the entry is informational,
    # not a closure -- the day already trades under the weekday rule too.
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day("2026-01-02") is True
    assert calendar.closure("2026-01-02") is None


def test_unannounced_weekend_is_closed_with_the_generic_reason(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day("2026-09-26") is False  # Saturday, no TWSE entry
    assert calendar.closure("2026-09-26") == "週末"
    assert calendar.is_trading_day("2026-09-27") is False  # Sunday, no TWSE entry
    assert calendar.closure("2026-09-27") == "週末"


def test_ordinary_weekday_with_no_entry_trades(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day("2026-09-24") is True
    assert calendar.closure("2026-09-24") is None


def test_date_input_accepts_date_object_and_slash_string(cache_dir):
    import datetime as dt

    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.is_trading_day(dt.date(2026, 9, 25)) is False
    assert calendar.is_trading_day("2026/09/25") is False


def test_entry_returns_none_for_an_unannounced_date(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.entry("2026-09-24") is None
    entry = calendar.entry("2026-09-25")
    assert entry == HolidayEntry(
        date="2026-09-25", name="中秋節", description="依規定放假1日。", is_trading_day=False
    )


# ----------------------------------------------------------------------
# Compensation trading day overriding a weekend (synthetic: no such date
# lands in the real 2026 response, but the rule must still hold)
# ----------------------------------------------------------------------


def test_trading_day_entry_overrides_a_weekend(cache_dir):
    payload = {
        "data": [["2026-01-03", "補行上班上課之開始交易日", "週六照常交易。"]],
    }
    calendar = MarketCalendar(fetch_json=lambda url: payload)
    assert _weekday("2026-01-03") == 5  # Saturday
    assert calendar.is_trading_day("2026-01-03") is True
    assert calendar.closure("2026-01-03") is None


def _weekday(date_str):
    import datetime as dt

    return dt.date.fromisoformat(date_str).weekday()


# ----------------------------------------------------------------------
# Cache: a covered year never touches the network again
# ----------------------------------------------------------------------


def test_year_entries_are_cached_to_disk(cache_dir):
    MarketCalendar(fetch_json=lambda url: REAL_2026).year_entries(2026)
    assert (cache_dir / "2026.json").is_file()


def test_cached_payload_is_the_raw_response(cache_dir):
    MarketCalendar(fetch_json=lambda url: REAL_2026).year_entries(2026)
    stored = json.loads((cache_dir / "2026.json").read_text("utf-8"))
    assert stored == REAL_2026


def test_cached_year_is_served_offline(cache_dir):
    MarketCalendar(fetch_json=lambda url: REAL_2026).year_entries(2026)

    calendar = MarketCalendar(fetch_json=_refusing_fetch)
    assert calendar.is_trading_day("2026-09-25") is False
    assert calendar.closure("2026-12-25") == "行憲紀念日"


def test_same_instance_never_refetches_a_year(cache_dir):
    calls = []

    def counting_fetch(url):
        calls.append(url)
        return REAL_2026

    calendar = MarketCalendar(fetch_json=counting_fetch)
    calendar.is_trading_day("2026-01-01")
    calendar.is_trading_day("2026-09-25")
    calendar.closure("2026-12-25")
    assert len(calls) == 1


# ----------------------------------------------------------------------
# An unpublished year is never silently "all weekdays open"
# ----------------------------------------------------------------------


def test_unpublished_year_raises_unknown_calendar_year(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: _unpublished(2027))
    with pytest.raises(UnknownCalendarYear) as excinfo:
        calendar.is_trading_day("2027-01-04")
    assert excinfo.value.year == 2027


def test_unpublished_year_response_is_never_cached(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: _unpublished(2027))
    with pytest.raises(UnknownCalendarYear):
        calendar.year_entries(2027)
    assert not (cache_dir / "2027.json").exists()


def test_unpublished_year_retries_the_network_on_every_call(cache_dir):
    calls = []

    def counting_fetch(url):
        calls.append(url)
        return _unpublished(2027)

    calendar = MarketCalendar(fetch_json=counting_fetch)
    for _ in range(3):
        with pytest.raises(UnknownCalendarYear):
            calendar.is_trading_day("2027-01-04")
    assert len(calls) == 3


def test_year_url_asks_for_january_first_of_the_requested_year(cache_dir):
    seen = {}

    def fetch(url):
        seen["url"] = url
        return REAL_2026

    MarketCalendar(fetch_json=fetch).year_entries(2026)
    assert seen["url"] == (
        "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
        "?date=20260101&response=json"
    )


# ----------------------------------------------------------------------
# next_trading_day / previous_trading_day
# ----------------------------------------------------------------------


def test_next_trading_day_skips_the_mid_autumn_stretch(cache_dir):
    # The exact ISSUES.md I-32 scenario: 中秋節 (Fri) into a weekend into
    # 教師節 (Mon) -- the next trading day is Tuesday 2026-09-29.
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.next_trading_day("2026-09-24") == "2026-09-29"
    assert calendar.next_trading_day("2026-09-25") == "2026-09-29"


def test_previous_trading_day_skips_the_mid_autumn_stretch(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    assert calendar.previous_trading_day("2026-09-29") == "2026-09-24"


def test_next_trading_day_gives_up_after_max_days(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    with pytest.raises(RuntimeError):
        calendar.next_trading_day("2026-09-25", max_days=1)


# ----------------------------------------------------------------------
# export_range: the payload the pre-open job writes to market-calendar.json
# ----------------------------------------------------------------------


def test_export_range_matches_the_mid_autumn_scenario(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    rows = calendar.export_range("2026-09-25", "2026-09-29")
    assert rows == [
        {"date": "2026-09-25", "trading_day": False, "closure": "中秋節"},
        {"date": "2026-09-26", "trading_day": False, "closure": "週末"},
        {"date": "2026-09-27", "trading_day": False, "closure": "週末"},
        {"date": "2026-09-28", "trading_day": False, "closure": "孔子誕辰紀念日/ 教師節"},
        {"date": "2026-09-29", "trading_day": True, "closure": None},
    ]


def test_export_range_end_before_start_raises(cache_dir):
    calendar = MarketCalendar(fetch_json=lambda url: REAL_2026)
    with pytest.raises(ValueError):
        calendar.export_range("2026-09-29", "2026-09-25")


# ----------------------------------------------------------------------
# Where the cache lives, and a cache that cannot be written
# ----------------------------------------------------------------------


def test_default_cache_lives_under_the_users_cache_home(monkeypatch, tmp_path):
    monkeypatch.delenv("DATA_SDK_MARKET_CALENDAR_CACHE_PATH", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert MarketCalendar.cache_dir() == str(tmp_path / "data_sdk" / "market_calendar")
    monkeypatch.delenv("XDG_CACHE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    assert MarketCalendar.cache_dir() == str(tmp_path / "home" / ".cache" / "data_sdk" / "market_calendar")


def test_an_unwritable_cache_still_serves_the_fetched_year(monkeypatch, tmp_path, caplog):
    blocked = tmp_path / "blocked"
    blocked.write_text("a file where the cache directory should be")
    monkeypatch.setenv("DATA_SDK_MARKET_CALENDAR_CACHE_PATH", str(blocked / "market_calendar"))
    cal = MarketCalendar(fetch_json=lambda url: REAL_2026)
    with caplog.at_level("WARNING"):
        assert cal.is_trading_day("2026-09-25") is False
        assert cal.closure("2026-09-25") == "中秋節"
    assert "not cached" in caplog.text


def test_a_corrupt_cache_file_is_fetched_again_and_replaced(cache_dir, caplog):
    (cache_dir / "2026.json").write_text("{not json", encoding="utf-8")
    cal = MarketCalendar(fetch_json=lambda url: REAL_2026)
    with caplog.at_level("WARNING"):
        assert cal.closure("2026-09-25") == "中秋節"
    assert "unreadable" in caplog.text
    assert json.loads((cache_dir / "2026.json").read_text("utf-8")) == REAL_2026
