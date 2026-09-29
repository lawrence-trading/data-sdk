"""DispositionList against real recorded TWSE / TPEx responses: no network.

The fixtures are TWSE's and TPEx's open-data responses recorded in full on
2026-09-29 (8 TWSE rows, 17 TPEx rows, including TPEx code 22211 announced
twice under two overlapping windows) -- not a hand-picked subset, so the
"effective on 2026-09-29" tests below exercise the real row count and the
real duplicate-code case.
"""

import datetime as dt
import json
from pathlib import Path

import pytest

from data_sdk import DispositionList
from data_sdk.wrappers.disposition import TPEX_DISPOSAL_URL, TWSE_PUNISH_URL

FIXTURES = Path(__file__).parent / "fixtures" / "disposition"


def _load(name):
    return json.loads((FIXTURES / name).read_text("utf-8"))


TWSE_PUNISH = _load("twse_punish.json")
TPEX_DISPOSAL = _load("tpex_disposal_information.json")

TODAY = dt.date(2026, 9, 29)

# The codes covering TODAY, in the order entries() emits them (TWSE rows
# first, then TPEx). TPEx's 22211 is announced twice under two overlapping
# windows and appears twice, undeduplicated -- entries()/effective_on()
# leave merging a duplicate code to the caller (see the module docstring).
TWSE_EFFECTIVE_CODES = ["2305", "2455", "2468", "3443", "6168", "6226", "6715", "8021"]
TPEX_EFFECTIVE_CODES = [
    "2221", "22211", "36053", "36054", "7772", "22211", "24552", "24553",
    "6218", "6945", "80212", "5314", "6221", "3441",
]
# 3467, 8227, 8358 all ended 2026-09-24, before TODAY -- not on this list.


def _fetch(url):
    if url == TWSE_PUNISH_URL:
        return TWSE_PUNISH
    if url == TPEX_DISPOSAL_URL:
        return TPEX_DISPOSAL
    raise AssertionError(f"unexpected url: {url}")


def _refusing_fetch(url):
    raise AssertionError("network touched on a cached date")


# ----------------------------------------------------------------------
# Parsing the real response shapes
# ----------------------------------------------------------------------


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_DISPOSITION_CACHE_PATH", str(tmp_path))
    return tmp_path


def test_entries_combine_both_exchanges(cache_dir):
    entries = DispositionList(fetch_json=_fetch).entries(TODAY)
    assert [e.code for e in entries] == TWSE_EFFECTIVE_CODES + [
        "2221", "22211", "36053", "36054", "7772", "22211", "24552", "24553",
        "6218", "6945", "80212", "5314", "6221", "3441", "3467", "8227", "8358",
    ]


def test_twse_full_width_tilde_period_is_parsed(cache_dir):
    """TWSE's DispositionPeriod uses ～ (full-width); TPEx's uses ~ (half-width)."""
    entries = DispositionList(fetch_json=_fetch).entries(TODAY)
    full_friend = next(e for e in entries if e.code == "2305")
    assert full_friend.start_date == "2026-09-18" and full_friend.end_date == "2026-09-30"


def test_roc_year_conversion(cache_dir):
    """ROC 115 -> 2026 (+1911), both the slashed and compact forms."""
    entries = DispositionList(fetch_json=_fetch).entries(TODAY)
    by_code = {e.code: e for e in entries}
    assert by_code["2455"].start_date == "2026-09-23"   # from "115/09/23"
    assert by_code["2221"].start_date == "2026-09-24"    # from "1150924"


def test_tpex_code_announced_twice_yields_two_undeduplicated_entries(cache_dir):
    """22211 was placed under disposition twice with overlapping windows;
    entries() reports both rather than picking one -- see the module
    docstring on why dedup is the caller's job."""
    entries = DispositionList(fetch_json=_fetch).entries(TODAY)
    windows = [(e.start_date, e.end_date) for e in entries if e.code == "22211"]
    assert windows == [("2026-09-24", "2026-10-06"), ("2026-09-23", "2026-10-01")]


# ----------------------------------------------------------------------
# effective_on: whose announced period covers a given date
# ----------------------------------------------------------------------


def test_effective_on_the_real_recorded_day(cache_dir):
    d = DispositionList(fetch_json=_fetch)
    assert [e.code for e in d.effective_on(TODAY)] == TWSE_EFFECTIVE_CODES + TPEX_EFFECTIVE_CODES


def test_effective_on_excludes_a_window_that_already_ended(cache_dir):
    """3467, 8227, 8358 all ended 2026-09-24 -- five days before TODAY."""
    d = DispositionList(fetch_json=_fetch)
    codes = {e.code for e in d.effective_on(TODAY)}
    assert not codes & {"3467", "8227", "8358"}


def test_effective_on_bounds_are_inclusive(cache_dir):
    """6221's window ends exactly on TODAY (2026-09-29) and still counts;
    3443's window starts exactly on TODAY and already counts."""
    d = DispositionList(fetch_json=_fetch)
    codes = {e.code for e in d.effective_on(TODAY)}
    assert "6221" in codes    # end_date == TODAY
    assert "3443" in codes    # start_date == TODAY
    assert "6221" not in {e.code for e in d.effective_on(dt.date(2026, 9, 30))}    # day after its end
    assert "3443" not in {e.code for e in d.effective_on(dt.date(2026, 9, 28))}    # day before its start


def test_effective_on_a_period_starting_tomorrow_is_excluded(cache_dir):
    twse = [{"Code": "1101", "Name": "台泥", "DispositionPeriod": "115/09/30～115/10/06"}]
    d = DispositionList(fetch_json=lambda url: twse if url == TWSE_PUNISH_URL else [])
    assert d.effective_on(TODAY) == []
    assert [e.code for e in d.effective_on(dt.date(2026, 9, 30))] == ["1101"]


def test_effective_on_a_period_ending_yesterday_is_excluded(cache_dir):
    twse = [{"Code": "1101", "Name": "台泥", "DispositionPeriod": "115/09/20～115/09/28"}]
    d = DispositionList(fetch_json=lambda url: twse if url == TWSE_PUNISH_URL else [])
    assert d.effective_on(TODAY) == []
    assert [e.code for e in d.effective_on(dt.date(2026, 9, 28))] == ["1101"]


# ----------------------------------------------------------------------
# Malformed rows are dropped, not raised on
# ----------------------------------------------------------------------


def test_a_row_with_no_code_or_an_unparsable_period_is_dropped(cache_dir):
    twse = [{"Code": "", "Name": "沒有代號", "DispositionPeriod": "115/09/18～115/09/30"},
            {"Code": "9999", "Name": "壞日期", "DispositionPeriod": "not-a-period"}]
    tpex = [{"SecuritiesCompanyCode": "", "CompanyName": "沒有代號", "DispositionPeriod": "1150924~1151006"}]
    d = DispositionList(fetch_json=lambda url: twse if url == TWSE_PUNISH_URL else tpex)
    assert d.entries(TODAY) == []


def test_a_response_missing_the_expected_keys_yields_no_rows_not_an_error(cache_dir):
    twse = [{"something": "else"}]
    d = DispositionList(fetch_json=lambda url: twse if url == TWSE_PUNISH_URL else [])
    assert d.entries(TODAY) == []


def test_a_non_list_twse_or_tpex_payload_yields_no_rows(cache_dir):
    d = DispositionList(fetch_json=lambda url: {} if url == TWSE_PUNISH_URL else {})
    assert d.entries(TODAY) == []


# ----------------------------------------------------------------------
# Cache: a covered date never touches the network again
# ----------------------------------------------------------------------


def test_a_date_is_cached_to_disk(cache_dir):
    DispositionList(fetch_json=_fetch).entries(TODAY)
    assert (cache_dir / "2026-09-29.json").is_file()


def test_cached_payload_is_the_raw_pair_plus_the_endpoint_and_schema(cache_dir):
    DispositionList(fetch_json=_fetch).entries(TODAY)
    stored = json.loads((cache_dir / "2026-09-29.json").read_text("utf-8"))
    assert stored["twse"] == TWSE_PUNISH
    assert stored["otc"] == TPEX_DISPOSAL
    assert stored["twse_url"] == TWSE_PUNISH_URL
    assert stored["otc_url"] == TPEX_DISPOSAL_URL
    assert isinstance(stored["schema"], int)


def test_cached_date_is_served_offline(cache_dir):
    DispositionList(fetch_json=_fetch).entries(TODAY)
    fresh = DispositionList(fetch_json=_refusing_fetch)
    assert [e.code for e in fresh.entries(TODAY)] == [e.code for e in DispositionList(fetch_json=_fetch).entries(TODAY)]


def test_same_instance_never_refetches_a_date(cache_dir):
    calls = []

    def counting_fetch(url):
        calls.append(url)
        return _fetch(url)

    d = DispositionList(fetch_json=counting_fetch)
    d.entries(TODAY)
    d.effective_on(TODAY)
    d.entries(TODAY)
    assert len(calls) == 2   # one TWSE + one TPEx call, ever


def test_a_different_date_is_a_separate_cache_entry(cache_dir):
    calls = []

    def counting_fetch(url):
        calls.append(url)
        return _fetch(url)

    d = DispositionList(fetch_json=counting_fetch)
    d.entries(TODAY)
    d.entries(dt.date(2026, 9, 30))
    assert len(calls) == 4
    assert (cache_dir / "2026-09-29.json").is_file()
    assert (cache_dir / "2026-09-30.json").is_file()


def test_a_corrupt_cache_file_is_fetched_again_and_replaced(cache_dir, caplog):
    (cache_dir / "2026-09-29.json").write_text("{not json", encoding="utf-8")
    d = DispositionList(fetch_json=_fetch)
    with caplog.at_level("WARNING"):
        entries = d.entries(TODAY)
    assert [e.code for e in entries] == [e.code for e in DispositionList(fetch_json=_fetch).entries(TODAY)]
    assert "unreadable" in caplog.text
    stored = json.loads((cache_dir / "2026-09-29.json").read_text("utf-8"))
    assert stored["twse"] == TWSE_PUNISH and stored["otc"] == TPEX_DISPOSAL


def test_a_cache_from_a_different_endpoint_or_schema_is_refetched_not_replayed(cache_dir, caplog):
    """A cache written before TWSE_PUNISH_URL moved to the openapi endpoint
    (or by any older schema) carries no ``schema``/``twse_url``/``otc_url``
    -- exactly the shape MarketCalendar-style caches used before this fix.
    It must never be replayed as if it still answered for the current
    endpoint: this is the fix for the stale-cache bug (ISSUES.md I-55
    follow-up) where a hand-written or partial cache entry from an earlier
    run silently froze a day's disposition list."""
    stale = {"twse": [{"Code": "0001", "Name": "stale", "DispositionPeriod": "115/09/01～115/09/02"}], "otc": []}
    (cache_dir / "2026-09-29.json").write_text(json.dumps(stale), encoding="utf-8")
    d = DispositionList(fetch_json=_fetch)
    with caplog.at_level("WARNING"):
        entries = d.entries(TODAY)
    assert [e.code for e in entries] == [e.code for e in DispositionList(fetch_json=_fetch).entries(TODAY)]
    assert "different endpoint or schema" in caplog.text
    stored = json.loads((cache_dir / "2026-09-29.json").read_text("utf-8"))
    assert stored["twse_url"] == TWSE_PUNISH_URL and stored["otc_url"] == TPEX_DISPOSAL_URL


def test_an_unwritable_cache_still_serves_the_fetched_date(monkeypatch, tmp_path, caplog):
    blocked = tmp_path / "blocked"
    blocked.write_text("a file where the cache directory should be")
    monkeypatch.setenv("DATA_SDK_DISPOSITION_CACHE_PATH", str(blocked / "disposition"))
    d = DispositionList(fetch_json=_fetch)
    with caplog.at_level("WARNING"):
        entries = d.entries(TODAY)
    assert [e.code for e in entries] == [e.code for e in DispositionList(fetch_json=_fetch).entries(TODAY)]
    assert "not cached" in caplog.text


def test_default_cache_lives_under_the_users_cache_home(monkeypatch, tmp_path):
    monkeypatch.delenv("DATA_SDK_DISPOSITION_CACHE_PATH", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path))
    assert DispositionList.cache_dir() == str(tmp_path / "data_sdk" / "disposition")


# ----------------------------------------------------------------------
# Either exchange failing raises rather than returning a partial list
# ----------------------------------------------------------------------


def test_either_exchange_failing_raises_instead_of_a_partial_list(cache_dir):
    def flaky(url):
        if url == TWSE_PUNISH_URL:
            raise RuntimeError("TWSE unreachable")
        return TPEX_DISPOSAL

    with pytest.raises(RuntimeError):
        DispositionList(fetch_json=flaky).entries(TODAY)
    # The failed fetch must not have been cached as if it were a real answer.
    assert not (cache_dir / "2026-09-29.json").exists()
