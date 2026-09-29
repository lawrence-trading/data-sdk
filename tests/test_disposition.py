"""DispositionList against real recorded TWSE / TPEx responses: no network."""

import datetime as dt
import json
from pathlib import Path

import pytest

from data_sdk import DispositionEntry, DispositionList
from data_sdk.wrappers.disposition import TPEX_DISPOSAL_URL, TWSE_PUNISH_URL

FIXTURES = Path(__file__).parent / "fixtures" / "disposition"


def _load(name):
    return json.loads((FIXTURES / name).read_text("utf-8"))


TWSE_PUNISH = _load("twse_punish.json")
TPEX_DISPOSAL = _load("tpex_disposal_information.json")


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_DISPOSITION_CACHE_PATH", str(tmp_path))
    return tmp_path


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


def test_entries_combine_both_exchanges(cache_dir):
    entries = DispositionList(fetch_json=_fetch).entries(dt.date(2026, 9, 29))
    assert entries == [
        DispositionEntry(code="2305", name="全友", start_date="2026-09-18", end_date="2026-09-30"),
        DispositionEntry(code="2455", name="全新", start_date="2026-09-23", end_date="2026-10-05"),
        DispositionEntry(code="2221", name="大甲", start_date="2026-09-24", end_date="2026-10-06"),
        DispositionEntry(code="22211", name="大甲一", start_date="2026-09-24", end_date="2026-10-06"),
        DispositionEntry(code="36053", name="宏致三", start_date="2026-09-24", end_date="2026-10-02"),
    ]


def test_twse_full_width_tilde_period_is_parsed(cache_dir):
    """TWSE's 處置起迄時間 uses ～ (full-width); TPEx's DispositionPeriod uses ~."""
    entries = DispositionList(fetch_json=_fetch).entries(dt.date(2026, 9, 29))
    full_friend = next(e for e in entries if e.code == "2305")
    assert full_friend.start_date == "2026-09-18" and full_friend.end_date == "2026-09-30"


def test_roc_year_conversion(cache_dir):
    """ROC 115 -> 2026 (+1911), both the slashed and compact forms."""
    entries = DispositionList(fetch_json=_fetch).entries(dt.date(2026, 9, 29))
    by_code = {e.code: e for e in entries}
    assert by_code["2455"].start_date == "2026-09-23"   # from "115/09/23"
    assert by_code["2221"].start_date == "2026-09-24"    # from "1150924"


# ----------------------------------------------------------------------
# effective_on: whose announced period covers a given date
# ----------------------------------------------------------------------


def test_effective_on_filters_to_the_covering_window(cache_dir):
    d = DispositionList(fetch_json=_fetch)
    assert [e.code for e in d.effective_on(dt.date(2026, 9, 29))] == ["2305", "2455", "2221", "22211", "36053"]
    # 2026-09-18 is 2305's first day; the others have not started yet.
    assert [e.code for e in d.effective_on(dt.date(2026, 9, 18))] == ["2305"]
    # 36053's window ends 2026-10-02; the others still cover this date.
    assert [e.code for e in d.effective_on(dt.date(2026, 10, 3))] == ["2455", "2221", "22211"]
    # Past every window.
    assert d.effective_on(dt.date(2026, 10, 10)) == []


def test_effective_on_bounds_are_inclusive(cache_dir):
    d = DispositionList(fetch_json=_fetch)
    codes_at_start = {e.code for e in d.effective_on(dt.date(2026, 9, 18))}
    codes_at_end = {e.code for e in d.effective_on(dt.date(2026, 9, 30))}
    assert "2305" in codes_at_start   # start_date itself counts
    assert "2305" in codes_at_end     # end_date itself still counts


# ----------------------------------------------------------------------
# Malformed rows are dropped, not raised on
# ----------------------------------------------------------------------


def test_a_row_with_no_code_or_an_unparsable_period_is_dropped(cache_dir):
    twse = {
        "fields": ["證券代號", "證券名稱", "處置起迄時間"],
        "data": [["", "沒有代號", "115/09/18～115/09/30"], ["9999", "壞日期", "not-a-period"]],
    }
    tpex = [{"SecuritiesCompanyCode": "", "CompanyName": "沒有代號", "DispositionPeriod": "1150924~1151006"}]
    d = DispositionList(fetch_json=lambda url: twse if url == TWSE_PUNISH_URL else tpex)
    assert d.entries(dt.date(2026, 9, 29)) == []


def test_a_response_missing_the_expected_columns_yields_no_rows_not_an_error(cache_dir):
    twse = {"fields": ["something", "else"], "data": [[1, 2]]}
    d = DispositionList(fetch_json=lambda url: twse if url == TWSE_PUNISH_URL else [])
    assert d.entries(dt.date(2026, 9, 29)) == []


def test_a_non_dict_twse_or_non_list_tpex_payload_yields_no_rows(cache_dir):
    d = DispositionList(fetch_json=lambda url: [] if url == TWSE_PUNISH_URL else {})
    assert d.entries(dt.date(2026, 9, 29)) == []


# ----------------------------------------------------------------------
# Cache: a covered date never touches the network again
# ----------------------------------------------------------------------


def test_a_date_is_cached_to_disk(cache_dir):
    DispositionList(fetch_json=_fetch).entries(dt.date(2026, 9, 29))
    assert (cache_dir / "2026-09-29.json").is_file()


def test_cached_payload_is_the_raw_pair(cache_dir):
    DispositionList(fetch_json=_fetch).entries(dt.date(2026, 9, 29))
    stored = json.loads((cache_dir / "2026-09-29.json").read_text("utf-8"))
    assert stored == {"twse": TWSE_PUNISH, "otc": TPEX_DISPOSAL}


def test_cached_date_is_served_offline(cache_dir):
    DispositionList(fetch_json=_fetch).entries(dt.date(2026, 9, 29))
    fresh = DispositionList(fetch_json=_refusing_fetch)
    assert [e.code for e in fresh.entries(dt.date(2026, 9, 29))] == ["2305", "2455", "2221", "22211", "36053"]


def test_same_instance_never_refetches_a_date(cache_dir):
    calls = []

    def counting_fetch(url):
        calls.append(url)
        return _fetch(url)

    d = DispositionList(fetch_json=counting_fetch)
    d.entries(dt.date(2026, 9, 29))
    d.effective_on(dt.date(2026, 9, 29))
    d.entries(dt.date(2026, 9, 29))
    assert len(calls) == 2   # one TWSE + one TPEx call, ever


def test_a_different_date_is_a_separate_cache_entry(cache_dir):
    calls = []

    def counting_fetch(url):
        calls.append(url)
        return _fetch(url)

    d = DispositionList(fetch_json=counting_fetch)
    d.entries(dt.date(2026, 9, 29))
    d.entries(dt.date(2026, 9, 30))
    assert len(calls) == 4
    assert (cache_dir / "2026-09-29.json").is_file()
    assert (cache_dir / "2026-09-30.json").is_file()


def test_a_corrupt_cache_file_is_fetched_again_and_replaced(cache_dir, caplog):
    (cache_dir / "2026-09-29.json").write_text("{not json", encoding="utf-8")
    d = DispositionList(fetch_json=_fetch)
    with caplog.at_level("WARNING"):
        entries = d.entries(dt.date(2026, 9, 29))
    assert [e.code for e in entries] == ["2305", "2455", "2221", "22211", "36053"]
    assert "unreadable" in caplog.text
    assert json.loads((cache_dir / "2026-09-29.json").read_text("utf-8")) == {"twse": TWSE_PUNISH, "otc": TPEX_DISPOSAL}


def test_an_unwritable_cache_still_serves_the_fetched_date(monkeypatch, tmp_path, caplog):
    blocked = tmp_path / "blocked"
    blocked.write_text("a file where the cache directory should be")
    monkeypatch.setenv("DATA_SDK_DISPOSITION_CACHE_PATH", str(blocked / "disposition"))
    d = DispositionList(fetch_json=_fetch)
    with caplog.at_level("WARNING"):
        entries = d.entries(dt.date(2026, 9, 29))
    assert [e.code for e in entries] == ["2305", "2455", "2221", "22211", "36053"]
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
        DispositionList(fetch_json=flaky).entries(dt.date(2026, 9, 29))
    # The failed fetch must not have been cached as if it were a real answer.
    assert not (cache_dir / "2026-09-29.json").exists()
