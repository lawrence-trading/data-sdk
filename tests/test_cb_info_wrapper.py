"""CbInfoWrapper against injected fetchers: no FINMIND_API_TOKEN, no Shioaji
login. Every test passes its own fetch_info / fetch_daily_overview /
get_stock_codes so the real DataLoader/ShioajiWrapper is never constructed.
"""

import json

import pandas as pd
import pytest

from data_sdk import CB_FACE_VALUE_NT, CbInfoWrapper
from data_sdk.wrappers.cb_info_wrapper import CB_DAILY_OVERVIEW_COLUMNS, CB_INFO_COLUMNS


def test_cb_face_value_is_the_owners_frozen_constant():
    assert CB_FACE_VALUE_NT == 100


def _info_rows():
    return pd.DataFrame(
        [
            {"cb_id": "95881", "cb_name": "大成鋼一", "InitialDateOfConversion": "2024-01-01",
             "DueDateOfConversion": "2027-12-31", "IssuanceAmount": 500_000_000},
            {"cb_id": "61401", "cb_name": "大陸一", "InitialDateOfConversion": "2023-06-01",
             "DueDateOfConversion": "2026-05-31", "IssuanceAmount": 300_000_000},
        ]
    )


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_CB_CACHE_PATH", str(tmp_path))
    return tmp_path


def test_get_cb_info_keeps_the_documented_columns(cache_dir):
    wrapper = CbInfoWrapper(fetch_info=_info_rows)
    df = wrapper.get_cb_info()
    assert list(df.columns) == CB_INFO_COLUMNS
    assert df["cb_id"].tolist() == ["95881", "61401"]


def test_get_cb_info_caches_to_disk(cache_dir):
    wrapper = CbInfoWrapper(fetch_info=_info_rows)
    wrapper.get_cb_info()
    assert (cache_dir / "cb_info.parquet").is_file()

    def refuse():
        raise AssertionError("a warm cache must not re-fetch")

    warm = CbInfoWrapper(fetch_info=refuse).get_cb_info()
    assert len(warm) == 2


def test_get_cb_info_refresh_forces_a_cold_fetch(cache_dir):
    calls = {"n": 0}

    def fetch():
        calls["n"] += 1
        return _info_rows()

    wrapper = CbInfoWrapper(fetch_info=fetch)
    wrapper.get_cb_info()
    wrapper.get_cb_info()
    assert calls["n"] == 1  # the in-memory cache serves the second call
    wrapper.get_cb_info(refresh=True)
    assert calls["n"] == 2


def _overview_rows():
    return pd.DataFrame(
        [
            {"cb_id": "95881", "cb_name": "大成鋼一", "date": "2026-09-14",
             "ConversionPrice": 32.5, "InitialDateOfStopConversion": None,
             "DueDateOfStopConversion": None, "OutstandingAmount": 400_000_000,
             "PriceOfUnderlyingStock": 28.1},
            {"cb_id": "95881", "cb_name": "大成鋼一", "date": "2026-09-15",
             "ConversionPrice": 32.5, "InitialDateOfStopConversion": "2026-09-16",
             "DueDateOfStopConversion": "2026-09-20", "OutstandingAmount": 400_000_000,
             "PriceOfUnderlyingStock": 28.6},
            # a row dated "today" or later must never be picked as the latest
            {"cb_id": "95881", "cb_name": "大成鋼一", "date": "2026-09-16",
             "ConversionPrice": 32.5, "InitialDateOfStopConversion": "2026-09-16",
             "DueDateOfStopConversion": "2026-09-20", "OutstandingAmount": 999,
             "PriceOfUnderlyingStock": 99.9},
            {"cb_id": "61401", "cb_name": "大陸一", "date": "2026-09-15",
             "ConversionPrice": 11.0, "InitialDateOfStopConversion": None,
             "DueDateOfStopConversion": None, "OutstandingAmount": 200_000_000,
             "PriceOfUnderlyingStock": 9.8},
        ]
    )


def test_get_cb_daily_overview_takes_the_latest_row_strictly_before_today(cache_dir):
    wrapper = CbInfoWrapper(fetch_daily_overview=_overview_rows)
    df = wrapper.get_cb_daily_overview(day="2026-09-16").set_index("cb_id")

    assert list(df.reset_index().columns) == CB_DAILY_OVERVIEW_COLUMNS
    assert df.loc["95881", "date"] == pd.Timestamp("2026-09-15")
    assert df.loc["95881", "InitialDateOfStopConversion"] == "2026-09-16"
    assert df.loc["95881", "OutstandingAmount"] == 400_000_000
    assert df.loc["61401", "date"] == pd.Timestamp("2026-09-15")


def test_get_cb_daily_overview_caches_per_day(cache_dir):
    wrapper = CbInfoWrapper(fetch_daily_overview=_overview_rows)
    wrapper.get_cb_daily_overview(day="2026-09-16")
    assert (cache_dir / "cb_daily_overview" / "2026-09-16.parquet").is_file()

    def refuse():
        raise AssertionError("a warm day must not re-fetch")

    warm = CbInfoWrapper(fetch_daily_overview=refuse).get_cb_daily_overview(day="2026-09-16")
    assert len(warm) == 2


def test_get_cb_daily_overview_raises_without_any_row_before_the_day(cache_dir):
    wrapper = CbInfoWrapper(fetch_daily_overview=_overview_rows)
    with pytest.raises(RuntimeError, match="no row dated before"):
        wrapper.get_cb_daily_overview(day="2020-01-01")


# ---------------------------------------------------------------------
# get_cb_underlying_map: the three outcomes (candidate, override, unresolved)
# ---------------------------------------------------------------------

@pytest.fixture
def overrides_file(tmp_path):
    path = tmp_path / "overrides.json"
    path.write_text(
        json.dumps({"_readme": "ignore me, not a cb_id", "99901": "2454"}, ensure_ascii=False),
        encoding="utf-8",
    )
    return str(path)


def test_underlying_map_three_outcomes(cache_dir, overrides_file):
    wrapper = CbInfoWrapper(get_stock_codes=lambda: {"2330", "2317"})

    df = wrapper.get_cb_underlying_map(
        cb_ids=["23301", "99901", "77777"], overrides_path=overrides_file
    ).set_index("cb_id")

    # "2330" is a real stock code: the candidate (first 4 chars) resolves.
    assert df.loc["23301", "underlying_stock_id"] == "2330"
    assert df.loc["23301", "source"] == "candidate"
    assert not df.loc["23301", "underlying_unresolved"]

    # "9990" is not a real stock code, but the override table has "99901".
    assert df.loc["99901", "underlying_stock_id"] == "2454"
    assert df.loc["99901", "source"] == "override"
    assert not df.loc["99901", "underlying_unresolved"]

    # "7777" is not a real stock code and has no override entry.
    assert df.loc["77777", "underlying_stock_id"] is None
    assert df.loc["77777", "source"] is None
    assert df.loc["77777", "underlying_unresolved"]


def test_underlying_map_ignores_the_readme_key_as_a_cb_id(cache_dir, overrides_file):
    wrapper = CbInfoWrapper(get_stock_codes=lambda: set())
    df = wrapper.get_cb_underlying_map(cb_ids=["_readme"], overrides_path=overrides_file)
    # "_readme" is a metadata key in the overrides file, not a real cb_id --
    # it must resolve as unresolved, never as a false "override" hit.
    assert df.iloc[0]["underlying_unresolved"]


def test_underlying_map_defaults_to_every_cb_in_get_cb_info(cache_dir, overrides_file):
    wrapper = CbInfoWrapper(fetch_info=_info_rows, get_stock_codes=lambda: {"9588", "6140"})
    df = wrapper.get_cb_underlying_map(overrides_path=overrides_file)
    assert set(df["cb_id"]) == {"95881", "61401"}
    assert set(df["underlying_stock_id"]) == {"9588", "6140"}


def test_the_default_overrides_file_exists_and_is_valid_json():
    from data_sdk.wrappers.cb_info_wrapper import _default_overrides_path, _load_overrides

    path = _default_overrides_path()
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    assert isinstance(raw, dict)
    # every real entry (not a documentation key) maps to a string
    overrides = _load_overrides(path)
    assert all(isinstance(v, str) for v in overrides.values())
