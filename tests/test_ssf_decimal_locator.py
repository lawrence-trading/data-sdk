"""The frozen locator table and the §8.1 calibration on synthetic data."""

import pandas as pd
import polars as pl
import pytest

from data_sdk import (
    LocatorEntry,
    get_ssf_decimal_locator,
    price_raw_to_1e4,
    ssf_decimal_locator_calibration_day,
    ssf_decimal_locator_version,
)
from data_sdk.wrappers import ssf_decimal_locator as loc


def test_table_is_versioned_and_cites_the_query():
    version = ssf_decimal_locator_version()
    day = ssf_decimal_locator_calibration_day()
    assert version.startswith(day + ".")
    assert len(day) == 8 and day.isdigit()
    assert "DailyMarketReportFut" in loc.SSF_DECIMAL_LOCATOR_QUERY
    assert "round(log10(Last / max(price_raw)))" in loc.SSF_DECIMAL_LOCATOR_QUERY
    assert "gcd" in loc.SSF_DECIMAL_LOCATOR_QUERY
    assert len(loc.SSF_DECIMAL_LOCATOR_TABLE) > 300


def test_lookup_by_prod_id_or_root():
    entry = get_ssf_decimal_locator("CDFI6")
    assert isinstance(entry, LocatorEntry)
    assert entry == get_ssf_decimal_locator("CDF")
    assert entry.locator == 2
    assert entry.tick_1e4 == price_raw_to_1e4(entry.tick_raw, entry.locator)
    assert get_ssf_decimal_locator("GDF").locator == 3


def test_unknown_product_has_no_entry():
    assert get_ssf_decimal_locator("ZZZI6") is None
    assert get_ssf_decimal_locator("QQ9") is None


def test_integer_rescaling():
    assert price_raw_to_1e4(239000, 2) == 2390_0000
    assert price_raw_to_1e4(4226500, 3) == 4226_5000
    assert price_raw_to_1e4(15905, 0) == 15905_0000
    with pytest.raises(ValueError):
        price_raw_to_1e4(1, 5)


def test_twse_ladder_bands():
    assert loc.twse_tick_1e4(loc.decimal_to_1e4("9.99")) == 100
    assert loc.twse_tick_1e4(loc.decimal_to_1e4("10")) == 500
    assert loc.twse_tick_1e4(loc.decimal_to_1e4("99.9")) == 1000
    assert loc.twse_tick_1e4(loc.decimal_to_1e4("499.5")) == 5000
    assert loc.twse_tick_1e4(loc.decimal_to_1e4("999")) == 10000
    assert loc.twse_tick_1e4(loc.decimal_to_1e4("2380")) == 50000


def _i024(rows):
    return pl.DataFrame(
        rows,
        schema={"prod_id": pl.String, "prod_msg_seq": pl.Int64, "match_index": pl.Int32,
                "calculated_flag": pl.String, "price_raw": pl.Int64},
        orient="row",
    ).lazy()


def _report(rows):
    return pd.DataFrame(rows, columns=["prod_id", "Contract", "ContractMonth(Week)", "Last"])


def test_calibration_reproduces_locator_and_tick_and_flags_disagreement():
    i024 = _i024([
        # CDF at 2380 stepping by 1.00 (raw 100) under locator 2: off the ladder (5.00)
        ("CDFI6", 1, 0, "0", 238000), ("CDFI6", 2, 0, "0", 238100),
        ("CDFI6", 3, 0, "0", 238300), ("CDFI6", 4, 0, "0", 238300),
        # GDF around 4226.5 with locator 3, stepping by 0.1 (raw 100)
        ("GDFI6", 1, 0, "0", 4226500), ("GDFI6", 2, 0, "0", 4226600),
        # a 60-dollar SSF stepping by 0.10 (raw 10): on the ladder
        ("ABCI6", 1, 0, "0", 6000), ("ABCI6", 2, 0, "0", 6010), ("ABCI6", 3, 0, "0", 6030),
        # a trial match that must not count, and a spread that must be dropped
        ("ABCI6", 0, 0, "1", 9999), ("ABCI6/J6", 1, 0, "0", 30),
        # a product that printed one price: tick unknown
        ("ONEI6", 1, 0, "0", 1000),
    ])
    report = _report([
        ("CDFI6", "CDF", "202609", "2383"),
        ("GDFI6", "GDF", "202609", "4226.6"),
        ("ABCI6", "ABC", "202609", "60.3"),
        ("ONEI6", "ONE", "202609", "10"),
        ("NOTI6", "NOT", "202609", "1"),  # not traded: no i024 row, no output row
        (None, "CDF", "202609/202610", "21"),
    ])

    out = loc.calibrate_locator(report, i024)
    by_id = {row["prod_id"]: row for row in out.to_dicts()}

    assert set(by_id) == {"CDFI6", "GDFI6", "ABCI6", "ONEI6"}
    assert by_id["CDFI6"]["locator"] == 2
    assert by_id["CDFI6"]["tick_raw"] == 100
    assert by_id["CDFI6"]["n_steps"] == 2
    assert by_id["CDFI6"]["ladder_tick_raw"] == 500
    assert by_id["CDFI6"]["agrees"] is False
    assert by_id["GDFI6"]["locator"] == 3
    assert by_id["GDFI6"]["tick_raw"] == 100
    assert by_id["ABCI6"]["locator"] == 2
    assert by_id["ABCI6"]["tick_raw"] == 10
    assert by_id["ABCI6"]["agrees"] is True
    assert by_id["ABCI6"]["max_price_raw"] == 6030  # the trial print is excluded
    assert by_id["ONEI6"]["tick_raw"] == 0
    assert by_id["ONEI6"]["agrees"] is None

    bad = loc.disagreements(out)["prod_id"].to_list()
    assert bad == ["CDFI6", "GDFI6"]

    table = loc.freeze_table(out)
    assert table["CDF"] == ("CDF", 2, 100, 10000, 1, 2, False)
    assert table["GDF"][1] == 3
    assert table["ONE"][2] is None and table["ONE"][6] is None


def test_freeze_drops_a_root_whose_months_disagree_on_locator():
    out = loc.calibrate_locator(
        _report([("XXFI6", "XXF", "202609", "100"), ("XXFJ6", "XXF", "202610", "100")]),
        _i024([("XXFI6", 1, 0, "0", 10000), ("XXFJ6", 1, 0, "0", 1000)]),
    )
    assert loc.freeze_table(out) == {}


def test_rendered_module_round_trips():
    table = {"CDF": ("CDF", 2, 100, 10000, 5, 767, False)}
    source = loc.render_table_module(table, "20260916.1", "20260916")
    namespace = {}
    exec(source, namespace)
    assert namespace["SSF_DECIMAL_LOCATOR_TABLE"] == table
    assert namespace["SSF_DECIMAL_LOCATOR_VERSION"] == "20260916.1"
    assert namespace["SSF_DECIMAL_LOCATOR_CALIBRATION_DAY"] == "20260916"
