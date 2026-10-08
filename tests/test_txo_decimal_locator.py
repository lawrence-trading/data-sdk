"""The frozen TXO locator table and its calibration on synthetic data."""

import pandas as pd
import polars as pl
import pytest

from data_sdk import (
    TxoLocatorEntry,
    get_txo_decimal_locator,
    price_raw_to_1e4,
    taifex_option_prod_id,
    txo_decimal_locator_calibration_day,
    txo_decimal_locator_version,
)
from data_sdk.wrappers import txo_decimal_locator as loc


def test_table_is_versioned_and_cites_the_query():
    version = txo_decimal_locator_version()
    day = txo_decimal_locator_calibration_day()
    assert version.startswith(day + ".")
    assert len(day) == 8 and day.isdigit()
    assert "DailyMarketReportOpt" in loc.TXO_DECIMAL_LOCATOR_QUERY
    assert "round(log10(Close / max(price_raw)))" in loc.TXO_DECIMAL_LOCATOR_QUERY
    assert "gcd" in loc.TXO_DECIMAL_LOCATOR_QUERY


def test_frozen_entry_was_calibrated_not_guessed():
    entry = get_txo_decimal_locator("TXO")
    assert isinstance(entry, TxoLocatorEntry)
    assert entry == get_txo_decimal_locator("TXO24000J6")
    assert entry.locator == 3
    assert entry.tick_1e4 == price_raw_to_1e4(entry.tick_raw, entry.locator)
    # Calibrated against a real archived day with hundreds of traded
    # contracts (not a handful) and thousands of observed price changes.
    assert entry.n_products > 100
    assert entry.n_steps > 100


def test_unknown_root_has_no_entry():
    assert get_txo_decimal_locator("ZZZ") is None
    assert get_txo_decimal_locator("QQ9X6") is None


def test_prod_id_mapping_call_and_put_month_codes():
    assert taifex_option_prod_id("TXO", "202610", "24000", "買權") == "TXO24000J6"
    assert taifex_option_prod_id("TXO", "202610", "24000", "賣權") == "TXO24000V6"
    assert taifex_option_prod_id("TXO", "202701", "18500", "買權") == "TXO18500A7"
    assert taifex_option_prod_id("TXO", "202701", "18500", "賣權") == "TXO18500M7"


def test_prod_id_mapping_excludes_weeklies_and_spreads():
    assert taifex_option_prod_id("TXO", "202610W1", "24000", "買權") is None
    assert taifex_option_prod_id("TXO", "202610F2", "24000", "買權") is None
    assert taifex_option_prod_id("TXO", "202610/202611", "24000", "買權") is None


def test_prod_id_mapping_handles_a_decimal_strike_string():
    # Real TXO strikes are integer-point, but the report serves strings; a
    # defensive float() parse must not crash on an unexpected decimal.
    assert taifex_option_prod_id("TXO", "202610", "24000.0", "買權") == "TXO24000J6"


def _i024(rows):
    return pl.DataFrame(
        rows,
        schema={"prod_id": pl.String, "prod_msg_seq": pl.Int64, "match_index": pl.Int32,
                "calculated_flag": pl.String, "price_raw": pl.Int64},
        orient="row",
    ).lazy()


def _report(rows):
    return pd.DataFrame(
        rows, columns=["Contract", "ContractMonth(Week)", "StrikePrice", "CallPut", "Close"]
    )


def test_calibration_reproduces_locator_and_tick():
    i024 = _i024([
        ("TXO24000J6", 1, 0, "0", 100), ("TXO24000J6", 2, 0, "0", 150),
        ("TXO24000J6", 3, 0, "0", 200), ("TXO24000J6", 4, 0, "0", 200),
        ("TXO23000V6", 1, 0, "0", 300), ("TXO23000V6", 2, 0, "0", 350),
        # a trial match that must not count
        ("TXO24000J6", 0, 0, "1", 9999),
        # a different root entirely: must never join into the TXO calibration
        ("DHO02600J6", 1, 0, "0", 5000),
    ])
    report = _report([
        ("TXO", "202610", "24000", "買權", "0.2"),
        ("TXO", "202610", "23000", "賣權", "0.35"),
        # a weekly contract: no prod_id, must be dropped before the join
        ("TXO", "202610W1", "24500", "買權", "0.1"),
        # a not-traded contract: no i024 rows, so the inner join drops it
        ("TXO", "202610", "25000", "買權", "0.1"),
        ("DHO", "202610", "26", "買權", "0.5"),
    ])

    out = loc.calibrate_locator(report, i024)
    by_id = {row["prod_id"]: row for row in out.to_dicts()}

    assert set(by_id) == {"TXO24000J6", "TXO23000V6"}
    assert by_id["TXO24000J6"]["locator"] == 3
    assert by_id["TXO24000J6"]["max_price_raw"] == 200  # the trial print is excluded
    assert by_id["TXO24000J6"]["tick_raw"] == 50
    assert by_id["TXO23000V6"]["locator"] == 3
    assert by_id["TXO23000V6"]["tick_raw"] == 50

    bad = loc.disagreements(out)
    assert bad.height == 0

    table = loc.freeze_table(out)
    assert table["TXO"] == ("TXO", 3, 50, 500, 2, 3)


def test_freeze_is_empty_when_contracts_disagree_on_locator():
    out = loc.calibrate_locator(
        _report([("TXO", "202610", "24000", "買權", "20"), ("TXO", "202611", "24000", "買權", "2000")]),
        _i024([("TXO24000J6", 1, 0, "0", 10000), ("TXO24000K6", 1, 0, "0", 10000)]),
    )
    assert loc.freeze_table(out) == {}


def test_rendered_module_round_trips():
    table = {"TXO": ("TXO", 3, 100, 1000, 413, 2478)}
    source = loc.render_table_module(table, "20261007.1", "20261007")
    namespace = {}
    exec(source, namespace)
    assert namespace["TXO_DECIMAL_LOCATOR_TABLE"] == table
    assert namespace["TXO_DECIMAL_LOCATOR_VERSION"] == "20261007.1"
    assert namespace["TXO_DECIMAL_LOCATOR_CALIBRATION_DAY"] == "20261007"
