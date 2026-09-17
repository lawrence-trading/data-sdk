"""FinLabWrapper against synthetic tables: no finlab import, no token."""

import datetime as dt
import os
import sys

import pandas as pd
import pytest

from data_sdk import FinLabAlignmentError, FinLabWrapper
from data_sdk.wrappers.finlab_wrapper import (
    DATASETS,
    align_latest_rows,
    latest_before_today,
)

TODAY = dt.date(2026, 9, 17)


def _table(dates, **columns):
    index = pd.DatetimeIndex(pd.to_datetime(dates), name="date")
    return pd.DataFrame(columns, index=index)


def _aligned_tables():
    dates = ["2026-09-14", "2026-09-15", "2026-09-16"]
    return {
        "margin_transactions:融資今日餘額": _table(dates, **{"2330": [100, 110, 120], "2317": [5, 5, 6]}),
        "margin_transactions:融券今日餘額": _table(dates, **{"2330": [1, 2, 3], "2317": [0, 0, 0]}),
        "security_lending_sell:借券賣出餘額": _table(dates, **{"2330": [7, 8, 9], "2317": [1, 1, 1]}),
        "intraday_trading:得先賣後買當沖": _table(dates, **{"2330": ["Y", "Y", "Y"], "2317": ["N", "N", "Y"]}),
    }


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_FINLAB_CACHE_PATH", str(tmp_path))
    monkeypatch.delenv("FINLAB_API_TOKEN", raising=False)
    monkeypatch.setattr(FinLabWrapper, "_logged_in", False)
    return tmp_path


def test_latest_before_today_is_strictly_before():
    frame = _table(["2026-09-16", "2026-09-17"], **{"2330": [1, 2]})
    report_date, row = latest_before_today(frame, TODAY)
    assert report_date == dt.date(2026, 9, 16)
    assert row["2330"] == 1


def test_latest_before_today_raises_when_nothing_completed():
    frame = _table(["2026-09-17"], **{"2330": [1]})
    with pytest.raises(RuntimeError, match="no row dated before"):
        latest_before_today(frame, TODAY)


def test_align_latest_rows_raises_on_misaligned_report_dates():
    frames = {
        "a": _table(["2026-09-15", "2026-09-16"], **{"2330": [1, 2]}),
        "b": _table(["2026-09-15"], **{"2330": [1]}),
    }
    with pytest.raises(FinLabAlignmentError, match="a=2026-09-16, b=2026-09-15"):
        align_latest_rows(frames, TODAY)


def test_wrapper_raises_on_misaligned_tables(cache_dir):
    tables = _aligned_tables()
    # The lending table lags one day: FinLab published three, it published two.
    lagging = tables["security_lending_sell:借券賣出餘額"].iloc[:2]
    tables["security_lending_sell:借券賣出餘額"] = lagging
    wrapper = FinLabWrapper(fetch=lambda name: tables[name])

    with pytest.raises(FinLabAlignmentError, match="lending_balance=2026-09-15"):
        wrapper.get_ssf_reference(TODAY)


def test_wrapper_returns_last_completed_day_per_stock(cache_dir):
    tables = _aligned_tables()
    wrapper = FinLabWrapper(fetch=lambda name: tables[name])

    report_date, frame = wrapper.get_ssf_reference(TODAY)

    assert report_date == dt.date(2026, 9, 16)
    assert list(frame.columns) == list(DATASETS)
    assert frame.loc["2330", "margin_balance"] == 120
    assert frame.loc["2317", "short_balance"] == 0
    assert frame.loc["2330", "lending_balance"] == 9
    assert frame.loc["2317", "day_trade_sell_first"] == "Y"


def test_cached_day_is_served_without_a_fetch_or_token(cache_dir):
    tables = _aligned_tables()
    calls = []

    def fetch(name):
        calls.append(name)
        return tables[name]

    FinLabWrapper(fetch=fetch).get_ssf_reference(TODAY)
    assert len(calls) == len(DATASETS)

    def refuse(name):
        raise AssertionError("cache miss on a warm day")

    assert "FINLAB_API_TOKEN" not in os.environ
    report_date, frame = FinLabWrapper(fetch=refuse).get_ssf_reference(TODAY)
    assert report_date == dt.date(2026, 9, 16)
    assert frame.loc["2330", "margin_balance"] == 120


def test_cache_holds_only_rows_before_the_query_day(cache_dir):
    tables = _aligned_tables()
    wrapper = FinLabWrapper(fetch=lambda name: tables[name])
    table = wrapper.get_table("margin_transactions:融資今日餘額", dt.date(2026, 9, 16))
    assert table.index.max() == pd.Timestamp("2026-09-15")
    path = wrapper.cache_path("margin_transactions:融資今日餘額", dt.date(2026, 9, 16))
    assert os.path.isfile(path)
    assert os.path.basename(path) == "20260916.parquet"
    assert all(ord(ch) < 128 for ch in path)


def test_cold_fetch_without_token_names_the_variable(cache_dir):
    with pytest.raises(RuntimeError, match="FINLAB_API_TOKEN"):
        FinLabWrapper().get_table("margin_transactions:融資今日餘額", TODAY)
    assert "finlab" not in sys.modules
