"""FinLabWrapper against synthetic tables: no finlab import, no token."""

import datetime as dt
import os
import sys

import pandas as pd
import pytest

from data_sdk import FinLabAlignmentError, FinLabWrapper
from data_sdk.wrappers.finlab_wrapper import (
    DATASETS,
    DIVIDEND_ANNOUNCEMENT_DATASET,
    MATERIAL_ANNOUNCEMENT_DATASET,
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
        # FinLab publishes 借券賣出餘額 in shares.
        "security_lending_sell:借券賣出餘額": _table(dates, **{"2330": [7000, 8000, 9400], "2317": [1000, 1000, 600]}),
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
    assert frame.loc["2330", "lending_balance"] == 9      # 9400 shares -> 9 張
    assert frame.loc["2317", "lending_balance"] == 1      # 600 shares -> 1 張
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


class _GuardedIndexFrame(pd.DataFrame):
    """Stands in for finlab's FinlabDataFrame, which raises on ``df.index = ...``."""

    @property
    def _constructor(self):
        return _GuardedIndexFrame

    def __setattr__(self, name, value):
        if name == "index":
            raise AttributeError("不要直接覆寫 FinlabDataFrame 的 index！")
        super().__setattr__(name, value)


def test_a_frame_that_refuses_index_assignment_is_read_as_a_plain_frame(cache_dir):
    tables = {name: _GuardedIndexFrame(frame) for name, frame in _aligned_tables().items()}
    report_date, frame = FinLabWrapper(fetch=lambda name: tables[name]).get_ssf_reference(TODAY)
    assert report_date == dt.date(2026, 9, 16)
    assert frame.loc["2330", "lending_balance"] == 9


def test_cold_fetch_without_token_names_the_variable(cache_dir):
    with pytest.raises(RuntimeError, match="FINLAB_API_TOKEN"):
        FinLabWrapper().get_table("margin_transactions:融資今日餘額", TODAY)
    assert "finlab" not in sys.modules


def _announcement_rows():
    return pd.DataFrame(
        [
            {"stock_id": "9958", "date": "2026-06-25", "name": "世紀鋼",
             "title": "公告本公司除息基準日暨轉換公司債停止轉換期間", "info": "...",
             "key_date": "2026-07-01"},
            {"stock_id": "9958", "date": "2026-01-10", "name": "世紀鋼",
             "title": "公告本公司董事會決議", "info": "...", "key_date": "2026-01-11"},
            {"stock_id": "2330", "date": "2026-06-20", "name": "台積電",
             "title": "法人說明會", "info": "...", "key_date": "2026-06-21"},
        ]
    )


def test_material_announcements_calls_the_confirmed_dataset_id():
    seen = {}

    def fetch(dataset):
        seen["dataset"] = dataset
        return _announcement_rows()

    FinLabWrapper(fetch=fetch).get_material_announcements()
    assert seen["dataset"] == MATERIAL_ANNOUNCEMENT_DATASET == "important_info_announcement"


def test_material_announcements_shape_and_filters():
    wrapper = FinLabWrapper(fetch=lambda dataset: _announcement_rows())

    all_rows = wrapper.get_material_announcements()
    assert list(all_rows.columns) == ["stock_id", "announce_date", "category_raw"]
    assert len(all_rows) == 3

    by_stock = wrapper.get_material_announcements(stock_ids=["9958"])
    assert set(by_stock["stock_id"]) == {"9958"}
    assert len(by_stock) == 2

    recent = wrapper.get_material_announcements(stock_ids=["9958"], since="2026-03-01")
    assert len(recent) == 1
    assert recent.iloc[0]["category_raw"] == "公告本公司除息基準日暨轉換公司債停止轉換期間"
    assert recent.iloc[0]["announce_date"] == pd.Timestamp("2026-06-25")


def _dividend_rows():
    # Column names and NaT patterns match an already-fetched local copy
    # (~/finlab_db/dividend_announcement.feather) exactly: 2330 a plain cash
    # dividend (no stock leg), 2317 a combined 除權息 event (both legs, two
    # funding sources each), 1101 a stock-only 除權 event (zero cash).
    return pd.DataFrame(
        [
            {"stock_id": "2330", "公告日期": "2026-08-12", "公司名稱": "台積電", "除權交易日": pd.NaT, "除息交易日": "2026-10-20",
             "盈餘分配之股東現金股利(元/股)": 3.0, "法定盈餘公積、資本公積發放之現金(元/股)": 0.0,
             "盈餘轉增資配股(元/股)": 0.0, "法定盈餘公積、資本公積轉增資配股(元/股)": 0.0},
            {"stock_id": "2317", "公告日期": "2026-08-14", "公司名稱": "鴻海", "除權交易日": "2026-10-21", "除息交易日": "2026-10-21",
             "盈餘分配之股東現金股利(元/股)": 2.0, "法定盈餘公積、資本公積發放之現金(元/股)": 0.5,
             "盈餘轉增資配股(元/股)": 0.1, "法定盈餘公積、資本公積轉增資配股(元/股)": 0.0},
            {"stock_id": "1101", "公告日期": "2026-07-01", "公司名稱": "台泥", "除權交易日": "2026-09-01", "除息交易日": pd.NaT,
             "盈餘分配之股東現金股利(元/股)": 0.0, "法定盈餘公積、資本公積發放之現金(元/股)": 0.0,
             "盈餘轉增資配股(元/股)": 0.15, "法定盈餘公積、資本公積轉增資配股(元/股)": 0.0},
        ]
    )


def test_dividend_announcements_calls_the_confirmed_dataset_id():
    seen = {}

    def fetch(dataset):
        seen["dataset"] = dataset
        return _dividend_rows()

    FinLabWrapper(fetch=fetch).get_dividend_announcements()
    assert seen["dataset"] == DIVIDEND_ANNOUNCEMENT_DATASET == "dividend_announcement"


def test_dividend_announcements_shape_and_sums():
    wrapper = FinLabWrapper(fetch=lambda dataset: _dividend_rows())
    out = wrapper.get_dividend_announcements()

    assert list(out.columns) == [
        "stock_id", "company_name", "ex_rights_date", "ex_dividend_date",
        "cash_dividend_per_share", "stock_dividend_per_share", "announce_date",
    ]
    assert len(out) == 3

    tsmc = out[out["stock_id"] == "2330"].iloc[0]
    assert pd.isna(tsmc["ex_rights_date"])
    assert tsmc["ex_dividend_date"] == pd.Timestamp("2026-10-20")
    assert tsmc["cash_dividend_per_share"] == 3.0
    assert tsmc["stock_dividend_per_share"] == 0.0
    assert tsmc["announce_date"] == pd.Timestamp("2026-08-12")

    foxconn = out[out["stock_id"] == "2317"].iloc[0]
    assert foxconn["cash_dividend_per_share"] == 2.5  # two cash columns summed
    assert foxconn["stock_dividend_per_share"] == 0.1

    tcc = out[out["stock_id"] == "1101"].iloc[0]
    assert pd.isna(tcc["ex_dividend_date"])
    assert tcc["cash_dividend_per_share"] == 0.0
    assert tcc["stock_dividend_per_share"] == 0.15


def test_dividend_announcements_keeps_every_announcement_of_a_corrected_event():
    """A correction is a second row for the same stock and ex-date with a
    later announce_date; the SDK returns both so a consumer can pick the
    latest instead of guessing which one the table kept."""
    rows = _dividend_rows()
    rows.loc[len(rows)] = {
        "stock_id": "2330", "公告日期": "2026-09-02", "公司名稱": "台積電", "除權交易日": pd.NaT,
        "除息交易日": "2026-10-20", "盈餘分配之股東現金股利(元/股)": 3.5,
        "法定盈餘公積、資本公積發放之現金(元/股)": 0.0, "盈餘轉增資配股(元/股)": 0.0,
        "法定盈餘公積、資本公積轉增資配股(元/股)": 0.0,
    }
    out = FinLabWrapper(fetch=lambda dataset: rows).get_dividend_announcements()

    tsmc = out[out["stock_id"] == "2330"].sort_values("announce_date")
    assert list(tsmc["announce_date"]) == [pd.Timestamp("2026-08-12"), pd.Timestamp("2026-09-02")]
    assert list(tsmc["cash_dividend_per_share"]) == [3.0, 3.5]


def test_dividend_announcements_since_filters_on_either_date():
    wrapper = FinLabWrapper(fetch=lambda dataset: _dividend_rows())

    upcoming = wrapper.get_dividend_announcements(since="2026-10-01")
    assert set(upcoming["stock_id"]) == {"2330", "2317"}  # 1101's 2026-09-01 excluded
