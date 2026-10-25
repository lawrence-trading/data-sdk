"""Offline tests for get_order_book_stocks(mode=...) (no network, no NFS)."""
import datetime as dt

import pandas as pd
import polars as pl
import pytest

from data_sdk.wrappers.order_book_wrapper import get_order_book_stocks

DAY = "2026-06-30"
NAME = "twseudp_20260630_0825_all.parquet"

COLUMNS = (
    [
        ("timestamp", pl.Datetime("us")), ("message_length", pl.Int32), ("business_type", pl.Int32),
        ("format_code", pl.Int32), ("format_version", pl.Int32), ("transmission_number", pl.Int32),
        ("stock_code", pl.Utf8), ("match_time", pl.Int64), ("display_item", pl.Int32),
        ("limit_up_limit_down", pl.Int32), ("status_note", pl.Int32), ("cumulative_volume", pl.Int64),
        ("checksum", pl.Int32), ("terminal_code", pl.Int32), ("deal_price", pl.Float64), ("deal_volume", pl.Int32),
    ]
    + [(f"bid_price_{k}", pl.Float64) for k in range(1, 6)]
    + [(f"bid_volume_{k}", pl.Int32) for k in range(1, 6)]
    + [(f"ask_price_{k}", pl.Float64) for k in range(1, 6)]
    + [(f"ask_volume_{k}", pl.Int32) for k in range(1, 6)]
)
CAPTURED = dt.datetime(2026, 6, 30, 1, 0, 0)


def _write(path, tns, deal_price, timestamp, cumulative_dtype=pl.Int64):
    """One day file: odd tn are 2330, even tn are 0050; match_time is 09:00:00 + tn seconds."""
    n = len(tns)
    data = {}
    for name, dtype in COLUMNS:
        if name == "transmission_number":
            values = tns
        elif name == "stock_code":
            values = ["2330" if t % 2 else "0050" for t in tns]
        elif name == "match_time":
            values = [90_000_000_000 + t * 1_000_000 for t in tns]
        elif name == "deal_price":
            values = [deal_price] * n
        elif name == "timestamp":
            values = [timestamp] * n
        elif dtype == pl.Float64:
            values = [1.0] * n
        else:
            values = [1] * n
        data[name] = pl.Series(name, values, dtype=cumulative_dtype if name == "cumulative_volume" else dtype)
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(data).write_parquet(path)


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    native, tej = tmp_path / "native", tmp_path / "tej"
    native.mkdir()
    tej.mkdir()
    monkeypatch.setenv("DATA_SDK_ORDER_BOOK_PARQUET_PATH", str(native))
    monkeypatch.setenv("DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH", str(tej))
    return native, tej


def _read(mode, **kwargs):
    kwargs.setdefault("format_match_time", False)
    return get_order_book_stocks(DAY, is_twse=True, lazy=True, mode=mode, **kwargs).collect()


def test_native_and_tej_modes_read_their_own_directory(dirs):
    native, tej = dirs
    _write(native / NAME, [1, 2, 4], deal_price=10.0, timestamp=CAPTURED, cumulative_dtype=pl.Int32)
    _write(tej / NAME, [1, 2, 3, 4, 5], deal_price=20.0, timestamp=None)
    assert _read("native").equals(pl.read_parquet(native / NAME))
    assert _read("tej").equals(pl.read_parquet(tej / NAME))


def test_merged_returns_tej_when_both_files_exist(dirs):
    native, tej = dirs
    _write(native / NAME, [1, 2, 4, 6], deal_price=10.0, timestamp=CAPTURED)
    _write(tej / NAME, [1, 2, 3, 4, 5], deal_price=20.0, timestamp=None)
    assert _read("merged").equals(pl.read_parquet(tej / NAME))


def test_merged_returns_the_only_file_present(dirs):
    native, tej = dirs
    _write(tej / NAME, [1, 2, 3], deal_price=20.0, timestamp=None)
    assert _read("merged").equals(pl.read_parquet(tej / NAME))
    (tej / NAME).unlink()
    _write(native / NAME, [1, 2], deal_price=10.0, timestamp=CAPTURED)
    assert _read("merged").equals(pl.read_parquet(native / NAME))


def test_merged_is_the_default_mode(dirs):
    native, tej = dirs
    _write(native / NAME, [1, 6], deal_price=10.0, timestamp=CAPTURED)
    _write(tej / NAME, [1, 2], deal_price=20.0, timestamp=None)
    lf = get_order_book_stocks(DAY, is_twse=True, lazy=True, format_match_time=False)
    assert lf.collect()["transmission_number"].to_list() == [1, 2]


@pytest.mark.parametrize("mode", ["native", "tej"])
def test_missing_day_raises_file_not_found(dirs, mode):
    lf = get_order_book_stocks(DAY, is_twse=True, lazy=True, mode=mode)  # lazy: raised at collect
    with pytest.raises(FileNotFoundError):
        lf.collect()
    with pytest.raises(FileNotFoundError):
        get_order_book_stocks(DAY, is_twse=True, mode=mode)


def test_merged_missing_day_raises_at_the_call_even_when_lazy(dirs):
    with pytest.raises(FileNotFoundError, match=NAME):
        get_order_book_stocks(DAY, is_twse=True, lazy=True, mode="merged")


@pytest.mark.parametrize("mode", ["native", "tej", "merged"])
def test_sid_and_match_time_format_apply_in_every_mode(dirs, mode):
    native, tej = dirs
    _write(native / NAME, [1, 2, 4, 7], deal_price=10.0, timestamp=CAPTURED)
    _write(tej / NAME, [1, 2, 3, 4, 5], deal_price=20.0, timestamp=None)
    out = get_order_book_stocks(DAY, is_twse=True, sid="2330", mode=mode)
    assert isinstance(out, pd.DataFrame)
    assert set(out["stock_code"]) == {"2330"}
    assert out["match_time"].iloc[0] == "09:00:01.000000"
    expected = {"native": [1, 7], "tej": [1, 3, 5], "merged": [1, 3, 5]}[mode]
    assert out["transmission_number"].tolist() == expected


def test_merged_without_tej_path_set_warns_and_falls_back_to_native(dirs, tmp_path, monkeypatch, capsys):
    native, tej = dirs
    _write(native / NAME, [1, 2], deal_price=10.0, timestamp=CAPTURED)
    _write(tej / NAME, [1, 2, 3], deal_price=20.0, timestamp=None)
    monkeypatch.delenv("DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH")
    monkeypatch.chdir(tmp_path)  # no twseudp_* file at the top level, so "." has no TEJ day
    assert _read("merged").equals(pl.read_parquet(native / NAME))
    assert "DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH not set" in capsys.readouterr().err


def test_unknown_mode_is_rejected(dirs):
    with pytest.raises(ValueError):
        get_order_book_stocks(DAY, is_twse=True, mode="both")
