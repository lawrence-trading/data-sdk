"""Offline tests for FinMindWrapper's read surface (no network, no NFS)."""
import os

import pandas as pd
import pytest

from data_sdk.wrappers.finmind_broker_wrapper import FinMindWrapper


def _rows(day, codes, buy_dtype="int32"):
    frame = pd.DataFrame({
        "securities_trader": ["元大"] * len(codes),
        "price": [1.0] * len(codes),
        "buy": pd.Series([1000] * len(codes), dtype=buy_dtype),
        "sell": pd.Series([0] * len(codes), dtype=buy_dtype),
        "securities_trader_id": ["9800"] * len(codes),
        "stock_id": codes,
        "date": [day] * len(codes),
    })
    return frame


@pytest.fixture
def archive(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_FINMIND_BROKER_PATH", str(tmp_path / "stocks"))
    monkeypatch.setenv("DATA_SDK_FINMIND_BROKER_WARRANT_PATH", str(tmp_path / "warrants"))
    return tmp_path


def test_stock_and_warrant_archives_come_from_their_own_env_vars(archive):
    stock_path = FinMindWrapper.broker_day_path("2026-08-06")
    warrant_path = FinMindWrapper.broker_day_path("2026-08-06", warrant=True)
    assert stock_path == str(archive / "stocks" / "2026-08-06.parquet")
    assert warrant_path == str(archive / "warrants" / "2026-08-06.parquet")


def test_get_broker_filters_by_code_and_branch(archive):
    wrapper = FinMindWrapper.__new__(FinMindWrapper)   # no DataLoader login needed
    day = "2026-08-06"
    frame = _rows(day, ["030001", "700448"])
    frame.loc[1, "securities_trader_id"] = "9227"
    path = FinMindWrapper.broker_day_path(day, warrant=True)
    os.makedirs(os.path.dirname(path))
    frame.to_parquet(path, index=False)

    assert wrapper.archived_stock_ids(day, warrant=True) == {"030001", "700448"}
    by_code = wrapper.get_broker(day, sid="700448", warrant=True)
    assert by_code["securities_trader_id"].tolist() == ["9227"]
    by_branch = wrapper.get_broker(day, trader_id="9800", warrant=True)
    assert by_branch["stock_id"].tolist() == ["030001"]
    assert len(wrapper.get_broker(day, warrant=True)) == 2
    with pytest.raises(FileNotFoundError):
        wrapper.get_broker(day, sid="700448")   # the stock archive has no such day
