import os
import stat

import pandas as pd
import pytest

from data_sdk.wrappers import shioaji_wrapper as sw
from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper


class _Api:
    def __init__(self):
        self.calls = 0
        self.Contracts = type("C", (), {"Stocks": {"2330": object()}, "Futures": {"TXF": object()}})()

    def ticks(self, contract, date):
        self.calls += 1
        return {"ts": [1, 2], "close": [10.0, 11.0]}

    def usage(self):
        return ""


@pytest.fixture
def wrapper(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_SHIOAJI_TICKS_PATH", str(tmp_path / "ticks"))
    monkeypatch.setenv("DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH", str(tmp_path / "futures"))
    api = _Api()
    monkeypatch.setattr(ShioajiWrapper, "_api", api)
    monkeypatch.setattr(ShioajiWrapper, "_ensure_contracts_fetched", lambda self: None)
    w = ShioajiWrapper.__new__(ShioajiWrapper)
    return w, api, tmp_path


@pytest.mark.parametrize("kind", ["ticks", "futures"])
def test_miss_publishes_a_0666_file_and_no_tmp(wrapper, kind):
    w, api, tmp_path = wrapper
    if kind == "ticks":
        w.get_order_book("2026-01-05", "2330")
        path = tmp_path / "ticks" / "2330_2026-01-05.parquet"
    else:
        w.get_futures_ticks("2026-01-05", "TXF")
        path = tmp_path / "futures" / "TXF_2026-01-05.parquet"
    assert stat.S_IMODE(path.stat().st_mode) == 0o666
    assert [p.name for p in path.parent.iterdir()] == [path.name]
    assert len(pd.read_parquet(path)) == 2
