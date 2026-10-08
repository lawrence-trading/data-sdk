"""get_twii_weights against a fake DataLoader: no network, no token."""

import types

import pandas as pd
import pytest

from data_sdk.wrappers.finmind_broker_wrapper import FinMindWrapper


def _fake_weight_frame():
    return pd.DataFrame(
        [
            {"rank": 1, "stock_id": "2330", "stock_name": "台積電", "weight_per": 32.5,
             "date": "2026-09-16", "type": "twse"},
            {"rank": 2, "stock_id": "2317", "stock_name": "鴻海", "weight_per": 4.1,
             "date": "2026-09-16", "type": "twse"},
            {"rank": 1, "stock_id": "5380", "stock_name": "台塑化上櫃", "weight_per": 1.2,
             "date": "2026-09-16", "type": "tpex"},
        ]
    )


class _NullLimiter:
    def acquire(self, n):
        pass


@pytest.fixture
def wrapper(monkeypatch):
    """A wrapper whose shared client and rate limiter are stand-ins, so
    construction and a cold fetch never touch the network or a token."""
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_market_value_weight=lambda start_date, end_date: _fake_weight_frame()
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))
    return FinMindWrapper()


def test_cold_fetch_keeps_only_twse_rows(wrapper, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    frame = wrapper.get_twii_weights(day="2026-09-16")

    assert frame.index.name == "stock_id"
    assert sorted(frame.index) == ["2317", "2330"]
    assert list(frame.columns) == ["weight_per", "date"]
    assert frame.loc["2330", "weight_per"] == 32.5


def test_cold_fetch_writes_the_cache_file(wrapper, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    wrapper.get_twii_weights(day="2026-09-16")

    path = tmp_path / "2026-09-16.parquet"
    assert path.is_file() and path.stat().st_size > 0


def test_a_cached_day_is_served_without_touching_the_fake_api(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)

    def refuse(start_date, end_date):
        raise AssertionError("the network must not be touched on a cached day")

    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_market_value_weight=refuse
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))
    cached = pd.DataFrame({"weight_per": [10.0]}, index=pd.Index(["2454"], name="stock_id"))
    cached["date"] = "2026-09-16"
    cached.to_parquet(tmp_path / "2026-09-16.parquet")

    frame = FinMindWrapper().get_twii_weights(day="2026-09-16")
    assert frame.loc["2454", "weight_per"] == 10.0


def test_default_day_is_todays_taipei_date(wrapper, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    monkeypatch.setattr(FinMindWrapper, "_taipei_today_str", staticmethod(lambda: "2026-09-16"))

    wrapper.get_twii_weights()

    assert (tmp_path / "2026-09-16.parquet").is_file()
