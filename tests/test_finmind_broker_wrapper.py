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


def _fake_weight_frame_stale(as_of):
    """Same shape as _fake_weight_frame but published on an earlier date,
    modelling the common case where the table has not republished since
    before the requested day (docs/pages/tx.md §2.2: "date <= day 裡最新的
    一筆")."""
    return pd.DataFrame(
        [
            {"rank": 1, "stock_id": "2330", "stock_name": "台積電", "weight_per": 32.1,
             "date": as_of, "type": "twse"},
            {"rank": 2, "stock_id": "2317", "stock_name": "鴻海", "weight_per": 4.0,
             "date": as_of, "type": "twse"},
        ]
    )


def test_a_day_with_no_row_of_its_own_falls_back_to_the_latest_earlier_row(monkeypatch, tmp_path):
    """Nothing published on "2026-09-30" itself -- the last real row, dated
    "2026-08-14", is inside the lookback window and wins."""
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    seen_args = {}

    def fetch(start_date, end_date):
        seen_args["start_date"] = start_date
        seen_args["end_date"] = end_date
        return _fake_weight_frame_stale("2026-08-14")

    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_market_value_weight=fetch
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    frame = FinMindWrapper().get_twii_weights(day="2026-09-30")

    # Queried a window ending at day, not a single exact-day match.
    assert seen_args["end_date"] == "2026-09-30"
    assert seen_args["start_date"] < "2026-09-30"
    assert sorted(frame.index) == ["2317", "2330"]
    assert (frame["date"] == "2026-08-14").all()
    # Cached under the requested day, so a second call for the same day
    # is a cache hit even though the row itself is dated earlier.
    assert (tmp_path / "2026-09-30.parquet").is_file()


def test_an_empty_window_raises_and_writes_no_cache_file(monkeypatch, tmp_path):
    """No row anywhere in the lookback window: this must surface as a loud
    failure, never a silently cached empty frame that every later caller on
    the same day would keep reading back as "zero weighted constituents"."""
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_market_value_weight=lambda start_date, end_date: pd.DataFrame(
            columns=["rank", "stock_id", "stock_name", "weight_per", "date", "type"]
        )
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    with pytest.raises(RuntimeError, match="no TAIEX weight row"):
        FinMindWrapper().get_twii_weights(day="2026-09-30")

    assert not (tmp_path / "2026-09-30.parquet").exists()


def test_only_the_single_latest_date_in_the_window_is_kept(monkeypatch, tmp_path):
    """A window spanning two different published dates keeps only the rows
    of the most recent one, never a mix of two different as-of dates."""
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)

    def fetch(start_date, end_date):
        return pd.concat([
            _fake_weight_frame_stale("2026-07-01"),
            pd.DataFrame([
                {"rank": 1, "stock_id": "2454", "stock_name": "聯發科",
                 "weight_per": 2.0, "date": "2026-09-01", "type": "twse"},
            ]),
        ], ignore_index=True)

    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_market_value_weight=fetch
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    frame = FinMindWrapper().get_twii_weights(day="2026-09-30")

    assert sorted(frame.index) == ["2454"]
    assert (frame["date"] == "2026-09-01").all()
