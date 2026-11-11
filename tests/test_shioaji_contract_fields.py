"""The futures/option contract columns keep one meaning on both Shioaji API shapes.

Shioaji 1.7 renamed and dropped contract attributes (``category`` became
``root``, ``symbol`` and ``unit`` are gone, dates are ISO). These tests feed
``get_futures_contracts`` / ``get_option_contracts`` fake contracts of each
shape; the attribute values are copied from the two versions' own contract
files of 2026-11-11. No login.
"""
import datetime as dt
import types

import pytest
import shioaji as sj

from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper


class _Contract:
    def __init__(self, **fields):
        for k, v in fields.items():
            setattr(self, k, v)


def _old_future(code, symbol, name, underlying_code="2330", kind="S"):
    return _Contract(code=code, symbol=symbol, name=name, category=code[:3],
                     delivery_month="202612", delivery_date="2026/12/16",
                     underlying_kind=kind, underlying_code=underlying_code,
                     unit=1.0, update_date="2026/11/11")


def _new_future(code, name, underlying_code="2330", kind="S", delivery_date="2026-12-16"):
    return _Contract(code=code, name=name, root=code[:3],
                     delivery_month="202612", delivery_date=delivery_date,
                     underlying_kind=kind, underlying_code=underlying_code,
                     update_date="2026-11-11")


@pytest.fixture
def wrapper(monkeypatch):
    for var in ("FINLAB_API_TOKEN", "FINLAB_TOKEN", "SHIOAJI_API_KEY", "SHIOAJI_SECRET_KEY"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(ShioajiWrapper, "_ref_count", 0)
    monkeypatch.setattr(ShioajiWrapper, "_ensure_contracts_fetched", lambda self: None)
    w = ShioajiWrapper()
    yield w
    ShioajiWrapper._api = None


def _api(futures=(), options=()):
    contracts = types.SimpleNamespace(Futures=[list(futures)], Options=[list(options)])
    return types.SimpleNamespace(Contracts=contracts, logout=lambda: None)


def test_futures_rows_are_the_same_on_both_api_shapes(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _api(futures=[
        _old_future("CDFL6", "CDF202612", "台積電期貨12"),
        _old_future("CDFR1", "CDFR1", "台積電期貨近月"),
    ]))
    old = wrapper.get_futures_contracts()
    monkeypatch.setattr(ShioajiWrapper, "_api", _api(futures=[
        _new_future("CDFL6", "台積電期貨 202612"),
        _new_future("CDFR1", "台積電期貨 近月"),
    ]))
    new = wrapper.get_futures_contracts()

    same = ["code", "symbol", "category", "delivery_month", "delivery_date",
            "underlying_kind", "underlying_code"]
    assert new[same].to_dict("records") == old[same].to_dict("records")
    assert new["symbol"].tolist() == ["CDF202612", "CDFR1"]
    assert new["category"].tolist() == ["CDF", "CDF"]
    assert new["delivery_date"].tolist() == ["2026/12/16", "2026/12/16"]


def test_names_and_underlyings_are_the_vendors_own_text(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _api(futures=[
        _new_future("TXFL6", "臺股期貨 202612", underlying_code="IX0001", kind="I"),
        _new_future("OAFL6", "富邦上証ETF期貨 202612", underlying_code="006205"),
    ]))
    df = wrapper.get_futures_contracts()

    assert df["name"].tolist() == ["臺股期貨 202612", "富邦上証ETF期貨 202612"]
    assert df["underlying_code"].tolist() == ["IX0001", "006205"]
    assert df["unit"].isna().all()


def test_a_date_object_is_written_as_slash_text(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _api(futures=[
        _new_future("CDFL6", "台積電期貨 202612", delivery_date=dt.date(2026, 12, 16)),
    ]))
    assert wrapper.get_futures_contracts()["delivery_date"].tolist() == ["2026/12/16"]


def test_option_rows_on_the_new_shape(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _api(options=[
        _Contract(code="TXO33700K6", name="臺指選擇權 202611 C 33700", root="TXO",
                  delivery_month="202611", delivery_date="2026-11-18",
                  strike_price=33700.0, option_right=sj.OptionRight.Call,
                  underlying_kind="I", underlying_code="IX0001", multiplier=50.0,
                  update_date="2026-11-11"),
    ]))
    row = wrapper.get_option_contracts().iloc[0]

    assert row["category"] == "TXO"
    assert row["symbol"] is None
    assert row["delivery_date"] == "2026/11/18"
    assert row["update_date"] == "2026/11/11"
    assert row["strike_1e4"] == 337_000_000
    assert row["option_right"] == "C"
    assert row["multiplier"] == 50


@pytest.mark.parametrize("value, expected", [
    ("2026/12/16", "2026/12/16"), ("2026-12-16", "2026/12/16"),
    (dt.date(2026, 12, 16), "2026/12/16"), (None, None), ("", ""),
])
def test_slash_date(value, expected):
    assert ShioajiWrapper._slash_date(value) == expected
