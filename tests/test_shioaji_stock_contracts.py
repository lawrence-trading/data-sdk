"""Unit tests for ShioajiWrapper.get_stock_contracts() against a fake client.

No network, no broker login, no FinLab token: the shared client slot on the
wrapper is filled with a stand-in whose Contracts.Stocks mimics Shioaji's
per-exchange containers (TSE / OTC / OES), each iterating over stock
contract objects.
"""

import os
import types

import pytest
import shioaji as sj

from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper


EXPECTED_COLUMNS = ["code", "name", "exchange", "category", "reference",
                    "limit_up", "limit_down", "day_trade", "unit",
                    "margin_trading_balance", "short_selling_balance",
                    "update_date"]


class _FakeStock:
    """Attribute bag with the same field names as shioaji.Stock."""

    def __init__(self, **fields):
        for k, v in fields.items():
            setattr(self, k, v)


class _FakeStocks:
    """Mimics shioaji Contracts.Stocks: iterating yields exchanges, and
    iterating an exchange yields its contracts."""

    def __init__(self, exchanges):
        self._exchanges = exchanges

    def __iter__(self):
        return iter(self._exchanges)


def _fake_api(stocks, status=sj.FetchStatus.Fetched):
    contracts = types.SimpleNamespace(status=status, Stocks=stocks)
    return types.SimpleNamespace(
        Contracts=contracts,
        logout=lambda: None,
    )


@pytest.fixture
def no_credentials(monkeypatch):
    for var in ("FINLAB_API_TOKEN", "FINLAB_TOKEN",
                "SHIOAJI_API_KEY", "SHIOAJI_SECRET_KEY"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def wrapper(monkeypatch, no_credentials):
    """A wrapper whose shared client is pre-bound to a fake, so no login runs."""
    def _no_login():
        raise AssertionError("login must not be attempted in a unit test")

    monkeypatch.setattr(sj, "Shioaji", _no_login)
    monkeypatch.setattr(ShioajiWrapper, "_api", None)
    monkeypatch.setattr(ShioajiWrapper, "_ref_count", 0)
    w = ShioajiWrapper()
    yield w
    # Drop the fake before the wrapper's __del__ tries to log it out.
    ShioajiWrapper._api = None


def _sample_stocks():
    tse = [
        _FakeStock(code="2330", name="台積電", exchange="TSE", category="24",
                   reference=1000.0, limit_up=1100.0, limit_down=900.0,
                   day_trade="Yes", unit=1000),
        _FakeStock(code="2412", name="中華電", exchange="TSE", category="27",
                   reference=125.5, limit_up=138.0, limit_down=113.0,
                   day_trade="OnlyBuy", unit=1000),
    ]
    otc = [
        _FakeStock(code="6488", name="環球晶", exchange="OTC", category="24",
                   reference=452.0, limit_up=497.0, limit_down=407.0,
                   day_trade="Yes", unit=1000),
    ]
    oes = [
        _FakeStock(code="7402", name="邑錡", exchange="OES", category="31",
                   reference=48.35, limit_up=53.1, limit_down=43.55,
                   day_trade="No", unit=1000),
    ]
    return _FakeStocks([tse, otc, oes])


def test_one_row_per_stock_with_twelve_columns(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_stocks()))

    df = wrapper.get_stock_contracts()

    assert list(df.columns) == EXPECTED_COLUMNS
    assert len(df) == 4
    assert df["code"].tolist() == ["2330", "2412", "6488", "7402"]
    assert df["exchange"].tolist() == ["TSE", "TSE", "OTC", "OES"]


def test_price_columns_are_passed_through_untouched(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_stocks()))

    df = wrapper.get_stock_contracts().set_index("code")

    assert df.loc["2330", "reference"] == 1000.0
    assert df.loc["2330", "limit_up"] == 1100.0
    assert df.loc["2330", "limit_down"] == 900.0
    assert df.loc["7402", "limit_up"] == 53.1
    assert df[["reference", "limit_up", "limit_down"]].notna().all().all()
    assert df.loc["2330", "name"] == "台積電"
    assert df.loc["2412", "day_trade"] == "OnlyBuy"


def test_missing_field_becomes_none(wrapper, monkeypatch):
    stocks = _FakeStocks([[_FakeStock(code="0050", name="元大台灣50",
                                      exchange="TSE")]])
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(stocks))

    df = wrapper.get_stock_contracts()

    assert len(df) == 1
    row = df.iloc[0]
    assert row["code"] == "0050"
    for col in ("category", "reference", "limit_up", "limit_down",
                "day_trade", "unit", "margin_trading_balance",
                "short_selling_balance", "update_date"):
        assert row[col] is None


def test_empty_contract_set_gives_empty_frame_with_columns(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_FakeStocks([])))

    df = wrapper.get_stock_contracts()

    assert list(df.columns) == EXPECTED_COLUMNS
    assert df.empty


def test_waits_for_contract_download_before_reading(wrapper, monkeypatch):
    """A partially downloaded contract set must not be read."""
    monkeypatch.setattr(ShioajiWrapper, "_api",
                        _fake_api(_sample_stocks(), status=sj.FetchStatus.Fetching))
    monkeypatch.setattr(ShioajiWrapper, "_CONTRACTS_FETCH_TIMEOUT_S", 0.2)

    with pytest.raises(RuntimeError, match="contract download did not complete"):
        wrapper.get_stock_contracts()


def test_smoke_runs_without_any_token_in_environment(wrapper, monkeypatch):
    for var in ("FINLAB_API_TOKEN", "FINLAB_TOKEN",
                "SHIOAJI_API_KEY", "SHIOAJI_SECRET_KEY"):
        assert var not in os.environ
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_stocks()))

    assert len(wrapper.get_stock_contracts()) == 4


def test_balance_fields_are_passed_through_untouched(wrapper, monkeypatch):
    """The two 融資/融券 figures and their as-of stamp reach the frame under
    SinoPac's own field names, with no rescaling and no interpretation."""
    stocks = _FakeStocks([[_FakeStock(code="2330", name="台積電", exchange="TSE",
                                      margin_trading_balance=12345,
                                      short_selling_balance=678,
                                      day_trade="Yes",
                                      update_date="2026/09/16")]])
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(stocks))

    row = wrapper.get_stock_contracts().iloc[0]

    assert row["margin_trading_balance"] == 12345
    assert row["short_selling_balance"] == 678
    assert row["update_date"] == "2026/09/16"


def test_stock_contract_model_carries_the_three_price_fields():
    """The plan's limit-price source: Shioaji's Stock contract class itself
    must expose reference / limit_up / limit_down, plus every other column
    the wrapper reads, so the column mapping is one-to-one with no rename."""
    exposed = set(dir(sj.Stock))
    assert set(EXPECTED_COLUMNS) <= exposed
