"""ShioajiWrapper's login and contract loading on Shioaji 1.7."""

import inspect
from types import SimpleNamespace

import pytest
import shioaji

from data_sdk.wrappers import shioaji_wrapper as sw
from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper


class _Contracts:
    def __init__(self):
        self.calls = []
        self.error = None

    def preload(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error


class _Shioaji:
    def __init__(self, simulation):
        self.simulation = simulation
        self.login_kwargs = None
        self.contracts = _Contracts()

    def login(self, **kwargs):
        self.login_kwargs = kwargs


@pytest.fixture
def fake_sj(monkeypatch):
    monkeypatch.setattr(sw, "sj", SimpleNamespace(
        Shioaji=_Shioaji,
        SecurityType=SimpleNamespace(Stock="STK", Futures="FUT"),
    ))
    monkeypatch.setattr(ShioajiWrapper, "_api", None)
    monkeypatch.setenv("SHIOAJI_API_KEY", "key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "secret")
    return ShioajiWrapper.__new__(ShioajiWrapper)


def test_login_passes_only_the_keys(fake_sj):
    api = ShioajiWrapper._ensure_api()
    assert api.simulation is True
    assert api.login_kwargs == {"api_key": "key", "secret_key": "secret"}


def test_stock_and_futures_contracts_are_preloaded(fake_sj):
    fake_sj._ensure_contracts_fetched()
    assert ShioajiWrapper._api.contracts.calls == [{"security_types": ["STK", "FUT"], "timeout": 60_000}]


def test_a_failed_preload_raises_shioajis_own_error(fake_sj):
    ShioajiWrapper._ensure_api().contracts.error = ValueError("contracts: Timeout: 60000ms")
    with pytest.raises(ValueError, match="contracts: Timeout: 60000ms"):
        fake_sj._ensure_contracts_fetched()


def test_the_installed_shioaji_has_the_api_the_wrapper_calls():
    major, minor = (int(part) for part in shioaji.__version__.split(".")[:2])
    assert (major, minor) >= (1, 7)
    assert "contracts_timeout" not in inspect.signature(shioaji.Shioaji.login).parameters
    assert hasattr(shioaji.SecurityType, "Futures")
