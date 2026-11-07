"""ShioajiWrapper's login and contract loading under both Shioaji login APIs."""

from types import SimpleNamespace

import pytest

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
        self.Contracts = SimpleNamespace(status="Fetched")

    def login(self, **kwargs):
        self.login_kwargs = kwargs


@pytest.fixture
def fake_sj(monkeypatch):
    monkeypatch.setattr(sw, "sj", SimpleNamespace(
        Shioaji=_Shioaji,
        SecurityType=SimpleNamespace(Stock="STK", Futures="FUT"),
        FetchStatus=SimpleNamespace(Fetched="Fetched"),
    ))
    monkeypatch.setattr(ShioajiWrapper, "_api", None)
    monkeypatch.setenv("SHIOAJI_API_KEY", "key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "secret")
    return ShioajiWrapper.__new__(ShioajiWrapper)


@pytest.mark.parametrize("downloads, extra", [(True, {"contracts_timeout": 60_000}), (False, {})])
def test_contracts_timeout_goes_only_to_a_login_that_downloads_contracts(fake_sj, monkeypatch, downloads, extra):
    monkeypatch.setattr(sw, "_LOGIN_DOWNLOADS_CONTRACTS", downloads)
    api = ShioajiWrapper._ensure_api()
    assert api.simulation is True
    assert api.login_kwargs == {"api_key": "key", "secret_key": "secret", **extra}


def test_contracts_are_preloaded_when_login_does_not_download_them(fake_sj, monkeypatch):
    monkeypatch.setattr(sw, "_LOGIN_DOWNLOADS_CONTRACTS", False)
    fake_sj._ensure_contracts_fetched()
    assert ShioajiWrapper._api.contracts.calls == [{"security_types": ["STK", "FUT"], "timeout": 60_000}]


def test_a_failed_preload_raises_shioajis_own_error(fake_sj, monkeypatch):
    monkeypatch.setattr(sw, "_LOGIN_DOWNLOADS_CONTRACTS", False)
    ShioajiWrapper._ensure_api().contracts.error = ValueError("contracts: Timeout: 60000ms")
    with pytest.raises(ValueError, match="contracts: Timeout: 60000ms"):
        fake_sj._ensure_contracts_fetched()


def test_a_login_that_downloads_contracts_waits_for_the_fetch_status(fake_sj, monkeypatch):
    monkeypatch.setattr(sw, "_LOGIN_DOWNLOADS_CONTRACTS", True)
    monkeypatch.setattr(ShioajiWrapper, "_CONTRACTS_FETCH_TIMEOUT_S", 0.2)
    api = ShioajiWrapper._ensure_api()
    api.Contracts.status = "Fetching"
    with pytest.raises(RuntimeError, match="download did not complete"):
        fake_sj._ensure_contracts_fetched()
    api.Contracts.status = "Fetched"
    fake_sj._ensure_contracts_fetched()
    assert api.contracts.calls == []


def test_the_installed_shioaji_selects_the_matching_contract_wait():
    import shioaji

    major, minor = (int(part) for part in shioaji.__version__.split(".")[:2])
    assert sw._LOGIN_DOWNLOADS_CONTRACTS == ((major, minor) < (1, 7))
    if sw._LOGIN_DOWNLOADS_CONTRACTS:
        assert hasattr(shioaji.FetchStatus, "Fetched")
    else:
        assert hasattr(shioaji.SecurityType, "Futures")
