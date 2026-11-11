"""Unit tests for ShioajiWrapper's login against fake clients of both Shioaji API shapes.

No network and no broker login: sj.Shioaji is replaced by a stand-in that records the calls.
Before 1.7, login() takes contracts_timeout and downloads the contract file itself; from 1.7,
login() has no such parameter, loads the contracts before it returns, and refuses a later
fetch_contracts() on the same session; its Contracts has no status attribute.
"""
import pytest
import shioaji as sj

from data_sdk.wrappers import shioaji_wrapper as sw
from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper


class _ClientBefore17:
    calls = []

    def __init__(self, simulation=False):
        self.simulation = simulation

    def login(self, api_key, secret_key, fetch_contract=True, contracts_timeout=0, contracts_cb=None,
              subscribe_trade=True, receive_window=30000):
        type(self).calls.append(("login", {"contracts_timeout": contracts_timeout}))

    def fetch_contracts(self, contract_download=False, contracts_timeout=30000, contracts_cb=None):
        type(self).calls.append(("fetch_contracts", {"contract_download": contract_download}))


class _Contracts17:
    """Shioaji 1.7's Contracts: no status, no len(), not iterable; Stocks iterates exchange groups."""

    def __init__(self, n):
        self.Stocks = [["TSE"] * n, ["OTC"] * n, []]


class _Client17:
    calls = []
    loaded = 3

    def __init__(self, simulation=False):
        self.simulation = simulation
        self.Contracts = _Contracts17(type(self).loaded)

    def login(self, api_key, secret_key, subscribe_trade=True, receive_window=30000, force_refresh=False):
        type(self).calls.append(("login", {}))

    def fetch_contracts(self, contract_download=False, contracts_timeout=30000, contracts_cb=None):
        type(self).calls.append(("fetch_contracts", {"contract_download": contract_download,
                                                     "contracts_timeout": contracts_timeout}))


@pytest.fixture
def fresh_wrapper(monkeypatch, tmp_path):
    monkeypatch.setattr(ShioajiWrapper, "_api", None)
    monkeypatch.setenv("SJ_CONTRACTS_PATH", str(tmp_path))
    monkeypatch.setenv("SHIOAJI_API_KEY", "fake-key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "fake-secret")
    yield
    ShioajiWrapper._api = None


def test_before_17_login_downloads_the_contracts(monkeypatch, fresh_wrapper):
    _ClientBefore17.calls = []
    monkeypatch.setattr(sw.sj, "Shioaji", _ClientBefore17)
    api = ShioajiWrapper._ensure_api()
    assert isinstance(api, _ClientBefore17) and api.simulation is True
    assert _ClientBefore17.calls == [
        ("login", {"contracts_timeout": ShioajiWrapper._CONTRACTS_FETCH_TIMEOUT_S * 1000})]


def test_17_logs_in_and_never_calls_fetch_contracts(monkeypatch, fresh_wrapper):
    _Client17.calls = []
    monkeypatch.setattr(sw.sj, "Shioaji", _Client17)
    api = ShioajiWrapper._ensure_api()
    assert isinstance(api, _Client17) and api.simulation is True
    assert _Client17.calls == [("login", {})]


def test_17_contracts_loaded_by_login_count_as_fetched(monkeypatch, fresh_wrapper):
    _Client17.calls = []
    monkeypatch.setattr(_Client17, "loaded", 3)
    monkeypatch.setattr(sw.sj, "Shioaji", _Client17)
    ShioajiWrapper()._ensure_contracts_fetched()
    assert _Client17.calls == [("login", {})]


def test_17_an_empty_contract_set_after_login_is_an_error(monkeypatch, fresh_wrapper):
    _Client17.calls = []
    monkeypatch.setattr(_Client17, "loaded", 0)
    monkeypatch.setattr(sw.sj, "Shioaji", _Client17)
    with pytest.raises(RuntimeError, match="no contracts"):
        ShioajiWrapper()._ensure_contracts_fetched()


def test_the_installed_shioaji_takes_one_of_the_two_shapes():
    import inspect
    login = inspect.signature(sj.Shioaji.login).parameters
    fetch = inspect.signature(sj.Shioaji.fetch_contracts).parameters
    assert "contract_download" in fetch and "contracts_timeout" in fetch
    assert "api_key" in login and "secret_key" in login
