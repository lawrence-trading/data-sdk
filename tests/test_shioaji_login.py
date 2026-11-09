"""Unit tests for ShioajiWrapper's login against fake clients of both Shioaji API shapes.

No network and no broker login: sj.Shioaji is replaced by a stand-in that records the calls.
Before 1.7, login() takes contracts_timeout and downloads the contract file itself; from 1.7,
login() has no such parameter and the download is asked for with fetch_contracts().
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


class _Client17:
    calls = []

    def __init__(self, simulation=False):
        self.simulation = simulation

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


def test_17_logs_in_then_asks_for_the_contract_download(monkeypatch, fresh_wrapper):
    _Client17.calls = []
    monkeypatch.setattr(sw.sj, "Shioaji", _Client17)
    api = ShioajiWrapper._ensure_api()
    assert isinstance(api, _Client17) and api.simulation is True
    assert _Client17.calls == [
        ("login", {}),
        ("fetch_contracts", {"contract_download": True,
                             "contracts_timeout": ShioajiWrapper._CONTRACTS_FETCH_TIMEOUT_S * 1000}),
    ]


def test_the_installed_shioaji_takes_one_of_the_two_shapes():
    import inspect
    login = inspect.signature(sj.Shioaji.login).parameters
    fetch = inspect.signature(sj.Shioaji.fetch_contracts).parameters
    assert "contract_download" in fetch and "contracts_timeout" in fetch
    assert "api_key" in login and "secret_key" in login
