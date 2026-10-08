"""Unit tests for ShioajiWrapper.get_option_contracts() against a fake client.

No network, no broker login, no FinLab token -- same fake-Contracts style as
test_shioaji_stock_contracts.py's get_stock_contracts() tests.

A separate, real-login integration test (pytest.mark.integration, skipped
without SHIOAJI_API_KEY/SHIOAJI_SECRET_KEY) pins down the two field values
this module cannot otherwise confirm: the exact live option_right.value
string, and that `multiplier` really is populated by the server.
"""

import os
import types

import pandas as pd
import pytest
import shioaji as sj

from data_sdk.wrappers.shioaji_wrapper import ShioajiWrapper


EXPECTED_COLUMNS = ["code", "symbol", "name", "category", "delivery_month",
                    "delivery_date", "strike_1e4", "option_right",
                    "underlying_kind", "underlying_code", "unit",
                    "multiplier", "update_date"]


class _FakeOption:
    """Attribute bag with the same field names as shioaji.Option."""

    def __init__(self, **fields):
        for k, v in fields.items():
            setattr(self, k, v)


class _FakeOptionRight:
    """Stands in for the real shioaji.OptionRight enum."""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class _FakeGroups:
    """Mimics shioaji Contracts.Options: iterating yields per-root groups,
    and iterating a group yields its contracts."""

    def __init__(self, groups):
        self._groups = groups

    def __iter__(self):
        return iter(self._groups)


def _fake_api(options, status=sj.FetchStatus.Fetched):
    contracts = types.SimpleNamespace(status=status, Options=options)
    return types.SimpleNamespace(Contracts=contracts, logout=lambda: None)


@pytest.fixture
def no_credentials(monkeypatch):
    for var in ("FINLAB_API_TOKEN", "FINLAB_TOKEN", "SHIOAJI_API_KEY", "SHIOAJI_SECRET_KEY"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def wrapper(monkeypatch, no_credentials):
    def _no_login():
        raise AssertionError("login must not be attempted in a unit test")

    monkeypatch.setattr(sj, "Shioaji", _no_login)
    monkeypatch.setattr(ShioajiWrapper, "_api", None)
    monkeypatch.setattr(ShioajiWrapper, "_ref_count", 0)
    w = ShioajiWrapper()
    yield w
    ShioajiWrapper._api = None


def _sample_options():
    txo = [
        _FakeOption(code="TXO24000J6", symbol="TXO202610024000C", name="TXO202610C24000",
                    category="TXO", delivery_month="202610", delivery_date="2026/10/21",
                    strike_price=24000.0, option_right=_FakeOptionRight("Call"),
                    underlying_kind="I", underlying_code="001", unit=1,
                    multiplier=50, update_date="2026/10/07"),
        _FakeOption(code="TXO24000V6", symbol="TXO202610024000P", name="TXO202610P24000",
                    category="TXO", delivery_month="202610", delivery_date="2026/10/21",
                    strike_price="24000", option_right=_FakeOptionRight("Put"),
                    underlying_kind="I", underlying_code="001", unit=1,
                    multiplier=50, update_date="2026/10/07"),
    ]
    dho = [
        _FakeOption(code="DHO02600J6", symbol="DHO2026100260C", name="DHO202610C26.0",
                    category="DHO", delivery_month="202610", delivery_date="2026/10/21",
                    strike_price=26.0, option_right=_FakeOptionRight("Call"),
                    underlying_kind="S", underlying_code="2454", unit=2000,
                    multiplier=None, update_date="2026/10/07"),
    ]
    return _FakeGroups([txo, dho])


def test_one_row_per_contract_with_thirteen_columns(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_options()))

    df = wrapper.get_option_contracts()

    assert list(df.columns) == EXPECTED_COLUMNS
    assert len(df) == 3
    assert df["code"].tolist() == ["TXO24000J6", "TXO24000V6", "DHO02600J6"]


def test_strike_price_is_rescaled_to_integer_1e4_never_a_float(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_options()))

    df = wrapper.get_option_contracts().set_index("code")

    assert df.loc["TXO24000J6", "strike_1e4"] == 240_000_000  # 24000 * 10000
    # Int64 (nullable), never float64: a mix of real strikes and a missing
    # one must not silently put a float back on this money column.
    assert df["strike_1e4"].dtype == "Int64"
    # a string strike (as Shioaji may return it) rescales identically
    assert df.loc["TXO24000V6", "strike_1e4"] == 240_000_000
    assert df.loc["DHO02600J6", "strike_1e4"] == 260_000


def test_option_right_is_normalized_through_its_value(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_options()))

    df = wrapper.get_option_contracts().set_index("code")

    assert df.loc["TXO24000J6", "option_right"] == "Call"
    assert df.loc["TXO24000V6", "option_right"] == "Put"


def test_option_right_also_accepts_a_plain_string(wrapper, monkeypatch):
    # A caller that already normalized (or a future Shioaji release that
    # returns a plain "C"/"P" string) must not break _option_right_str.
    options = _FakeGroups([[
        _FakeOption(code="TXO24000J6", symbol="s", name="n", category="TXO",
                    delivery_month="202610", delivery_date="2026/10/21",
                    strike_price=24000.0, option_right="C",
                    underlying_kind="I", underlying_code="001", unit=1,
                    multiplier=50, update_date="2026/10/07"),
    ]])
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(options))

    df = wrapper.get_option_contracts()
    assert df.iloc[0]["option_right"] == "C"


def test_multiplier_is_passed_through_untouched(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_sample_options()))

    df = wrapper.get_option_contracts().set_index("code")

    assert df.loc["TXO24000J6", "multiplier"] == 50
    assert df["multiplier"].dtype == "Int64"
    assert pd.isna(df.loc["DHO02600J6", "multiplier"])


def test_missing_field_becomes_none(wrapper, monkeypatch):
    options = _FakeGroups([[_FakeOption(code="TXO24000J6", category="TXO")]])
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(options))

    df = wrapper.get_option_contracts()

    assert len(df) == 1
    row = df.iloc[0]
    assert row["code"] == "TXO24000J6"
    for col in ("symbol", "name", "delivery_month", "delivery_date",
                "option_right", "underlying_kind", "underlying_code", "unit",
                "update_date"):
        assert row[col] is None
    # Int64 (nullable): a missing value here is pd.NA, not None.
    assert pd.isna(row["strike_1e4"])
    assert pd.isna(row["multiplier"])


def test_empty_contract_set_gives_empty_frame_with_columns(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api", _fake_api(_FakeGroups([])))

    df = wrapper.get_option_contracts()

    assert list(df.columns) == EXPECTED_COLUMNS
    assert df.empty


def test_waits_for_contract_download_before_reading(wrapper, monkeypatch):
    monkeypatch.setattr(ShioajiWrapper, "_api",
                        _fake_api(_sample_options(), status=sj.FetchStatus.Fetching))
    monkeypatch.setattr(ShioajiWrapper, "_CONTRACTS_FETCH_TIMEOUT_S", 0.2)

    with pytest.raises(RuntimeError, match="contract download did not complete"):
        wrapper.get_option_contracts()


def test_option_contract_model_carries_the_fields_this_wrapper_reads():
    """Every attribute this wrapper reads must exist on the real shioaji
    Option/Contract class -- checked against the installed package, not
    assumed (docs/tasks/42 §4.1's "point at a live object, don't guess")."""
    exposed = set(dir(sj.Option))
    read_fields = {"code", "symbol", "name", "category", "delivery_month",
                   "delivery_date", "strike_price", "option_right",
                   "underlying_kind", "underlying_code", "unit", "multiplier",
                   "update_date"}
    assert read_fields <= exposed


@pytest.mark.integration
def test_live_option_right_and_multiplier_values():
    """Confirms, against a real Shioaji login, the two things a fake object
    cannot: option_right.value's exact live string ("C"/"P" vs "Call"/"Put")
    and that `multiplier` is actually populated by the server for a real TXO
    contract, not merely present as a class attribute (docs/tasks/42's own
    instruction: write this as an integration test and skip it, never fake
    having verified it).

    Gated on its own opt-in variable, never on SHIOAJI_API_KEY/
    SHIOAJI_SECRET_KEY alone: those two are ambient on this machine (used by
    the real deployment), and a plain ``pytest`` run must never place a real
    -- even paper-account -- login just because they happen to be exported in
    the shell it runs in.
    """
    if os.environ.get("DATA_SDK_RUN_SHIOAJI_INTEGRATION_TESTS") != "1":
        pytest.skip(
            "set DATA_SDK_RUN_SHIOAJI_INTEGRATION_TESTS=1 (plus a real "
            "SHIOAJI_API_KEY/SHIOAJI_SECRET_KEY) to run this against a live Shioaji login"
        )
    if not (os.environ.get("SHIOAJI_API_KEY") and os.environ.get("SHIOAJI_SECRET_KEY")):
        pytest.skip("needs a real Shioaji login (SHIOAJI_API_KEY/SHIOAJI_SECRET_KEY)")

    df = ShioajiWrapper().get_option_contracts()
    txo = df[df["code"].str.startswith("TXO")]
    assert not txo.empty
    assert set(txo["option_right"].dropna().unique()) <= {"C", "P", "Call", "Put"}
    assert txo["multiplier"].notna().any()
