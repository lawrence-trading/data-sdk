"""The trading-calendar range follows the furthest listed delivery date."""

import pytest

from data_sdk.wrappers.finmind_broker_wrapper import FinMindWrapper


def test_range_ends_past_the_furthest_delivery_date():
    start, end = FinMindWrapper.calendar_range_covering(
        "2026-09-17", ["2026/09/16", "2027/06/16", "2026/12/16"], margin_days=7
    )
    assert start == "2026-09-17"
    assert end == "2027-06-23"


def test_range_never_ends_before_start():
    start, end = FinMindWrapper.calendar_range_covering("2026-09-17", ["2026-01-21"])
    assert end == start


def test_empty_delivery_list_is_an_error():
    with pytest.raises(ValueError):
        FinMindWrapper.calendar_range_covering("2026-09-17", [])


def test_covering_call_passes_the_derived_range_through(monkeypatch):
    seen = {}

    def fake_get_trading_dates(self, start_date, end_date):
        seen["range"] = (start_date, end_date)
        return "calendar"

    monkeypatch.setattr(FinMindWrapper, "get_trading_dates", fake_get_trading_dates)
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    result = FinMindWrapper().get_trading_dates_covering("2026-09-17", ["2027/03/17"])
    assert result == "calendar"
    assert seen["range"] == ("2026-09-17", "2027-03-24")
