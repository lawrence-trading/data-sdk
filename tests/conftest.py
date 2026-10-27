import pytest


@pytest.fixture(autouse=True)
def _no_ambient_market_closures(monkeypatch, tmp_path_factory):
    """A FinMindCalendar built without an explicit closure list reads
    DATA_SDK_MARKET_CLOSURES; pointing it at a file that does not exist keeps
    a checkout's own closure list out of every test."""
    monkeypatch.setenv("DATA_SDK_MARKET_CLOSURES", str(tmp_path_factory.getbasetemp() / "no-such-closures.json"))
