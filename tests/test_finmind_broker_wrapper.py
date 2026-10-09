"""get_twii_weights against a fake DataLoader: no network, no token."""

import types

import pandas as pd
import pytest

from data_sdk.wrappers.finmind_broker_wrapper import FinMindWrapper


def _weight_row(stock_id, name, weight, date, kind="twse"):
    return {"rank": 1, "stock_id": stock_id, "stock_name": name, "weight_per": weight,
            "date": date, "type": kind}


def _published_table():
    """Two monthly TWSE publications (2026-08-31, 2026-09-30) plus daily TPEx
    rows -- the dataset's real shape, observed 2026-10-09."""
    return pd.DataFrame([
        _weight_row("2330", "台積電", 32.1, "2026-08-31"),
        _weight_row("2317", "鴻海", 4.0, "2026-08-31"),
        _weight_row("2330", "台積電", 32.5, "2026-09-30"),
        _weight_row("2317", "鴻海", 4.1, "2026-09-30"),
        _weight_row("2454", "聯發科", 2.0, "2026-09-30"),
        _weight_row("5380", "台塑化上櫃", 1.2, "2026-09-30", "tpex"),
        _weight_row("5380", "台塑化上櫃", 1.3, "2026-10-08", "tpex"),
    ])


class _FakeWeightApi:
    """Answers ``taiwan_stock_market_value_weight`` the way FinMind does: a
    ``stock_id`` query returns that stock's rows across the range, an
    all-stocks query returns one exact date, and an all-stocks date RANGE
    comes back as an empty, column-less frame."""

    def __init__(self, table):
        self.table = table
        self.calls = []

    def taiwan_stock_market_value_weight(self, stock_id="", start_date="", end_date=""):
        self.calls.append((stock_id, start_date, end_date))
        t = self.table
        if stock_id:
            rows = t[(t["stock_id"] == stock_id) & (t["date"] >= start_date)
                     & (t["date"] <= (end_date or start_date))]
        elif end_date and end_date != start_date:
            return pd.DataFrame()
        else:
            rows = t[t["date"] == start_date]
        return rows.reset_index(drop=True) if len(rows) else pd.DataFrame()


class _NullLimiter:
    def acquire(self, n):
        pass


def _install(monkeypatch, api):
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    monkeypatch.setattr(FinMindWrapper, "_api", api)
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))
    return api


@pytest.fixture
def api(monkeypatch):
    """A wrapper whose shared client and rate limiter are stand-ins, so
    construction and a cold fetch never touch the network or a token."""
    return _install(monkeypatch, _FakeWeightApi(_published_table()))


def test_cold_fetch_keeps_only_twse_rows_of_the_latest_publication(api, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    frame = FinMindWrapper().get_twii_weights(day="2026-10-08")

    assert frame.index.name == "stock_id"
    assert sorted(frame.index) == ["2317", "2330", "2454"]
    assert list(frame.columns) == ["weight_per", "date"]
    assert frame.loc["2330", "weight_per"] == 32.5
    assert (frame["date"] == "2026-09-30").all()


def test_the_all_stocks_query_names_one_exact_date_never_a_range(api, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    FinMindWrapper().get_twii_weights(day="2026-10-08")

    probe, fetch = api.calls
    assert probe[0] == "2330" and probe[1] < "2026-10-08" and probe[2] == "2026-10-08"
    assert fetch == ("", "2026-09-30", "2026-09-30")


def test_cold_fetch_writes_the_cache_file(api, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    FinMindWrapper().get_twii_weights(day="2026-10-08")

    path = tmp_path / "2026-10-08.parquet"
    assert path.is_file() and path.stat().st_size > 0


def test_a_cached_day_is_served_without_touching_the_fake_api(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    class _Refuse:
        def taiwan_stock_market_value_weight(self, **kwargs):
            raise AssertionError("the network must not be touched on a cached day")

    _install(monkeypatch, _Refuse())
    cached = pd.DataFrame({"weight_per": [10.0]}, index=pd.Index(["2454"], name="stock_id"))
    cached["date"] = "2026-09-30"
    cached.to_parquet(tmp_path / "2026-10-08.parquet")

    frame = FinMindWrapper().get_twii_weights(day="2026-10-08")
    assert frame.loc["2454", "weight_per"] == 10.0


def test_default_day_is_todays_taipei_date(api, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    monkeypatch.setattr(FinMindWrapper, "_taipei_today_str", staticmethod(lambda: "2026-10-08"))

    FinMindWrapper().get_twii_weights()

    assert (tmp_path / "2026-10-08.parquet").is_file()


def test_a_publication_day_itself_uses_its_own_table(api, monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    frame = FinMindWrapper().get_twii_weights(day="2026-09-30")

    assert (frame["date"] == "2026-09-30").all()


def test_a_day_between_publications_uses_the_earlier_one(api, monkeypatch, tmp_path):
    """Nothing published between 2026-08-31 and 2026-09-30: a day in that
    gap reads the 2026-08-31 table, never the later one."""
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))

    frame = FinMindWrapper().get_twii_weights(day="2026-09-15")

    assert sorted(frame.index) == ["2317", "2330"]
    assert (frame["date"] == "2026-08-31").all()
    # Cached under the requested day, so a second call for the same day
    # is a cache hit even though the row itself is dated earlier.
    assert (tmp_path / "2026-09-15.parquet").is_file()


def test_no_publication_in_the_window_raises_and_writes_no_cache_file(monkeypatch, tmp_path):
    """No row anywhere in the lookback window: this must surface as a loud
    failure, never a silently cached empty frame that every later caller on
    the same day would keep reading back as "zero weighted constituents"."""
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    _install(monkeypatch, _FakeWeightApi(_published_table()[lambda t: t["type"] == "tpex"]))

    with pytest.raises(RuntimeError, match="no TAIEX weight row"):
        FinMindWrapper().get_twii_weights(day="2026-10-08")

    assert not (tmp_path / "2026-10-08.parquet").exists()


def test_a_listed_date_whose_table_has_no_twse_rows_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_SDK_FINMIND_WEIGHT_PATH", str(tmp_path))
    api = _FakeWeightApi(_published_table())
    original = api.taiwan_stock_market_value_weight

    def probe_only(stock_id="", start_date="", end_date=""):
        if stock_id:
            return original(stock_id=stock_id, start_date=start_date, end_date=end_date)
        return pd.DataFrame()

    api.taiwan_stock_market_value_weight = probe_only
    _install(monkeypatch, api)

    with pytest.raises(RuntimeError, match="no TAIEX weight row in FinMind's 2026-09-30 table"):
        FinMindWrapper().get_twii_weights(day="2026-10-08")

    assert not (tmp_path / "2026-10-08.parquet").exists()


def _fake_daily_frame(rows):
    return pd.DataFrame(
        [{"date": d, "stock_id": "TAIEX", "open": c, "max": c, "min": c, "close": c} for d, c in rows]
    )


def test_get_index_prevclose_picks_the_latest_date_strictly_before_before(monkeypatch):
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    seen_args = {}

    def fetch(stock_id, start_date, end_date):
        seen_args["stock_id"] = stock_id
        seen_args["start_date"] = start_date
        seen_args["end_date"] = end_date
        return _fake_daily_frame([
            ("2026-09-28", 17800.12),
            ("2026-09-29", 17850.50),
            # A row dated exactly "before" must never win -- this method
            # answers the PREVIOUS close, not today's.
            ("2026-09-30", 17999.99),
        ])

    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(taiwan_stock_daily=fetch))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    close_1e2, trading_day = FinMindWrapper().get_index_prevclose(before="2026-09-30")

    assert seen_args["stock_id"] == "TAIEX"
    assert seen_args["end_date"] == "2026-09-30"
    assert seen_args["start_date"] < "2026-09-30"
    assert trading_day == "2026-09-29"
    assert close_1e2 == 1785050


def test_get_index_prevclose_defaults_to_todays_taipei_date(monkeypatch):
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    monkeypatch.setattr(FinMindWrapper, "_taipei_today_str", staticmethod(lambda: "2026-09-16"))
    seen_args = {}

    def fetch(stock_id, start_date, end_date):
        seen_args["end_date"] = end_date
        return _fake_daily_frame([("2026-09-15", 17500.00)])

    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(taiwan_stock_daily=fetch))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    close_1e2, trading_day = FinMindWrapper().get_index_prevclose()

    assert seen_args["end_date"] == "2026-09-16"
    assert trading_day == "2026-09-15"
    assert close_1e2 == 1750000


def test_get_index_prevclose_raises_when_only_todays_row_is_in_the_window(monkeypatch):
    """Every row in the fetched window is dated "before" itself (e.g. the
    exchange has not published yesterday's close yet) -- this must raise
    loudly, never fall back to today's own close as a stand-in 昨收."""
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_daily=lambda stock_id, start_date, end_date: _fake_daily_frame([
            ("2026-09-30", 17999.99),
        ])
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    with pytest.raises(RuntimeError, match="no TAIEX daily row dated before"):
        FinMindWrapper().get_index_prevclose(before="2026-09-30")


def test_get_index_prevclose_raises_on_a_wholly_empty_window(monkeypatch):
    monkeypatch.setattr(FinMindWrapper, "__init__", lambda self: None)
    monkeypatch.setattr(FinMindWrapper, "_api", types.SimpleNamespace(
        taiwan_stock_daily=lambda stock_id, start_date, end_date: pd.DataFrame(
            columns=["date", "stock_id", "close"]
        )
    ))
    monkeypatch.setattr(FinMindWrapper, "limiter", classmethod(lambda cls: _NullLimiter()))

    with pytest.raises(RuntimeError, match="no TAIEX daily row in"):
        FinMindWrapper().get_index_prevclose(before="2026-09-30")
