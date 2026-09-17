"""TaifexWrapper against injected payloads: no network."""

import datetime as dt
import json

import pytest

from data_sdk import TaifexWrapper, taifex_prod_id


def _report_rows():
    def row(contract, month, session, last="2380", volume="10"):
        return {
            "Date": "20260916", "Contract": contract, "ContractMonth(Week)": month,
            "Open": last, "High": last, "Low": last, "Last": last, "Change": "0",
            "%": "0.00%", "Volume": volume, "SettlementPrice": last,
            "OpenInterest": "100", "BestBid": last, "BestAsk": last,
            "HistoricalHigh": last, "HistoricalLow": last, "TradingHalt": "",
            "TradingSession": session,
            "Volume(ExecutionsAmongSpreadOrderAndSingleOrderOnly)": "",
        }
    return [
        row("CDF", "202609", "一般"),
        row("CDF", "202609", "盤後"),
        row("CDF", "202609/202610", "一般", last="21"),
        row("TX", "202610", "一般", last="46078"),
        row("MTX", "202609W4", "一般", last="45758"),
    ]


def _adjusted_rows():
    return [
        {"Date": "20260916", "Contract": "CBA", "StockCode": "2002", "ContractName": "中鋼",
         "TWSEStock/TPExStock/TWSEETF": "上市普通股", "Futures/Options": "股票選擇權",
         "UnderlyingSecurityShares": "2000", "UnderlyingSecurityDistributionofCashDividends": "300",
         "UnderlyingSecurityFairValueOfPreemptiveRightsToParticipateInCashCapitalIncrease": "0",
         "DeliveryMonths": "202609"},
        {"Date": "20260916", "Contract": "CM1", "StockCode": "2330", "ContractName": "台積電",
         "TWSEStock/TPExStock/TWSEETF": "上市普通股", "Futures/Options": "股票期貨",
         "UnderlyingSecurityShares": "2114", "UnderlyingSecurityDistributionofCashDividends": "0",
         "UnderlyingSecurityFairValueOfPreemptiveRightsToParticipateInCashCapitalIncrease": "0",
         "DeliveryMonths": "202609,202610"},
    ]


PAUSE_HEADER = "商品代碼,標的證券代號,標的簡稱,標的類別,商品類別,存續到期月份,暫停交易生效日期,"


def _pause_csv(rows=()):
    lines = ["2026/09/17,", PAUSE_HEADER, *rows]
    return ("\r\n".join(lines) + "\r\n").encode("cp950")


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_TAIFEX_CACHE_PATH", str(tmp_path))
    return tmp_path


def test_prod_id_mapping():
    assert taifex_prod_id("CDF", "202609") == "CDFI6"
    assert taifex_prod_id("CDF", "202703") == "CDFC7"
    assert taifex_prod_id("TX", "202610") == "TXFJ6"
    assert taifex_prod_id("MTX", "202612") == "MXFL6"
    assert taifex_prod_id("TE", "202609") == "EXFI6"
    assert taifex_prod_id("TF", "202609") == "FXFI6"
    assert taifex_prod_id("MTX", "202609W4") == "MX4I6"
    assert taifex_prod_id("CM1", "202609") == "CM1I6"
    assert taifex_prod_id("CDF", "202609/202610") is None


def test_daily_report_keeps_regular_session_and_derives_prod_id(cache_dir):
    wrapper = TaifexWrapper(fetch_json=lambda url: _report_rows())
    frame = wrapper.get_daily_market_report_fut()

    assert (frame["TradingSession"] == "一般").all()
    assert len(frame) == 4
    assert frame["prod_id"].tolist() == ["CDFI6", None, "TXFJ6", "MX4I6"]
    assert all(isinstance(v, str) for v in frame["Last"])  # published strings, never floats
    assert (cache_dir / "daily_market_report_fut" / "20260916.json").is_file()


def test_daily_report_cached_day_is_served_offline(cache_dir):
    TaifexWrapper(fetch_json=lambda url: _report_rows()).get_daily_market_report_fut()

    def refuse(url):
        raise AssertionError("network touched on a cached day")

    frame = TaifexWrapper(fetch_json=refuse).get_daily_market_report_fut("2026-09-16")
    assert len(frame) == 4


def test_daily_report_for_a_day_the_endpoint_no_longer_serves_raises(cache_dir):
    wrapper = TaifexWrapper(fetch_json=lambda url: _report_rows())
    with pytest.raises(FileNotFoundError, match="20260915"):
        wrapper.get_daily_market_report_fut("20260915")
    # The live day was still cached on the way.
    assert (cache_dir / "daily_market_report_fut" / "20260916.json").is_file()


def test_adjusted_info_keeps_stock_futures_only(cache_dir):
    wrapper = TaifexWrapper(fetch_json=lambda url: _adjusted_rows())
    frame = wrapper.get_ssf_adjusted_info()
    assert frame["Contract"].tolist() == ["CM1"]
    assert frame.loc[0, "UnderlyingSecurityShares"] == "2114"
    both = wrapper.get_ssf_adjusted_info("20260916", futures_only=False)
    assert len(both) == 2


def test_trade_pause_parses_cp950_with_as_of_date(cache_dir):
    payload = _pause_csv([
        "CM1,2330,台積電,上市普通股,股票期貨,202609,2026/09/18,",
        "CBA,2002,中鋼,上市普通股,股票選擇權,202609,2026/09/18,",
    ])
    wrapper = TaifexWrapper(fetch_bytes=lambda url: payload)

    as_of, frame = wrapper.get_ssf_trade_pause()

    assert as_of == dt.date(2026, 9, 17)
    assert frame["商品代碼"].tolist() == ["CM1"]
    assert frame.loc[0, "暫停交易生效日期"] == "2026/09/18"
    assert list(frame.columns) == PAUSE_HEADER.rstrip(",").split(",")
    assert (cache_dir / "ssf_trade_pause" / "20260917.csv").read_bytes() == payload


def test_trade_pause_empty_table_is_an_empty_frame(cache_dir):
    wrapper = TaifexWrapper(fetch_bytes=lambda url: _pause_csv())
    as_of, frame = wrapper.get_ssf_trade_pause()
    assert as_of == dt.date(2026, 9, 17)
    assert frame.empty
    assert "商品類別" in frame.columns


def test_trade_pause_cached_day_is_served_offline(cache_dir):
    TaifexWrapper(fetch_bytes=lambda url: _pause_csv()).get_ssf_trade_pause()

    def refuse(url):
        raise AssertionError("network touched on a cached day")

    as_of, _ = TaifexWrapper(fetch_bytes=refuse).get_ssf_trade_pause("2026-09-17")
    assert as_of == dt.date(2026, 9, 17)


def test_cached_json_is_the_raw_payload(cache_dir):
    rows = _report_rows()
    TaifexWrapper(fetch_json=lambda url: rows).get_daily_market_report_fut()
    stored = json.loads((cache_dir / "daily_market_report_fut" / "20260916.json").read_text("utf-8"))
    assert stored == rows
