"""The CSV form of TAIFEX's OpenAPI endpoints, read against the real recorded answer: no network.

``fixtures/taifex/daily_market_report_fut_20261030.csv`` is the answer of ``DailyMarketReportFut`` recorded in
full on 2026-11-01 (``fixtures/taifex/NOTE.md``): a UTF-8 CSV with a BOM and Chinese headers, 2,150 rows of the
trading day 2026-10-30, where the endpoint had answered a JSON array before. The wrapper must hand its callers the
same rows either way, every cell the published string, and keep the day handling it has: a fetch is stored under the
day the payload carries and a read for a specific day answers from that store or not at all.
"""

import csv
import io
import json
from pathlib import Path

import pytest

from data_sdk import TaifexResponseError, TaifexWrapper
from data_sdk.wrappers import taifex_wrapper as module

FIXTURES = Path(__file__).parent / "fixtures" / "taifex"
RAW = (FIXTURES / "daily_market_report_fut_20261030.csv").read_bytes()

FUT = module.DAILY_MARKET_REPORT_FUT_URL
OPT = module.DAILY_MARKET_REPORT_OPT_URL
SSF = module.SSF_ADJUSTED_INFO_URL

# The keys of the JSON form, in its order: what the wrapper documents.
JSON_KEYS = [
    "Date", "Contract", "ContractMonth(Week)", "Open", "High", "Low", "Last", "Change", "%", "Volume",
    "SettlementPrice", "OpenInterest", "BestBid", "BestAsk", "HistoricalHigh", "HistoricalLow", "TradingHalt",
    "TradingSession", "Volume(ExecutionsAmongSpreadOrderAndSingleOrderOnly)",
]

# Lines 2 and 3 of the recorded file, as the JSON form would carry them.
FIRST_ROW = {
    "Date": "20261030", "Contract": "ZFF", "ContractMonth(Week)": "202611", "Open": "3788", "High": "3790.2",
    "Low": "3741.8", "Last": "3780.6", "Change": "-19.6", "%": "-0.52%", "Volume": "270", "SettlementPrice": "3782",
    "OpenInterest": "447", "BestBid": "3780.8", "BestAsk": "3788.8", "HistoricalHigh": "3816.2",
    "HistoricalLow": "3508.2", "TradingHalt": "", "TradingSession": "一般",
    "Volume(ExecutionsAmongSpreadOrderAndSingleOrderOnly)": "",
}
SECOND_ROW = {
    **FIRST_ROW, "Open": "3794.2", "High": "3804.8", "Low": "3793.8", "Last": "3793.8", "Change": "-6.4",
    "%": "-0.17%", "Volume": "27", "SettlementPrice": "NULL", "OpenInterest": "-", "BestBid": "3793.6",
    "BestAsk": "3799.2", "TradingSession": "盤後",
}

OLD_JSON_DAY = [
    {**FIRST_ROW, "Date": "20261029", "Contract": "TX", "ContractMonth(Week)": "202611", "SettlementPrice": "45700"},
    {**FIRST_ROW, "Date": "20261029", "Contract": "TX", "ContractMonth(Week)": "202611", "SettlementPrice": "NULL",
     "TradingSession": "盤後"},
]


class FakeResponse:
    def __init__(self, content, content_type):
        self.content = content
        self.headers = {"Content-Type": content_type} if content_type else {}

    def raise_for_status(self):
        return None


@pytest.fixture
def cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_TAIFEX_CACHE_PATH", str(tmp_path))
    return tmp_path


@pytest.fixture
def serve(monkeypatch):
    """Make the transport's ``requests.get`` answer ``body`` for every URL; returns the list of URLs asked."""
    asked = []

    def install(body, content_type="application/octet-stream"):
        def get(url, timeout):
            asked.append(url)
            return FakeResponse(body, content_type)

        monkeypatch.setattr("requests.get", get)
        return asked

    return install


def test_the_transport_reads_the_recorded_csv_as_the_rows_of_the_json_form(serve):
    serve(RAW)
    rows = TaifexWrapper._http_json(FUT)

    assert len(rows) == 2150
    assert list(rows[0]) == JSON_KEYS
    assert rows[0] == FIRST_ROW
    assert rows[1] == SECOND_ROW
    assert all(list(row) == JSON_KEYS for row in rows)
    assert {row["Date"] for row in rows} == {"20261030"}
    assert all(isinstance(value, str) for row in rows for value in row.values())  # published strings, never floats


def test_every_cell_of_the_recorded_csv_comes_through_as_published(serve):
    serve(RAW)
    rows = TaifexWrapper._http_json(FUT)
    published = list(csv.reader(io.StringIO(RAW.decode("utf-8-sig"), newline="")))[1:]

    assert [list(row.values()) for row in rows] == published
    assert any(row["SettlementPrice"] == "NULL" for row in rows) and any(row["Last"] == "-" for row in rows)
    assert any(row["%"].endswith("%") for row in rows)


def test_the_regular_session_frame_is_the_same_whichever_form_the_endpoint_answered(serve, cache_dir):
    serve(RAW)
    frame = TaifexWrapper().get_daily_market_report_fut()

    assert (frame["TradingSession"] == "一般").all() and len(frame) == 1969
    assert list(frame.columns) == [*JSON_KEYS, "prod_id"]
    month = frame[(frame["Contract"] == "TX") & (frame["ContractMonth(Week)"] == "202611")]
    assert month["SettlementPrice"].tolist() == ["45300"] and month["prod_id"].tolist() == ["TXFK6"]
    spread = frame[frame["ContractMonth(Week)"].str.contains("/")]
    assert len(spread) > 0 and spread["prod_id"].isna().all()
    assert (cache_dir / "daily_market_report_fut" / "20261030.json").is_file()


def test_the_stored_file_is_the_rows_as_json(serve, cache_dir):
    serve(RAW)
    TaifexWrapper().get_daily_market_report_fut()

    stored = json.loads((cache_dir / "daily_market_report_fut" / "20261030.json").read_text("utf-8"))
    assert len(stored) == 2150 and stored[0] == FIRST_ROW and stored[1] == SECOND_ROW


def test_a_stored_csv_day_is_served_offline_and_a_json_day_from_before_still_reads(serve, cache_dir):
    asked = serve(RAW)
    wrapper = TaifexWrapper()
    wrapper.get_daily_market_report_fut()
    (cache_dir / "daily_market_report_fut" / "20261029.json").write_text(json.dumps(OLD_JSON_DAY), encoding="utf-8")
    asked.clear()

    csv_day = wrapper.get_daily_market_report_fut("2026-10-30")
    json_day = wrapper.get_daily_market_report_fut("20261029")

    assert asked == []  # both answered from the store
    assert len(csv_day) == 1969
    assert json_day["SettlementPrice"].tolist() == ["45700"] and json_day["prod_id"].tolist() == ["TXFK6"]


def test_a_day_the_csv_does_not_carry_raises_and_the_day_it_carries_is_stored(serve, cache_dir):
    serve(RAW)
    with pytest.raises(FileNotFoundError, match="20261029") as exc_info:
        TaifexWrapper().get_daily_market_report_fut("2026-10-29")

    assert "20261030" in str(exc_info.value)
    assert (cache_dir / "daily_market_report_fut" / "20261030.json").is_file()
    assert not (cache_dir / "daily_market_report_fut" / "20261029.json").exists()


def test_an_unreadable_answer_falls_back_to_the_latest_stored_day(serve, cache_dir):
    (cache_dir / "daily_market_report_fut").mkdir()
    (cache_dir / "daily_market_report_fut" / "20261029.json").write_text(json.dumps(OLD_JSON_DAY), encoding="utf-8")
    asked = serve(b"<html><body>502 Bad Gateway</body></html>", "text/html")

    frame = TaifexWrapper().get_daily_market_report_fut()

    assert len(asked) == 2  # the request and its one retry
    assert frame["Date"].tolist() == ["20261029"]


def test_an_unreadable_answer_with_nothing_stored_names_the_endpoint_the_content_type_and_the_body(serve, cache_dir):
    serve(b"<html><body>502 Bad Gateway</body></html>", "text/html")

    with pytest.raises(RuntimeError) as exc_info:
        TaifexWrapper().get_daily_market_report_fut()

    message = str(exc_info.value)
    assert "daily_market_report_fut" in message and FUT in message
    assert "neither JSON nor its CSV form" in message and "text/html" in message and "502 Bad Gateway" in message


def test_a_csv_that_lost_a_required_column_is_refused_naming_it(serve):
    serve(RAW.replace("結算價".encode("utf-8"), "清算價".encode("utf-8"), 1))

    with pytest.raises(TaifexResponseError, match="SettlementPrice") as exc_info:
        TaifexWrapper._http_json(FUT)
    assert "neither JSON nor its CSV form" in str(exc_info.value) and FUT in str(exc_info.value)


def test_a_row_with_a_missing_field_is_refused_not_read_with_shifted_columns(serve):
    lines = RAW.split(b"\r\n")
    lines[2] = lines[2].replace(b",-0.17%", b"", 1)  # one field fewer on line 3
    serve(b"\r\n".join(lines))

    with pytest.raises(TaifexResponseError, match=r"line 3 has 18 fields, its header 19"):
        TaifexWrapper._http_json(FUT)


def test_a_row_with_a_filled_extra_field_is_refused_not_read_with_a_dropped_cell(serve):
    lines = RAW.split(b"\r\n")
    lines[2] = lines[2] + b",45300"  # one field more on line 3, and not an empty one
    serve(b"\r\n".join(lines))

    with pytest.raises(TaifexResponseError, match=r"line 3 has 20 fields, its header 19"):
        TaifexWrapper._http_json(FUT)


def test_a_cell_beyond_the_csv_field_size_limit_is_refused_as_a_taifex_response_error(serve):
    header = RAW.split(b"\r\n")[0]
    serve(header + b"\r\n20261030,TX," + b"9" * (csv.field_size_limit() + 1) + b"\r\n")

    with pytest.raises(TaifexResponseError, match="neither JSON nor its CSV form") as exc_info:
        TaifexWrapper._http_json(FUT)
    assert "field larger than field limit" in str(exc_info.value)


@pytest.mark.parametrize("payload", [b"", "﻿".encode("utf-8"), b"\r\n"], ids=["empty", "bom only", "blank line"])
def test_a_payload_without_a_header_line_is_refused(payload):
    with pytest.raises(TaifexResponseError, match="no header line"):
        TaifexWrapper.parse_openapi_csv(payload, {})


def test_an_empty_answer_is_refused_naming_the_endpoint(serve):
    serve(b"", "text/plain")

    with pytest.raises(TaifexResponseError, match="no header line") as exc_info:
        TaifexWrapper._http_json(FUT)
    assert FUT in str(exc_info.value) and "text/plain" in str(exc_info.value)


def test_a_csv_that_is_not_utf8_is_refused(serve):
    serve(RAW.decode("utf-8-sig").encode("cp950"))

    with pytest.raises(TaifexResponseError, match="neither JSON nor its CSV form"):
        TaifexWrapper._http_json(FUT)


def test_trailing_empty_cells_blank_lines_and_a_missing_last_line_end_are_tolerated():
    payload = "日期,契約,新欄位,\r\n20261030,TX,a b ,,\r\n\r\n20261030,MTX,,,"
    rows = TaifexWrapper.parse_openapi_csv(payload.encode("utf-8"), {"日期": "Date", "契約": "Contract"})

    assert rows == [
        {"Date": "20261030", "Contract": "TX", "新欄位": "a b "},  # an unknown header keeps its text; cells are not stripped
        {"Date": "20261030", "Contract": "MTX", "新欄位": ""},
    ]


def test_a_header_cell_with_spaces_around_it_still_maps_to_its_key():
    payload = " 日期 ,契約代號\t, 新欄位 ,\r\n20261030,TX, a b ,\r\n".encode("utf-8")
    rows = TaifexWrapper.parse_openapi_csv(payload, {"日期": "Date", "契約代號": "Contract"}, ("Date", "Contract"))

    assert rows == [{"Date": "20261030", "Contract": "TX", "新欄位": " a b "}]  # the header is stripped, the cells are not


def test_a_repeated_key_is_refused():
    payload = "日期,Date\r\n20261030,20261030\r\n".encode("utf-8")
    with pytest.raises(TaifexResponseError, match="repeats"):
        TaifexWrapper.parse_openapi_csv(payload, {"日期": "Date"})


def test_a_json_answer_reads_with_or_without_a_byte_order_mark(serve):
    body = json.dumps(OLD_JSON_DAY, ensure_ascii=False).encode("utf-8")
    serve(body, "application/json")
    assert TaifexWrapper._http_json(FUT) == OLD_JSON_DAY
    serve(b"\xef\xbb\xbf" + body, "application/json")
    assert TaifexWrapper._http_json(FUT) == OLD_JSON_DAY


@pytest.mark.parametrize("url", [OPT, SSF])
def test_the_sibling_endpoints_keep_reading_json(serve, url):
    rows = [{"Date": "20261030", "Contract": "TXO", "TradingSession": "一般"}]
    serve(json.dumps(rows, ensure_ascii=False).encode("utf-8"), "application/json")
    assert TaifexWrapper._http_json(url) == rows


@pytest.mark.parametrize("url", [OPT, SSF])
def test_the_sibling_endpoints_have_no_csv_reader_so_a_csv_from_them_is_refused_as_before(serve, url):
    serve(RAW)
    with pytest.raises(TaifexResponseError, match="did not return JSON") as exc_info:
        TaifexWrapper._http_json(url)
    assert url in str(exc_info.value) and "application/octet-stream" in str(exc_info.value)
