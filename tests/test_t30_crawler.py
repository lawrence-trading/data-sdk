"""Offline tests for T30Crawler's parser (no network)."""
from decimal import Decimal

import pytest

from data_sdk.crawlers.t30_crawler import RECORD_LEN, T30Crawler, T30Record

# Two real records from 2026-10-07.
TSMC_BYTES = (b"2330  02840000002585000002330000020261006000024  1" + "台積電".encode("cp950")
              + b" " * 10 + b"00000000000000000010X0" + b" " * 12)
WARRANT_BYTES = (b"71000000010900000008500000006100020261006000000W20"
                 + "精材群益5C購02".encode("cp950")
                 + b"  " + b"000000000000000000000 0" + b" " * 11)


def test_parses_a_listed_stock():
    record = T30Crawler().parse_record(TSMC_BYTES, "TSE")
    assert record == T30Record(
        "2330", "TSE", "stock", Decimal("2840"), Decimal("2585"), Decimal("2330"),
        "20261006", "0", "0", "0", "0", "24", "  ", "1", "台積電",
        0, 0, 0, 0, "1", "", "0", "X", "0",
    )


def test_parses_an_otc_warrant_with_the_shifted_tail():
    record = T30Crawler().parse_record(WARRANT_BYTES, "OTC")
    assert record == T30Record(
        "710000", "OTC", "warrant", Decimal("10.9"), Decimal("8.5"), Decimal("6.1"),
        "20261006", "0", "0", "0", "0", "00", "W2", "0", "精材群益5C購02",
        0, 0, 0, 0, "0", "0", "0", " ", "0",
    )


@pytest.mark.parametrize("code, sec_type, kind", [
    ("0050", "  ", "etf"),
    ("00400A", "  ", "etf"),
    ("2881A", "  ", "other"),
    ("7610", "  ", "stock"),
    ("03300T", "W4", "warrant"),
])
def test_classify(code, sec_type, kind):
    assert T30Crawler.classify(code, sec_type) == kind


def test_parse_accepts_newline_separated_records():
    content = TSMC_BYTES + b"\r\n" + TSMC_BYTES + b"\n"
    assert [r.code for r in T30Crawler().parse(content, "TSE")] == ["2330", "2330"]


def test_parse_rejects_a_truncated_file():
    with pytest.raises(ValueError):
        T30Crawler().parse(TSMC_BYTES[:RECORD_LEN - 1], "TSE")
