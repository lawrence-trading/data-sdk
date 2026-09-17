"""i025 (day high / low) is a first-class FOP parquet format."""

import os

import polars as pl
import pytest

from data_sdk import get_fop_parquet
from data_sdk.wrappers.fop_wrapper import _FOP_FORMATS

ARCHIVE = "/mnt/nfs/backup/fop_parquets"
ARCHIVED_DAY = "2026-07-31"


def test_i025_is_an_accepted_format():
    assert "i025" in _FOP_FORMATS
    assert _FOP_FORMATS.index("i024") < _FOP_FORMATS.index("i025") < _FOP_FORMATS.index("i081")


def test_i025_loads_from_a_synthetic_archive(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_FOP_PARQUET_PATH", str(tmp_path))
    frame = pl.DataFrame({
        "prod_id": ["CDFI6", "CDFI6", "TXFI6"],
        "prod_msg_seq": [3, 7, 2],
        "info_time": ["08:45:00.051000"] * 3,
        "day_high_price_sign": ["0", "0", "0"],
        "day_high_price_raw": [239000, 239500, 4609300],
        "day_low_price_sign": ["0", "0", "0"],
        "day_low_price_raw": [237500, 237500, 4580000],
        "show_time": ["08:45:00.049000"] * 3,
    })
    frame.write_parquet(tmp_path / "futures_day_20260101_0840_i025.parquet")

    df = get_fop_parquet("2026-01-01", "futures", "i025")
    assert len(df) == 3
    assert df["day_high_price_raw"].max() == 4609300

    lazy = get_fop_parquet("20260101", "futures", "i025", instrument="CDFI6", lazy=True)
    assert lazy.collect()["prod_msg_seq"].to_list() == [3, 7]


def test_i025_missing_file_raises_with_path(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_SDK_FOP_PARQUET_PATH", str(tmp_path))
    with pytest.raises(FileNotFoundError, match="i025"):
        get_fop_parquet("2026-01-01", "futures", "i025")


@pytest.mark.skipif(
    not os.path.isfile(f"{ARCHIVE}/futures_day_20260731_0840_i025.parquet"),
    reason="NFS archive not mounted",
)
def test_i025_loads_from_the_nfs_archive(monkeypatch):
    monkeypatch.delenv("DATA_SDK_FOP_PARQUET_PATH", raising=False)
    lazy = get_fop_parquet(ARCHIVED_DAY, "futures", "i025", lazy=True)
    head = lazy.head(100).collect()
    assert head.height == 100
    for column in ("prod_id", "prod_msg_seq", "day_high_price_raw", "day_low_price_raw"):
        assert column in head.columns
