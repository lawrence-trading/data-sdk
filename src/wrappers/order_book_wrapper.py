from pathlib import Path
from typing import Optional, Union

import pandas as pd
import polars as pl

from .paths import env_path

_ORDER_BOOK_MODES = ("native", "tej", "merged")


def _order_book_base_path() -> Path:
    return env_path("DATA_SDK_ORDER_BOOK_PARQUET_PATH")


def _tej_order_book_base_path() -> Path:
    return env_path("DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH")


def _stocks_lazy(day_plain: str, is_twse: bool, mode: str) -> pl.LazyFrame:
    """Scan one day of regular-lot Format 6 from the native capture, the TEJ conversion, or both.

    merged: TEJ rows plus the native rows whose transmission_number TEJ lacks, in
    transmission_number order. With only one file present that file is returned; with
    neither, the native scan is returned so the FileNotFoundError matches mode="native".
    """
    name = f"{'twseudp' if is_twse else 'otcudp'}_{day_plain}_0825_all.parquet"
    if mode == "native":
        return pl.scan_parquet(str(_order_book_base_path() / name))
    tej_path = _tej_order_book_base_path() / name
    if mode == "tej":
        return pl.scan_parquet(str(tej_path))

    native_path = _order_book_base_path() / name
    if not native_path.exists():
        return pl.scan_parquet(str(tej_path if tej_path.exists() else native_path))
    # Native files before 2026-02-23 store cumulative_volume as int32; TEJ files use int64.
    native = pl.scan_parquet(str(native_path)).with_columns(pl.col("cumulative_volume").cast(pl.Int64))
    if not tej_path.exists():
        return native
    key = "transmission_number"
    # A second scan of the TEJ file for the keys: reusing the first one makes polars cache
    # the shared scan, and a caller's filter then no longer reaches the TEJ parquet.
    tej_keys = pl.scan_parquet(str(tej_path), cache=False).select(key)
    native_only = native.join(tej_keys, on=key, how="anti").sort(key)
    return pl.scan_parquet(str(tej_path)).merge_sorted(native_only, key=key)


def _format_match_time(df_lazy: pl.LazyFrame) -> pl.LazyFrame:
    mt = (
        pl.col("match_time")
        .cast(pl.Int64)
        .cast(pl.Utf8)
        .str.zfill(12)
    )
    return df_lazy.with_columns(
        pl.concat_str(
            [
                mt.str.slice(0, 2), pl.lit(":"),
                mt.str.slice(2, 2), pl.lit(":"),
                mt.str.slice(4, 2), pl.lit("."),
                mt.str.slice(6),
            ]
        ).alias("match_time")
    )

def get_order_book_stocks(
    day: str,
    is_twse: bool,
    sid: Optional[str] = None,
    lazy: bool = False,
    format_match_time: bool = True,
    mode: str = "merged",
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load order book (regular lots) from parquet for a given day, optionally filtered by stock.

    Native parquet path is taken from DATA_SDK_ORDER_BOOK_PARQUET_PATH (e.g. /mnt/nfs/backup/parquets);
    TEJ parquet path from DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH (default /mnt/nfs/backup/tej_parquets).
    Files are named twseudp_YYYYMMDD_0825_all.parquet (TWSE) or otcudp_YYYYMMDD_0825_all.parquet (OTC)
    in both directories, with the same 36 columns.

    Args:
        day: Trading day, e.g. "2026-02-11" or "20260211".
        is_twse: True for TWSE, False for OTC.
        sid: Stock code. If None, returns the entire day for all stocks.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.
        format_match_time: If True, rewrite match_time to "HH:MM:SS.ffffff". Pass False to
            keep the on-disk Int64 (HHMMSSffffff), which a filter can push down to the
            parquet row-group statistics; the formatted column is computed, so a predicate
            on it forces a full scan.
        mode: "native" reads the native capture only (the behaviour before this option).
            "tej" reads the TEJ conversion only (2020-06-01 .. 2026-06-30; timestamp is null).
            "merged" (default) reads both and joins them on transmission_number: TEJ rows,
            plus native rows whose transmission_number TEJ lacks, in transmission_number
            order. Rows with the same transmission_number are taken to be the same message.

    Returns:
        pandas DataFrame or polars LazyFrame. match_time is formatted as HH:MM:SS.ffffff
        unless format_match_time is False, in which case it stays the raw Int64.
    """
    if mode not in _ORDER_BOOK_MODES:
        raise ValueError(f"mode must be one of {_ORDER_BOOK_MODES}, got {mode!r}")
    df_lazy = _stocks_lazy(day.replace("-", ""), is_twse, mode)
    if sid is not None:
        df_lazy = df_lazy.filter(pl.col("stock_code") == sid)
    if format_match_time:
        df_lazy = _format_match_time(df_lazy)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()


def get_order_book_odd_lots(
    day: str,
    is_twse: bool,
    sid: Optional[str] = None,
    lazy: bool = False,
    format_match_time: bool = True,
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load order book (odd lots) from parquet for a given day, optionally filtered by stock.

    Parquet path is taken from DATA_SDK_ORDER_BOOK_PARQUET_PATH.
    Files are named twse_ip5_YYYYMMDD_0825_all.parquet (TWSE) or otc_ip5_YYYYMMDD_0825_all.parquet (OTC).

    Args:
        day: Trading day, e.g. "2026-02-11" or "20260211".
        is_twse: True for TWSE, False for OTC.
        sid: Stock code. If None, returns the entire day for all stocks.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.
        format_match_time: If True, rewrite match_time to "HH:MM:SS.ffffff". Pass False to
            keep the on-disk Int64 (HHMMSSffffff), which a filter can push down to the
            parquet row-group statistics; the formatted column is computed, so a predicate
            on it forces a full scan.

    Returns:
        pandas DataFrame or polars LazyFrame. match_time is formatted as HH:MM:SS.ffffff
        unless format_match_time is False, in which case it stays the raw Int64.
    """
    base = _order_book_base_path()
    day_plain = day.replace("-", "")
    prefix = "twse_ip5" if is_twse else "otc_ip5"
    path = base / f"{prefix}_{day_plain}_0825_all.parquet"

    df_lazy = pl.scan_parquet(str(path))
    if sid is not None:
        df_lazy = df_lazy.filter(pl.col("stock_code") == sid)
    if format_match_time:
        df_lazy = _format_match_time(df_lazy)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()


def get_order_book_warrant(
    day: str,
    is_twse: bool,
    sid: Optional[str] = None,
    lazy: bool = False,
    format_match_time: bool = True,
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load order book (warrant) from parquet for a given day, optionally filtered by warrant code.

    Parquet path is taken from DATA_SDK_ORDER_BOOK_PARQUET_PATH.
    Files are named twse_warranty_YYYYMMDD_0825_all.parquet (TWSE) or otc_warranty_YYYYMMDD_0825_all.parquet (OTC).

    Args:
        day: Trading day, e.g. "2026-02-11" or "20260211".
        is_twse: True for TWSE, False for OTC.
        sid: Warrant code. If None, returns the entire day for all warrants.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.
        format_match_time: If True, rewrite match_time to "HH:MM:SS.ffffff". Pass False to
            keep the on-disk Int64 (HHMMSSffffff), which a filter can push down to the
            parquet row-group statistics; the formatted column is computed, so a predicate
            on it forces a full scan.

    Returns:
        pandas DataFrame or polars LazyFrame. match_time is formatted as HH:MM:SS.ffffff
        unless format_match_time is False, in which case it stays the raw Int64.
    """
    base = _order_book_base_path()
    day_plain = day.replace("-", "")
    prefix = "twse_warranty" if is_twse else "otc_warranty"
    path = base / f"{prefix}_{day_plain}_0825_all.parquet"

    df_lazy = pl.scan_parquet(str(path))
    if sid is not None:
        df_lazy = df_lazy.filter(pl.col("stock_code") == sid)
    if format_match_time:
        df_lazy = _format_match_time(df_lazy)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()