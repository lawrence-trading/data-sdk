import os
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import numpy as np
import pandas as pd
import polars as pl
import pyarrow.parquet as pq

from .paths import env_path

_ORDER_BOOK_MODES = ("native", "tej", "merged")
_KEY = "transmission_number"
_NATIVE_ONLY_ROWS: Dict[Tuple, pl.DataFrame] = {}  # per (native file, tej file): native rows TEJ lacks


def _order_book_base_path() -> Path:
    return env_path("DATA_SDK_ORDER_BOOK_PARQUET_PATH")


def _tej_order_book_base_path() -> Path:
    return env_path("DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH")


def _native_only_rows(native_path: Path, tej_path: Path) -> pl.DataFrame:
    """Native rows whose transmission_number is absent from the TEJ file.

    Both files are in transmission_number order and TEJ numbers run from 1 with few
    gaps, so these are the native rows outside TEJ's range or inside one of its gaps.
    There are at most a few thousand per day; they are read once and cached per file
    pair (path, size, mtime) for the life of the process.
    """
    ns, ts = os.stat(native_path), os.stat(tej_path)
    stamp = (str(native_path), ns.st_size, ns.st_mtime_ns, str(tej_path), ts.st_size, ts.st_mtime_ns)
    if stamp not in _NATIVE_ONLY_ROWS:
        tej_keys = pl.scan_parquet(str(tej_path)).select(_KEY).collect()[_KEY].to_numpy()
        steps = np.nonzero(np.diff(tej_keys) > 1)[0]
        gaps = np.concatenate([np.arange(tej_keys[i] + 1, tej_keys[i + 1]) for i in steps]) if len(steps) else []
        missing = (pl.col(_KEY) < int(tej_keys[0])) | (pl.col(_KEY) > int(tej_keys[-1]))
        if len(gaps):
            missing = missing | pl.col(_KEY).is_in(pl.Series(gaps, dtype=pl.Int32).implode())
        _NATIVE_ONLY_ROWS[stamp] = pl.scan_parquet(str(native_path)).filter(missing).collect()
    return _NATIVE_ONLY_ROWS[stamp]


def _stocks_lazy(day_plain: str, is_twse: bool, mode: str) -> pl.LazyFrame:
    """Scan one day of regular-lot Format 6 from the native capture, the TEJ conversion, or both.

    merged: the TEJ file plus the native rows whose transmission_number TEJ lacks, in
    transmission_number order (rows with the same transmission_number are the same
    message in both files). Both files must exist, otherwise FileNotFoundError is
    raised here, before any scan is built.
    """
    name = f"{'twseudp' if is_twse else 'otcudp'}_{day_plain}_0825_all.parquet"
    if mode == "native":
        return pl.scan_parquet(str(_order_book_base_path() / name))
    tej_path = _tej_order_book_base_path() / name
    if mode == "tej":
        return pl.scan_parquet(str(tej_path))

    native_path = _order_book_base_path() / name
    missing = [str(p) for p in (native_path, tej_path) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"mode='merged' needs both files; missing: {', '.join(missing)}")
    if pq.read_metadata(str(tej_path)).num_rows == 0:  # an empty TEJ day adds nothing
        return pl.scan_parquet(str(native_path))
    tej = pl.scan_parquet(str(tej_path))
    extra = _native_only_rows(native_path, tej_path)
    if extra.is_empty():
        return tej
    return pl.concat([tej, extra.lazy()], how="vertical_relaxed").sort(_KEY)


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
    TEJ parquet path from DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH (e.g. /mnt/nfs/backup/tej_parquets).
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
            "merged" (default) reads both: the TEJ file plus the native rows whose
            transmission_number TEJ lacks, in transmission_number order. Rows with the
            same transmission_number are taken to be the same message. Both files must
            exist; a day present in only one source raises FileNotFoundError at the
            call, so use "native" or "tej" for days outside the overlap.

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