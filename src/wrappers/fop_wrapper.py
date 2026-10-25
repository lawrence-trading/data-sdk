from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd
import polars as pl

from .paths import env_path

_FOP_TYPES = {
    "futures": "futures",
    "future": "futures",
    "options": "options",
    "option": "options",
}
_FOP_FORMATS = ("i024", "i081", "i083", "i084")
_FOP_SESSION_TOKENS = {"day": "0840", "night": "1455"}


def _fop_base_path() -> Path:
    return env_path("DATA_SDK_FOP_PARQUET_PATH")


def get_fop_parquet(
    day: str,
    type: str,
    format: str,
    session: str = "day",
    instrument: Optional[str] = None,
    lazy: bool = False,
) -> Union[pd.DataFrame, pl.LazyFrame]:
    """Load a raw TAIFEX futures/options (FOP) market-data parquet for a given day.

    Parquet path is taken from DATA_SDK_FOP_PARQUET_PATH (e.g. /mnt/nfs/backup/fop_parquets).
    Files are named {futures|options}_{day|night}_YYYYMMDD_{0840|1455}_{i024|i081|i083}.parquet;
    i084 files use a snapshot infix: {futures|options}_{day|night}_snapshot_YYYYMMDD_{0840|1455}_i084.parquet.

    Formats:
        i024: deal (match) messages; multi-level sweeps expand to one row per leg
              (match_index 0..total_match_count-1); total_qty/buy_cnt/sell_cnt are
              session-cumulative; calculated_flag '1' marks pre-open trial matches.
        i081: order-book deltas; update_action NEW/CHG/DEL on entry_type BID/ASK
              (price_level 1-5), OVE overlays the best implied level on
              IMPL_BID/IMPL_ASK; apply entries in (channel_seq, entry_index) order.
        i083: order-book snapshots broadcast around session open; the last snapshot
              per prod_id is the seed book for replaying i081.
        i084: snapshot-channel refresh cycles (REFRESH_BEGIN -> ORDER_DATA ->
              REFRESH_COMPLETE); last_seq is populated only on REFRESH_BEGIN and
              REFRESH_COMPLETE rows and refers to the realtime channel_seq; its own
              channel_seq resets every cycle. Only captured for recent dates
              (~2026-06-26 onward).

    Caveats:
        - The `timestamp` column is the producer's replay wall clock and is
          unreliable; use `info_time` / `match_time` (exchange clocks, "HH:MM:SS.ffffff").
        - Prices are `price_raw` (a raw integer), signed by `price_sign` ('-'
          means negative); there is no decimal scaling. The true decimal locator
          is per product (TAIFEX I010; e.g. TXF=2, GDF=3) and is not captured
          here, so rescale price_raw / 10**locator yourself when you need the
          product's natural units.
        - Rows are in arrival order without dedup; sort by (channel_seq, entry_index)
          defensively before sequential processing.
        - prod_msg_seq counts I024/I025/I081/I083 per product; I025 is not captured,
          so per-product sequence gaps are expected and usually benign.
        - i081/i084 files are large (750 MB - 1.1 GB); prefer lazy=True so filters
          and projections are pushed down to the parquet scan.

    Args:
        day: Trading day, e.g. "2026-07-03" or "20260703".
        type: "futures" or "options" (singular forms accepted).
        format: One of "i024", "i081", "i083", "i084".
        session: "day" (0840 files) or "night" (1455 files).
        instrument: Product id (prod_id), e.g. "TXFG6". If None, returns all products.
        lazy: If True, return a polars LazyFrame; otherwise collect and return pandas DataFrame.

    Returns:
        pandas DataFrame or polars LazyFrame with the raw parquet columns.
    """
    kind = _FOP_TYPES.get(type)
    if kind is None:
        raise ValueError(f"type must be one of {sorted(set(_FOP_TYPES.values()))}, got {type!r}")
    if format not in _FOP_FORMATS:
        raise ValueError(f"format must be one of {list(_FOP_FORMATS)}, got {format!r}")
    token = _FOP_SESSION_TOKENS.get(session)
    if token is None:
        raise ValueError(f"session must be one of {list(_FOP_SESSION_TOKENS)}, got {session!r}")

    day_plain = day.replace("-", "")
    infix = "_snapshot" if format == "i084" else ""
    path = _fop_base_path() / f"{kind}_{session}{infix}_{day_plain}_{token}_{format}.parquet"
    if not path.is_file():
        hint = (
            " (i084 snapshot captures only exist for recent dates, ~2026-06-26 onward)"
            if format == "i084"
            else ""
        )
        raise FileNotFoundError(f"FOP parquet not found: {path}{hint}")

    df_lazy = pl.scan_parquet(str(path))
    if instrument is not None:
        df_lazy = df_lazy.filter(pl.col("prod_id") == instrument)

    if lazy:
        return df_lazy
    return df_lazy.collect().to_pandas()


# --- Order-book reconstruction (i083/i084 snapshots + i081 deltas + i024 deals) ---

_FOP_BOOK_DEPTH = 5
_FOP_SOURCE_NAMES = {0: "i024", 1: "i081", 2: "i083", 3: "i084"}
_FOP_ACTION_CODES = {"NEW": 0, "CHG": 1, "DEL": 2, "OVE": 3}  # 4 = snapshot placement
_FOP_SIDE_CODES = {"BID": 0, "ASK": 1, "IMPL_BID": 2, "IMPL_ASK": 3}

# Unified per-entry event schema shared by the four staging helpers so that
# pl.concat(how="vertical") is exact.
_FOP_EVENT_COLUMNS = (
    "prod_msg_seq",  # i64
    "kind_rank",     # i8: 0 = realtime message (i024/i081/i083), 1 = i084 snapshot
    "snap_gid",      # i64: time-ordered i084 snapshot-group id (0 for realtime)
    "group_ord",     # i32: entry_index / md_index / match_index within the message
    "source_id",     # i8: 0=i024, 1=i081, 2=i083, 3=i084
    "action_code",   # i8: NEW=0, CHG=1, DEL=2, OVE=3, snapshot=4, none=-1
    "side_code",     # i8: BID=0, ASK=1, IMPL_BID=2, IMPL_ASK=3, none=-1
    "price_level",   # i32, -1 when absent
    "px",            # i64 signed raw price (price_sign applied to price_raw)
    "qty",           # i64
    "is_trial",      # bool (calculated_flag == '1' on i083/i024)
    "info_time",     # str
    "match_time",    # str, i024 only
    "total_qty",     # i64 session-cumulative volume, i024 only
)


def _fop_signed_px() -> pl.Expr:
    return (
        pl.col("price_raw").cast(pl.Int64).fill_null(0)
        * pl.when(pl.col("price_sign") == "-").then(-1).otherwise(1)
    ).alias("px")


def _fop_side_code() -> pl.Expr:
    return (
        pl.col("entry_type")
        .replace_strict(_FOP_SIDE_CODES, default=-1, return_dtype=pl.Int8)
        .fill_null(-1)
        .alias("side_code")
    )


def _stage_i081(day: str, type: str, session: str, instrument: str) -> pl.LazyFrame:
    lf = get_fop_parquet(day, type, "i081", session=session, instrument=instrument, lazy=True)
    lf = lf.select(
        "prod_msg_seq", "info_time", "entry_index", "update_action",
        "entry_type", "price_sign", "price_raw", "quantity", "price_level",
    )
    # Retransmitted messages repeat (prod_msg_seq, entry_index) verbatim: keep first.
    lf = lf.unique(subset=["prod_msg_seq", "entry_index"], keep="first", maintain_order=True)
    return lf.select(
        pl.col("prod_msg_seq").cast(pl.Int64).fill_null(0),
        pl.lit(0, pl.Int8).alias("kind_rank"),
        pl.lit(0, pl.Int64).alias("snap_gid"),
        pl.col("entry_index").cast(pl.Int32).fill_null(0).alias("group_ord"),
        pl.lit(1, pl.Int8).alias("source_id"),
        pl.col("update_action")
        .replace_strict(_FOP_ACTION_CODES, default=-1, return_dtype=pl.Int8)
        .fill_null(-1)
        .alias("action_code"),
        _fop_side_code(),
        pl.col("price_level").cast(pl.Int32).fill_null(-1),
        _fop_signed_px(),
        pl.col("quantity").cast(pl.Int64).fill_null(0).alias("qty"),
        pl.lit(False).alias("is_trial"),
        pl.col("info_time"),
        pl.lit(None, pl.String).alias("match_time"),
        pl.lit(None, pl.Int64).alias("total_qty"),
    )


def _stage_i083(day: str, type: str, session: str, instrument: str) -> pl.LazyFrame:
    lf = get_fop_parquet(day, type, "i083", session=session, instrument=instrument, lazy=True)
    lf = lf.select(
        "prod_msg_seq", "info_time", "calculated_flag", "entry_index",
        "entry_type", "price_sign", "price_raw", "quantity", "price_level",
    )
    lf = lf.unique(subset=["prod_msg_seq", "entry_index"], keep="first", maintain_order=True)
    return lf.select(
        pl.col("prod_msg_seq").cast(pl.Int64).fill_null(0),
        pl.lit(0, pl.Int8).alias("kind_rank"),
        pl.lit(0, pl.Int64).alias("snap_gid"),
        pl.col("entry_index").cast(pl.Int32).fill_null(0).alias("group_ord"),
        pl.lit(2, pl.Int8).alias("source_id"),
        pl.lit(4, pl.Int8).alias("action_code"),
        _fop_side_code(),
        pl.col("price_level").cast(pl.Int32).fill_null(-1),
        _fop_signed_px(),
        pl.col("quantity").cast(pl.Int64).fill_null(0).alias("qty"),
        (pl.col("calculated_flag") == "1").fill_null(False).alias("is_trial"),
        pl.col("info_time"),
        pl.lit(None, pl.String).alias("match_time"),
        pl.lit(None, pl.Int64).alias("total_qty"),
    )


def _stage_i084(day: str, type: str, session: str, instrument: str) -> Optional[pl.LazyFrame]:
    try:
        lf = get_fop_parquet(day, type, "i084", session=session, instrument=None, lazy=True)
    except FileNotFoundError:
        return None  # snapshot captures only exist for recent dates
    lf = lf.filter(
        (pl.col("prod_id") == instrument) & (pl.col("message_type") == "ORDER_DATA")
    )
    lf = lf.select(
        "prod_msg_seq", "info_time", "channel_seq", "entry_pos", "md_index",
        "entry_type", "price_sign", "price_raw", "quantity", "price_level",
    )
    # One snapshot group = one product block within one carousel cycle; its rows
    # are contiguous in the file (arrival order). channel_seq resets every cycle
    # and info_time strings wrap at midnight in night sessions, so neither can
    # order groups globally: keep file order and start a new group whenever
    # (info_time, channel_seq, entry_pos) changes.
    lf = lf.with_columns(
        (
            (pl.col("info_time") != pl.col("info_time").shift(1))
            | (pl.col("channel_seq") != pl.col("channel_seq").shift(1))
            | (pl.col("entry_pos") != pl.col("entry_pos").shift(1))
        )
        .fill_null(True)
        .cum_sum()
        .cast(pl.Int64)
        .alias("snap_gid")
    )
    return lf.select(
        pl.col("prod_msg_seq").cast(pl.Int64).fill_null(0),
        pl.lit(1, pl.Int8).alias("kind_rank"),
        pl.col("snap_gid"),
        pl.col("md_index").cast(pl.Int32).fill_null(0).alias("group_ord"),
        pl.lit(3, pl.Int8).alias("source_id"),
        pl.lit(4, pl.Int8).alias("action_code"),
        _fop_side_code(),
        pl.col("price_level").cast(pl.Int32).fill_null(-1),
        _fop_signed_px(),
        pl.col("quantity").cast(pl.Int64).fill_null(0).alias("qty"),
        pl.lit(False).alias("is_trial"),
        pl.col("info_time"),
        pl.lit(None, pl.String).alias("match_time"),
        pl.lit(None, pl.Int64).alias("total_qty"),
    )


def _stage_i024(day: str, type: str, session: str, instrument: str) -> pl.LazyFrame:
    lf = get_fop_parquet(day, type, "i024", session=session, instrument=instrument, lazy=True)
    lf = lf.select(
        "prod_msg_seq", "info_time", "match_time", "calculated_flag",
        "match_index", "price_sign", "price_raw", "quantity", "total_qty",
    )
    lf = lf.unique(subset=["prod_msg_seq", "match_index"], keep="first", maintain_order=True)
    return lf.select(
        pl.col("prod_msg_seq").cast(pl.Int64).fill_null(0),
        pl.lit(0, pl.Int8).alias("kind_rank"),
        pl.lit(0, pl.Int64).alias("snap_gid"),
        pl.col("match_index").cast(pl.Int32).fill_null(0).alias("group_ord"),
        pl.lit(0, pl.Int8).alias("source_id"),
        pl.lit(-1, pl.Int8).alias("action_code"),
        pl.lit(-1, pl.Int8).alias("side_code"),
        pl.lit(-1, pl.Int32).alias("price_level"),
        _fop_signed_px(),
        pl.col("quantity").cast(pl.Int64).fill_null(0).alias("qty"),
        (pl.col("calculated_flag") == "1").fill_null(False).alias("is_trial"),
        pl.col("info_time"),
        pl.col("match_time"),
        pl.col("total_qty").cast(pl.Int64).alias("total_qty"),
    )


try:  # numba JITs the sequential replay loop (~10x); optional at runtime
    from numba import njit as _njit
    _HAS_NUMBA = True
except Exception:  # pragma: no cover - keep the package importable without numba
    _HAS_NUMBA = False

    def _njit(*_args, **_kwargs):
        def _decorate(fn):
            return fn
        return _decorate


@_njit(cache=True)
def _fop_write_book(out_px, out_qty, out_val, lad_px, lad_qty, lad_val, m):
    # Flatten the 4x5 ladders into row m: bids 0-4, asks 5-9, implied bids
    # 10-14, implied asks 15-19.
    for k in range(5):
        out_px[m, k] = lad_px[0, k]
        out_px[m, 5 + k] = lad_px[1, k]
        out_px[m, 10 + k] = lad_px[2, k]
        out_px[m, 15 + k] = lad_px[3, k]
        out_qty[m, k] = lad_qty[0, k]
        out_qty[m, 5 + k] = lad_qty[1, k]
        out_qty[m, 10 + k] = lad_qty[2, k]
        out_qty[m, 15 + k] = lad_qty[3, k]
        out_val[m, k] = lad_val[0, k]
        out_val[m, 5 + k] = lad_val[1, k]
        out_val[m, 10 + k] = lad_val[2, k]
        out_val[m, 15 + k] = lad_val[3, k]


@_njit(cache=True)
def _fop_flush_book(out_px, out_qty, out_val, out_emit_idx, out_stale, out_trial,
                    lad_px, lad_qty, lad_val, m, stale, event_src,
                    event_first_row, event_trial):
    # A real book never crosses at the exchange; an i081 message that leaves the
    # top of book crossed proves a silently lost update (chain broken).
    if (
        event_src == 1
        and not stale
        and lad_val[0, 0]
        and lad_val[1, 0]
        and lad_px[0, 0] >= lad_px[1, 0]
    ):
        stale = True
    _fop_write_book(out_px, out_qty, out_val, lad_px, lad_qty, lad_val, m)
    out_emit_idx[m] = event_first_row
    out_stale[m] = stale
    out_trial[m] = event_trial
    return m + 1, stale


@_njit(cache=True)
def _replay_kernel(seq, rank, gid, src, act, side, lvl, px, qty, trial,
                   out_px, out_qty, out_val, out_emit_idx,
                   out_stale, out_trial, out_deal):
    n = seq.shape[0]
    lad_px = np.zeros((4, 5), dtype=np.int64)
    lad_qty = np.zeros((4, 5), dtype=np.int64)
    lad_val = np.zeros((4, 5), dtype=np.bool_)

    stale = True
    seen_realtime = False
    last_seen = np.int64(-1)
    m = 0

    have_key = False
    cur_seq = np.int64(-1)
    cur_rank = np.int8(-1)
    cur_gid = np.int64(-1)
    cur_src = np.int8(-1)
    skip_event = False
    pending = False
    event_first_row = 0
    event_trial = False
    event_src = np.int8(-1)

    for i in range(n):
        s = src[i]
        sq = seq[i]

        if s == 0:  # i024 deal leg: emit with the prevailing book, no mutation
            if pending:
                m, stale = _fop_flush_book(
                    out_px, out_qty, out_val, out_emit_idx, out_stale, out_trial,
                    lad_px, lad_qty, lad_val, m, stale, event_src,
                    event_first_row, event_trial)
                pending = False
            if not seen_realtime:
                seen_realtime = True
                if sq == 1:
                    stale = False  # complete chain from an empty book
            _fop_write_book(out_px, out_qty, out_val, lad_px, lad_qty, lad_val, m)
            out_emit_idx[m] = i
            out_stale[m] = stale
            out_trial[m] = trial[i]
            out_deal[m] = True
            m += 1
            if sq > last_seen:
                last_seen = sq
            continue

        is_new_key = (
            not have_key
            or sq != cur_seq
            or rank[i] != cur_rank
            or gid[i] != cur_gid
            or s != cur_src
        )
        if is_new_key:
            if pending:
                m, stale = _fop_flush_book(
                    out_px, out_qty, out_val, out_emit_idx, out_stale, out_trial,
                    lad_px, lad_qty, lad_val, m, stale, event_src,
                    event_first_row, event_trial)
                pending = False
            have_key = True
            cur_seq = sq
            cur_rank = rank[i]
            cur_gid = gid[i]
            cur_src = s
            event_first_row = i
            event_src = s
            skip_event = False
            if s == 1:  # i081 increment message
                if not seen_realtime:
                    seen_realtime = True
                    if sq == 1:
                        stale = False
                elif sq <= last_seen:
                    # Already consumed (cross-format duplicate or covered by a
                    # snapshot at the same seq): drop the whole message.
                    skip_event = True
                    continue
                event_trial = False
                last_seen = sq
            else:  # i083 / i084 snapshot: adopt wholesale (seq >= last_seen by sort)
                seen_realtime = True
                for a in range(4):
                    for k in range(5):
                        lad_px[a, k] = 0
                        lad_qty[a, k] = 0
                        lad_val[a, k] = False
                stale = False
                event_trial = trial[i]
                if sq > last_seen:
                    last_seen = sq
        elif skip_event:
            continue

        sd = side[i]
        lv = lvl[i]
        ac = act[i]
        pending = True
        if sd < 0 or lv < 1 or lv > 5 or ac < 0:
            continue  # malformed / empty-book marker: skip entry silently
        idx = lv - 1
        p = px[i]
        q = qty[i]
        if ac == 4:  # snapshot placement; price=0 & qty=0 means "level not present"
            if p == 0 and q == 0:
                lad_px[sd, idx] = 0
                lad_qty[sd, idx] = 0
                lad_val[sd, idx] = False
            else:
                lad_px[sd, idx] = p
                lad_qty[sd, idx] = q
                lad_val[sd, idx] = True
        elif ac == 0:  # NEW: insert, shift down, level 5 falls off
            if idx > 0 and not lad_val[sd, idx - 1]:
                stale = True  # would create a hole: chain is broken
            for j in range(4, idx, -1):
                lad_px[sd, j] = lad_px[sd, j - 1]
                lad_qty[sd, j] = lad_qty[sd, j - 1]
                lad_val[sd, j] = lad_val[sd, j - 1]
            lad_px[sd, idx] = p
            lad_qty[sd, idx] = q
            lad_val[sd, idx] = True
        elif ac == 1:  # CHG: overwrite in place
            if not lad_val[sd, idx]:
                stale = True
            lad_px[sd, idx] = p
            lad_qty[sd, idx] = q
            lad_val[sd, idx] = True
        elif ac == 2:  # DEL: remove, shift up, level 5 empties
            if not lad_val[sd, idx]:
                stale = True
            for j in range(idx, 4):
                lad_px[sd, j] = lad_px[sd, j + 1]
                lad_qty[sd, j] = lad_qty[sd, j + 1]
                lad_val[sd, j] = lad_val[sd, j + 1]
            lad_px[sd, 4] = 0
            lad_qty[sd, 4] = 0
            lad_val[sd, 4] = False
        elif ac == 3:  # OVE: overwrite; price=0 & qty=0 clears the level
            if p == 0 and q == 0:
                lad_px[sd, idx] = 0
                lad_qty[sd, idx] = 0
                lad_val[sd, idx] = False
            else:
                lad_px[sd, idx] = p
                lad_qty[sd, idx] = q
                lad_val[sd, idx] = True

    if pending:
        m, stale = _fop_flush_book(
            out_px, out_qty, out_val, out_emit_idx, out_stale, out_trial,
            lad_px, lad_qty, lad_val, m, stale, event_src,
            event_first_row, event_trial)

    return m


def _replay_fop_events(events: pl.DataFrame) -> dict:
    """Fold the seq-sorted unified event table into per-event book states.

    Pure function of the event table (no I/O): ladder semantics are a port of
    taifex-resolver's OrderBookManager (TAIFEX manual worked examples pp. 89-99).
    The sequential state-machine loop runs in the numba-JIT'd _replay_kernel
    (falling back to interpreted execution if numba is unavailable).
    Returns numpy arrays: emit_idx (input-row index of each output row, for
    metadata gather), px/qty/valid (m x 20: bids 0-4, asks 5-9, implied bids
    10-14, implied asks 15-19), is_stale, is_trial, is_deal.
    """
    def col(name, dtype):
        return np.ascontiguousarray(events[name].to_numpy(), dtype=dtype)

    seq = col("prod_msg_seq", np.int64)
    rank = col("kind_rank", np.int8)
    gid = col("snap_gid", np.int64)
    src = col("source_id", np.int8)
    act = col("action_code", np.int8)
    side = col("side_code", np.int8)
    lvl = col("price_level", np.int32)
    px = col("px", np.int64)
    qty = col("qty", np.int64)
    trial = col("is_trial", np.bool_)

    n_deals = int(events.filter(pl.col("source_id") == 0).height)
    non_deal = events.filter(pl.col("source_id") != 0)
    n_book = (
        int(
            non_deal.select(
                pl.struct(
                    ["prod_msg_seq", "kind_rank", "snap_gid", "source_id"]
                ).n_unique()
            ).item()
        )
        if non_deal.height
        else 0
    )
    cap = n_deals + n_book
    out_px = np.zeros((cap, 20), dtype=np.int64)
    out_qty = np.zeros((cap, 20), dtype=np.int64)
    out_val = np.zeros((cap, 20), dtype=np.bool_)
    out_emit_idx = np.zeros(cap, dtype=np.int64)
    out_stale = np.zeros(cap, dtype=np.bool_)
    out_trial = np.zeros(cap, dtype=np.bool_)
    out_deal = np.zeros(cap, dtype=np.bool_)

    m = _replay_kernel(
        seq, rank, gid, src, act, side, lvl, px, qty, trial,
        out_px, out_qty, out_val, out_emit_idx, out_stale, out_trial, out_deal,
    )

    return {
        "count": m,
        "emit_idx": out_emit_idx[:m],
        "px": out_px[:m],
        "qty": out_qty[:m],
        "valid": out_val[:m],
        "is_stale": out_stale[:m],
        "is_trial": out_trial[:m],
        "is_deal": out_deal[:m],
    }


def _fop_order_book_schema() -> dict:
    schema = {
        "prod_id": pl.String,
        "info_time": pl.String,
        "update_time": pl.String,
        "source": pl.String,
        "prod_msg_seq": pl.Int64,
        "is_trial": pl.Boolean,
        "is_stale": pl.Boolean,
        "deal_price": pl.Int64,
        "deal_volume": pl.Int64,
        "cumulative_volume": pl.Int64,
    }
    for side in ("bid", "ask"):
        for k in range(1, _FOP_BOOK_DEPTH + 1):
            schema[f"{side}_price_{k}"] = pl.Int64
        for k in range(1, _FOP_BOOK_DEPTH + 1):
            schema[f"{side}_volume_{k}"] = pl.Int64
    for side in ("impl_bid", "impl_ask"):
        schema[f"{side}_price"] = pl.Int64
        schema[f"{side}_volume"] = pl.Int64
    return schema


def _assemble_fop_order_book(
    events: pl.DataFrame,
    replay: dict,
    instrument: str,
) -> pl.DataFrame:
    m = replay["count"]

    idx = pl.Series("emit_idx", replay["emit_idx"])
    meta = events.select(
        pl.col("info_time").gather(idx),
        pl.col("match_time").gather(idx),
        pl.col("prod_msg_seq").gather(idx),
        pl.col("source_id").gather(idx),
        pl.col("px").gather(idx),
        pl.col("qty").gather(idx),
        pl.col("total_qty").gather(idx),
    )

    px = replay["px"]
    qty = replay["qty"]
    val = replay["valid"]
    data = {
        "info_time": meta["info_time"],
        "match_time": meta["match_time"],
        "prod_msg_seq": meta["prod_msg_seq"],
        "source_id": meta["source_id"],
        "event_px": meta["px"],
        "event_qty": meta["qty"],
        "total_qty": meta["total_qty"],
        "is_trial": replay["is_trial"],
        "is_stale": replay["is_stale"],
        "is_deal": replay["is_deal"],
    }
    for base, off in (("bid", 0), ("ask", 5), ("impl_bid", 10), ("impl_ask", 15)):
        levels = range(_FOP_BOOK_DEPTH) if off < 10 else range(1)
        for k in levels:
            name = f"{base}_{k + 1}" if off < 10 else base
            data[f"_px_{name}"] = px[:, off + k]
            data[f"_qty_{name}"] = qty[:, off + k]
            data[f"_val_{name}"] = val[:, off + k]
    df = pl.DataFrame(data)

    def _price_expr(name: str, alias: str) -> pl.Expr:
        return (
            pl.when(pl.col(f"_val_{name}"))
            .then(pl.col(f"_px_{name}"))
            .alias(alias)
        )

    def _volume_expr(name: str, alias: str) -> pl.Expr:
        return (
            pl.when(pl.col(f"_val_{name}"))
            .then(pl.col(f"_qty_{name}"))
            .alias(alias)
        )

    book_exprs = []
    for base in ("bid", "ask"):
        for k in range(_FOP_BOOK_DEPTH):
            book_exprs.append(_price_expr(f"{base}_{k + 1}", f"{base}_price_{k + 1}"))
        for k in range(_FOP_BOOK_DEPTH):
            book_exprs.append(_volume_expr(f"{base}_{k + 1}", f"{base}_volume_{k + 1}"))
    for base in ("impl_bid", "impl_ask"):
        book_exprs.append(_price_expr(base, f"{base}_price"))
        book_exprs.append(_volume_expr(base, f"{base}_volume"))

    out = df.select(
        pl.lit(instrument).alias("prod_id"),
        pl.col("info_time"),
        # deals carry match_time; book events fall back to their info_time so
        # every row has an update_time.
        pl.coalesce(pl.col("match_time"), pl.col("info_time")).alias("update_time"),
        pl.col("source_id")
        .replace_strict(_FOP_SOURCE_NAMES, return_dtype=pl.String)
        .alias("source"),
        pl.col("prod_msg_seq"),
        pl.col("is_trial"),
        pl.col("is_stale"),
        pl.when(pl.col("is_deal")).then(pl.col("event_px")).alias("deal_price"),
        pl.when(pl.col("is_deal")).then(pl.col("event_qty")).alias("deal_volume"),
        pl.when(pl.col("is_deal")).then(pl.col("total_qty")).alias("cumulative_volume"),
        *book_exprs,
    )
    # Guarantee the exact output dtypes regardless of null-only shortcuts.
    return out.cast(_fop_order_book_schema())


def get_fop_order_book(
    day: str,
    type: str,
    instrument: str,
    session: str = "day",
    to_pandas: bool = True,
) -> Union[pd.DataFrame, pl.DataFrame]:
    """Reconstruct a TWSE-like 5-level order-book time series for one TAIFEX product.

    Reads the day's i083 (session-open snapshots), i084 (5-second snapshot
    carousel), i081 (order-book deltas) and i024 (deals) parquets via
    get_fop_parquet (lazy scans with prod_id/column pushdown), merges them into
    one per-product event stream ordered by prod_msg_seq, and replays it with
    the taifex-resolver OrderBookManager semantics (TAIFEX manual pp. 89-99):

        - i083/i084 snapshot with seq >= the book's last seq: adopted wholesale
          as the new source of truth; clears the stale flag. Older snapshots
          and duplicate realtime messages are dropped.
        - i081 entries apply in entry_index order: NEW inserts at price_level
          and shifts lower levels down (level 5 falls off); CHG rewrites in
          place; DEL removes and shifts up; OVE overlays the implied level
          (price=0 & qty=0 clears it). Applied best-effort: an entry that
          cannot apply cleanly (CHG/DEL on an empty level, NEW creating a
          hole), or a message that leaves the top of book crossed
          (bid_1 >= ask_1 is impossible at the exchange, so an update was
          silently lost), sets is_stale=True until the next snapshot adoption.
        - i024 deal legs emit one row each carrying the prevailing book;
          deals never mutate the book.

    One output row per event: an i081/i083 message, an i084 product snapshot,
    or an i024 deal leg (source column: 'i081'/'i083'/'i084'/'i024').

    Columns:
        prod_id, info_time, update_time, source, prod_msg_seq,
        is_trial, is_stale, deal_price, deal_volume, cumulative_volume,
        bid_price_1..5, bid_volume_1..5, ask_price_1..5, ask_volume_1..5,
        impl_bid_price, impl_bid_volume, impl_ask_price, impl_ask_volume.
        update_time is the event's own exchange timestamp: match_time on deal
        (i024) rows, info_time on order-book (i081/i083/i084) rows.
        Absent book levels are null (TWSE parquet convention). impl_* is the
        best implied (derived) level from IMPL_BID/IMPL_ASK entries.

    Caveats:
        - All price columns (bid/ask/impl book prices and deal_price) are the
          raw integer price_raw, signed by price_sign; no decimal scaling is
          applied. The product's true decimal locator (TAIFEX I010; e.g. TXF
          is 2, GDF is 3) is not captured here, so divide by 10**locator
          yourself to get prices in the product's natural units.
        - Trial rows (is_trial=True): pre-open i083 snapshots and i024 trial
          matches (calculated_flag '1'). Trial snapshots are adopted as the
          book base (liquid products get no non-trial i083), so the first
          post-open book is best-effort until the first i084 re-base (~5 s).
        - is_stale=False is not a hard guarantee: a lost i081 packet that
          still applies cleanly is undetectable offline; the i084 carousel
          re-bases every ~5-7 s and bounds the error window. i084 captures
          only exist from ~2026-06-26; before that, a stale flag raised after
          the open never clears (no snapshot source intraday).
        - Rows are in replay (prod_msg_seq) order. Pre-open this differs from
          wall-clock order: empty i084 markers carry seq 0 and sort before
          the trial i083/i024 rows. From the session open onward info_time is
          monotonic.
        - The replay loop is JIT-compiled with numba; the most liquid products
          (TXF/MXF front month) then take ~1 s when the day's parquets are in
          the OS page cache, and typical products are 10-100x cheaper. Two
          one-time costs sit outside that: the first read of a day is dominated
          by the NFS scan of the i081/i084 files (~800 MB after projection) and
          takes on the order of 90 s, and the very first call in a process
          whose numba on-disk cache is cold pays ~1-2 s to compile the kernel.
          If numba is not installed the loop still runs (interpreted), just
          several times slower.

    Args:
        day: Trading day, e.g. "2026-07-03" or "20260703".
        type: "futures" or "options" (singular forms accepted).
        instrument: Product id (prod_id), e.g. "TXFG6". Required.
        session: "day" (0840 files) or "night" (1455 files).
        to_pandas: If True (default), return a pandas DataFrame; if False,
            return an eager polars DataFrame.

    Returns:
        One row per book/deal event, with a schema identical across days,
        instruments, and empty results. As polars (to_pandas=False), price and
        volume columns are nullable Int64 (null for absent book levels and on
        non-deal rows). As pandas (default), those columns are float64 with NaN
        in those same places so the pandas dtypes stay stable regardless of a
        day's data.

    Raises:
        FileNotFoundError: If the i024/i081/i083 parquet for the day/session is
            missing. A missing i084 parquet is tolerated (source skipped).
        ValueError: On invalid type/session/instrument.
    """
    if not instrument:
        raise ValueError("instrument is required, e.g. 'TXFG6'")

    stages = [
        _stage_i083(day, type, session, instrument),
        _stage_i084(day, type, session, instrument),
        _stage_i081(day, type, session, instrument),
        _stage_i024(day, type, session, instrument),
    ]
    events = pl.concat([s for s in stages if s is not None], how="vertical").collect()

    if events.height == 0:
        result = pl.DataFrame(schema=_fop_order_book_schema())
    else:
        # source_id descends so that on a (corrupt) cross-format seq collision the
        # snapshot processes first and the conflicting increment is dropped whole.
        events = events.sort(
            ["prod_msg_seq", "kind_rank", "snap_gid", "source_id", "group_ord"],
            descending=[False, False, False, True, False],
            maintain_order=True,
        )
        replay = _replay_fop_events(events)
        result = _assemble_fop_order_book(events, replay, instrument)

    if not to_pandas:
        return result
    # For the pandas conversion, cast nullable Int64 columns to Float64 so the
    # schema is identical across days/instruments/empty: pyarrow otherwise maps
    # null-free columns to int64 and null-containing ones to float64. prod_msg_seq
    # is never null, so it stays a stable int64.
    nullable_ints = [
        name
        for name, dtype in _fop_order_book_schema().items()
        if dtype == pl.Int64 and name != "prod_msg_seq"
    ]
    return result.with_columns(pl.col(nullable_ints).cast(pl.Float64)).to_pandas()
