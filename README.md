# data-sdk

## Integration w/ Jupyter

In your first block, you are required to install this repo via pip.
```
!pip install git+https://github.com/lawrence910426/data-sdk.git --force-reinstall
```

Then, you may now import the classes and use the data.
```python
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
    TEJWrapper,
    WarrantInfoWrapper,
    get_fop_order_book,
    get_fop_parquet,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
)
from pathlib import Path

shioaji_wrapper = ShioajiWrapper()
finmind_wrapper = FinMindWrapper()

df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
df_warrant = get_order_book_warrant("2026-01-02", is_twse=True)
df_w_sid   = get_order_book_warrant("2026-01-02", is_twse=True, sid="700339")
```

`match_time` comes back as `"HH:MM:SS.ffffff"`. That column is computed, so a filter on it
cannot reach the parquet's row-group statistics and forces a full scan. Pass
`format_match_time=False` to keep the on-disk `Int64` (`HHMMSSffffff`) and let the predicate
push down, which is what you want when scanning a whole day for a narrow time window.

```python
import polars as pl

pre_open = (
    get_order_book_stocks("2026-01-02", is_twse=True, lazy=True, format_match_time=False)
    .filter(pl.col("match_time") < 90_000_000_000)
    .collect()
)
```

`get_order_book_stocks` reads two sources of the same Format 6 feed, chosen with `mode`:

- `mode="native"`: our own capture in `DATA_SDK_ORDER_BOOK_PARQUET_PATH` (from 2025-05-20).
- `mode="tej"`: the TEJ conversion in `DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH` (2020-06-01 to 2026-06-30). `timestamp` is null.
- `mode="merged"` (default): both, joined on `transmission_number`. TEJ rows, plus native rows whose `transmission_number` TEJ lacks, in `transmission_number` order. A day in only one source returns that source.

Filters on the returned `LazyFrame` still reach both parquet files in `merged` mode.

```python
native = get_order_book_stocks("2026-01-02", is_twse=True, mode="native")
tej    = get_order_book_stocks("2022-03-01", is_twse=False, mode="tej")
```

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/lawrence910426/data-sdk.git
    cd data-sdk
    ```

2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3.  Install the package:
    ```bash
    pip install .
    ```

## Configuration

Set the following environment variables to configure the SDK:

### Cache Storage Paths
These variables govern where downloaded data is cached and read from.
If one is not set, it defaults to the current working directory and a
warning is printed to stderr.

```bash
export DATA_SDK_FINMIND_BROKER_PATH="/mnt/nfs/backup/finmind_broker"
export DATA_SDK_FINMIND_BROKER_WARRANT_PATH="/mnt/nfs/backup/finmind_broker/warrant_daily"
export DATA_SDK_SHIOAJI_TICKS_PATH="/mnt/nfs/backup/shioaji_ticks"
export DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH="/mnt/nfs/backup/shioaji_futures_ticks"
export DATA_SDK_ORDER_BOOK_PARQUET_PATH="/mnt/nfs/backup/parquets"
export DATA_SDK_TEJ_ORDER_BOOK_PARQUET_PATH="/mnt/nfs/backup/tej_parquets"  # defaults to this path if unset
export DATA_SDK_TEJ_CACHE_PATH="/mnt/nfs/backup/tej_cache"
export DATA_SDK_FOP_PARQUET_PATH="/mnt/nfs/backup/fop_parquets"
```

### API Keys
Required for fetching data from FinMind, Shioaji and TEJ.

```bash
export FINMIND_API_TOKEN=your_token
export SHIOAJI_API_KEY=your_api_key
export SHIOAJI_SECRET_KEY=your_secret_key
export TEJ_API_TOKEN=your_tej_token
```

### FinMind Rate Limiting
FinMind answers HTTP 402 once the hourly quota is gone, and its async client
drops the failed requests silently. Every FinMind caller draws from one token
bucket in a flock-guarded ledger, so separate processes share one budget.

```bash
export DATA_SDK_FINMIND_RATE_LIMIT=6000   # hourly quota; defaults to the account limit, else 600
export DATA_SDK_FINMIND_RATE_MARGIN=0.7   # fraction of the quota actually used
```

## Usage

### Wrappers

```python
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
    TEJWrapper,
    WarrantInfoWrapper,
    get_fop_order_book,
    get_fop_parquet,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
)
from pathlib import Path

# Read FinMind broker data. Reads never download: the archive is filled by a
# scheduled writer, and a missing day raises FileNotFoundError.
finmind = FinMindWrapper()
df = finmind.get_broker("2024-01-02", "2330")
day = finmind.get_broker_day("2024-01-02", sids=["2330", "2317"])

# Filling the archive (scheduled writer only): fetch exactly the missing
# stocks, re-check anything the batch dropped, then dedup + atomic write.
expected = finmind.get_traded_stock_ids("2024-01-02")
missing = expected - finmind.archived_stock_ids("2024-01-02")
result = finmind.fetch_broker_cells("2024-01-02", missing)
if result.complete:
    finmind.write_broker_day("2024-01-02", result.frame)

# Warrant branch report (TaiwanStockWarrantTradingDailyReport, from 2023-06-21):
# same layout and columns, one file per day under DATA_SDK_FINMIND_BROKER_WARRANT_PATH.
# Reads are pure; filter by warrant code and/or branch code.
wide = finmind.get_broker("2026-08-06", sid="700448", warrant=True)      # one warrant, every branch
desk = finmind.get_broker("2026-08-06", trader_id="9227", warrant=True)  # one branch, every warrant
day  = finmind.get_broker_day("2026-08-06", warrant=True)

# Filling it: the sponsor-tier whole-day storage object, one request per day,
# accepted only when it is dated, not thin, and covers the warrants that traded.
result = finmind.fetch_warrant_day("2026-08-06")
if result.complete:
    finmind.write_broker_day("2026-08-06", result.frame, warrant=True)


# Get Shioaji order book data (downloads if missing)
shioaji = ShioajiWrapper()
df_ticks = shioaji.get_order_book("2024-01-02", "2330")

# Get Shioaji futures ticks (downloads if missing; code = product, continuous
# near-month alias, or month symbol)
df_fut = shioaji.get_futures_ticks("2024-08-01", "CDFR1")
df_contracts = shioaji.get_futures_contracts()  # currently-listed contracts only

# TEJ monthly revenue announcements (TWN/EWSALE, incrementally cached as CSV;
# optionally set TEJ_API_TOKEN / DATA_SDK_TEJ_CACHE_PATH). Columns: coid,
# mdate (revenue month), annd_s (announcement date), d0001/d0002/d0003
# (revenue, prior-year revenue, YoY %)
tej = TEJWrapper()
df_rev = tej.get_ewsale()                        # from 2021-01-01 (subscription floor)
df_rev = tej.get_ewsale(min_date="2023-01-01")   # custom start for a cold fetch
# Warm calls re-fetch the trailing 60 days (EWSALE_REFETCH_DAYS) and merge: TEJ
# keeps adding rows for an announcement date days after the fact, so a
# strictly-newer cursor loses them. refetch_since widens that window for a
# one-off repair -- EWSALE_MIN_DATE rebuilds the whole table as a union-merge
# (~157k rows, ~31% of the 500k/day row quota; not for a daily job).
df_rev = tej.get_ewsale(refetch_since=TEJWrapper.EWSALE_MIN_DATE)

# Fetch Taiwan warrant metadata (cached to disk, requires FINMIND_API_TOKEN)
warrant_info = WarrantInfoWrapper(cache_dir=Path("/tmp/warrant_cache"))
df_summary = warrant_info.get_warrant_summary()   # all warrants with strike/expiry
df_names   = warrant_info.get_warrant_names()     # warrant code → stock_name
issuer_map = warrant_info.build_issuer_map()      # {warrant_id: issuer_name}

# Order book from parquet (requires DATA_SDK_ORDER_BOOK_PARQUET_PATH)
df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
df_ob_day = get_order_book_stocks("2026-01-02", is_twse=True)  # entire day, native + TEJ merged
df_ob_native = get_order_book_stocks("2026-01-02", is_twse=True, mode="native")  # own capture only
df_ob_tej = get_order_book_stocks("2021-06-29", is_twse=True, mode="tej")        # TEJ only
df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
df_warrant = get_order_book_warrant("2026-01-02", is_twse=True)           # all warrants
df_w_sid   = get_order_book_warrant("2026-01-02", is_twse=True, sid="700339")  # single warrant
```

### TAIFEX futures/options (FOP) parquet

Raw TAIFEX market-data parquets (requires `DATA_SDK_FOP_PARQUET_PATH`).
Formats: `i024` deals, `i081` order-book deltas, `i083` session-open order-book snapshots,
`i084` snapshot-channel refresh cycles (only captured for recent dates, ~2026-06-26 onward).

```python
from data_sdk import get_fop_parquet

# Deals for one instrument (day session)
df_deals = get_fop_parquet("2026-07-03", "futures", "i024", instrument="TXFG6")

# Night session options deltas as a LazyFrame (recommended for i081/i084 — files are 750MB+)
lf = get_fop_parquet("2026-07-03", "options", "i081", session="night", lazy=True)

# Snapshot channel (recent dates only)
df_snap = get_fop_parquet("2026-07-03", "futures", "i084", instrument="TXFG6")
```

Caveats:
- Use `info_time` / `match_time` for event time; the `timestamp` column is the producer's replay wall clock and is unreliable.
- Prices: `price_raw` is a raw integer, signed by `price_sign` (`'-'` means negative); there is no decimal scaling. The true decimal locator is per product (TAIFEX I010; e.g. TXF=2, GDF=3) and is not captured here, so rescale `price_raw / 10**locator` yourself when you need the product's natural units.

### TAIFEX reconstructed order book

`get_fop_order_book` replays i083/i084 snapshots + i081 deltas (+ i024 deals) into a
TWSE-like 5-level book time series for one product: one row per event with
`bid_price_1..5`/`bid_volume_1..5`/`ask_price_1..5`/`ask_volume_1..5`, best implied
levels (`impl_bid_*`/`impl_ask_*`), deal columns (`deal_price`/`deal_volume`/
`cumulative_volume`) on `source == "i024"` rows, plus `is_trial` and `is_stale` flags.
Snapshots re-base the book and clear `is_stale`; deltas apply best-effort and set
`is_stale` when they cannot apply cleanly or leave the top of book crossed.

```python
from data_sdk import get_fop_order_book

# TXF front month, day session (~1 s warm; ~90 s on the first read of a day,
# which is dominated by the NFS parquet scan). The replay loop is numba-JIT'd.
# All price columns are the raw integer price_raw (divide by 10**locator, e.g.
# 10**2 for TXF/MXF, for index points).
df_book = get_fop_order_book("2026-07-03", "futures", "TXFG6")

# Night session / options / polars-output variants
df_night = get_fop_order_book("2026-07-03", "futures", "TXFG6", session="night")
pl_book  = get_fop_order_book("2026-07-03", "options", "TXO20000G6", to_pandas=False)
```

Caveats:
- Returns a pandas `DataFrame` by default; pass `to_pandas=False` for an eager
  **polars** `DataFrame`. Price and volume columns are nullable `Int64` in polars
  and float64-with-NaN in pandas (so the pandas dtypes stay stable across days).
- All price columns (`*_price*`, `deal_price`) are the raw integer `price_raw`
  with no decimal scaling; divide by `10**locator` (e.g. 2 for TXF) yourself for
  the product's natural units — the caveat above applies.
- Pre-open trial snapshots are adopted as the book base and flagged `is_trial=True`.
- `is_stale=False` is best-effort, not a guarantee: a silently lost delta that still
  applies cleanly is undetectable; the i084 carousel re-bases every ~5 s and bounds
  the error. Before ~2026-06-26 there is no i084, so a stale flag raised after the
  open never clears.
- Rows are in replay (`prod_msg_seq`) order; i084-sourced rows carry a disclosure
  `info_time` up to a few seconds behind the neighbouring delta rows.

### Use LazyFrame for parquet reads (recommended)

When reading order-book parquet files, prefer `lazy=True` so the SDK returns a Polars `LazyFrame`.
This is recommended because it can significantly reduce parquet I/O by pushing filters/projections down to the parquet scan before materialization.

```python
from data_sdk import get_order_book_stocks
import polars as pl

# Return LazyFrame instead of eager pandas DataFrame
lf = get_order_book_stocks("2026-01-02", is_twse=True, lazy=True)

# Apply additional filters/columns lazily, then collect only when needed
df = (
    lf.filter(
        (pl.col("stock_code") == "2330")
        & (pl.col("match_time") > "13:25:00.00000")
    )
      .select(["match_time", "stock_code", "bid_price", "ask_price"])
      .collect()
      .to_pandas()
)
```

If you already know the stock id, you can also pass `sid` directly to reduce scanned data:

```python
lf = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330", lazy=True)
df = lf.collect().to_pandas()
```

### Crawlers

```python
from data_sdk.crawlers import get_intraday_lending_info

df_combined = get_intraday_lending_info("2024-01-02")
```

See `examples/example_crawler.py` for a complete example.
