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
If not set, they default to the current working directory, except
`DATA_SDK_TEJ_CACHE_PATH`, which defaults to `/mnt/nfs/backup/tej_cache`.

```bash
export DATA_SDK_FINMIND_BROKER_PATH="/mnt/nfs/backup/finmind_broker"
export DATA_SDK_SHIOAJI_TICKS_PATH="/mnt/nfs/backup/shioaji_ticks"
export DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH="/mnt/nfs/backup/shioaji_futures_ticks"
export DATA_SDK_ORDER_BOOK_PARQUET_PATH="/mnt/nfs/backup/parquets"
export DATA_SDK_TEJ_CACHE_PATH="/mnt/nfs/backup/tej_cache"
export DATA_SDK_FOP_PARQUET_PATH="/mnt/nfs/backup/fop_parquets"  # defaults to this path if unset
export DATA_SDK_FINLAB_CACHE_PATH="/mnt/nfs/backup/finlab_cache"    # defaults to this path if unset
export DATA_SDK_TAIFEX_CACHE_PATH="/mnt/nfs/backup/taifex_cache"    # defaults to this path if unset
export DATA_SDK_MARKET_CALENDAR_CACHE_PATH="$HOME/.cache/data_sdk/market_calendar"  # default: $XDG_CACHE_HOME/data_sdk/market_calendar, else ~/.cache/data_sdk/market_calendar
export DATA_SDK_FINMIND_CALENDAR_CACHE_PATH="$HOME/.cache/data_sdk/finmind_calendar"  # default: $XDG_CACHE_HOME/data_sdk/finmind_calendar, else ~/.cache/data_sdk/finmind_calendar; one JSON file per year, `rm` a year's file (or the whole directory) to clear it -- see "FinMind's taiwan_stock_trading_date" below
export DATA_SDK_FINMIND_WEIGHT_PATH="/mnt/nfs/backup/finmind_cache/twii_weight"     # defaults to this path if unset
export DATA_SDK_CB_CACHE_PATH="/mnt/nfs/backup/finmind_cache/cb"                    # defaults to this path if unset
```

### API Keys
Required for fetching data from FinMind, FinLab, Shioaji and TEJ. A wrapper
whose cache already holds the requested day never needs one; the TAIFEX
endpoints are public and need none at all.

```bash
export FINMIND_API_TOKEN=your_token
export FINLAB_API_TOKEN=your_finlab_token
export SHIOAJI_API_KEY=your_api_key
export SHIOAJI_SECRET_KEY=your_secret_key
export TEJ_API_TOKEN=your_tej_token
```

### Shioaji contract cache
Shioaji reuses a contract file it already cached that day without asking the
server again, so a file published later that day (today's 參考價 / 漲停價)
would never be seen. Unless `SJ_CONTRACTS_PATH` is already set, `ShioajiWrapper`
points it at an empty directory of the process's own before the first login;
a process therefore sees the server's contract file as of its first login, and
the directory is removed when the process exits.

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
    FinLabWrapper,
    FinMindCalendar,
    FinMindWrapper,
    MarketCalendar,
    ShioajiWrapper,
    TaifexWrapper,
    TEJWrapper,
    WarrantInfoWrapper,
    get_fop_order_book,
    get_fop_parquet,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
    get_ssf_decimal_locator,
    get_txo_decimal_locator,
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

# Get Shioaji order book data (downloads if missing)
shioaji = ShioajiWrapper()
df_ticks = shioaji.get_order_book("2024-01-02", "2330")

# Listed stocks with the exchange's reference / limit prices plus SinoPac's
# margin_trading_balance / short_selling_balance stamped update_date. Whether
# those two balances are the outstanding balance or the remaining quota is
# unverified: reconcile against FinLab before trusting either.
stocks = shioaji.get_stock_contracts()
# examples/check_stock_contracts.py checks those limits live against the tick
# ladder in exact decimal. Run on 2026-09-23 over 1978 ordinary stocks with a
# band: limit_up agreed on every one in every tick band; limit_down disagreed
# on one (1595 川寶, reference 74.1: the exchange published 66.8, the rule
# gives 66.7). The exchange's figure is the one to use; never recompute it.

# Trading calendar through the furthest listed delivery month (never a fixed
# 90-day window): the range is derived from the contracts themselves.
contracts = shioaji.get_futures_contracts()
calendar = finmind.get_trading_dates_covering("2026-09-17", contracts["delivery_date"])

# TAIEX constituent weights for the 臺指期 page (type == "twse" only; TPEx's
# own index has its own constituents). The latest published table with
# date <= day -- FinMind publishes the TWSE table once a month, so 2330's own
# history over WEIGHT_LOOKBACK_DAYS names the latest publication date and
# that one date is fetched for every stock (two requests); raises if the
# window has no publication. Cached per requested day; a warm day needs no
# token. Index is stock_id (str).
weights = finmind.get_twii_weights("2026-09-16")
weights.loc["2330", "weight_per"]

# FinLab SSF reference columns for the last completed trading day (strictly
# before today). The four tables must share one report_date or this raises
# FinLabAlignmentError. Cached per query day; a warm day needs no token.
report_date, ref = FinLabWrapper().get_ssf_reference()
ref.loc["2330", ["margin_balance", "short_balance", "lending_balance", "day_trade_sell_first"]]

# 重訊公告 (material-information announcements, 可轉債頁「轉換條件」用):
# dataset id confirmed via finlab.data.search("重訊") (no login needed --
# that search hits FinLab's public catalog, not data.get()); stock_id is
# the filing ISSUER's own code (a CB's underlying, never its own cb_id).
# This method does NOT classify category_raw into the owner's nine 禁止轉換
# categories -- that mapping is the CB page's own job.
announcements = FinLabWrapper().get_material_announcements(stock_ids=["9958"])

# TAIFEX public endpoints, cached under the day each payload carries. Prices
# come back as the published strings; prod_id is derived for the i024 join.
taifex = TaifexWrapper()
report = taifex.get_daily_market_report_fut()           # TradingSession == '一般'
adjusted = taifex.get_ssf_adjusted_info()                # 調整型 stock futures
as_of, paused = taifex.get_ssf_trade_pause()             # 商品類別 == '股票期貨'

# TWSE's published market calendar (開休市日期), cached one JSON file per year.
# is_trading_day/closure read a Sat/Sun-closed weekday rule overridden by
# TWSE's announced exceptions; a year TWSE has not published yet raises
# UnknownCalendarYear instead of silently treating every weekday as open.
# Demoted to a display-label source only (taiwan_dashboard ISSUES.md I-255):
# FinMindCalendar below is the one that decides trading/not now.
calendar = MarketCalendar()
calendar.is_trading_day("2026-09-25")       # False (中秋節)
calendar.closure("2026-09-25")              # "中秋節"; None on a trading day
calendar.next_trading_day("2026-09-25")     # "2026-09-29"
calendar.export_range("2026-09-25", "2026-09-29")  # [{"date", "trading_day", "closure"}, ...]

# FinMind's taiwan_stock_trading_date (taiwan_dashboard's single source of
# truth for "is today a trading day", ISSUES.md I-255), same read surface as
# MarketCalendar above, cached one JSON file per year under
# DATA_SDK_FINMIND_CALENDAR_CACHE_PATH (rm a year's <year>.json, or the whole
# directory, to clear it -- the next call just refetches and rebuilds it). A
# closed weekday's closure() name is borrowed from a MarketCalendar ONLY when
# it also calls that day closed and has a name for it (weekends are always
# "週末", an unnamed/disagreeing weekday is "休市") -- TWSE never decides
# trading/not here. FinMind only reaches the end of the current year; asking
# past that raises FinMindUnknownCalendarYear, same contract as
# MarketCalendar's own. The CURRENT year's cache is revalidated at most once
# a day: a freshly constructed FinMindCalendar whose cache file is older
# than 24h refetches on its first call for that year (an instance kept
# alive across days only ever revalidates once, on that first call --
# ISSUES.md I-268) so a same-year FinMind correction (a makeup workday, a
# typhoon day off) is picked up within a day; a failed revalidation keeps
# serving the existing cache and logs why. Either outcome -- success or
# failure -- records the attempt's time in a sibling <year>.json.revalidated_at
# marker (also under this cache directory, written atomically), so a FinMind
# outage backs off for the same 24h instead of every freshly constructed
# FinMindCalendar dialing out again while it lasts (ISSUES.md I-274); rm just
# the <year>.json to force an immediate refetch regardless of that marker. A
# PAST year's cache, once written, is never revalidated or refetched.
finmind_calendar = FinMindCalendar()
finmind_calendar.is_trading_day("2026-09-25")      # False, decided by FinMind alone
finmind_calendar.closure("2026-09-25")             # "中秋節" (borrowed from MarketCalendar)
finmind_calendar.export_range("2026-09-25", "2026-09-29")  # same row shape as MarketCalendar

# Frozen per-product decimal locator and tick (ssf_dashboard/docs/spec.md §8.1
# in the taiwan_dashboard monorepo). None means the product was never
# calibrated: render it as unknown, never guess.
entry = get_ssf_decimal_locator("CDFI6")
price_1e4 = 239000 * 10 ** (4 - entry.locator)         # 1/10000 NTD, integers only

# Get Shioaji futures ticks (downloads if missing; code = product, continuous
# near-month alias, or month symbol)
df_fut = shioaji.get_futures_ticks("2024-08-01", "CDFR1")
df_contracts = shioaji.get_futures_contracts()  # currently-listed contracts only

# TAIFEX option contracts (臺指選頁), every root (filter by category=="TXO"
# yourself). strike_1e4 is already integer 1/10000 NTD (never a float);
# multiplier (NT$ per point) is Shioaji's own field, read straight through.
df_options = shioaji.get_option_contracts()

# TAIFEX options daily report (the DailyMarketReportOpt counterpart of
# get_daily_market_report_fut; confirmed by a live GET, no credential).
opt_report = taifex.get_daily_market_report_opt()  # TradingSession == '一般'

# Frozen TXO root decimal locator/tick (docs/pages/txo.md §2.2): calibrated
# offline against DailyMarketReportOpt + the archived i024 options feed,
# plain-monthly contracts only (weeklies carry a different PROD-ID root).
txo_entry = get_txo_decimal_locator("TXO")
txo_price_1e4 = 24000 * 10 ** (4 - txo_entry.locator)

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

# Convertible-bond reference data (可轉債頁; cached to disk, cold fetch
# requires FINMIND_API_TOKEN). get_cb_daily_overview reads the last row
# strictly before `day`, the same rule finlab_wrapper applies to FinLab's
# own daily tables (duplicated, not shared -- different source). FinMind's
# own API answers only a single start_date==end_date query (a range answers
# just that first day, confirmed live), so this walks backward one calendar
# date at a time (bounded by MAX_DAILY_OVERVIEW_LOOKBACK_DAYS) rather than
# fetching once and filtering a local frame.
from data_sdk import CbInfoWrapper, CB_FACE_VALUE_NT

cb = CbInfoWrapper()
cb_info = cb.get_cb_info()                       # whole table, cached
overview = cb.get_cb_daily_overview("2026-09-16")  # one row per cb_id
# cb_id -> underlying stock_id, verified against real Shioaji stock codes
# (never the unverified first-4-characters guess); unresolved ids get
# underlying_stock_id=None, underlying_unresolved=True instead of a guess.
underlying = cb.get_cb_underlying_map()
face_value_nt = CB_FACE_VALUE_NT                 # 100, every CB, by owner decision

# Fetch Taiwan warrant metadata (cached to disk, requires FINMIND_API_TOKEN)
warrant_info = WarrantInfoWrapper(cache_dir=Path("/tmp/warrant_cache"))
df_summary = warrant_info.get_warrant_summary()   # all warrants with strike/expiry
df_names   = warrant_info.get_warrant_names()     # warrant code → stock_name
issuer_map = warrant_info.build_issuer_map()      # {warrant_id: issuer_name}

# Order book from parquet (requires DATA_SDK_ORDER_BOOK_PARQUET_PATH)
df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
df_ob_day = get_order_book_stocks("2026-01-02", is_twse=True)  # entire day
df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
df_warrant = get_order_book_warrant("2026-01-02", is_twse=True)           # all warrants
df_w_sid   = get_order_book_warrant("2026-01-02", is_twse=True, sid="700339")  # single warrant
```

### TAIFEX futures/options (FOP) parquet

Raw TAIFEX market-data parquets (requires `DATA_SDK_FOP_PARQUET_PATH`, defaults to `/mnt/nfs/backup/fop_parquets`).
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
