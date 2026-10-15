"""FinLab daily reference tables for the SSF page, cached per query day.

Four pulls, all daily tables indexed by report date with one column per
stock id:

    margin_balance         margin_transactions:融資今日餘額   (張)
    short_balance          margin_transactions:融券今日餘額   (張)
    lending_balance        security_lending_sell:借券賣出餘額 (shares; returned in 張)
    day_trade_sell_first   intraday_trading:得先賣後買當沖

FinLab lands these at about 19:24 Taipei, so the row a consumer may trust
is the last one *strictly before today* (``latest_before_today``); a table
with no such row raises. Sibling tables must agree on that ``report_date``
or the whole pull raises ``FinLabAlignmentError`` -- a consumer then
degrades the affected cells, it never renders a silently older row.

A second pull, ``get_trading_stats``, reads two more daily tables (``price:成交金額``
in NTD, ``price:成交股數`` in shares) for the last completed day's turnover and a
trailing volume average; it follows the same strictly-before-today and
alignment rules and is cached the same way.

A third pull, ``get_universe_stats``, reads ``price:成交金額``, ``price:成交股數`` and
``price:收盤價`` aligned to one report date, plus the undated ``company_basic_info``
table's issued share count and listing market, for the last completed day's
volume, turnover, close, and the share count that turns them into a turnover
rate and a market capitalisation.

Cache: ``DATA_SDK_FINLAB_CACHE_PATH`` (default ``/mnt/nfs/backup/finlab_cache``),
one parquet per ``(dataset, query day)`` holding the trailing rows dated
before that day. A warm read needs no token; a cold fetch needs
``FINLAB_API_TOKEN`` and imports ``finlab`` lazily.
"""

import datetime as dt
import os
import tempfile
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

# 重訊公告 (material-information announcements), confirmed 2026-10-08:
#   - finlab.data.search("重訊") and search("重大訊息") both resolve to exactly
#     one dataset, "important_info_announcement" (no login needed: the
#     catalog search hits FinLab's public Firestore metadata, not the
#     licensed data.get() endpoint).
#   - That catalog entry's own columns are name, title, info (table_format,
#     data_type "event"); confirmed from an already-fetched local copy
#     (~/finlab_db/important_info_announcement.feather, 1,257,774 rows) that
#     data.get() actually returns stock_id, date, name, title, info, key_date
#     -- one row per announcement, stock_id keyed to the ISSUER's own 4-digit
#     equity code.
#   - Verified against a convertible bond known to stop conversion around
#     every ex-dividend date: 世紀鋼 (stock_id "9958") has a "停止轉換"
#     announcement titled 「公告本公司除息基準日暨轉換公司債停止轉換期間」 on
#     2019-08-21, 2020-07-23, 2021-08-03, 2022-08-22, 2025-07-11 and
#     2026-06-25 -- filed under the issuer's stock_id, never under the CB's
#     own 5-character cb_id. A consumer must resolve cb_id -> stock_id first
#     (CbInfoWrapper.get_cb_underlying_map) before reading this table.
MATERIAL_ANNOUNCEMENT_DATASET = "important_info_announcement"

# 除權息預告表 (ISSUES.md I-317, confirmed 2026-10-12 against an already-
# fetched local copy, ~/finlab_db/dividend_announcement.feather, 29,338
# rows): FinLab's own catalog id for this is "dividend_announcement", not
# the "dividend_tse"/"dividend_otc" tables (those are keyed by past EX-DATE
# with the exchange's own post-facto reference/limit prices, not a forward-
# looking announcement). One row per distribution event, columns confirmed
# from that local copy: stock_id, 公告日期, 公司名稱, 股利所屬期間,
# 權利分派基準日, 盈餘轉增資配股(元/股), 法定盈餘公積、資本公積轉增資配股
# (元/股), 除權交易日, 盈餘分配之股東現金股利(元/股), 法定盈餘公積、資本
# 公積發放之現金(元/股), 除息交易日, 現金股利發放日, 現金增資總股數(股),
# 現金增資認股比率(%), 現金增資認購價(元/股), 參加分派總股數, 公告時間,
# 普通股每股面額, 市場別, key_date. A combined 除權息 event carries both an
# 除權交易日 and an 除息交易日; either can be NaT alone (verified: 1727 中華化
# has only 除權交易日 with zero cash, several rows below have only 除息交易日
# with zero stock).
DIVIDEND_ANNOUNCEMENT_DATASET = "dividend_announcement"

TAIPEI = dt.timezone(dt.timedelta(hours=8))

DATASETS = {
    "margin_balance": "margin_transactions:融資今日餘額",
    "short_balance": "margin_transactions:融券今日餘額",
    "lending_balance": "security_lending_sell:借券賣出餘額",
    "day_trade_sell_first": "intraday_trading:得先賣後買當沖",
}

# The daily price tables behind ``get_trading_stats``: turnover in NTD and
# volume in shares, both date-indexed with one column per stock id.
TRADING_STATS_DATASETS = {
    "turnover": "price:成交金額",
    "volume": "price:成交股數",
}

# The daily price tables behind ``get_universe_stats``: the close in NTD joins
# the two ``get_trading_stats`` tables. A separate map, so the alignment gate of
# ``get_trading_stats`` (turnover and volume only) is unchanged.
UNIVERSE_STATS_DATASETS = {
    "turnover": "price:成交金額",
    "volume": "price:成交股數",
    "close": "price:收盤價",
}

# 公司基本資料, one row per company (undated): the issued ordinary share count
# (or the originally issued TDR units) and the listing market -- "sii" 上市,
# "otc" 上櫃, "rotc" 興櫃. Confirmed 2026-10-15 against an already-fetched local
# copy (~/finlab_db/company_basic_info.feather, 2,352 rows).
COMPANY_BASIC_INFO_DATASET = "company_basic_info"
SHARES_OUTSTANDING_COLUMN = "已發行普通股數或TDR原發行股數"
LISTING_MARKET_COLUMN = "市場別"

# ``get_trading_stats`` averages volume over this many completed trading days
# per stock, and reports no average for a stock with fewer than
# ``VOLUME_MIN_DAYS`` days of data (a new listing, a long suspension).
VOLUME_WINDOW_DAYS = 5
VOLUME_MIN_DAYS = 3

# Rows kept per cached table: enough that a holiday cluster still leaves a
# completed trading day in the cache, small enough to be a few KB per stock.
CACHE_TRAILING_ROWS = 10

# FinLab's 借券賣出餘額 is in shares; one 張 is 1000 shares.
SHARES_PER_LOT = 1000


class FinLabAlignmentError(RuntimeError):
    """Sibling FinLab tables do not share one latest completed report date."""


def taipei_today() -> dt.date:
    return dt.datetime.now(TAIPEI).date()


def latest_before_today(frame: pd.DataFrame, today: dt.date) -> Tuple[dt.date, pd.Series]:
    """The last row dated strictly before ``today`` and its report date.

    Raises RuntimeError when the table has no completed trading-day row.
    """
    index = pd.to_datetime(frame.index)
    eligible = frame.loc[index < pd.Timestamp(today)]
    if eligible.empty:
        raise RuntimeError("FinLab table has no row dated before " f"{today.isoformat()}")
    row = eligible.iloc[-1]
    return pd.Timestamp(eligible.index[-1]).date(), row


def align_latest_rows(
    frames: Dict[str, pd.DataFrame], today: dt.date
) -> Tuple[dt.date, Dict[str, pd.Series]]:
    """Latest-before-today row of every table, requiring one shared date.

    Returns ``(report_date, {key: row})``. Raises ``FinLabAlignmentError``
    naming every table's date when they disagree, and RuntimeError when any
    table has no completed row at all.
    """
    rows: Dict[str, pd.Series] = {}
    dates: Dict[str, dt.date] = {}
    for key, frame in frames.items():
        dates[key], rows[key] = latest_before_today(frame, today)
    distinct = set(dates.values())
    if len(distinct) != 1:
        detail = ", ".join(f"{key}={value.isoformat()}" for key, value in dates.items())
        raise FinLabAlignmentError(f"FinLab tables are not aligned: {detail}")
    return distinct.pop(), rows


class FinLabWrapper:
    """FinLab ``data.get`` behind a per-day NFS cache."""

    _logged_in = False
    _DEFAULT_CACHE_DIR = "/mnt/nfs/backup/finlab_cache"

    #: Fetches one dataset and returns FinLab's frame (date index, stock
    #: columns). Swapped in tests; the default imports finlab on first use.
    _fetch: Optional[Callable[[str], pd.DataFrame]] = None

    def __init__(self, fetch: Optional[Callable[[str], pd.DataFrame]] = None):
        if fetch is not None:
            self._fetch = fetch

    # ------------------------------------------------------------------
    # Cache paths
    # ------------------------------------------------------------------

    @staticmethod
    def cache_dir() -> str:
        return os.environ.get("DATA_SDK_FINLAB_CACHE_PATH", FinLabWrapper._DEFAULT_CACHE_DIR)

    @staticmethod
    def _slug(dataset: str) -> str:
        # ASCII-only filenames: the table name as is, the Chinese column
        # name as its UTF-8 hex, e.g. margin_transactions__e89e8de8b387....
        table, _, column = dataset.partition(":")
        return f"{table}__{column.encode('utf-8').hex()}"

    @classmethod
    def cache_path(cls, dataset: str, today: dt.date) -> str:
        return os.path.join(cls.cache_dir(), cls._slug(dataset), f"{today.strftime('%Y%m%d')}.parquet")

    @staticmethod
    def _write_atomic(frame: pd.DataFrame, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".parquet.tmp")
        os.close(fd)
        try:
            frame.to_parquet(tmp)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    # ------------------------------------------------------------------
    # Network
    # ------------------------------------------------------------------

    @classmethod
    def _login(cls):
        if cls._logged_in:
            return
        token = os.environ.get("FINLAB_API_TOKEN")
        if not token:
            raise RuntimeError(
                "FINLAB_API_TOKEN is not set and the FinLab cache has no entry for this day"
            )
        import finlab

        finlab.login(token)
        cls._logged_in = True

    def _fetch_dataset(self, dataset: str) -> pd.DataFrame:
        if self._fetch is not None:
            return self._fetch(dataset)
        self._login()
        from finlab import data

        return data.get(dataset)

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    def get_table(self, dataset: str, today: Optional[dt.date] = None) -> pd.DataFrame:
        """Trailing rows of ``dataset`` dated strictly before ``today``.

        ``today`` defaults to the current Taipei date. A cached parquet for
        ``(dataset, today)`` is served as is, with no token; otherwise the
        table is fetched, the last ``CACHE_TRAILING_ROWS`` rows before
        ``today`` are cached, and those rows are returned. The index is a
        DatetimeIndex, columns are stock ids as str.
        """
        today = today or taipei_today()
        path = self.cache_path(dataset, today)
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            frame = pd.read_parquet(path)
            frame.index = pd.to_datetime(frame.index)
            return frame

        # A plain DataFrame: finlab's FinlabDataFrame refuses an index
        # assignment (its lookahead-bias guard), and these daily tables need
        # nothing from its report-date semantics.
        full = pd.DataFrame(self._fetch_dataset(dataset)).copy()
        full.index = pd.to_datetime(full.index)
        full.columns = [str(c) for c in full.columns]
        trailing = full.loc[full.index < pd.Timestamp(today)].tail(CACHE_TRAILING_ROWS)
        self._write_atomic(trailing, path)
        return trailing

    def get_ssf_reference(self, today: Optional[dt.date] = None) -> Tuple[dt.date, pd.DataFrame]:
        """The four SSF reference columns for the last completed trading day.

        Returns ``(report_date, frame)``: ``frame`` is indexed by stock id
        with columns ``margin_balance``, ``short_balance`` (張, as FinLab
        publishes them), ``lending_balance`` (張: FinLab publishes shares,
        divided by 1000 and rounded) and
        ``day_trade_sell_first`` (FinLab's own flag value, untouched). A stock
        absent from a table is NaN in that column -- 0 is never substituted.

        Raises ``FinLabAlignmentError`` when the four tables do not share one
        ``report_date``, and RuntimeError when any of them has no row before
        ``today``.
        """
        today = today or taipei_today()
        frames = {key: self.get_table(name, today) for key, name in DATASETS.items()}
        report_date, rows = align_latest_rows(frames, today)
        out = pd.DataFrame({key: row for key, row in rows.items()})
        # 借券賣出餘額 is published in shares; every other column is in 張.
        out["lending_balance"] = (pd.to_numeric(out["lending_balance"], errors="coerce") / SHARES_PER_LOT).round()
        out.index = out.index.astype(str)
        out.index.name = "stock_id"
        return report_date, out[list(DATASETS)]

    def get_trading_stats(self, today: Optional[dt.date] = None) -> Tuple[dt.date, pd.DataFrame]:
        """Last completed day's turnover and a trailing volume average per stock.

        Returns ``(report_date, frame)``: ``frame`` is indexed by stock id with

            prev_turnover          ``price:成交金額`` on ``report_date``, NTD
                                   (float; NaN when the stock has no row that
                                   day)
            avg_volume_5d_shares   simple mean of ``price:成交股數`` over the
                                   ``VOLUME_WINDOW_DAYS`` most recent completed
                                   trading days *that have data for the stock*
                                   (shares, float); today is never included.
                                   NaN when fewer than ``VOLUME_MIN_DAYS`` days
                                   have data
            volume_days            how many days the average was taken over
                                   (0..``VOLUME_WINDOW_DAYS``)

        A missing cell is NaN, never 0. Both tables must share one
        ``report_date`` (``FinLabAlignmentError`` otherwise); the caller that
        also holds ``get_ssf_reference`` compares the two report dates itself.
        Both tables go through ``get_table``, so a warm day needs no token and
        the window is bounded by ``CACHE_TRAILING_ROWS``.
        """
        today = today or taipei_today()
        frames = {key: self.get_table(name, today) for key, name in TRADING_STATS_DATASETS.items()}
        report_date, rows = align_latest_rows(frames, today)
        volume = frames["volume"].apply(pd.to_numeric, errors="coerce")
        volume = volume.loc[volume.index < pd.Timestamp(today)]
        has_data = volume.notna()
        # Rank each cell among the stock's own data days, newest first: the
        # newest data day is 1, so "<= window" keeps its most recent days.
        newest_first = has_data.iloc[::-1].cumsum().iloc[::-1]
        recent = volume.where(has_data & (newest_first <= VOLUME_WINDOW_DAYS))
        days = recent.notna().sum()
        mean = recent.mean()
        mean[days < VOLUME_MIN_DAYS] = float("nan")
        out = pd.DataFrame(
            {
                "prev_turnover": pd.to_numeric(rows["turnover"], errors="coerce"),
                "avg_volume_5d_shares": mean,
                "volume_days": days,
            }
        )
        out["volume_days"] = out["volume_days"].fillna(0).astype(int)
        out.index = out.index.astype(str)
        out.index.name = "stock_id"
        return report_date, out

    def get_universe_stats(self, today: Optional[dt.date] = None) -> Tuple[dt.date, pd.DataFrame]:
        """Last completed day's volume, turnover and close, and the issued share
        count, for every stock FinLab knows.

        Returns ``(report_date, frame)``: ``frame`` is indexed by stock id with

            prev_volume_shares   ``price:成交股數`` on ``report_date`` (shares,
                                 float)
            prev_turnover        ``price:成交金額`` on ``report_date``, NTD
                                 (float)
            prev_close           ``price:收盤價`` on ``report_date``, NTD per
                                 share (float)
            shares_outstanding   ``company_basic_info``'s 已發行普通股數或TDR原發行
                                 股數 (shares, float; NaN for a stock without a
                                 positive count)
            market               ``company_basic_info``'s 市場別 ("sii", "otc",
                                 "rotc"; NaN for a stock it does not list)

        The turnover rate (volume / shares) and the market capitalisation
        (close x shares) are the consumer's to compute. A missing cell is NaN,
        never 0, and the index is the union of the three price tables' stocks
        and ``company_basic_info``'s. The three price tables must share one
        ``report_date`` (``FinLabAlignmentError`` otherwise) and go through
        ``get_table``, so a warm day needs no token; ``company_basic_info`` is
        one undated table fetched on every call through ``_fetch_dataset``
        (like ``get_dividend_announcements``) and not cached.
        """
        today = today or taipei_today()
        frames = {key: self.get_table(name, today) for key, name in UNIVERSE_STATS_DATASETS.items()}
        report_date, rows = align_latest_rows(frames, today)
        prices = pd.DataFrame(
            {
                "prev_volume_shares": pd.to_numeric(rows["volume"], errors="coerce"),
                "prev_turnover": pd.to_numeric(rows["turnover"], errors="coerce"),
                "prev_close": pd.to_numeric(rows["close"], errors="coerce"),
            }
        )
        info = pd.DataFrame(self._fetch_dataset(COMPANY_BASIC_INFO_DATASET)).copy()
        info["stock_id"] = info["stock_id"].astype(str)
        info = info.drop_duplicates("stock_id", keep="last").set_index("stock_id")
        shares = pd.to_numeric(info[SHARES_OUTSTANDING_COLUMN], errors="coerce")
        company = pd.DataFrame(
            {"shares_outstanding": shares.where(shares > 0), "market": info[LISTING_MARKET_COLUMN]}
        )
        prices.index = prices.index.astype(str)
        out = prices.join(company, how="outer")
        out.index.name = "stock_id"
        return report_date, out.sort_index()

    def get_material_announcements(
        self, stock_ids: Optional[List[str]] = None, since=None
    ) -> pd.DataFrame:
        """重訊公告 rows from the ``important_info_announcement`` dataset.

        See the module-level comment above ``MATERIAL_ANNOUNCEMENT_DATASET``
        for how that dataset id was confirmed and for its real shape. Returns
        ``stock_id, announce_date, category_raw`` (one row per announcement):

            stock_id       the filing ISSUER's own 4-digit equity code -- for
                           a convertible bond this is its *underlying stock*,
                           never the bond's own cb_id (resolve with
                           ``CbInfoWrapper.get_cb_underlying_map`` first)
            announce_date  the announcement's own timestamp (pandas Timestamp)
            category_raw   the announcement's title, verbatim -- this method
                           does NOT classify it into the owner's nine 禁止轉換
                           categories (docs/tasks/42 §3); that mapping needs
                           the full announcement text and belongs to the CB
                           page task, not here.

        ``stock_ids`` (optional): keep only these issuer stock ids. ``since``
        (optional, a date or ``"YYYY-MM-DD"``): keep only announcements on or
        after it. Both default to no filtering (the whole history, ~1.26M
        rows as of 2026-10-08).

        Every call re-fetches the dataset through ``_fetch_dataset`` (no
        per-day cache like ``get_table``'s trailing-rows file, because this
        is one accumulating event log rather than a daily snapshot table); a
        caller that wants to avoid repeated ``finlab.data.get()`` calls
        caches the result itself.
        """
        raw = self._fetch_dataset(MATERIAL_ANNOUNCEMENT_DATASET)
        frame = pd.DataFrame(raw).copy()
        frame = frame.rename(columns={"date": "announce_date", "title": "category_raw"})
        frame["stock_id"] = frame["stock_id"].astype(str)
        frame["announce_date"] = pd.to_datetime(frame["announce_date"])
        if stock_ids is not None:
            wanted = {str(s) for s in stock_ids}
            frame = frame[frame["stock_id"].isin(wanted)]
        if since is not None:
            frame = frame[frame["announce_date"] >= pd.Timestamp(since)]
        return frame[["stock_id", "announce_date", "category_raw"]].reset_index(drop=True)

    def get_dividend_announcements(self, *, since=None) -> pd.DataFrame:
        """除權息預告表 (``dividend_announcement`` dataset -- see the module-
        level comment above ``DIVIDEND_ANNOUNCEMENT_DATASET`` for how that
        id was confirmed). Returns, one row per announced distribution
        event:

            stock_id                   the issuer's own 4-digit equity code
            company_name                FinLab's own 公司名稱
            ex_rights_date              除權交易日 (stock dividend), NaT if none
            ex_dividend_date            除息交易日 (cash dividend), NaT if none
            cash_dividend_per_share     元/股, real cash paid per share --
                                         盈餘分配之股東現金股利 plus 法定盈餘
                                         公積、資本公積發放之現金 (both are
                                         cash, just different funding
                                         sources on the books), 0 when absent
            stock_dividend_per_share    元/股, the equivalent pair of
                                         stock-dividend columns summed the
                                         same way, 0 when absent
            announce_date               公告日期, the day this row was
                                         announced; a corrected event is a
                                         new row for the same stock and
                                         ex-date with a later announce_date,
                                         the old row stays in the table

        This method does NOT decide which of those two evaporates a market-
        cap-weighted index (a stock dividend is base-value-adjusted, not a
        real outflow) -- that rule belongs to the consumer (docs/pages/
        tx.md), not the SDK.

        ``since`` (optional, a date or ``"YYYY-MM-DD"``): keep only rows
        whose ``ex_rights_date`` or ``ex_dividend_date`` falls on or after
        it -- the caller decides what "upcoming" means. Defaults to no
        filtering (the whole announced history).

        Every call re-fetches the dataset through ``_fetch_dataset`` (no
        per-day cache, the same choice ``get_material_announcements`` makes
        and for the same reason: this is an accumulating announcement log,
        not a daily snapshot table)."""
        raw = self._fetch_dataset(DIVIDEND_ANNOUNCEMENT_DATASET)
        frame = pd.DataFrame(raw).copy()
        cash = (
            pd.to_numeric(frame["盈餘分配之股東現金股利(元/股)"], errors="coerce").fillna(0)
            + pd.to_numeric(frame["法定盈餘公積、資本公積發放之現金(元/股)"], errors="coerce").fillna(0)
        )
        stock = (
            pd.to_numeric(frame["盈餘轉增資配股(元/股)"], errors="coerce").fillna(0)
            + pd.to_numeric(frame["法定盈餘公積、資本公積轉增資配股(元/股)"], errors="coerce").fillna(0)
        )
        out = pd.DataFrame({
            "stock_id": frame["stock_id"].astype(str),
            "company_name": frame["公司名稱"].astype(str),
            "ex_rights_date": pd.to_datetime(frame["除權交易日"]),
            "ex_dividend_date": pd.to_datetime(frame["除息交易日"]),
            "cash_dividend_per_share": cash,
            "stock_dividend_per_share": stock,
            "announce_date": pd.to_datetime(frame["公告日期"]),
        })
        if since is not None:
            since_ts = pd.Timestamp(since)
            out = out[(out["ex_rights_date"] >= since_ts) | (out["ex_dividend_date"] >= since_ts)]
        return out.reset_index(drop=True)
