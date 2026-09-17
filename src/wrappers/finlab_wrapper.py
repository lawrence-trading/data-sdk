"""FinLab daily reference tables for the SSF page, cached per query day.

Four pulls, all daily tables indexed by report date with one column per
stock id:

    margin_balance         margin_transactions:融資今日餘額   (張)
    short_balance          margin_transactions:融券今日餘額   (張)
    lending_balance        security_lending_sell:借券賣出餘額 (張)
    day_trade_sell_first   intraday_trading:得先賣後買當沖

FinLab lands these at about 19:24 Taipei, so the row a consumer may trust
is the last one *strictly before today* (``latest_before_today``); a table
with no such row raises. Sibling tables must agree on that ``report_date``
or the whole pull raises ``FinLabAlignmentError`` -- a consumer then
degrades the affected cells, it never renders a silently older row.

Cache: ``DATA_SDK_FINLAB_CACHE_PATH`` (default ``/mnt/nfs/backup/finlab_cache``),
one parquet per ``(dataset, query day)`` holding the trailing rows dated
before that day. A warm read needs no token; a cold fetch needs
``FINLAB_API_TOKEN`` and imports ``finlab`` lazily.
"""

import datetime as dt
import os
import tempfile
from typing import Callable, Dict, Optional, Tuple

import pandas as pd

TAIPEI = dt.timezone(dt.timedelta(hours=8))

DATASETS = {
    "margin_balance": "margin_transactions:融資今日餘額",
    "short_balance": "margin_transactions:融券今日餘額",
    "lending_balance": "security_lending_sell:借券賣出餘額",
    "day_trade_sell_first": "intraday_trading:得先賣後買當沖",
}

# Rows kept per cached table: enough that a holiday cluster still leaves a
# completed trading day in the cache, small enough to be a few KB per stock.
CACHE_TRAILING_ROWS = 10


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

        full = self._fetch_dataset(dataset)
        full = full.copy()
        full.index = pd.to_datetime(full.index)
        full.columns = [str(c) for c in full.columns]
        trailing = full.loc[full.index < pd.Timestamp(today)].tail(CACHE_TRAILING_ROWS)
        self._write_atomic(trailing, path)
        return trailing

    def get_ssf_reference(self, today: Optional[dt.date] = None) -> Tuple[dt.date, pd.DataFrame]:
        """The four SSF reference columns for the last completed trading day.

        Returns ``(report_date, frame)``: ``frame`` is indexed by stock id
        with columns ``margin_balance``, ``short_balance``,
        ``lending_balance`` (張, as FinLab publishes them) and
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
        out.index = out.index.astype(str)
        out.index.name = "stock_id"
        return report_date, out[list(DATASETS)]
