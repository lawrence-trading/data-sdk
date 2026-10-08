"""Convertible-bond (可轉債) reference data via FinMind, for the CB page.

Two FinMind datasets, both confirmed present on this machine's installed
FinMind package (``DataLoader.taiwan_stock_convertible_bond_info`` /
``taiwan_stock_convertible_bond_daily_overview``), plus a verification
layer that maps a CB's own code to its underlying stock:

    get_cb_info             static per-bond facts, whole table cached (like
                             ``WarrantInfoWrapper.get_warrant_summary``)
    get_cb_daily_overview   the daily table, read with the same
                             latest-strictly-before-today rule
                             ``finlab_wrapper.py`` applies to FinLab's daily
                             tables -- duplicated here, not imported, because
                             the source (FinMind) and its own date semantics
                             are unrelated to FinLab's
    get_cb_underlying_map   cb_id -> underlying stock_id, verified against
                             Shioaji's real stock codes with a hand-written
                             override table for the rest (ISSUES.md I-216)

Money: a CB's face value is NT$100 per unit, always -- see
``CB_FACE_VALUE_NT`` below. No price column here is a float on a money path;
``ConversionPrice`` / ``PriceOfUnderlyingStock`` etc. stay FinMind's own
values (strings/floats as published) because this wrapper is a pass-through,
the same stance ``taifex_wrapper.py`` takes on exchange-published prices --
the 1/10000 NTD integer conversion happens one layer up, in the page's own
read model, not here.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Callable, Iterable, List, Optional

import pandas as pd

# The owner's 2026-10-08 decision (ISSUES.md I-216): every convertible bond's
# face value is NT$100, full stop -- no per-bond lookup, no "verify this"
# code path second-guessing it.
CB_FACE_VALUE_NT = 100

# taiwan_stock_convertible_bond_info()'s own columns.
CB_INFO_COLUMNS = ["cb_id", "cb_name", "InitialDateOfConversion", "DueDateOfConversion", "IssuanceAmount"]

# The daily-overview columns this wrapper keeps, per docs/tasks/42 §2: the
# conversion price, the stop-conversion window, outstanding amount and the
# underlying stock's own price. ``cb_name``/``date`` are the row's own key.
CB_DAILY_OVERVIEW_COLUMNS = [
    "cb_id", "cb_name", "date",
    "ConversionPrice", "InitialDateOfStopConversion", "DueDateOfStopConversion",
    "OutstandingAmount", "PriceOfUnderlyingStock",
]

_OVERRIDES_README_KEYS = {"_readme", "_comment_zh"}


def _default_overrides_path() -> str:
    return str(Path(__file__).resolve().parent / "cb_underlying_overrides.json")


def _load_overrides(path: str) -> dict:
    """The hand-written ``{cb_id: stock_id}`` table, minus its own docstring keys."""
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return {k: str(v) for k, v in raw.items() if k not in _OVERRIDES_README_KEYS}


class CbInfoWrapper:
    """FinMind convertible-bond data plus the cb_id -> underlying-stock map."""

    def __init__(
        self,
        cache_dir: Optional[str] = None,
        fetch_info: Optional[Callable[[], pd.DataFrame]] = None,
        fetch_daily_overview: Optional[Callable[[], pd.DataFrame]] = None,
        get_stock_codes: Optional[Callable[[], Iterable[str]]] = None,
    ):
        # All three network-shaped dependencies are injectable so every test
        # runs with a synthetic fetch/stock-code source and no
        # FINMIND_API_TOKEN, no Shioaji login (docs/tasks/42 §測試).
        self._cache_dir = cache_dir or self.cache_dir()
        self._fetch_info = fetch_info
        self._fetch_daily_overview = fetch_daily_overview
        self._get_stock_codes = get_stock_codes
        self._info_cache: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @staticmethod
    def cache_dir() -> str:
        return os.environ.get("DATA_SDK_CB_CACHE_PATH", "/mnt/nfs/backup/finmind_cache/cb")

    def _info_cache_path(self) -> str:
        return os.path.join(self._cache_dir, "cb_info.parquet")

    def _overview_cache_path(self, day: str) -> str:
        return os.path.join(self._cache_dir, "cb_daily_overview", f"{day}.parquet")

    @staticmethod
    def _write_atomic(frame: pd.DataFrame, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".parquet.tmp")
        os.close(fd)
        try:
            frame.to_parquet(tmp)
            os.chmod(tmp, 0o644)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    # ------------------------------------------------------------------
    # Network (lazy; only reached on a cold cache)
    # ------------------------------------------------------------------

    def _default_fetch_info(self) -> pd.DataFrame:
        token = os.environ.get("FINMIND_API_TOKEN")
        if not token:
            raise RuntimeError(
                "FINMIND_API_TOKEN is not set and the CB info cache is empty"
            )
        from FinMind.data import DataLoader

        api = DataLoader()
        api.login_by_token(api_token=token)
        return api.taiwan_stock_convertible_bond_info()

    def _default_fetch_daily_overview(self) -> pd.DataFrame:
        token = os.environ.get("FINMIND_API_TOKEN")
        if not token:
            raise RuntimeError(
                "FINMIND_API_TOKEN is not set and the CB daily-overview cache is empty"
            )
        from FinMind.data import DataLoader

        api = DataLoader()
        api.login_by_token(api_token=token)
        return api.taiwan_stock_convertible_bond_daily_overview()

    # ------------------------------------------------------------------
    # Read surface
    # ------------------------------------------------------------------

    def get_cb_info(self, refresh: bool = False) -> pd.DataFrame:
        """可轉債總覽: ``cb_id, cb_name, InitialDateOfConversion,
        DueDateOfConversion, IssuanceAmount``. Whole table, cached to
        ``<DATA_SDK_CB_CACHE_PATH>/cb_info.parquet`` (``WarrantInfoWrapper
        .get_warrant_summary``'s own pattern); ``refresh=True`` forces a
        cold fetch.
        """
        path = self._info_cache_path()
        if not refresh and self._info_cache is not None:
            return self._info_cache
        if not refresh and os.path.isfile(path) and os.path.getsize(path) > 0:
            self._info_cache = pd.read_parquet(path)
            return self._info_cache

        fetch = self._fetch_info or self._default_fetch_info
        frame = pd.DataFrame(fetch())[CB_INFO_COLUMNS].copy()
        frame["cb_id"] = frame["cb_id"].astype(str)
        self._write_atomic(frame, path)
        self._info_cache = frame
        return frame

    def get_cb_daily_overview(self, day=None) -> pd.DataFrame:
        """可轉債每日總覽資訊, one row per cb_id: the last published row
        strictly before ``day`` (default: today, Taipei) -- the same
        latest-before-today rule ``finlab_wrapper.latest_before_today``
        applies to FinLab's daily tables, duplicated here because this
        table's source and date semantics are FinMind's own, not FinLab's.

        Columns: ``CB_DAILY_OVERVIEW_COLUMNS`` (cb_id, cb_name, date,
        ConversionPrice, InitialDateOfStopConversion,
        DueDateOfStopConversion, OutstandingAmount, PriceOfUnderlyingStock).
        Cached per ``day`` to
        ``<DATA_SDK_CB_CACHE_PATH>/cb_daily_overview/<day>.parquet``.
        """
        import datetime as dt

        taipei = dt.timezone(dt.timedelta(hours=8))
        day = day or dt.datetime.now(taipei).date().isoformat()
        path = self._overview_cache_path(str(day))
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            return pd.read_parquet(path)

        fetch = self._fetch_daily_overview or self._default_fetch_daily_overview
        raw = pd.DataFrame(fetch()).copy()
        raw["date"] = pd.to_datetime(raw["date"])
        eligible = raw[raw["date"] < pd.Timestamp(day)]
        if eligible.empty:
            raise RuntimeError(f"CB daily overview has no row dated before {day}")
        latest_idx = eligible.groupby("cb_id")["date"].idxmax()
        out = eligible.loc[latest_idx, CB_DAILY_OVERVIEW_COLUMNS].reset_index(drop=True)
        out["cb_id"] = out["cb_id"].astype(str)

        self._write_atomic(out, path)
        return out

    def get_cb_underlying_map(
        self,
        cb_ids: Optional[List[str]] = None,
        overrides_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """cb_id -> its underlying stock_id, verified rather than guessed.

        A cb_id's first 4 characters are tried as a stock_id and checked
        against ``ShioajiWrapper.get_stock_contracts()['code']`` (or the
        injected ``get_stock_codes``); a hit is ``source="candidate"``. A
        miss is looked up in the hand-written override table at
        ``overrides_path`` (default ``cb_underlying_overrides.json`` next to
        this file -- see that file for when to add an entry); a hit there is
        ``source="override"``. A cb_id resolved by neither gets
        ``underlying_stock_id=None``, ``source=None``,
        ``underlying_unresolved=True`` -- the unverified first-4-characters
        guess is never substituted (ISSUES.md I-216).

        ``cb_ids`` defaults to every id in ``get_cb_info()``.

        Columns: cb_id, underlying_stock_id, source, underlying_unresolved.
        """
        if cb_ids is None:
            cb_ids = self.get_cb_info()["cb_id"].tolist()
        overrides = _load_overrides(overrides_path or _default_overrides_path())
        codes = self._stock_codes()

        rows = []
        for cb_id in cb_ids:
            cb_id = str(cb_id)
            candidate = cb_id[:4]
            if candidate in codes:
                rows.append((cb_id, candidate, "candidate", False))
            elif cb_id in overrides:
                rows.append((cb_id, overrides[cb_id], "override", False))
            else:
                rows.append((cb_id, None, None, True))
        out = pd.DataFrame(
            rows, columns=["cb_id", "underlying_stock_id", "source", "underlying_unresolved"]
        )
        # object dtype, not pandas' inferred "str" extension type: a mix of
        # real codes and a missing one must keep a plain None, not silently
        # turn into float NaN just because every other value is a string.
        for column in ("cb_id", "underlying_stock_id", "source"):
            out[column] = out[column].astype(object).where(out[column].notna(), None)
        return out

    def _stock_codes(self) -> set:
        if self._get_stock_codes is not None:
            return {str(c) for c in self._get_stock_codes()}
        from .shioaji_wrapper import ShioajiWrapper

        return {str(c) for c in ShioajiWrapper().get_stock_contracts()["code"]}
