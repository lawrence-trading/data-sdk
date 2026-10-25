import os
import tempfile
from typing import Optional

import pandas as pd

from .paths import env_path


class TEJWrapper:
    """TEJ (tejapi) datasets with an incremental CSV cache.

    The cache directory comes from ``DATA_SDK_TEJ_CACHE_PATH`` (e.g.
    ``/mnt/nfs/backup/tej_cache``); the API key from ``TEJ_API_TOKEN``
    (required). tejapi configuration is global module state, so it is
    applied once per process.
    """

    _configured = False

    #: Subscription floor for TWN/EWSALE (dataStartYear = 2021).
    EWSALE_MIN_DATE = "2021-01-01"

    #: Trailing window, in days, that every warm call re-fetches. TWN/EWSALE is
    #: not append-only in announcement order: rows stamped ``annd_s = D`` keep
    #: landing in the table for days after D (measured 2026-08-10 -- annd_s
    #: 2026-07-07 held 2 rows when the cursor walked past it and 255 a month
    #: later). A ``{"gt": max_annd}`` cursor can never look back at a day it has
    #: already passed, so those late arrivals were lost for good. Re-asking for
    #: a trailing window makes the merge self-healing instead. 60 days is ~2x
    #: the observed settling time and spans the current 10th-of-the-month
    #: filing peak plus the previous one.
    EWSALE_REFETCH_DAYS = 60

    def __init__(self):
        if not TEJWrapper._configured:
            import tejapi

            api_key = os.environ.get("TEJ_API_TOKEN")
            if not api_key:
                raise RuntimeError(
                    "TEJ_API_TOKEN environment variable is not set"
                )
            tejapi.ApiConfig.api_key = api_key
            tejapi.ApiConfig.ignoretz = True
            TEJWrapper._configured = True

    @staticmethod
    def _cache_dir():
        path = str(env_path("DATA_SDK_TEJ_CACHE_PATH"))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _write_atomic(df, path):
        """Write CSV via a temp file + rename: the cache lives on shared
        NFS, so a concurrent reader must never see a torn file."""
        fd, tmp_path = tempfile.mkstemp(
            dir=os.path.dirname(path), suffix=".csv.tmp"
        )
        os.close(fd)
        try:
            df.to_csv(tmp_path, index=False)
            os.replace(tmp_path, path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    @staticmethod
    def _read_cache(path):
        """Read the CSV cache re-imposing the dtypes CSV cannot carry:
        ``coid`` must stay str at parse time (numeric-looking ids -- a
        leading-zero coid inferred as int is unrepairable afterwards),
        ``mdate``/``annd_s`` datetime64[ns] (parse_dates may infer a coarser
        resolution; consumers compare against ns DatetimeIndexes)."""
        df = pd.read_csv(path, dtype={"coid": str}, parse_dates=["mdate", "annd_s"])
        for col in ("mdate", "annd_s"):
            df[col] = df[col].astype("datetime64[ns]")
        return df

    def get_ewsale(
        self,
        min_date: str = EWSALE_MIN_DATE,
        refetch_since: Optional[str] = None,
    ) -> pd.DataFrame:
        """TWN/EWSALE monthly-revenue announcements, incrementally cached.

        Columns: ``coid`` (str), ``mdate`` (revenue month), ``annd_s``
        (announcement date), ``d0001``/``d0002``/``d0003`` (revenue, prior-year
        revenue, YoY %). Cold start fetches ``annd_s >= min_date``; warm calls
        re-fetch the trailing :data:`EWSALE_REFETCH_DAYS` days and merge,
        deduping on ``(coid, mdate, annd_s)`` keep-last so the freshly fetched
        row wins over the cached one.

        ``mdate`` is part of the dedupe key because one company legitimately
        holds several rows on one ``annd_s``: TEJ emits the current revenue
        month and, on ~1-2% of announcements, a restated prior-year comparison
        base (``mdate`` one year earlier, ``d0001`` only, ``d0002``/``d0003``
        NaN). Keying on ``(coid, annd_s)`` treated the two as duplicates and
        kept whichever the API returned last, destroying the real announcement
        whenever that was the base row.

        ``refetch_since="YYYY-MM-DD"`` overrides the window start for a one-off
        deeper repair; :data:`EWSALE_MIN_DATE` rebuilds the whole table as a
        union-merge (~157k rows, ~31% of the 500k/day TEJ row quota -- not for
        a daily job). Prefer it over deleting ``ewsale.csv``: a delete leaves
        the shared cache absent for concurrent readers, and the legacy-parquet
        branch below would re-seed from a stale ``ewsale.parquet`` rather than
        cold-fetching.

        Cached as ``ewsale.csv`` (dtypes re-imposed on read); a legacy
        ``ewsale.parquet`` from <= 0.4.0 is migrated in place on first read
        and left for older installs sharing the NFS cache dir.
        """
        import tejapi

        cache_dir = self._cache_dir()
        path = os.path.join(cache_dir, "ewsale.csv")
        legacy_parquet = os.path.join(cache_dir, "ewsale.parquet")

        df = None
        if os.path.isfile(path):
            df = self._read_cache(path)
        elif os.path.isfile(legacy_parquet):
            # One-time migration from the pre-0.4.1 parquet cache. The parquet
            # is deliberately left in place: the cache dir is shared NFS and
            # machines on older data-sdk still read/write it; new code never
            # looks at it again once ewsale.csv exists.
            df = self._normalize_ewsale(pd.read_parquet(legacy_parquet))
            self._write_atomic(df, path)
            print(f"[TEJ EWSALE] migrated {legacy_parquet} -> {path} ({len(df)} rows)")

        if df is not None and len(df) == 0:
            # A truncated cache would make max() NaT, and NaT.strftime yields
            # the literal string "NaT" -- fall through to a cold fetch instead
            # of sending that to the API.
            df = None

        if df is not None:
            # Anchor the trailing window on the newest cached announcement but
            # never later than today: one future-dated glitch row would
            # otherwise pin the window forward and blind every later call for
            # good -- the same "one bad row moves the cursor" failure the
            # window exists to kill. Clamping the anchor down can only widen
            # coverage, since the query has no upper bound.
            anchor = min(df["annd_s"].max(), pd.Timestamp.today().normalize())
            start = anchor - pd.Timedelta(days=self.EWSALE_REFETCH_DAYS)
            if refetch_since is not None:
                start = pd.Timestamp(refetch_since)
            # Hard subscription floor (dataStartYear = 2021); asking below it
            # is an API error, not an empty result.
            start = max(start, pd.Timestamp(self.EWSALE_MIN_DATE))

            df_new = tejapi.get(
                "TWN/EWSALE",
                annd_s={"gte": start.strftime("%Y-%m-%d")},
                paginate=True,
            )
            if len(df_new) > 0:
                n_before = len(df)
                df_new = self._normalize_ewsale(df_new)
                # Cached frame first, re-fetched second, keep="last": the fresh
                # row wins every collision, which is what lets the overlap
                # repair days the old cursor truncated.
                df = pd.concat([df, df_new], ignore_index=True)
                df = df.drop_duplicates(
                    subset=["coid", "mdate", "annd_s"], keep="last"
                )
                df = df.sort_values("annd_s").reset_index(drop=True)
                added = len(df) - n_before
                if added:
                    # The window re-fetches rows we already hold, so df_new is
                    # ~5k rows on every call; only pay the 7 MB atomic NFS
                    # rewrite when the merge actually grew the table. get_ewsale
                    # runs more than once per process (yoy_panel and
                    # announced_on_day each go through it) and an unconditional
                    # write would rename the shared cache file on every one.
                    # Trade-off: an in-place value revision of a row we already
                    # hold reaches the caller -- the merged frame is returned
                    # either way -- but only lands on disk the next time a row
                    # is added, at most a few days during a filing month.
                    self._write_atomic(df, path)
                print(
                    f"[TEJ EWSALE] refetched annd_s >= {start.date()}: "
                    f"{len(df_new)} returned, {added} new, total {len(df)}"
                )
        else:
            print(f"[TEJ EWSALE] cold fetch (annd_s >= {min_date})...")
            df = tejapi.get("TWN/EWSALE", annd_s={"gte": min_date}, paginate=True)
            df = self._normalize_ewsale(df)
            df = df.sort_values("annd_s").reset_index(drop=True)
            self._write_atomic(df, path)
            print(f"[TEJ EWSALE] cached {len(df)} rows at {path}")

        return df

    @staticmethod
    def _normalize_ewsale(df):
        df = df.reset_index(drop=True)
        df["coid"] = df["coid"].astype(str)
        df["mdate"] = pd.to_datetime(df["mdate"])
        df["annd_s"] = pd.to_datetime(df["annd_s"])
        return df
