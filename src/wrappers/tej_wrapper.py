import os

import pandas as pd

from .paths import env_path


class TEJWrapper:
    """TEJ (tejapi) datasets, read from the shared CSV cache.

    The cache directory comes from ``DATA_SDK_TEJ_CACHE_PATH`` (e.g.
    ``/mnt/nfs/backup/tej_cache``). Reads never call TEJ and need no token: the
    cache is written by the scheduled job in sysadmin-utils
    (``tej_wrapper/refresh_ewsale.py``), run by the galileo user.
    """

    @staticmethod
    def _cache_dir():
        return str(env_path("DATA_SDK_TEJ_CACHE_PATH"))

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

    def get_ewsale(self) -> pd.DataFrame:
        """TWN/EWSALE monthly-revenue announcements from ``ewsale.csv``.

        Columns: ``coid`` (str), ``mdate`` (revenue month), ``annd_s``
        (announcement date), ``d0001``/``d0002``/``d0003`` (revenue, prior-year
        revenue, YoY %). One company can hold several rows on one ``annd_s``:
        TEJ emits the current revenue month and, on ~1-2% of announcements, a
        restated prior-year comparison base (``mdate`` one year earlier,
        ``d0001`` only).

        Raises ``FileNotFoundError`` when the cache is missing. Prints the
        file's write time: the cache is only as fresh as the last refresh.
        """
        path = os.path.join(self._cache_dir(), "ewsale.csv")
        df = self._read_cache(path)
        written = pd.Timestamp.fromtimestamp(os.path.getmtime(path)).floor("s")
        print(f"[TEJ EWSALE] {path}: {len(df)} rows, written {written}")
        return df
