import os
from datetime import date, datetime, timedelta
from typing import Union

import pandas as pd
import polars as pl
from FinMind.data import DataLoader

from .paths import env_path


class VixWrapper:
    """Read intraday TAIFEX VIX from the local parquet archive and FinMind."""

    def __init__(self):
        self._api = None

    @staticmethod
    def get_vix_parquet(
        start_date: str, end_date: str, *, lazy: bool = False
    ) -> Union[pd.DataFrame, pl.LazyFrame]:
        """Read all available intraday TAIFEX VIX observations in an inclusive range.

        Both dates accept YYYY-MM-DD or YYYYMMDD. Daily files are read from
        DATA_SDK_VIX_PARQUET_PATH. A range with no daily files returns an empty
        result with the usual date, time and vix columns. By default, return a pandas
        DataFrame. Pass lazy=True to return the Polars LazyFrame.
        """
        dates = []
        for value in (start_date, end_date):
            date_format = "%Y-%m-%d" if "-" in value else "%Y%m%d"
            day = datetime.strptime(value, date_format).date()
            if day.strftime(date_format) != value:
                raise ValueError("Expected a date in YYYY-MM-DD or YYYYMMDD format")
            dates.append(day.isoformat())
        start, end = dates
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        directory = env_path("DATA_SDK_VIX_PARQUET_PATH")
        paths = sorted(
            path for path in directory.iterdir()
            if path.suffix == ".parquet" and start <= path.stem <= end and path.is_file()
        )
        if paths:
            frame = pl.scan_parquet(paths)
        else:
            frame = pl.LazyFrame(schema={"date": pl.Utf8, "time": pl.Utf8, "vix": pl.Float64})
        return frame if lazy else frame.collect().to_pandas()

    def get_vix_finmind(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Read TaiwanOptionVix directly from FinMind for the inclusive date range."""
        start, end = (datetime.strptime(value, "%Y-%m-%d" if "-" in value else "%Y%m%d")
                      for value in (start_date, end_date))
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        if self._api is None:
            self._api = DataLoader(token=os.environ.get("FINMIND_API_TOKEN", ""))
        recent = self._api.taiwan_option_vix(
            start_date=start.date().isoformat(), end_date=end.date().isoformat(), timeout=60
        )
        columns = ["date", "time", "vix"]
        if recent.empty:
            recent = pd.DataFrame(columns=columns)
        recent = recent.loc[:, columns].astype({"vix": "float64"})
        return recent.sort_values(["date", "time"], ignore_index=True)

    def get_vix(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Return intraday VIX for an inclusive range, ordered by date and time.

        Read the TAIFEX parquet archive before 2026-03-02 and FinMind's
        TaiwanOptionVix from that date onward. Accept YYYY-MM-DD or YYYYMMDD.
        """
        start, end = (
            date.fromisoformat(f"{s[:4]}-{s[4:6]}-{s[6:]}" if len(s) == 8 and s.isdigit() else s)
            for s in (start_date, end_date)
        )
        if start > end:
            raise ValueError("start_date must be on or before end_date")

        finmind_start = date(2026, 3, 2)
        if end < finmind_start:
            return self.get_vix_parquet(start.isoformat(), end.isoformat())

        recent = self.get_vix_finmind(max(start, finmind_start).isoformat(), end.isoformat())
        if start < finmind_start:
            historical = self.get_vix_parquet(start.isoformat(), (finmind_start - timedelta(days=1)).isoformat())
            return pd.concat([historical, recent], ignore_index=True)
        return recent
