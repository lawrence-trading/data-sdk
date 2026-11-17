import os
import sys

import pandas as pd
from FinMind.data import DataLoader

from .finmind_rate_limit import FinMindRateLimiter
from .paths import env_path


class FinMindWrapper:
    _api = None
    _ref_count = 0
    _limiter = None

    def __init__(self):
        if FinMindWrapper._api is None:
            token = os.environ.get("FINMIND_API_TOKEN")
            if not token:
                print("Warning: FINMIND_API_TOKEN not set. Download may fail.", file=sys.stderr)

            FinMindWrapper._api = DataLoader()
            if token:
                FinMindWrapper._api.login_by_token(api_token=token)
        FinMindWrapper._ref_count += 1

    def __del__(self):
        FinMindWrapper._ref_count -= 1
        if FinMindWrapper._ref_count == 0:
            FinMindWrapper._api = None

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    @staticmethod
    def broker_dir():
        return str(env_path("DATA_SDK_FINMIND_BROKER_PATH"))

    @staticmethod
    def warrant_broker_dir():
        return str(env_path("DATA_SDK_FINMIND_BROKER_WARRANT_PATH"))

    @staticmethod
    def broker_day_path(day, output_dir=None, warrant=False):
        """One day's archive file, ``{dir}/{day}.parquet``, in the stock archive
        (``DATA_SDK_FINMIND_BROKER_PATH``) or the warrant archive
        (``DATA_SDK_FINMIND_BROKER_WARRANT_PATH``)."""
        if output_dir is None:
            output_dir = FinMindWrapper.warrant_broker_dir() if warrant else FinMindWrapper.broker_dir()
        return os.path.join(output_dir, f"{day}.parquet")

    # ------------------------------------------------------------------
    # Read surface -- pure. Never downloads, never writes.
    # ------------------------------------------------------------------

    def get_broker(self, day, sid=None, trader_id=None, warrant=False):
        """Broker rows for ``day`` from the archive, filtered by ``sid`` (one
        instrument, every branch) and/or ``trader_id`` (one branch, every
        instrument); neither filter returns the whole day.

        ``warrant=True`` reads the warrant archive
        (``DATA_SDK_FINMIND_BROKER_WARRANT_PATH``), whose rows have the same
        columns with warrant codes in ``stock_id``.

        Reads never download: the archive is filled by the scheduled writer,
        and a missing day raises instead of costing a whole-day fetch here.
        """
        path = self.broker_day_path(day, warrant=warrant)
        self._require_day_file(day, path)
        filters = []
        if sid is not None:
            filters.append(("stock_id", "==", str(sid)))
        if trader_id is not None:
            filters.append(("securities_trader_id", "==", str(trader_id)))
        out = pd.read_parquet(path, filters=filters or None)
        return out.reset_index(drop=True)

    def get_broker_day(self, day, sids=None, columns=None, warrant=False):
        """Whole-day broker rows, optionally projected and filtered."""
        path = self.broker_day_path(day, warrant=warrant)
        self._require_day_file(day, path)
        filters = None
        if sids is not None:
            filters = [("stock_id", "in", [str(s) for s in sids])]
        out = pd.read_parquet(
            path, columns=list(columns) if columns else None, filters=filters
        )
        return out.reset_index(drop=True)

    def archived_stock_ids(self, day, output_dir=None, warrant=False):
        """Distinct stock (or warrant) ids already stored for ``day``; empty set
        if absent."""
        path = self.broker_day_path(day, output_dir, warrant=warrant)
        if not os.path.isfile(path) or os.path.getsize(path) == 0:
            return set()
        frame = pd.read_parquet(path, columns=["stock_id"])
        return set(frame["stock_id"].astype(str))

    @staticmethod
    def _require_day_file(day, path):
        if not os.path.isfile(path):
            raise FileNotFoundError(f"FinMind broker file not found: {path} (day {day})")
        if os.path.getsize(path) == 0:
            raise FileNotFoundError(f"FinMind broker file is empty: {path} (day {day})")

    def get_trading_dates(self, start_date, end_date):
        """Taiwan trading calendar between the two dates, inclusive.

        Thin accessor over FinMind's ``taiwan_stock_trading_date`` dataset so
        consumers never reach the underlying DataLoader directly. Returns the
        DataLoader's DataFrame (a ``date`` column of ``YYYY-MM-DD`` strings).
        """
        self.limiter().acquire(1)
        return FinMindWrapper._api.taiwan_stock_trading_date(
            start_date=start_date, end_date=end_date
        )

    # ------------------------------------------------------------------
    # Rate limit -- one FinMind budget shared by every caller and host.
    # ------------------------------------------------------------------

    @classmethod
    def limiter(cls):
        if cls._limiter is None:
            ledger = os.environ.get("DATA_SDK_FINMIND_RATE_LEDGER") or os.path.join(
                cls.broker_dir(), ".finmind_rate_ledger.json"
            )
            cls._limiter = FinMindRateLimiter(ledger, capacity=cls._account_limit())
        return cls._limiter

    @classmethod
    def _account_limit(cls):
        """The account's hourly quota, or None when it cannot be read.

        ``api_usage`` is not a trustworthy live counter, so only the ceiling is
        taken from here; pacing comes from the local ledger.
        """
        try:
            return int(FinMindWrapper._api.api_request_limit)
        except Exception:
            return None
