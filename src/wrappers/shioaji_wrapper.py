import os
import tempfile
import time
import pandas as pd
import shioaji as sj
from pathlib import Path
import sys

class ShioajiWrapper:
    _api = None
    _ref_count = 0
    # This process's own contract-cache directory (see _ensure_api); it lives
    # until the interpreter exits, so every login in the process shares it.
    _contracts_cache = None
    # Max wait for Shioaji's contract-file download, in seconds.
    _CONTRACTS_FETCH_TIMEOUT_S = 60

    def __init__(self):
        # No login here. get_order_book / get_futures_ticks serve an already
        # cached parquet without touching the API, so constructing a wrapper
        # must not require credentials or a network round trip; the client is
        # built by the first call that actually needs one (see _ensure_api).
        ShioajiWrapper._ref_count += 1

    @classmethod
    def _ensure_api(cls):
        """Build and log in the shared client, once, on first real use."""
        if cls._api is not None:
            return cls._api

        # simulation=True: the flag only swaps the order/portfolio plane for
        # api/v1/paper/*. Market data -- ticks, kbars, snapshots, contracts --
        # hits the same endpoints either way, and our key is provisioned
        # paper-only, so a production login is rejected outright with
        # "Token doesn't have production permission". NB: were this wrapper
        # ever to place real orders, they would route to the paper endpoint.
        # Shioaji keeps its contract file in a cache directory, and once it has
        # written one on a given day, later logins that day reuse it without
        # asking the server: a file the exchange publishes after that is never
        # seen, and 參考價 / 漲停價 stay the previous day's. Unless the caller
        # set SJ_CONTRACTS_PATH itself, this process points it at an empty
        # directory of its own before its first login, so a process sees the
        # server's contract file as of that first login; later logins in the
        # same process reuse what it downloaded.
        if not os.environ.get("SJ_CONTRACTS_PATH"):
            if cls._contracts_cache is None:
                cls._contracts_cache = tempfile.TemporaryDirectory(prefix="data-sdk-shioaji-contracts-")
            os.environ["SJ_CONTRACTS_PATH"] = cls._contracts_cache.name
        api = sj.Shioaji(simulation=True)
        api.login(
            api_key=os.environ.get("SHIOAJI_API_KEY"),
            secret_key=os.environ.get("SHIOAJI_SECRET_KEY"),
            # login() takes milliseconds; block until the contract file
            # is downloaded instead of returning immediately (default 0).
            contracts_timeout=cls._CONTRACTS_FETCH_TIMEOUT_S * 1000,
        )
        # Publish only once login has returned. Binding a never-logged-in
        # client here would make every later call skip login and reuse a dead
        # handle, turning one clear auth error into a 60s hang elsewhere.
        cls._api = api
        return cls._api

    @property
    def api(self):
        """The logged-in Shioaji client, logging in on first access."""
        return ShioajiWrapper._ensure_api()

    def __del__(self):
        ShioajiWrapper._ref_count -= 1
        if ShioajiWrapper._ref_count == 0 and ShioajiWrapper._api is not None:
            try:
                ShioajiWrapper._api.logout()
            except Exception as e:
                # API might be already closed or network issues
                pass
            ShioajiWrapper._api = None

    def _ensure_contracts_fetched(self):
        """Block until Shioaji's contract file is downloaded.

        login() waits up to contracts_timeout for the download but may return
        with it still incomplete (it does not raise on timeout), and iterating
        or indexing Contracts before then sees a partial/empty set. Poll
        Contracts.status until Fetched; raise if it never completes.

        Every method that needs the network funnels through here, so this is
        also where the lazy login happens.
        """
        ShioajiWrapper._ensure_api()
        deadline = time.monotonic() + ShioajiWrapper._CONTRACTS_FETCH_TIMEOUT_S
        while ShioajiWrapper._api.Contracts.status != sj.FetchStatus.Fetched:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Shioaji contract download did not complete within "
                    f"{ShioajiWrapper._CONTRACTS_FETCH_TIMEOUT_S}s "
                    f"(Contracts.status={ShioajiWrapper._api.Contracts.status})."
                )
            time.sleep(0.1)

    def _download_ticks_to_parquet(self, day, sid, dest_path: Path, data_dir: Path) -> pd.DataFrame:
        self._ensure_contracts_fetched()

        ticks = ShioajiWrapper._api.ticks(
            contract=ShioajiWrapper._api.Contracts.Stocks[sid],
            date=str(day),
        )
        print(f"Downloading {sid} {day}")
        print(ShioajiWrapper._api.usage())
        df = pd.DataFrame({**ticks})
        data_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest_path)
        return df

    def get_order_book(self, day, sid):
        # Determine parquet path
        data_dir = Path(os.environ.get("DATA_SDK_SHIOAJI_TICKS_PATH", "."))
        if not os.environ.get("DATA_SDK_SHIOAJI_TICKS_PATH"):
            print("Warning: DATA_SDK_SHIOAJI_TICKS_PATH not set. Using current directory.", file=sys.stderr)

        dest_path = data_dir / f"{sid}_{day}.parquet"

        if dest_path.exists():
            if dest_path.stat().st_size == 0:
                dest_path.unlink(missing_ok=True)
            else:
                try:
                    return pd.read_parquet(dest_path)
                except Exception:
                    dest_path.unlink(missing_ok=True)

        return self._download_ticks_to_parquet(day, sid, dest_path, data_dir)

    def _download_futures_ticks_to_parquet(self, day, code, dest_path: Path, data_dir: Path) -> pd.DataFrame:
        self._ensure_contracts_fetched()

        contract = ShioajiWrapper._api.Contracts.Futures[code]
        if contract is None:
            raise KeyError(f"Unknown futures contract: {code}")
        ticks = ShioajiWrapper._api.ticks(
            contract=contract,
            date=str(day),
        )
        print(f"Downloading futures {code} {day}")
        print(ShioajiWrapper._api.usage())
        df = pd.DataFrame({**ticks})
        data_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(dest_path)
        return df

    def get_futures_ticks(self, day, code):
        """Ticks for one futures contract on one day, cached as parquet.

        ``code`` is a Shioaji futures contract code: a product ("TXF"), a
        continuous near-month alias ("CDFR1"), or a month symbol ("CDF202609").
        An empty tick day is cached as an empty parquet, so it is not
        re-downloaded.
        """
        # Determine parquet path
        data_dir = Path(os.environ.get("DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH", "."))
        if not os.environ.get("DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH"):
            print("Warning: DATA_SDK_SHIOAJI_FUTURES_TICKS_PATH not set. Using current directory.", file=sys.stderr)

        dest_path = data_dir / f"{code}_{day}.parquet"

        if dest_path.exists():
            if dest_path.stat().st_size == 0:
                dest_path.unlink(missing_ok=True)
            else:
                try:
                    return pd.read_parquet(dest_path)
                except Exception:
                    dest_path.unlink(missing_ok=True)

        return self._download_futures_ticks_to_parquet(day, code, dest_path, data_dir)

    def get_futures_contracts(self) -> pd.DataFrame:
        """One row per listed futures contract.

        Columns: code, symbol, name, category, delivery_month, delivery_date,
        underlying_kind, underlying_code, unit.

        Shioaji downloads the live contract file at login, so this covers
        currently-listed contracts only; expired or delisted contracts are
        absent.
        """
        self._ensure_contracts_fetched()

        fields = ["code", "symbol", "name", "category", "delivery_month",
                  "delivery_date", "underlying_kind", "underlying_code", "unit"]
        rows = []
        for product in ShioajiWrapper._api.Contracts.Futures:
            for contract in product:
                rows.append({f: getattr(contract, f, None) for f in fields})
        return pd.DataFrame(rows, columns=fields)

    def get_stock_contracts(self) -> pd.DataFrame:
        """One row per listed stock, across every exchange (TSE, OTC, OES).

        Columns: code, name, exchange, category, reference, limit_up,
        limit_down, day_trade, unit, margin_trading_balance,
        short_selling_balance, update_date.

        The three price columns come straight from the exchange's contract
        file, so ``reference`` already reflects ex-rights / ex-dividend
        adjustments, disposal-period resets and new listings, and
        ``limit_up`` / ``limit_down`` are the exchange's own ceiling and
        floor, not a locally computed ``reference * 1.10``. Shioaji
        downloads the contract file at login, so this is available all day,
        unlike the pre-open-only UDP reference-price broadcast.

        ``margin_trading_balance`` / ``short_selling_balance`` are the
        contract file's 融資 / 融券 figures, stamped by ``update_date``
        (the contract file's own as-of date). Whether they carry the
        *outstanding balance* (餘額) or the *remaining quota* (限額 minus
        餘額) is unverified: no consumer has compared them against a public
        table yet. They are passed through untouched under SinoPac's field
        names; a consumer must reconcile them against FinLab
        ``margin_transactions:融資今日餘額`` / ``融券今日餘額`` for the same
        ``update_date`` before trusting either reading.

        ``day_trade`` (``Yes`` / ``No`` / ``OnlyBuy``) is only meaningful
        once the exchange has published the day's flags, after 08:30 Taipei.

        Not for a request path: call it once per morning and persist the
        frame.
        """
        self._ensure_contracts_fetched()

        fields = ["code", "name", "exchange", "category", "reference",
                  "limit_up", "limit_down", "day_trade", "unit",
                  "margin_trading_balance", "short_selling_balance",
                  "update_date"]
        rows = []
        for exchange in ShioajiWrapper._api.Contracts.Stocks:
            for contract in exchange:
                rows.append({f: getattr(contract, f, None) for f in fields})
        return pd.DataFrame(rows, columns=fields)
