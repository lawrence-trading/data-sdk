"""Calibrate the TXO (臺指選) per-root decimal locator and tick.

The spec is docs/pages/txo.md §2.2 in the taiwan_dashboard monorepo that
consumes this SDK.

Joins TAIFEX ``DailyMarketReportOpt`` (``TradingSession == '一般'``,
``Contract == 'TXO'``, plain monthly contracts only) to the archived
``options_day_<YYYYMMDD>_0840_i024.parquet`` on a PROD-ID this script
reconstructs (``txo_decimal_locator.taifex_option_prod_id``), and for every
matched contract prints ``-round(log10(Close / max(price_raw)))`` (the
locator) alongside the gcd of its consecutive distinct ``price_raw`` values
(the tick), then the contracts whose locator disagrees with the root's own
most common value.

Offline: the daily report comes from the SDK's TAIFEX cache when it holds
the day, so no network is needed once a day is cached. The endpoint serves
only its latest day, so a first run must ask for that day.

Usage:
    .venv/bin/python submodules/data-sdk/scripts/calibrate_txo_locator.py --day 20261007
    .venv/bin/python submodules/data-sdk/scripts/calibrate_txo_locator.py --day 20261007 --write-table

``--write-table`` regenerates ``src/wrappers/txo_decimal_locator_table.py``
with version ``<day>.<n>`` where n increments when the same day is refrozen.
"""

import argparse
import sys
from pathlib import Path

import polars as pl

from data_sdk import TaifexWrapper, get_fop_parquet
from data_sdk.wrappers import txo_decimal_locator as loc

TABLE_PATH = Path(__file__).resolve().parents[1] / "src" / "wrappers" / "txo_decimal_locator_table.py"


def _next_version(day: str) -> str:
    current = loc.txo_decimal_locator_version()
    if current.startswith(day + "."):
        return f"{day}.{int(current.split('.')[1]) + 1}"
    return f"{day}.1"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day", required=True, help="trading day, YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--write-table", action="store_true", help="regenerate the frozen table module")
    parser.add_argument("--all-rows", action="store_true", help="print every contract, not only the summary")
    args = parser.parse_args()
    day = args.day.replace("-", "")

    report = TaifexWrapper().get_daily_market_report_opt(day)
    i024 = get_fop_parquet(day, "options", "i024", lazy=True)
    calibrated = loc.calibrate_locator(report, i024)
    bad = loc.disagreements(calibrated)

    pl.Config.set_tbl_rows(-1)
    pl.Config.set_tbl_cols(-1)
    pl.Config.set_fmt_str_lengths(24)
    if args.all_rows:
        print(calibrated)
    n = calibrated.height
    print(f"day={day} TXO plain-monthly contracts joined: {n}")
    print("locator histogram:")
    print(calibrated.group_by("locator").len().sort("locator"))
    print(f"\ndisagreements with the root's own most common locator: {bad.height}")
    print(bad)
    weak = bad.filter(pl.col("n_steps") < 20).height
    print(f"of which {weak} rest on fewer than 20 price changes (gcd may overshoot the tick)")

    if args.write_table:
        table = loc.freeze_table(calibrated)
        version = _next_version(day)
        TABLE_PATH.write_text(loc.render_table_module(table, version, day), encoding="utf-8")
        print(f"\nfroze {len(table)} root(s) as version {version} -> {TABLE_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
