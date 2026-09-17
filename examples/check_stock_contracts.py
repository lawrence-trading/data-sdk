"""Live acceptance check for ShioajiWrapper.get_stock_contracts().

Needs SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY in the environment (paper-plane
key; the wrapper logs in with simulation=True). Nothing else: no FinLab
token, no DATA_SDK_* paths.

Verifies, on the exchange's own contract file:
  1. one row per listed stock with the nine expected columns;
  2. reference / limit_up / limit_down are non-null for a liquid name;
  3. across every TWSE tick band, limit_up == limit_up_price(reference) and
     limit_down == limit_down_price(reference) from
     intraday_short_research/common/pricing.py, so a mismatch pins down
     either a wrong tick ladder or a wrong reference.

Usage:
    .venv/bin/python submodules/data-sdk/examples/check_stock_contracts.py \
        [--pricing PATH/TO/pricing.py] [--liquid 2330]

Exit status is 0 when every check passes, 1 otherwise.
"""

import argparse
import importlib.util
import sys
from pathlib import Path

import pandas as pd

from data_sdk import ShioajiWrapper

EXPECTED_COLUMNS = ["code", "name", "exchange", "category", "reference",
                    "limit_up", "limit_down", "day_trade", "unit"]

# TWSE tick bands by price: (lower bound inclusive, upper bound exclusive).
TICK_BANDS = [
    ("<10", 0.0, 10.0),
    ("10-50", 10.0, 50.0),
    ("50-100", 50.0, 100.0),
    ("100-500", 100.0, 500.0),
    ("500-1000", 500.0, 1000.0),
    (">=1000", 1000.0, float("inf")),
]

DEFAULT_PRICING = (Path(__file__).resolve().parents[2]
                   / "intraday_short_research" / "common" / "pricing.py")


def load_pricing(path: Path):
    spec = importlib.util.spec_from_file_location("pricing", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--pricing", type=Path, default=DEFAULT_PRICING,
                    help="path to intraday_short_research/common/pricing.py")
    ap.add_argument("--liquid", default="2330",
                    help="liquid stock code whose prices must be non-null")
    args = ap.parse_args()

    pricing = load_pricing(args.pricing)
    ok = True

    df = ShioajiWrapper().get_stock_contracts()
    print(f"rows={len(df)} columns={list(df.columns)}")
    print(df["exchange"].value_counts(dropna=False).to_string())

    if list(df.columns) != EXPECTED_COLUMNS:
        print(f"FAIL columns differ from {EXPECTED_COLUMNS}")
        ok = False
    dupes = df["code"].duplicated().sum()
    if dupes:
        print(f"FAIL {dupes} duplicated codes")
        ok = False
    if df.empty:
        print("FAIL empty frame")
        return 1

    liquid = df[df["code"] == args.liquid]
    if liquid.empty:
        print(f"FAIL {args.liquid} not in contract set")
        ok = False
    else:
        row = liquid.iloc[0]
        print(f"{args.liquid} {row['name']}: reference={row['reference']} "
              f"limit_up={row['limit_up']} limit_down={row['limit_down']} "
              f"day_trade={row['day_trade']} unit={row['unit']}")
        if row[["reference", "limit_up", "limit_down"]].isna().any():
            print(f"FAIL {args.liquid} has a null price column")
            ok = False

    prices = df[["code", "name", "exchange", "reference",
                 "limit_up", "limit_down"]].copy()
    for col in ("reference", "limit_up", "limit_down"):
        prices[col] = pd.to_numeric(prices[col], errors="coerce")
    # A zero reference is the exchange's "no band today" marker (a fresh
    # listing in its first sessions); there is nothing to compare against.
    prices = prices[prices["reference"] > 0].dropna(
        subset=["reference", "limit_up", "limit_down"])
    print(f"{len(prices)} rows with a positive reference and both limits")

    prices["expect_up"] = pricing.limit_up_price(prices["reference"])
    prices["expect_down"] = pricing.limit_down_price(prices["reference"])
    # Contract prices are centi-NTD floats; compare with half-tick slack.
    prices["up_ok"] = (prices["limit_up"] - prices["expect_up"]).abs() < 0.005
    prices["down_ok"] = (
        prices["limit_down"] - prices["expect_down"]).abs() < 0.005

    for label, lo, hi in TICK_BANDS:
        band = prices[(prices["reference"] >= lo) & (prices["reference"] < hi)]
        if band.empty:
            print(f"band {label:>8}: no names")
            continue
        bad = band[~(band["up_ok"] & band["down_ok"])]
        status = "PASS" if bad.empty else "FAIL"
        print(f"band {label:>8}: {len(band):5d} names, "
              f"{len(bad)} mismatches -> {status}")
        if not bad.empty:
            ok = False
            cols = ["code", "name", "exchange", "reference", "limit_up",
                    "expect_up", "limit_down", "expect_down"]
            print(bad[cols].head(10).to_string(index=False))

    print("RESULT", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
