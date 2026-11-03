import os
import pandas as pd
from data_sdk import (
    FinMindWrapper,
    ShioajiWrapper,
    TEJWrapper,
    get_fop_order_book,
    get_fop_parquet,
    get_order_book_odd_lots,
    get_order_book_stocks,
    get_order_book_warrant,
)

def main():
    print("Fetching FinMind broker data...")
    try:
        finmind = FinMindWrapper()
        df_finmind = finmind.get_broker("2026-01-02", "2330")
        print(f"FinMind data shape: {df_finmind.shape}")
        print(df_finmind.head())
    except Exception as e:
        print(f"FinMind error: {e}")

    print("\nFetching order book from parquet (stocks)...")
    try:
        df_ob = get_order_book_stocks("2026-01-02", is_twse=True, sid="2330")
        print(f"Order book stocks shape: {df_ob.shape}")
        print(df_ob.head())
    except Exception as e:
        print(f"Order book stocks error: {e}")

    print("\nFetching order book from parquet (odd lots)...")
    try:
        df_odd = get_order_book_odd_lots("2026-01-02", is_twse=True, sid="2330")
        print(f"Order book odd lots shape: {df_odd.shape}")
        print(df_odd.head())
    except Exception as e:
        print(f"Order book odd lots error: {e}")

    print("\nFetching order book from parquet (warrant, single sid)...")
    try:
        df_w_sid = get_order_book_warrant("2026-06-02", is_twse=True, sid="058998")
        print(f"Order book warrant (sid) shape: {df_w_sid.shape}")
        print(df_w_sid.head())
    except Exception as e:
        print(f"Order book warrant (sid) error: {e}")

    print("\nFetching FOP parquet (TAIFEX futures deals, single instrument)...")
    try:
        df_fop = get_fop_parquet("2026-07-03", "futures", "i024", instrument="TXFG6")
        print(f"FOP i024 shape: {df_fop.shape}")
        print(df_fop.head())
    except Exception as e:
        print(f"FOP parquet error: {e}")

    print("\nReconstructing FOP order book (TAIFEX futures, single instrument)...")
    try:
        df_book = get_fop_order_book("2026-07-03", "futures", "TXFG6")
        print(f"FOP order book shape: {df_book.shape}")
        print(df_book.head())
    except Exception as e:
        print(f"FOP order book error: {e}")

    print("\nFetching Shioaji order book data...")
    try:
        shioaji = ShioajiWrapper()
        df_shioaji = shioaji.get_order_book("2026-01-02", "2330")
        print(f"Shioaji data shape: {df_shioaji.shape}")
        print(df_shioaji.head())
    except Exception as e:
        print(f"Shioaji error: {e}")

    print("\nFetching Shioaji futures ticks (continuous near-month TXFR1)...")
    try:
        df_fut = shioaji.get_futures_ticks("2024-08-01", "TXFR1")
        print(f"Shioaji futures ticks shape: {df_fut.shape}")
        print(df_fut.head())
    except Exception as e:
        print(f"Shioaji futures ticks error: {e}")

    print("\nFetching Shioaji futures contracts (currently listed)...")
    try:
        df_contracts = shioaji.get_futures_contracts()
        print(f"Shioaji futures contracts shape: {df_contracts.shape}")
        print(df_contracts.head())
    except Exception as e:
        print(f"Shioaji futures contracts error: {e}")

    print("\nFetching TEJ monthly revenue announcements (TWN/EWSALE)...")
    try:
        tej = TEJWrapper()
        df_rev = tej.get_ewsale()
        print(f"TEJ revenue shape: {df_rev.shape}")
        print(df_rev.head())
    except Exception as e:
        print(f"TEJ revenue error: {e}")

if __name__ == "__main__":
    main()
