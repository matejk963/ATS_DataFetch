#!/usr/bin/env python3
"""
Detailed Price Jump Analysis
===========================

Analyze the specific price jumps in the quarterly spread data
"""

import pandas as pd
import sys

def analyze_detailed_jumps():
    """Detailed analysis of price jumps"""
    
    print("🔍 DETAILED PRICE JUMP ANALYSIS")
    print("=" * 50)
    
    # Load the spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads_out_of_sample/debq2_25_frbq2_25_tr_ba_data_data_fetch_engine_method_synthetic.parquet"
    
    try:
        df = pd.read_parquet(spread_file)
        print(f"📊 Loaded spread data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        
        # Find all price jumps > 8
        df['price_diff'] = df['price'].diff()
        large_jumps = df[abs(df['price_diff']) > 8].copy()
        
        print(f"\n⚠️  LARGE PRICE JUMPS (>8):")
        for idx, row in large_jumps.iterrows():
            print(f"   {idx}: {row['price']:.2f} (jump: {row['price_diff']:+.2f})")
        
        # Focus on February 27 jumps
        print(f"\n🔍 FEBRUARY 27 DETAILED ANALYSIS:")
        feb_27_data = df[(df.index >= '2025-02-27 10:40:00') & (df.index <= '2025-02-27 10:50:00')]
        
        if not feb_27_data.empty:
            for idx, row in feb_27_data.iterrows():
                price_diff = row.get('price_diff', 0)
                print(f"   {idx}: {row['price']:.2f} (change: {price_diff:+.2f}) - Action: {row.get('action', 'N/A')}")
        
        # Check if this could be a quarter transition
        # Q1 ends March 31, so February 27 could be transition period
        print(f"\n🔍 TRANSITION LOGIC CHECK:")
        print(f"   Feb 27, 2025 could be Q1 transition period")
        print(f"   If n_s=3, transition starts 3 business days before Q1 end")
        print(f"   Q1 ends March 31, 2025 (Monday)")
        print(f"   Last business day: March 31")
        print(f"   3 business days back: March 27 (Thursday)")
        print(f"   But seeing jumps on Feb 27... this suggests different transition logic")
        
        # Look for patterns around quarter boundaries
        print(f"\n🔍 QUARTER BOUNDARY ANALYSIS:")
        
        # End of December (Q4 2024 → Q1 2025)
        dec_data = df[(df.index >= '2024-12-27') & (df.index <= '2024-12-31')]
        if not dec_data.empty:
            print(f"\n📅 DECEMBER 27-31 DATA:")
            for idx, row in dec_data.head(10).iterrows():
                price_diff = row.get('price_diff', 0)
                print(f"   {idx}: {row['price']:.2f} (change: {price_diff:+.2f})")
        
        # Check for quarterly contract delivery dates
        print(f"\n🔍 CONTRACT ANALYSIS:")
        print(f"   debq2_25: Q2 2025 delivery (April-June 2025)")
        print(f"   frbq2_25: Q2 2025 delivery (April-June 2025)")
        print(f"   Both contracts deliver in same quarter")
        print(f"   Price jumps suggest relative period (q_X) is changing")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    analyze_detailed_jumps()

if __name__ == "__main__":
    main()