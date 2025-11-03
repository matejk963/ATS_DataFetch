#!/usr/bin/env python3
"""
Check Dec 27 Jump
================

Analyze why the Dec 27 massive drop wasn't filtered
"""

import pandas as pd
import sys

def check_dec27_jump():
    """Check the Dec 27 price jump"""
    
    print("🔍 CHECKING DEC 27 PRICE JUMP")
    print("=" * 40)
    
    # Load original data
    original_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq2_25_frbq2_25_tr_ba_data.parquet"
    filtered_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/debq2_25_frbq2_25_filtered.parquet"
    
    try:
        df_orig = pd.read_parquet(original_file)
        df_filt = pd.read_parquet(filtered_file)
        
        # Focus on Dec 27 period
        dec_27_data_orig = df_orig[(df_orig.index >= '2024-12-27 08:00:00') & (df_orig.index <= '2024-12-27 11:00:00')]
        dec_27_data_filt = df_filt[(df_filt.index >= '2024-12-27 08:00:00') & (df_filt.index <= '2024-12-27 11:00:00')]
        
        print(f"📊 Dec 27 original data: {len(dec_27_data_orig)} records")
        print(f"📊 Dec 27 filtered data: {len(dec_27_data_filt)} records")
        
        if not dec_27_data_orig.empty:
            print(f"\n📅 ORIGINAL DEC 27 DATA:")
            print(f"   Price range: {dec_27_data_orig['price'].min():.2f} to {dec_27_data_orig['price'].max():.2f}")
            
            # Show first 10 records
            print(f"\n📋 First 10 Dec 27 records (original):")
            for idx, row in dec_27_data_orig.head(10).iterrows():
                print(f"   {idx}: {row['price']:.2f}")
        
        if not dec_27_data_filt.empty:
            print(f"\n📅 FILTERED DEC 27 DATA:")
            print(f"   Price range: {dec_27_data_filt['price'].min():.2f} to {dec_27_data_filt['price'].max():.2f}")
            
            # Show first 10 records  
            print(f"\n📋 First 10 Dec 27 records (filtered):")
            for idx, row in dec_27_data_filt.head(10).iterrows():
                print(f"   {idx}: {row['price']:.2f}")
        
        # Check the largest price changes in original data
        df_orig['price_diff'] = df_orig['price'].diff()
        large_changes = df_orig[abs(df_orig['price_diff']) > 10].sort_values('price_diff', key=abs, ascending=False)
        
        print(f"\n⚠️  LARGEST PRICE CHANGES IN ORIGINAL DATA:")
        for idx, row in large_changes.head(10).iterrows():
            print(f"   {idx}: {row['price']:.2f} (change: {row['price_diff']:+.2f})")
        
        # Check what happened in filtering process
        print(f"\n🔍 WHY WASN'T DEC 27 FILTERED?")
        print(f"   The filter used 5x hourly rolling std (±0.54 threshold)")
        print(f"   But Dec 27 has changes of {large_changes.iloc[0]['price_diff']:.2f}")
        print(f"   This suggests the filter threshold was too conservative")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    check_dec27_jump()

if __name__ == "__main__":
    main()