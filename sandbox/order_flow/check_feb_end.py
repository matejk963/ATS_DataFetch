#!/usr/bin/env python3
"""
Check End of February
====================

Analyze what's still happening at end of February after filtering
"""

import pandas as pd
import matplotlib.pyplot as plt
import sys

def check_feb_end():
    """Check end of February price movements"""
    
    print("🔍 CHECKING END OF FEBRUARY")
    print("=" * 40)
    
    # Load both datasets
    original_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq2_25_frbq2_25_tr_ba_data.parquet"
    filtered_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/debq2_25_frbq2_25_filtered.parquet"
    
    try:
        df_orig = pd.read_parquet(original_file)
        df_filt = pd.read_parquet(filtered_file)
        
        # Focus on end of February
        feb_data_orig = df_orig[(df_orig.index >= '2025-02-26') & (df_orig.index <= '2025-02-28')]
        feb_data_filt = df_filt[(df_filt.index >= '2025-02-26') & (df_filt.index <= '2025-02-28')]
        
        print(f"📊 Feb 26-28 original: {len(feb_data_orig)} records")
        print(f"📊 Feb 26-28 filtered: {len(feb_data_filt)} records")
        
        # Calculate price changes
        df_orig['price_diff'] = df_orig['price'].diff()
        df_filt['price_diff'] = df_filt['price'].diff()
        
        # Show Feb price ranges
        if not feb_data_orig.empty:
            print(f"\n📅 ORIGINAL FEB 26-28:")
            print(f"   Price range: {feb_data_orig['price'].min():.2f} to {feb_data_orig['price'].max():.2f}")
            
        if not feb_data_filt.empty:
            print(f"\n📅 FILTERED FEB 26-28:")
            print(f"   Price range: {feb_data_filt['price'].min():.2f} to {feb_data_filt['price'].max():.2f}")
        
        # Find large changes in Feb period - original
        feb_changes_orig = feb_data_orig[abs(feb_data_orig['price'].diff()) > 5]
        if not feb_changes_orig.empty:
            print(f"\n⚠️  LARGE CHANGES IN ORIGINAL FEB DATA:")
            for idx, row in feb_changes_orig.iterrows():
                change = row['price'] - feb_data_orig[feb_data_orig.index < idx]['price'].iloc[-1] if len(feb_data_orig[feb_data_orig.index < idx]) > 0 else 0
                print(f"   {idx}: {row['price']:.2f} (change: {change:+.2f})")
        
        # Find large changes in Feb period - filtered  
        feb_changes_filt = feb_data_filt[abs(feb_data_filt['price'].diff()) > 3]
        if not feb_changes_filt.empty:
            print(f"\n⚠️  REMAINING LARGE CHANGES IN FILTERED FEB DATA:")
            for idx, row in feb_changes_filt.iterrows():
                diff_val = row.get('price_diff', 0)
                print(f"   {idx}: {row['price']:.2f} (change: {diff_val:+.2f})")
        
        # Check the specific Feb 27 area
        feb27_orig = df_orig[(df_orig.index >= '2025-02-27 10:40:00') & (df_orig.index <= '2025-02-27 11:00:00')]
        feb27_filt = df_filt[(df_filt.index >= '2025-02-27 10:40:00') & (df_filt.index <= '2025-02-27 11:00:00')]
        
        print(f"\n🔍 FEB 27 10:40-11:00 COMPARISON:")
        print(f"   Original records: {len(feb27_orig)}")
        print(f"   Filtered records: {len(feb27_filt)}")
        
        if not feb27_orig.empty and not feb27_filt.empty:
            print(f"\n📋 ORIGINAL FEB 27 10:40-11:00:")
            for idx, row in feb27_orig.head(10).iterrows():
                print(f"   {idx}: {row['price']:.2f}")
                
            print(f"\n📋 FILTERED FEB 27 10:40-11:00:")
            for idx, row in feb27_filt.head(10).iterrows():
                print(f"   {idx}: {row['price']:.2f}")
        
        # Plot comparison
        plt.figure(figsize=(15, 8))
        
        plt.subplot(2, 1, 1)
        plt.plot(feb_data_orig.index, feb_data_orig['price'], 'b-', alpha=0.7, label='Original')
        plt.title('Feb 26-28 Original vs Filtered')
        plt.ylabel('Price')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.subplot(2, 1, 2)
        plt.plot(feb_data_filt.index, feb_data_filt['price'], 'g-', alpha=0.7, label='Filtered')
        plt.ylabel('Price')
        plt.xlabel('Time')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/feb_comparison.png', dpi=150)
        print(f"\n📊 Feb comparison saved: feb_comparison.png")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    check_feb_end()

if __name__ == "__main__":
    main()