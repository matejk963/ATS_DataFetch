#!/usr/bin/env python3
"""
Analyze Quarter Transition Price Jump
====================================

Examine the debq2_25_frbq2_25 spread for price jumps around quarter transitions
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sys
import os

# Add paths
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

def analyze_quarter_transition():
    """Analyze price jumps around quarter transitions"""
    
    print("🔍 ANALYZING QUARTER TRANSITION PRICE JUMP")
    print("=" * 50)
    
    # Load the spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads_out_of_sample/debq2_25_frbq2_25_tr_ba_data_data_fetch_engine_method_synthetic.parquet"
    
    try:
        df = pd.read_parquet(spread_file)
        print(f"📊 Loaded spread data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        print(f"📋 Columns: {list(df.columns)}")
        
        # Find the price column
        price_col = None
        for col in df.columns:
            if 'price' in col.lower():
                price_col = col
                break
        
        if price_col is None:
            print("❌ No price column found!")
            return
            
        print(f"💰 Using price column: {price_col}")
        print(f"💰 Price range: {df[price_col].min():.2f} to {df[price_col].max():.2f}")
        
        # Focus on the transition period around end of Q1 (March) and Q2 (June)
        # Q1 ends March 31, Q2 ends June 30
        
        # Analyze March transition (Q1 to Q2)
        print("\n🔍 MARCH TRANSITION ANALYSIS (Q1 → Q2)")
        march_data = df[(df.index >= '2025-03-20') & (df.index <= '2025-04-10')]
        if not march_data.empty:
            print(f"📊 March transition period: {len(march_data)} records")
            print(f"📅 Date range: {march_data.index.min()} to {march_data.index.max()}")
            print(f"💰 Price range: {march_data[price_col].min():.2f} to {march_data[price_col].max():.2f}")
            
            # Look for jumps
            march_data['price_diff'] = march_data[price_col].diff()
            large_jumps = march_data[abs(march_data['price_diff']) > 5]
            if not large_jumps.empty:
                print(f"⚠️  Large price jumps (>5) in March:")
                for idx, row in large_jumps.head(10).iterrows():
                    print(f"   {idx}: {row[price_col]:.2f} (jump: {row['price_diff']:+.2f})")
        
        # Analyze June transition (Q2 to Q3) - This is where the user saw the issue
        print("\n🔍 JUNE TRANSITION ANALYSIS (Q2 → Q3)")
        june_data = df[(df.index >= '2025-06-20') & (df.index <= '2025-07-10')]
        if not june_data.empty:
            print(f"📊 June transition period: {len(june_data)} records")
            print(f"📅 Date range: {june_data.index.min()} to {june_data.index.max()}")
            print(f"💰 Price range: {june_data[price_col].min():.2f} to {june_data[price_col].max():.2f}")
            
            # Look for jumps
            june_data['price_diff'] = june_data[price_col].diff()
            large_jumps = june_data[abs(june_data['price_diff']) > 5]
            if not large_jumps.empty:
                print(f"⚠️  Large price jumps (>5) in June:")
                for idx, row in large_jumps.head(10).iterrows():
                    print(f"   {idx}: {row[price_col]:.2f} (jump: {row['price_diff']:+.2f})")
            
            # Check specific dates around June 26 (transition date)
            print(f"\n📅 JUNE 26 TRANSITION DETAILS:")
            june_26_data = june_data[(june_data.index >= '2025-06-24') & (june_data.index <= '2025-06-30')]
            if not june_26_data.empty:
                for idx, row in june_26_data.iterrows():
                    price_diff = row.get('price_diff', 0)
                    print(f"   {idx}: {row[price_col]:.2f} (change: {price_diff:+.2f})")
        
        # Analyze September transition (Q3 to Q4)
        print("\n🔍 SEPTEMBER TRANSITION ANALYSIS (Q3 → Q4)")
        sept_data = df[(df.index >= '2025-09-20') & (df.index <= '2025-10-10')]
        if not sept_data.empty:
            print(f"📊 September transition period: {len(sept_data)} records")
            print(f"📅 Date range: {sept_data.index.min()} to {sept_data.index.max()}")
            print(f"💰 Price range: {sept_data[price_col].min():.2f} to {sept_data[price_col].max():.2f}")
            
            # Look for jumps
            sept_data['price_diff'] = sept_data[price_col].diff()
            large_jumps = sept_data[abs(sept_data['price_diff']) > 5]
            if not large_jumps.empty:
                print(f"⚠️  Large price jumps (>5) in September:")
                for idx, row in large_jumps.head(10).iterrows():
                    print(f"   {idx}: {row[price_col]:.2f} (jump: {row['price_diff']:+.2f})")
        
        # Overall price jump analysis
        print("\n🔍 OVERALL LARGE PRICE JUMPS")
        df['price_diff'] = df[price_col].diff()
        all_large_jumps = df[abs(df['price_diff']) > 8]
        if not all_large_jumps.empty:
            print(f"⚠️  All large price jumps (>8) in dataset:")
            for idx, row in all_large_jumps.iterrows():
                print(f"   {idx}: {row[price_col]:.2f} (jump: {row['price_diff']:+.2f})")
        
        # Create a plot
        plt.figure(figsize=(15, 8))
        
        # Plot the full time series
        plt.subplot(2, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=0.5, alpha=0.7)
        plt.title('debq2_25_frbq2_25 Spread Price - Full Time Series')
        plt.ylabel('Spread Price')
        plt.grid(True, alpha=0.3)
        
        # Mark transition dates
        plt.axvline(x=pd.Timestamp('2025-03-26'), color='red', linestyle='--', alpha=0.7, label='Q1→Q2 Transition')
        plt.axvline(x=pd.Timestamp('2025-06-26'), color='red', linestyle='--', alpha=0.7, label='Q2→Q3 Transition')  
        plt.axvline(x=pd.Timestamp('2025-09-26'), color='red', linestyle='--', alpha=0.7, label='Q3→Q4 Transition')
        plt.legend()
        
        # Zoom in on June transition
        plt.subplot(2, 1, 2)
        if not june_data.empty:
            plt.plot(june_data.index, june_data[price_col], linewidth=1, marker='o', markersize=2)
            plt.title('June Transition Detail (Q2→Q3)')
            plt.ylabel('Spread Price')
            plt.axvline(x=pd.Timestamp('2025-06-26'), color='red', linestyle='--', alpha=0.7, label='Transition Date')
            plt.grid(True, alpha=0.3)
            plt.legend()
        
        plt.tight_layout()
        plt.savefig('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/quarter_transition_analysis.png', dpi=150, bbox_inches='tight')
        print(f"\n📊 Plot saved: quarter_transition_analysis.png")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    analyze_quarter_transition()

if __name__ == "__main__":
    main()