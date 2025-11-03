#!/usr/bin/env python3
"""
Plot Filtered Spread
===================

Plot the cleaned filtered spread data
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys

def plot_filtered_spread():
    """Plot the filtered spread data"""
    
    print("📊 PLOTTING FILTERED SPREAD")
    print("=" * 40)
    
    # Load the filtered spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/debq2_25_frbq2_25_filtered.parquet"
    
    try:
        df = pd.read_parquet(spread_file)
        print(f"📊 Loaded filtered data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        print(f"📋 Columns: {list(df.columns)}")
        
        price_col = 'price'
        print(f"💰 Price range: {df[price_col].min():.2f} to {df[price_col].max():.2f}")
        
        # Calculate statistics
        print(f"\n📈 PRICE STATISTICS:")
        print(f"   Mean: {df[price_col].mean():.2f}")
        print(f"   Std:  {df[price_col].std():.2f}")
        print(f"   Min:  {df[price_col].min():.2f}")
        print(f"   Max:  {df[price_col].max():.2f}")
        
        # Calculate price changes
        df['price_diff'] = df[price_col].diff()
        
        # Check for any remaining large jumps
        large_jumps = df[abs(df['price_diff']) > 3]
        if not large_jumps.empty:
            print(f"\n⚠️  Remaining large changes (>3):")
            for idx, row in large_jumps.head(5).iterrows():
                print(f"   {idx}: {row[price_col]:.2f} (change: {row['price_diff']:+.2f})")
        else:
            print(f"\n✅ No large jumps remaining (>3)")
        
        # Create the plot
        plt.figure(figsize=(16, 10))
        
        # Main price plot
        plt.subplot(2, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=0.8, color='darkgreen', alpha=0.8)
        plt.title('Filtered debq2_25_frbq2_25 Spread Price', fontsize=16, fontweight='bold')
        plt.ylabel('Price', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Mark theoretical quarter transition dates
        quarter_transitions = [
            ('2024-12-27', 'Q4→Q1 Transition'),
            ('2025-03-27', 'Q1→Q2 Transition'),
        ]
        
        colors = ['red', 'orange']
        for i, (trans_date, label) in enumerate(quarter_transitions):
            try:
                plt.axvline(x=pd.Timestamp(trans_date), color=colors[i], linestyle='--', 
                           alpha=0.8, linewidth=2, label=label)
            except:
                pass
        
        plt.legend(loc='upper left')
        
        # Add summary text
        plt.text(0.02, 0.95, f'Clean Data: {len(df):,} records', 
                transform=plt.gca().transAxes, fontsize=11, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.8))
        
        # Price changes plot
        plt.subplot(2, 1, 2)
        plt.plot(df.index, df['price_diff'], linewidth=0.6, color='blue', alpha=0.7)
        plt.title('Price Changes (Filtered)', fontsize=14, fontweight='bold')
        plt.ylabel('Price Change', fontsize=12)
        plt.xlabel('Time', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Mark reasonable change thresholds
        plt.axhline(y=2, color='orange', linestyle=':', alpha=0.6, label='±2 reference')
        plt.axhline(y=-2, color='orange', linestyle=':', alpha=0.6)
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        
        # Mark quarter transitions on diff plot too
        for i, (trans_date, label) in enumerate(quarter_transitions):
            try:
                plt.axvline(x=pd.Timestamp(trans_date), color=colors[i], linestyle='--', 
                           alpha=0.8, linewidth=2)
            except:
                pass
        
        plt.legend()
        
        # Statistics text
        plt.text(0.02, 0.95, f'Max change: {df["price_diff"].max():.2f}, Min change: {df["price_diff"].min():.2f}', 
                transform=plt.gca().transAxes, fontsize=10, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8))
        
        plt.tight_layout()
        
        # Save the plot
        save_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/clean_spread_plot.png'
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"\n📊 Clean plot saved: {save_path}")
        
        # Daily statistics
        print(f"\n📈 DAILY PRICE CHANGE ANALYSIS:")
        daily_data = df.resample('D')[price_col].agg(['mean', 'std', 'min', 'max', 'count'])
        daily_data['daily_range'] = daily_data['max'] - daily_data['min']
        
        print(f"   Average daily range: {daily_data['daily_range'].mean():.2f}")
        print(f"   Max daily range: {daily_data['daily_range'].max():.2f}")
        print(f"   Days with >5 range: {(daily_data['daily_range'] > 5).sum()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    plot_filtered_spread()

if __name__ == "__main__":
    main()