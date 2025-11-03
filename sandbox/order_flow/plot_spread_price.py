#!/usr/bin/env python3
"""
Plot Spread Price
================

Plot the price data from the given spread file
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys

def plot_spread_price():
    """Plot spread price data"""
    
    print("📊 PLOTTING SPREAD PRICE")
    print("=" * 40)
    
    # Load the spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq2_25_frbq2_25_tr_ba_data.parquet"
    
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
        
        # Calculate price differences to identify jumps
        df['price_diff'] = df[price_col].diff()
        large_jumps = df[abs(df['price_diff']) > 5]
        
        if not large_jumps.empty:
            print(f"\n⚠️  Large price jumps (>5):")
            for idx, row in large_jumps.head(10).iterrows():
                print(f"   {idx}: {row[price_col]:.2f} (jump: {row['price_diff']:+.2f})")
        
        # Create the plot
        plt.figure(figsize=(15, 10))
        
        # Main price plot
        plt.subplot(2, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=0.8, alpha=0.8, color='blue')
        plt.title('debq2_25_frbq2_25 Spread Price', fontsize=14, fontweight='bold')
        plt.ylabel('Price', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Mark large jumps
        if not large_jumps.empty:
            plt.scatter(large_jumps.index, large_jumps[price_col], 
                       color='red', s=50, alpha=0.7, zorder=5, label='Large Jumps (>5)')
            plt.legend()
        
        # Mark theoretical quarter transition dates
        quarter_transitions = [
            '2024-12-27',  # Q4 2024 → Q1 2025
            '2025-03-27',  # Q1 2025 → Q2 2025
        ]
        
        for trans_date in quarter_transitions:
            try:
                plt.axvline(x=pd.Timestamp(trans_date), color='green', linestyle='--', 
                           alpha=0.7, linewidth=2)
            except:
                pass
        
        # Add text annotations for transitions
        plt.text(0.02, 0.95, 'Green lines: Expected quarter transitions', 
                transform=plt.gca().transAxes, fontsize=10, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.7))
        
        # Price difference plot
        plt.subplot(2, 1, 2)
        plt.plot(df.index, df['price_diff'], linewidth=0.6, alpha=0.7, color='orange')
        plt.title('Price Changes', fontsize=12, fontweight='bold')
        plt.ylabel('Price Difference', fontsize=12)
        plt.xlabel('Time', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Mark large changes
        plt.axhline(y=5, color='red', linestyle=':', alpha=0.7, label='±5 threshold')
        plt.axhline(y=-5, color='red', linestyle=':', alpha=0.7)
        plt.legend()
        
        # Mark quarter transitions on diff plot too
        for trans_date in quarter_transitions:
            try:
                plt.axvline(x=pd.Timestamp(trans_date), color='green', linestyle='--', 
                           alpha=0.7, linewidth=2)
            except:
                pass
        
        plt.tight_layout()
        
        # Save the plot
        save_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/spread_price_plot.png'
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"\n📊 Plot saved: {save_path}")
        
        # Show summary statistics
        print(f"\n📈 PRICE STATISTICS:")
        print(f"   Mean: {df[price_col].mean():.2f}")
        print(f"   Std:  {df[price_col].std():.2f}")
        print(f"   Min:  {df[price_col].min():.2f}")
        print(f"   Max:  {df[price_col].max():.2f}")
        
        print(f"\n📈 PRICE CHANGE STATISTICS:")
        print(f"   Mean change: {df['price_diff'].mean():.3f}")
        print(f"   Std change:  {df['price_diff'].std():.3f}")
        print(f"   Max increase: {df['price_diff'].max():.2f}")
        print(f"   Max decrease: {df['price_diff'].min():.2f}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    plot_spread_price()

if __name__ == "__main__":
    main()