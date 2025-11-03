#!/usr/bin/env python3
"""
Plot Clean Data
==============

Plot the final clean spread data with natural gaps
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys

def plot_clean_data():
    """Plot the clean spread data"""
    
    print("📊 PLOTTING CLEAN DATA")
    print("=" * 40)
    
    # Load the clean spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/debq2_25_frbq2_25_clean_gaps.parquet"
    
    try:
        df = pd.read_parquet(spread_file)
        print(f"📊 Loaded clean data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        print(f"📋 Columns: {list(df.columns)}")
        
        price_col = 'price'
        
        # Count valid vs NaN
        valid_data = df[price_col].dropna()
        nan_count = df[price_col].isna().sum()
        
        print(f"💰 Valid data points: {len(valid_data)}")
        print(f"📊 NaN values: {nan_count}")
        print(f"📈 Data coverage: {len(valid_data)/len(df)*100:.2f}%")
        
        if not valid_data.empty:
            print(f"💰 Price range: {valid_data.min():.2f} to {valid_data.max():.2f}")
            
            # Calculate statistics
            print(f"\n📈 CLEAN DATA STATISTICS:")
            print(f"   Mean: {valid_data.mean():.2f}")
            print(f"   Std:  {valid_data.std():.2f}")
            print(f"   Min:  {valid_data.min():.2f}")
            print(f"   Max:  {valid_data.max():.2f}")
            
            # Calculate price changes on valid data only
            price_changes = valid_data.diff().dropna()
            
            print(f"\n📈 PRICE CHANGES:")
            print(f"   Max increase: {price_changes.max():.2f}")
            print(f"   Max decrease: {price_changes.min():.2f}")
            print(f"   Std change: {price_changes.std():.3f}")
            print(f"   Mean change: {price_changes.mean():.3f}")
            
            # Check for any remaining large changes
            large_changes = price_changes[abs(price_changes) > 2]
            if not large_changes.empty:
                print(f"\n⚠️  Changes > 2:")
                for idx, change in large_changes.items():
                    print(f"   {idx}: {change:+.2f}")
            else:
                print(f"\n✅ All price changes ≤ 2.0")
        
        # Create the plot
        plt.figure(figsize=(16, 10))
        
        # Main price plot
        plt.subplot(2, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=1.0, color='darkgreen', alpha=0.9, marker='o', markersize=1)
        plt.title('Clean debq2_25_frbq2_25 Spread Price (Natural Gaps)', fontsize=16, fontweight='bold')
        plt.ylabel('Price', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Mark theoretical quarter transition dates
        quarter_transitions = [
            ('2024-12-27', 'Q4→Q1 Transition', 'red'),
            ('2025-03-27', 'Q1→Q2 Transition', 'orange'),
        ]
        
        for trans_date, label, color in quarter_transitions:
            try:
                plt.axvline(x=pd.Timestamp(trans_date), color=color, linestyle='--', 
                           alpha=0.8, linewidth=2, label=label)
            except:
                pass
        
        plt.legend(loc='upper left')
        
        # Add summary text
        plt.text(0.02, 0.95, f'Valid Data: {len(valid_data):,} points ({len(valid_data)/len(df)*100:.1f}%)', 
                transform=plt.gca().transAxes, fontsize=11, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.8))
        
        # Price changes plot
        plt.subplot(2, 1, 2)
        if not valid_data.empty:
            changes = valid_data.diff()
            plt.plot(changes.index, changes, linewidth=0.8, color='blue', alpha=0.8, marker='o', markersize=0.5)
        
        plt.title('Clean Price Changes', fontsize=14, fontweight='bold')
        plt.ylabel('Price Change', fontsize=12)
        plt.xlabel('Time', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Mark reasonable change thresholds
        plt.axhline(y=2, color='orange', linestyle=':', alpha=0.6, label='±2 reference')
        plt.axhline(y=-2, color='orange', linestyle=':', alpha=0.6)
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        
        # Mark quarter transitions on diff plot too
        for trans_date, label, color in quarter_transitions:
            try:
                plt.axvline(x=pd.Timestamp(trans_date), color=color, linestyle='--', 
                           alpha=0.8, linewidth=2)
            except:
                pass
        
        plt.legend()
        
        # Statistics text
        if not price_changes.empty:
            plt.text(0.02, 0.95, f'Max: {price_changes.max():.2f}, Min: {price_changes.min():.2f}, Std: {price_changes.std():.3f}', 
                    transform=plt.gca().transAxes, fontsize=10, 
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8))
        
        plt.tight_layout()
        
        # Save the plot
        save_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/final_clean_spread.png'
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"\n📊 Final clean plot saved: {save_path}")
        
        # Data structure analysis
        print(f"\n📈 DATA STRUCTURE ANALYSIS:")
        
        # Resample to daily to see coverage
        if not valid_data.empty:
            daily_coverage = df.resample('D')[price_col].count()
            trading_days = daily_coverage[daily_coverage > 0]
            
            print(f"   Trading days with data: {len(trading_days)}")
            print(f"   Average points per trading day: {daily_coverage.mean():.1f}")
            print(f"   Best coverage day: {daily_coverage.max()} points")
            
            # Time gaps analysis
            time_diffs = valid_data.index.to_series().diff()
            large_gaps = time_diffs[time_diffs > pd.Timedelta(hours=1)]
            
            print(f"   Gaps > 1 hour: {len(large_gaps)}")
            if not large_gaps.empty:
                print(f"   Largest gap: {large_gaps.max()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    plot_clean_data()

if __name__ == "__main__":
    main()