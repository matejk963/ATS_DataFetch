#!/usr/bin/env python3
"""
Plot Spread Data with DateTime X-Axis
====================================

Comprehensive visualization with proper datetime x-axis formatting.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [16, 12]

def plot_spread_with_datetime(spread_name="debm12_25_debq1_26"):
    """Plot spread data with datetime x-axis"""
    
    print(f"🎨 Creating comprehensive datetime plots for {spread_name}...")
    
    # Define file paths
    base_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test"
    
    files = {
        'Real': f"{base_path}/{spread_name}_tr_ba_datatest_merged_clean_real.parquet",
        'Merged (Cleaned)': f"{base_path}/{spread_name}_tr_ba_datatest_merged_clean_merged.parquet"
    }
    
    # Load data
    data = {}
    for name, filepath in files.items():
        if os.path.exists(filepath):
            df = pd.read_parquet(filepath)
            df.index = pd.to_datetime(df.index)
            data[name] = df
            print(f"   ✅ Loaded {name}: {len(df)} records")
            print(f"      📅 Date range: {df.index.min()} to {df.index.max()}")
        else:
            print(f"   ⚠️  File not found: {name}")
    
    if not data:
        print("❌ No data files found")
        return
    
    # Create comprehensive plot with 4 subplots
    fig, axes = plt.subplots(2, 2, figsize=(20, 14))
    fig.suptitle(f'Spread Analysis: {spread_name} (DateTime Timeline)', fontsize=16, fontweight='bold')
    
    colors = ['blue', 'red']
    
    # Plot 1: Combined price timeline
    ax1 = axes[0, 0]
    for i, (name, df) in enumerate(data.items()):
        if len(df) > 0:
            ax1.scatter(df.index, df['price'], alpha=0.6, s=8, c=colors[i], label=f'{name} ({len(df)} points)')
    
    ax1.set_title('Price Timeline (All Data Sources)')
    ax1.set_ylabel('Price')
    ax1.set_xlabel('DateTime')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Format x-axis for better readability
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    
    # Plot 2: Price distribution comparison
    ax2 = axes[0, 1]
    for i, (name, df) in enumerate(data.items()):
        if len(df) > 0:
            ax2.hist(df['price'], bins=30, alpha=0.6, color=colors[i], label=name, edgecolor='black')
    
    ax2.set_title('Price Distribution Comparison')
    ax2.set_xlabel('Price')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Volume over time
    ax3 = axes[1, 0]
    for i, (name, df) in enumerate(data.items()):
        if len(df) > 0 and 'volume' in df.columns:
            # Create hourly volume aggregation for better visibility
            hourly_volume = df.resample('1h')['volume'].sum().dropna()
            ax3.plot(hourly_volume.index, hourly_volume.values, 
                    alpha=0.8, marker='o', markersize=3, label=f'{name} (Hourly Sum)')
    
    ax3.set_title('Volume Timeline (Hourly Aggregation)')
    ax3.set_ylabel('Volume')
    ax3.set_xlabel('DateTime')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax3.xaxis.set_major_locator(mdates.HourLocator(interval=12))
    
    # Plot 4: Daily statistics
    ax4 = axes[1, 1]
    for i, (name, df) in enumerate(data.items()):
        if len(df) > 0:
            # Daily aggregation
            daily_stats = df.groupby(df.index.date).agg({
                'price': ['mean', 'std', 'count'],
                'volume': 'sum'
            }).round(3)
            
            daily_stats.columns = ['price_mean', 'price_std', 'trade_count', 'volume_sum']
            daily_stats.index = pd.to_datetime(daily_stats.index)
            
            ax4.plot(daily_stats.index, daily_stats['price_mean'], 
                    marker='o', markersize=4, label=f'{name} Daily Mean', color=colors[i])
            
            # Add error bars for standard deviation
            ax4.fill_between(daily_stats.index, 
                           daily_stats['price_mean'] - daily_stats['price_std'],
                           daily_stats['price_mean'] + daily_stats['price_std'],
                           alpha=0.2, color=colors[i])
    
    ax4.set_title('Daily Price Statistics')
    ax4.set_ylabel('Daily Mean Price ± Std')
    ax4.set_xlabel('Date')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.tick_params(axis='x', rotation=45)
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / f'{spread_name}_datetime_analysis.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ DateTime analysis plot saved: {plot_path}")
    
    # Print detailed statistics
    print(f"\\n📊 Detailed DateTime Analysis:")
    print(f"{'Source':<20} {'Records':<8} {'Date Range':<35} {'Price Range':<20}")
    print("-" * 85)
    
    for name, df in data.items():
        if len(df) > 0:
            date_range = f"{df.index.min().strftime('%Y-%m-%d %H:%M')} to {df.index.max().strftime('%Y-%m-%d %H:%M')}"
            price_range = f"{df['price'].min():.3f} to {df['price'].max():.3f}"
            print(f"{name:<20} {len(df):<8} {date_range:<35} {price_range:<20}")
            
            # Show some datetime-based statistics
            total_days = (df.index.max() - df.index.min()).days
            avg_trades_per_day = len(df) / max(total_days, 1)
            print(f"   📅 Total period: {total_days} days")
            print(f"   📊 Average trades per day: {avg_trades_per_day:.1f}")
            
            if 'volume' in df.columns:
                daily_volume = df.groupby(df.index.date)['volume'].sum()
                print(f"   💹 Average daily volume: {daily_volume.mean():.1f}")
            print()

def main():
    """Main function"""
    plot_spread_with_datetime("debm12_25_debq1_26")

if __name__ == "__main__":
    main()