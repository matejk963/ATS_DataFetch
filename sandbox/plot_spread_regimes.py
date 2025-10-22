#!/usr/bin/env python3
"""
Plot Spread with Regime Analysis
===============================

Create comprehensive visualization of the debm11_25 vs debq1_26 spread
showing structural breaks and regime changes.
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
plt.rcParams['figure.figsize'] = [18, 12]

def plot_spread_with_regimes():
    """Create comprehensive spread plot with regime analysis"""
    
    print("🎨 Creating comprehensive spread regime plot...")
    
    # Load spread data
    spread_data = pd.read_parquet('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test/debm11_25_debq1_26_tr_ba_dataanalysis_real.parquet')
    spread_data.index = pd.to_datetime(spread_data.index)
    spread_data = spread_data.sort_index()
    
    # Define the major regimes
    regimes = [
        {'start': '2025-09-10 11:12', 'end': '2025-09-17 09:02', 'level': -5.1, 'trades': 5, 'color': 'red', 'label': 'Regime 1: Thin Market'},
        {'start': '2025-10-08 09:21', 'end': '2025-10-10 12:03', 'level': -3.5, 'trades': 45, 'color': 'orange', 'label': 'Regime 2: Liquidity Surge'},
        {'start': '2025-10-10 12:33', 'end': '2025-10-14 09:22', 'level': -2.7, 'trades': 53, 'color': 'green', 'label': 'Regime 3: Peak Activity'},
        {'start': '2025-10-14 12:45', 'end': '2025-10-16 15:40', 'level': -1.7, 'trades': 25, 'color': 'blue', 'label': 'Regime 4: Stabilizing'}
    ]
    
    # Create comprehensive plot with 3 subplots
    fig, axes = plt.subplots(3, 1, figsize=(18, 14))
    fig.suptitle('DEBM11_25 vs DEBQ1_26 Spread: Regime Analysis and Structural Breaks', 
                 fontsize=16, fontweight='bold')
    
    # Plot 1: Main spread timeline with regime overlays
    ax1 = axes[0]
    
    # Plot all data points
    ax1.scatter(spread_data.index, spread_data['price'], alpha=0.8, s=40, c='navy', 
               label=f'Spread Trades ({len(spread_data)} total)', zorder=5)
    
    # Add regime level lines
    for regime in regimes:
        start_time = pd.Timestamp(regime['start'])
        end_time = pd.Timestamp(regime['end'])
        
        # Create regime line
        regime_times = [start_time, end_time]
        regime_levels = [regime['level'], regime['level']]
        
        ax1.plot(regime_times, regime_levels, color=regime['color'], linewidth=6, alpha=0.8,
                label=f"{regime['label']}: {regime['level']:.1f} EUR/MWh ({regime['trades']} trades)")
        
        # Add regime background shading
        ax1.axvspan(start_time, end_time, alpha=0.1, color=regime['color'])
        
        # Add regime annotations
        mid_time = start_time + (end_time - start_time) / 2
        ax1.annotate(f'{regime["level"]:.1f}\n({regime["trades"]} trades)', 
                    xy=(mid_time, regime['level']), 
                    xytext=(10, 10), textcoords='offset points',
                    bbox=dict(boxstyle='round,pad=0.3', facecolor=regime['color'], alpha=0.7),
                    fontsize=9, fontweight='bold', color='white')
    
    ax1.set_title('Spread Price Timeline with Market Regimes', fontsize=14, fontweight='bold')
    ax1.set_ylabel('Spread Price (EUR/MWh)', fontsize=12)
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Format x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    
    # Plot 2: Trading activity and price correlation
    ax2 = axes[1]
    
    # Create daily trading activity
    daily_data = spread_data.groupby(spread_data.index.date).agg({
        'price': ['mean', 'count'],
        'volume': 'sum'
    }).round(3)
    daily_data.columns = ['avg_price', 'trade_count', 'total_volume']
    daily_data.index = pd.to_datetime(daily_data.index)
    
    # Plot trading activity
    bars = ax2.bar(daily_data.index, daily_data['trade_count'], alpha=0.7, 
                   color='lightblue', width=pd.Timedelta(hours=18), label='Daily Trade Count')
    
    # Add price level on secondary y-axis
    ax2_twin = ax2.twinx()
    line = ax2_twin.plot(daily_data.index, daily_data['avg_price'], 'ro-', 
                        linewidth=2, markersize=6, label='Daily Avg Price')
    
    ax2.set_title('Trading Activity vs Price Levels', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Daily Trade Count', fontsize=12, color='blue')
    ax2_twin.set_ylabel('Average Price (EUR/MWh)', fontsize=12, color='red')
    ax2.tick_params(axis='y', labelcolor='blue')
    ax2_twin.tick_params(axis='y', labelcolor='red')
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    
    # Add combined legend
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    # Plot 3: Price changes and structural breaks
    ax3 = axes[2]
    
    # Calculate price changes
    price_changes = spread_data['price'].diff().fillna(0)
    
    # Plot price changes as bars
    colors = ['red' if abs(x) > 1.0 else 'gray' for x in price_changes]
    ax3.bar(spread_data.index, price_changes, alpha=0.7, color=colors, 
           width=pd.Timedelta(hours=2), label='Price Changes')
    
    # Highlight major structural breaks
    large_changes = price_changes[abs(price_changes) > 1.0]
    if len(large_changes) > 0:
        ax3.bar(large_changes.index, large_changes.values, alpha=0.9, color='red', 
               width=pd.Timedelta(hours=2), label=f'Major Breaks (>{1.0} EUR/MWh)')
        
        # Annotate major breaks
        for timestamp, change in large_changes.items():
            ax3.annotate(f'{change:+.2f}', xy=(timestamp, change), 
                        xytext=(0, 10 if change > 0 else -20), textcoords='offset points',
                        ha='center', fontweight='bold', color='red')
    
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax3.set_title('Price Changes and Structural Breaks', fontsize=14, fontweight='bold')
    ax3.set_ylabel('Price Change (EUR/MWh)', fontsize=12)
    ax3.set_xlabel('Date', fontsize=12)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'debm11_25_debq1_26_spread_regimes_comprehensive.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Comprehensive spread plot saved: {plot_path}")
    
    # Print summary statistics
    print(f"\\n📊 SPREAD STATISTICS SUMMARY:")
    print(f"=" * 40)
    print(f"Total period: {spread_data.index.min().strftime('%Y-%m-%d')} to {spread_data.index.max().strftime('%Y-%m-%d')}")
    print(f"Total trades: {len(spread_data)}")
    print(f"Price range: {spread_data['price'].min():.3f} to {spread_data['price'].max():.3f} EUR/MWh")
    print(f"Total price movement: {spread_data['price'].max() - spread_data['price'].min():.3f} EUR/MWh")
    print(f"\\nRegime progression:")
    for i, regime in enumerate(regimes):
        if i > 0:
            prev_level = regimes[i-1]['level']
            change = regime['level'] - prev_level
            print(f"  Regime {i+1}: {prev_level:.1f} → {regime['level']:.1f} ({change:+.1f}) EUR/MWh")
    
    total_change = regimes[-1]['level'] - regimes[0]['level']
    print(f"\\nTotal regime evolution: {regimes[0]['level']:.1f} → {regimes[-1]['level']:.1f} ({total_change:+.1f}) EUR/MWh")

def main():
    """Main function"""
    plot_spread_with_regimes()

if __name__ == "__main__":
    main()