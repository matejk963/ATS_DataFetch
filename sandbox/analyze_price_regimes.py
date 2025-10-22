#!/usr/bin/env python3
"""
Analyze Price Regime Changes in Spread Data
==========================================

Detect structural breaks where price shifts dramatically and stays at new level.
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

def analyze_price_regimes():
    """Analyze price regime changes and structural breaks"""
    
    print("🔍 ANALYZING PRICE REGIME CHANGES: DEBM11_25 vs DEBQ1_26")
    print("=" * 65)
    
    # Load the original spread data (with outliers)
    spread_data = pd.read_parquet('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test/debm11_25_debq1_26_tr_ba_dataanalysis_real.parquet')
    spread_data.index = pd.to_datetime(spread_data.index)
    spread_data = spread_data.sort_index()
    
    print(f"📊 Data Summary:")
    print(f"   Records: {len(spread_data)}")
    print(f"   Date range: {spread_data.index.min()} to {spread_data.index.max()}")
    print(f"   Price range: {spread_data['price'].min():.3f} to {spread_data['price'].max():.3f}")
    
    # Analyze price levels over time
    print(f"\\n📈 TEMPORAL PRICE ANALYSIS:")
    
    # Sort by time and look for level shifts
    prices = spread_data['price'].values
    times = spread_data.index
    
    # Calculate rolling statistics to detect regime changes
    window = min(10, len(spread_data) // 4)
    if window > 1:
        rolling_mean = spread_data['price'].rolling(window=window, center=True).mean()
        rolling_std = spread_data['price'].rolling(window=window, center=True).std()
        
        print(f"   Rolling window: {window} trades")
        print(f"   Rolling mean range: {rolling_mean.min():.3f} to {rolling_mean.max():.3f}")
        print(f"   Rolling std range: {rolling_std.min():.3f} to {rolling_std.max():.3f}")
    
    # Detect large price jumps between consecutive trades
    price_changes = spread_data['price'].diff().fillna(0)
    large_jumps = price_changes[abs(price_changes) > 1.0]  # Jumps > 1 EUR/MWh
    
    print(f"\\n🚨 LARGE PRICE JUMPS (>1.0 EUR/MWh):")
    print(f"   Number of large jumps: {len(large_jumps)}")
    
    if len(large_jumps) > 0:
        print(f"   Jump sizes: {large_jumps.min():.3f} to {large_jumps.max():.3f}")
        print(f"\\n   Top 5 largest jumps:")
        top_jumps = large_jumps.abs().nlargest(5)
        for timestamp, jump in top_jumps.items():
            direction = "↑" if large_jumps[timestamp] > 0 else "↓"
            before_idx = spread_data.index.get_loc(timestamp) - 1
            if before_idx >= 0:
                before_price = spread_data.iloc[before_idx]['price']
                after_price = spread_data.loc[timestamp, 'price']
                print(f"      {timestamp}: {direction} {abs(large_jumps[timestamp]):.3f} ({before_price:.3f} → {after_price:.3f})")
    
    # Analyze price stability after jumps
    print(f"\\n🔄 PRICE REGIME ANALYSIS:")
    
    # Group consecutive similar prices to identify regimes
    price_tolerance = 0.5  # EUR/MWh tolerance for "same level"
    regimes = []
    current_regime_start = times[0]
    current_regime_price = prices[0]
    current_regime_count = 1
    
    for i in range(1, len(prices)):
        if abs(prices[i] - current_regime_price) <= price_tolerance:
            # Still in same regime
            current_regime_count += 1
        else:
            # Regime change detected
            regimes.append({
                'start': current_regime_start,
                'end': times[i-1],
                'price_level': current_regime_price,
                'count': current_regime_count,
                'duration_hours': (times[i-1] - current_regime_start).total_seconds() / 3600
            })
            
            # Start new regime
            current_regime_start = times[i]
            current_regime_price = prices[i]
            current_regime_count = 1
    
    # Add final regime
    regimes.append({
        'start': current_regime_start,
        'end': times[-1],
        'price_level': current_regime_price,
        'count': current_regime_count,
        'duration_hours': (times[-1] - current_regime_start).total_seconds() / 3600
    })
    
    print(f"   Detected {len(regimes)} price regimes (±{price_tolerance} EUR/MWh tolerance)")
    print(f"\\n   Price Regimes:")
    for i, regime in enumerate(regimes):
        print(f"      Regime {i+1}: {regime['price_level']:.3f} EUR/MWh")
        print(f"         Duration: {regime['duration_hours']:.1f} hours ({regime['count']} trades)")
        print(f"         Period: {regime['start'].strftime('%m-%d %H:%M')} to {regime['end'].strftime('%m-%d %H:%M')}")
    
    # Calculate regime transition analysis
    if len(regimes) > 1:
        print(f"\\n📊 REGIME TRANSITIONS:")
        for i in range(1, len(regimes)):
            prev_level = regimes[i-1]['price_level']
            curr_level = regimes[i]['price_level']
            transition = curr_level - prev_level
            print(f"      {regimes[i]['start'].strftime('%m-%d %H:%M')}: {prev_level:.3f} → {curr_level:.3f} ({transition:+.3f})")
    
    # Check for persistent level shifts vs temporary spikes
    print(f"\\n🎯 STRUCTURAL BREAK ANALYSIS:")
    long_regimes = [r for r in regimes if r['count'] >= 5]  # Regimes with at least 5 trades
    
    if len(long_regimes) > 1:
        print(f"   Found {len(long_regimes)} persistent price regimes:")
        for i, regime in enumerate(long_regimes):
            print(f"      Level {i+1}: {regime['price_level']:.3f} EUR/MWh ({regime['count']} trades, {regime['duration_hours']:.1f}h)")
        
        level_changes = []
        for i in range(1, len(long_regimes)):
            change = long_regimes[i]['price_level'] - long_regimes[i-1]['price_level']
            level_changes.append(change)
        
        if level_changes:
            print(f"\\n   Persistent level shifts: {[f'{c:+.3f}' for c in level_changes]}")
            print(f"   Average level shift: {np.mean([abs(c) for c in level_changes]):.3f} EUR/MWh")
    else:
        print(f"   No clear persistent regimes found")
    
    # Create visualization
    create_regime_plot(spread_data, regimes, large_jumps)
    
    return regimes, large_jumps

def create_regime_plot(spread_data, regimes, large_jumps):
    """Create visualization of price regimes and structural breaks"""
    
    fig, axes = plt.subplots(2, 1, figsize=(16, 10))
    fig.suptitle('Price Regime Analysis: DEBM11_25 vs DEBQ1_26 Spread', fontsize=14, fontweight='bold')
    
    # Plot 1: Price timeline with regime boundaries
    ax1 = axes[0]
    ax1.scatter(spread_data.index, spread_data['price'], alpha=0.7, s=20, c='blue', label='Spread Prices')
    
    # Add regime boundaries
    colors = ['red', 'green', 'orange', 'purple', 'brown']
    for i, regime in enumerate(regimes):
        color = colors[i % len(colors)]
        ax1.axhline(y=regime['price_level'], 
                   xmin=(regime['start'] - spread_data.index.min()).total_seconds() / (spread_data.index.max() - spread_data.index.min()).total_seconds(),
                   xmax=(regime['end'] - spread_data.index.min()).total_seconds() / (spread_data.index.max() - spread_data.index.min()).total_seconds(),
                   color=color, alpha=0.8, linewidth=3, 
                   label=f'Regime {i+1}: {regime["price_level"]:.3f}')
    
    ax1.set_title('Price Timeline with Regime Identification')
    ax1.set_ylabel('Spread Price (EUR/MWh)')
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Price changes and jumps
    ax2 = axes[1]
    price_changes = spread_data['price'].diff().fillna(0)
    ax2.bar(spread_data.index, price_changes, alpha=0.7, color='gray', width=pd.Timedelta(hours=1))
    
    # Highlight large jumps
    if len(large_jumps) > 0:
        ax2.bar(large_jumps.index, large_jumps.values, alpha=0.9, color='red', 
               width=pd.Timedelta(hours=1), label=f'Large Jumps (>1.0)')
    
    ax2.set_title('Price Changes Between Consecutive Trades')
    ax2.set_ylabel('Price Change (EUR/MWh)')
    ax2.set_xlabel('DateTime')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'debm11_25_debq1_26_regime_analysis.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"\\n✅ Regime analysis plot saved: {plot_path}")

def main():
    """Main function"""
    regimes, large_jumps = analyze_price_regimes()
    
    print(f"\\n🎯 CONCLUSION:")
    print(f"If price shifts dramatically and stays at new levels, this indicates:")
    print(f"• Structural breaks in the spread relationship")
    print(f"• Market regime changes (supply/demand fundamentals)")
    print(f"• Contract specification changes")
    print(f"• Liquidity provision changes")
    print(f"• NOT random outliers - these are persistent level shifts")

if __name__ == "__main__":
    main()