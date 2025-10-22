#!/usr/bin/env python3
"""
Investigate Sign Flips in Spread Data
====================================

Analyze the rapid sign flipping pattern in debm11_25 vs debq1_26 spread
to determine if this is a coefficient sign error or data source mixing.
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('default')
plt.rcParams['figure.figsize'] = [16, 12]

def investigate_sign_flips():
    """Investigate the sign flipping pattern in spread data"""
    
    print("🔍 Investigating sign flips in spread data...")
    
    # Load the complete dataset
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_test_merged.parquet'
    spread_data = pd.read_parquet(file_path)
    spread_data.index = pd.to_datetime(spread_data.index)
    spread_data = spread_data.sort_index()
    
    # Focus on trades only
    trades = spread_data[spread_data['action'].isin([1.0, -1.0])].copy()
    
    print(f"Loaded {len(trades):,} trade records")
    print(f"Price range: {trades['price'].min():.3f} to {trades['price'].max():.3f} EUR/MWh")
    
    # 1. Analyze sign flipping pattern
    analyze_sign_flips(trades)
    
    # 2. Look for data source mixing patterns
    analyze_data_sources(trades)
    
    # 3. Check for coefficient sign errors
    analyze_coefficient_signs(trades)
    
    # 4. Create diagnostic plots
    create_diagnostic_plots(trades)

def analyze_sign_flips(trades):
    """Analyze the pattern of sign flips"""
    
    print("\n📊 SIGN FLIP ANALYSIS:")
    print("=" * 50)
    
    # Calculate consecutive price differences
    trades['price_diff'] = trades['price'].diff()
    trades['price_abs_diff'] = trades['price_diff'].abs()
    
    # Identify large jumps (>5 EUR/MWh)
    large_jumps = trades[trades['price_abs_diff'] > 5.0].copy()
    
    print(f"Large price jumps (>5 EUR/MWh): {len(large_jumps)}")
    
    if len(large_jumps) > 0:
        print(f"Jump sizes: {large_jumps['price_abs_diff'].min():.1f} to {large_jumps['price_abs_diff'].max():.1f} EUR/MWh")
        
        # Analyze jump directions
        positive_jumps = large_jumps[large_jumps['price_diff'] > 0]
        negative_jumps = large_jumps[large_jumps['price_diff'] < 0]
        
        print(f"Positive jumps: {len(positive_jumps)} (avg: {positive_jumps['price_diff'].mean():.1f} EUR/MWh)")
        print(f"Negative jumps: {len(negative_jumps)} (avg: {negative_jumps['price_diff'].mean():.1f} EUR/MWh)")
        
        # Look for flip patterns (positive -> negative or vice versa)
        flips = []
        for i in range(len(large_jumps) - 1):
            current_jump = large_jumps.iloc[i]['price_diff']
            next_jump = large_jumps.iloc[i + 1]['price_diff']
            
            if (current_jump > 0 and next_jump < 0) or (current_jump < 0 and next_jump > 0):
                time_diff = large_jumps.iloc[i + 1].name - large_jumps.iloc[i].name
                flips.append({
                    'time1': large_jumps.iloc[i].name,
                    'time2': large_jumps.iloc[i + 1].name,
                    'jump1': current_jump,
                    'jump2': next_jump,
                    'time_between': time_diff
                })
        
        print(f"\nSign flip sequences: {len(flips)}")
        if len(flips) > 0:
            avg_time_between = pd.Series([f['time_between'] for f in flips]).mean()
            print(f"Average time between flips: {avg_time_between}")
            
            # Show first few flips
            print("\nFirst 5 sign flip sequences:")
            for i, flip in enumerate(flips[:5]):
                print(f"  {i+1}. {flip['time1']} → {flip['time2']} ({flip['time_between']})")
                print(f"     Jump: {flip['jump1']:+.1f} → {flip['jump2']:+.1f} EUR/MWh")

def analyze_data_sources(trades):
    """Check for patterns suggesting data source mixing"""
    
    print("\n🔄 DATA SOURCE MIXING ANALYSIS:")
    print("=" * 50)
    
    # Check if there are distinct price clusters
    price_clusters = []
    unique_prices = trades['price'].round(1).value_counts()
    
    print(f"Most common price levels:")
    for price, count in unique_prices.head(10).items():
        print(f"  {price:6.1f} EUR/MWh: {count:3d} trades")
    
    # Look for bimodal distribution
    positive_prices = trades[trades['price'] > 0]['price']
    negative_prices = trades[trades['price'] < 0]['price']
    
    print(f"\nPrice distribution:")
    print(f"  Positive prices: {len(positive_prices):,} trades (mean: {positive_prices.mean():.2f})")
    print(f"  Negative prices: {len(negative_prices):,} trades (mean: {negative_prices.mean():.2f})")
    
    # Check temporal clustering
    trades['hour'] = trades.index.hour
    trades['day'] = trades.index.date
    
    hourly_stats = trades.groupby('hour')['price'].agg(['mean', 'std', 'count'])
    print(f"\nHourly price patterns:")
    for hour, stats in hourly_stats.iterrows():
        if stats['count'] > 5:  # Only show hours with significant trading
            print(f"  Hour {hour:2d}: {stats['mean']:6.2f} ± {stats['std']:5.2f} EUR/MWh ({stats['count']} trades)")

def analyze_coefficient_signs(trades):
    """Check for coefficient sign error patterns"""
    
    print("\n⚖️  COEFFICIENT SIGN ERROR ANALYSIS:")
    print("=" * 50)
    
    # Check if flipping sign would make more sense
    trades_flipped = trades.copy()
    trades_flipped['price_flipped'] = -trades_flipped['price']
    
    # Compare volatility
    original_vol = trades['price'].std()
    flipped_vol = trades_flipped['price_flipped'].std()
    
    print(f"Original spread volatility: {original_vol:.3f} EUR/MWh")
    print(f"Sign-flipped volatility:   {flipped_vol:.3f} EUR/MWh")
    
    # Compare trend consistency
    original_trend = trades['price'].diff().abs().mean()
    flipped_trend = trades_flipped['price_flipped'].diff().abs().mean()
    
    print(f"Original avg price change: {original_trend:.3f} EUR/MWh")
    print(f"Flipped avg price change:  {flipped_trend:.3f} EUR/MWh")
    
    # Check for physical reasonableness
    # Energy spreads typically show seasonal patterns, not random sign flips
    monthly_original = trades.groupby(trades.index.to_period('M'))['price'].mean()
    monthly_flipped = trades_flipped.groupby(trades_flipped.index.to_period('M'))['price_flipped'].mean()
    
    print(f"\nMonthly averages:")
    print(f"Original  | Flipped")
    print(f"----------|--------")
    for month in monthly_original.index:
        orig = monthly_original[month]
        flip = monthly_flipped[month]
        print(f"{orig:8.2f}  | {flip:7.2f}")

def create_diagnostic_plots(trades):
    """Create diagnostic plots to visualize the issues"""
    
    print("\n🎨 Creating diagnostic plots...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Spread Data Diagnostic Analysis: Sign Flip Investigation', fontsize=16, fontweight='bold')
    
    # Plot 1: Price time series with highlighted jumps
    ax1 = axes[0, 0]
    
    # Plot all trades
    ax1.scatter(trades.index, trades['price'], alpha=0.6, s=20, c='blue', label='All Trades')
    
    # Highlight large jumps
    large_jumps = trades[trades['price'].diff().abs() > 5.0]
    if len(large_jumps) > 0:
        ax1.scatter(large_jumps.index, large_jumps['price'], s=100, c='red', 
                   marker='x', linewidth=3, label=f'Large Jumps (n={len(large_jumps)})')
    
    ax1.set_title('Price Time Series with Large Jumps Highlighted')
    ax1.set_ylabel('Price (EUR/MWh)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Price distribution histogram
    ax2 = axes[0, 1]
    
    ax2.hist(trades['price'], bins=50, alpha=0.7, color='blue', edgecolor='black')
    ax2.axvline(trades['price'].mean(), color='red', linestyle='--', linewidth=2, 
               label=f'Mean: {trades["price"].mean():.2f}')
    ax2.axvline(0, color='black', linestyle='-', alpha=0.5, label='Zero Line')
    
    ax2.set_title('Price Distribution')
    ax2.set_xlabel('Price (EUR/MWh)')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Price differences (jump sizes)
    ax3 = axes[1, 0]
    
    price_diffs = trades['price'].diff().dropna()
    ax3.scatter(range(len(price_diffs)), price_diffs, alpha=0.6, s=10, c='green')
    ax3.axhline(0, color='black', linestyle='-', alpha=0.5)
    ax3.axhline(5, color='red', linestyle='--', alpha=0.7, label='±5 EUR/MWh threshold')
    ax3.axhline(-5, color='red', linestyle='--', alpha=0.7)
    
    ax3.set_title('Consecutive Price Differences')
    ax3.set_xlabel('Trade Sequence')
    ax3.set_ylabel('Price Change (EUR/MWh)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Original vs Sign-flipped comparison
    ax4 = axes[1, 1]
    
    # Sample data for visibility
    sample_size = min(1000, len(trades))
    sample_indices = np.linspace(0, len(trades)-1, sample_size, dtype=int)
    sample_trades = trades.iloc[sample_indices]
    
    ax4.plot(sample_trades.index, sample_trades['price'], 'b-', alpha=0.7, 
            linewidth=1, label='Original Spread')
    ax4.plot(sample_trades.index, -sample_trades['price'], 'r--', alpha=0.7, 
            linewidth=1, label='Sign-flipped Spread')
    ax4.axhline(0, color='black', linestyle='-', alpha=0.5)
    
    ax4.set_title('Original vs Sign-flipped Comparison')
    ax4.set_xlabel('Date')
    ax4.set_ylabel('Price (EUR/MWh)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # Format x-axes
    for ax in [ax1, ax4]:
        ax.tick_params(axis='x', rotation=45)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'spread_sign_flip_investigation.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Diagnostic plot saved: {plot_path}")

def main():
    """Main function"""
    investigate_sign_flips()

if __name__ == "__main__":
    main()