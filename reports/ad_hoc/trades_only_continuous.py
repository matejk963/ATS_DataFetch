#!/usr/bin/env python3
"""
Trades Only - Compressed Continuous Plot
=======================================
Shows only actual trades with no time gaps in the visualization.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import os

# Load the data
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"
df = pd.read_parquet(data_path)

print(f"📊 Loaded {len(df):,} total records")

# Filter for TRADES ONLY (where price column has actual values, not NaN)
trades_only = df[df['price'].notna()].copy()
print(f"📈 Trades found: {len(trades_only):,} records")

if len(trades_only) == 0:
    print("❌ No trades found in the data!")
    exit()

# Select a time period with good trade coverage
# Find the period with most trades
trades_only['date'] = trades_only.index.date
daily_counts = trades_only.groupby('date').size()
print(f"📊 Daily trade counts:")
for date, count in daily_counts.head(10).items():
    print(f"   {date}: {count:,} trades")

# Select a week with good trade activity
best_dates = daily_counts.nlargest(7).index
week_start = pd.Timestamp(best_dates.min())
week_end = pd.Timestamp(best_dates.max()) + timedelta(days=1)

print(f"📅 Selected period: {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")

# Filter trades for the selected period
period_trades = trades_only[(trades_only.index >= week_start) & (trades_only.index < week_end)].copy()
period_trades = period_trades.sort_index()

print(f"📈 Period trades: {len(period_trades):,} records")

if len(period_trades) == 0:
    print("❌ No trades in selected period!")
    exit()

# Analyze trade data
print(f"📊 Trade Analysis:")
print(f"   Price range: {period_trades['price'].min():.3f} - {period_trades['price'].max():.3f} EUR/MWh")
print(f"   Volume range: {period_trades['volume'].min():.1f} - {period_trades['volume'].max():.1f} MW")
if 'action' in period_trades.columns:
    buy_trades = (period_trades['action'] > 0).sum()
    sell_trades = (period_trades['action'] < 0).sum()
    print(f"   Buy trades: {buy_trades:,}, Sell trades: {sell_trades:,}")

# Create compressed x-axis: sequential integers for trades only
x_positions = np.arange(len(period_trades))

# Create the plot
plt.style.use('default')
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, 
                               gridspec_kw={'height_ratios': [2, 1], 'hspace': 0.1})

# Trade prices plot - color by buy/sell if available
if 'action' in period_trades.columns and period_trades['action'].notna().any():
    # Color trades by buy/sell action
    buy_mask = period_trades['action'] > 0
    sell_mask = period_trades['action'] < 0
    neutral_mask = period_trades['action'] == 0
    
    if buy_mask.any():
        ax1.scatter(x_positions[buy_mask], period_trades['price'].values[buy_mask], 
                   color='green', s=20, alpha=0.7, label=f'Buy Trades ({buy_mask.sum():,})')
    if sell_mask.any():
        ax1.scatter(x_positions[sell_mask], period_trades['price'].values[sell_mask], 
                   color='red', s=20, alpha=0.7, label=f'Sell Trades ({sell_mask.sum():,})')
    if neutral_mask.any():
        ax1.scatter(x_positions[neutral_mask], period_trades['price'].values[neutral_mask], 
                   color='blue', s=20, alpha=0.7, label=f'Neutral ({neutral_mask.sum():,})')
else:
    # Simple trade price line if no action data
    ax1.plot(x_positions, period_trades['price'].values, 
             color='blue', linewidth=1.5, marker='o', markersize=2, 
             alpha=0.8, label=f'Trade Prices ({len(period_trades):,})')

ax1.set_ylabel('Trade Price (EUR/MWh)', fontsize=12, fontweight='bold')
ax1.set_title(f'DEBQ4_25 vs FRBQ4_25 - TRADES ONLY (Continuous, No Gaps)\n'
              f'{week_start.strftime("%B %d")} - {week_end.strftime("%B %d, %Y")} | '
              f'{len(period_trades):,} Trades', 
              fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', fontsize=11)
ax1.grid(True, alpha=0.3)

# Trade statistics
price_min = period_trades['price'].min()
price_max = period_trades['price'].max()
price_mean = period_trades['price'].mean()
ax1.text(0.02, 0.98, f'Price: {price_min:.3f} - {price_max:.3f} EUR/MWh\nMean: {price_mean:.3f} EUR/MWh', 
         transform=ax1.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Volume plot
if 'volume' in period_trades.columns and period_trades['volume'].notna().any():
    ax2.plot(x_positions, period_trades['volume'].values, 
             color='orange', linewidth=1.5, alpha=0.8, label='Trade Volume')
    ax2.fill_between(x_positions, period_trades['volume'].values, 
                     color='orange', alpha=0.3)
    ax2.set_ylabel('Volume\n(MW)', fontsize=12, fontweight='bold')
    
    vol_mean = period_trades['volume'].mean()
    vol_total = period_trades['volume'].sum()
    ax2.text(0.02, 0.98, f'Avg Volume: {vol_mean:.1f} MW\nTotal: {vol_total:.1f} MW', 
             transform=ax2.transAxes, fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
else:
    # If no volume, show trade frequency
    ax2.plot(x_positions, np.ones(len(x_positions)), 
             color='purple', linewidth=1.5, alpha=0.8, label='Trade Events')
    ax2.fill_between(x_positions, np.ones(len(x_positions)), 
                     color='purple', alpha=0.3)
    ax2.set_ylabel('Trade\nEvents', fontsize=12, fontweight='bold')
    ax2.text(0.02, 0.98, f'Trade Frequency: {len(period_trades):,} events', 
             transform=ax2.transAxes, fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

ax2.set_xlabel('Trade Sequence (Time Gaps Removed)', fontsize=12, fontweight='bold')
ax2.legend(loc='upper left', fontsize=11)
ax2.grid(True, alpha=0.3)

# Custom x-axis labels showing actual times at key positions
n_labels = 8
if len(period_trades) >= n_labels:
    label_positions = np.linspace(0, len(period_trades)-1, n_labels, dtype=int)
    label_times = [period_trades.index[pos].strftime('%m/%d %H:%M') for pos in label_positions]
    ax2.set_xticks(label_positions)
    ax2.set_xticklabels(label_times, rotation=45, ha='right')

# Add day boundary markers
unique_days = sorted(set(period_trades.index.date))
if len(unique_days) > 1:
    for day in unique_days[1:]:
        day_start = pd.Timestamp(day)
        day_mask = period_trades.index.date == day
        if day_mask.any():
            first_idx_of_day = np.where(day_mask)[0][0]
            ax1.axvline(x=first_idx_of_day, color='gray', linestyle='--', alpha=0.5, linewidth=1)
            day_name = day_start.strftime('%a %m/%d')
            ax1.text(first_idx_of_day, ax1.get_ylim()[1], day_name, 
                    rotation=90, ha='right', va='top', fontsize=9, alpha=0.7)

# Adjust layout
plt.subplots_adjust(hspace=0.1)

# Create output directory
output_dir = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc"
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, "trades-only-continuous-no-gaps.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"📁 Plot saved: {output_path}")

# Summary
print(f"\n📊 Trades-Only Continuous Plot Summary:")
print(f"   Total trades plotted: {len(period_trades):,} (sequential, no time gaps)")
print(f"   Time span: {period_trades.index.min().strftime('%Y-%m-%d %H:%M')} to {period_trades.index.max().strftime('%Y-%m-%d %H:%M')}")
print(f"   Price range: {price_min:.3f} - {price_max:.3f} EUR/MWh ({price_max-price_min:.3f} spread)")
print(f"   Average trade price: {price_mean:.3f} EUR/MWh")
if 'volume' in period_trades.columns:
    print(f"   Total volume: {period_trades['volume'].sum():.1f} MW")
print(f"   Pure trades visualization - all gaps removed")

# Close plot
plt.ioff()
plt.close()

print("✅ Trades-only plot completed")