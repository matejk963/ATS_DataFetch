#!/usr/bin/env python3
"""
Comprehensive analysis of DEBQ4_25 vs FRBQ4_25 spread data
Generated on 2025-08-24 - Headless version
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import seaborn as sns

# Set style
plt.style.use('default')
sns.set_palette("husl")

# Load data
print("Loading DEBQ4_25 vs FRBQ4_25 spread data...")
df = pd.read_parquet('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet')

# Calculate derived columns
df['spread'] = df['a_price'] - df['b_price']
df['mid_price'] = (df['b_price'] + df['a_price']) / 2
df['spread_bps'] = (df['spread'] / df['mid_price']) * 10000  # spread in basis points

# Filter for quote data (non-NaN bid/ask)
quotes = df[(df['b_price'].notna()) & (df['a_price'].notna())].copy()
trades = df[df['price'].notna()].copy()

print(f"Data loaded: {len(df)} total records")
print(f"Quote records: {len(quotes)}")
print(f"Trade records: {len(trades)}")

# Create figure with multiple subplots
fig = plt.figure(figsize=(20, 24))

# 1. Bid-Ask Spread Over Time
ax1 = plt.subplot(5, 2, 1)
quotes_sample = quotes[::50]  # Sample for performance
ax1.plot(quotes_sample.index, quotes_sample['spread'], alpha=0.7, linewidth=0.5, color='red')
ax1.set_title('Bid-Ask Spread Over Time (DEBQ4_25 vs FRBQ4_25)', fontsize=14, fontweight='bold')
ax1.set_ylabel('Spread (points)')
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

# 2. Price Evolution (Bid, Ask, Mid)
ax2 = plt.subplot(5, 2, 2)
ax2.plot(quotes_sample.index, quotes_sample['b_price'], alpha=0.8, linewidth=1, label='Bid', color='blue')
ax2.plot(quotes_sample.index, quotes_sample['a_price'], alpha=0.8, linewidth=1, label='Ask', color='red')
ax2.plot(quotes_sample.index, quotes_sample['mid_price'], alpha=0.8, linewidth=1, label='Mid', color='green')
if len(trades) > 0:
    ax2.scatter(trades.index, trades['price'], alpha=0.6, s=10, color='black', label='Trades', zorder=5)
ax2.set_title('Price Evolution', fontsize=14, fontweight='bold')
ax2.set_ylabel('Price')
ax2.legend()
ax2.grid(True, alpha=0.3)
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

# 3. Spread Distribution
ax3 = plt.subplot(5, 2, 3)
ax3.hist(quotes['spread'], bins=50, alpha=0.7, color='purple', edgecolor='black')
ax3.set_title('Spread Distribution', fontsize=14, fontweight='bold')
ax3.set_xlabel('Spread (points)')
ax3.set_ylabel('Frequency')
ax3.grid(True, alpha=0.3)
ax3.axvline(quotes['spread'].median(), color='red', linestyle='--', label=f'Median: {quotes["spread"].median():.3f}')
ax3.axvline(quotes['spread'].mean(), color='orange', linestyle='--', label=f'Mean: {quotes["spread"].mean():.3f}')
ax3.legend()

# 4. Spread in Basis Points Distribution
ax4 = plt.subplot(5, 2, 4)
spread_bps_clean = quotes['spread_bps'][quotes['spread_bps'] < 1000]  # Remove outliers
ax4.hist(spread_bps_clean, bins=50, alpha=0.7, color='orange', edgecolor='black')
ax4.set_title('Spread Distribution (Basis Points)', fontsize=14, fontweight='bold')
ax4.set_xlabel('Spread (basis points)')
ax4.set_ylabel('Frequency')
ax4.grid(True, alpha=0.3)
ax4.axvline(spread_bps_clean.median(), color='red', linestyle='--', label=f'Median: {spread_bps_clean.median():.1f} bps')
ax4.axvline(spread_bps_clean.mean(), color='orange', linestyle='--', label=f'Mean: {spread_bps_clean.mean():.1f} bps')
ax4.legend()

# 5. Intraday Spread Pattern
ax5 = plt.subplot(5, 2, 5)
quotes['hour'] = quotes.index.hour
quotes['minute'] = quotes.index.minute

# Create hourly aggregations
hourly_spread = quotes.groupby('hour')['spread'].agg(['mean', 'std', 'median']).reset_index()
ax5.errorbar(hourly_spread['hour'], hourly_spread['mean'], yerr=hourly_spread['std'], 
            capsize=5, marker='o', alpha=0.8, color='green')
ax5.set_title('Intraday Spread Pattern (Hourly Average)', fontsize=14, fontweight='bold')
ax5.set_xlabel('Hour of Day')
ax5.set_ylabel('Average Spread (points)')
ax5.grid(True, alpha=0.3)
ax5.set_xticks(range(9, 18))

# 6. Trading Volume Analysis
ax6 = plt.subplot(5, 2, 6)
if len(trades) > 0:
    volume_by_hour = trades.groupby(trades.index.hour)['volume'].sum()
    ax6.bar(volume_by_hour.index, volume_by_hour.values, alpha=0.7, color='brown')
    ax6.set_title('Trading Volume by Hour', fontsize=14, fontweight='bold')
    ax6.set_xlabel('Hour of Day')
    ax6.set_ylabel('Total Volume')
    ax6.grid(True, alpha=0.3)
else:
    ax6.text(0.5, 0.5, 'No trade data available', ha='center', va='center', transform=ax6.transAxes)
    ax6.set_title('Trading Volume by Hour (No Data)', fontsize=14, fontweight='bold')

# 7. Mid Price vs Spread Correlation
ax7 = plt.subplot(5, 2, 7)
sample_quotes = quotes[::100]  # Sample for performance
ax7.scatter(sample_quotes['mid_price'], sample_quotes['spread'], alpha=0.5, s=1)
ax7.set_title('Mid Price vs Spread Correlation', fontsize=14, fontweight='bold')
ax7.set_xlabel('Mid Price')
ax7.set_ylabel('Spread (points)')
ax7.grid(True, alpha=0.3)

# Add correlation coefficient
correlation = quotes['mid_price'].corr(quotes['spread'])
ax7.text(0.05, 0.95, f'Correlation: {correlation:.3f}', transform=ax7.transAxes, 
         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))

# 8. Time Series of Daily Statistics
ax8 = plt.subplot(5, 2, 8)
# Resample to daily data
daily_stats = quotes.resample('D').agg({
    'spread': ['mean', 'std', 'min', 'max'],
    'mid_price': 'mean'
}).round(4)

daily_stats.columns = ['spread_mean', 'spread_std', 'spread_min', 'spread_max', 'mid_price_mean']

ax8.plot(daily_stats.index, daily_stats['spread_mean'], marker='o', alpha=0.8, label='Mean Spread', color='red')
ax8.fill_between(daily_stats.index, 
                daily_stats['spread_mean'] - daily_stats['spread_std'],
                daily_stats['spread_mean'] + daily_stats['spread_std'], 
                alpha=0.3, color='red')
ax8.set_title('Daily Spread Statistics', fontsize=14, fontweight='bold')
ax8.set_ylabel('Spread (points)')
ax8.legend()
ax8.grid(True, alpha=0.3)
ax8.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax8.xaxis.set_major_locator(mdates.WeekdayLocator(interval=7))
plt.setp(ax8.xaxis.get_majorticklabels(), rotation=45)

# 9. Price Movement Analysis
ax9 = plt.subplot(5, 2, 9)
quotes['mid_price_change'] = quotes['mid_price'].diff()
quotes['spread_change'] = quotes['spread'].diff()

# Sample for performance
sample_changes = quotes[['mid_price_change', 'spread_change']].dropna()[::100]
ax9.scatter(sample_changes['mid_price_change'], sample_changes['spread_change'], alpha=0.5, s=1)
ax9.set_title('Price Movement vs Spread Change', fontsize=14, fontweight='bold')
ax9.set_xlabel('Mid Price Change')
ax9.set_ylabel('Spread Change')
ax9.grid(True, alpha=0.3)
ax9.axhline(y=0, color='black', linestyle='-', alpha=0.3)
ax9.axvline(x=0, color='black', linestyle='-', alpha=0.3)

# 10. Summary Statistics Table
ax10 = plt.subplot(5, 2, 10)
ax10.axis('off')

# Create summary statistics
summary_stats = {
    'Metric': [
        'Total Records',
        'Quote Records', 
        'Trade Records',
        'Date Range',
        'Avg Spread (points)',
        'Avg Spread (bps)',
        'Median Spread (points)',
        'Spread Std Dev',
        'Min Spread',
        'Max Spread',
        'Avg Mid Price',
        'Price Range'
    ],
    'Value': [
        f'{len(df):,}',
        f'{len(quotes):,}',
        f'{len(trades):,}',
        f'{df.index.min().strftime("%Y-%m-%d")} to {df.index.max().strftime("%Y-%m-%d")}',
        f'{quotes["spread"].mean():.4f}',
        f'{quotes["spread_bps"].mean():.1f}',
        f'{quotes["spread"].median():.4f}',
        f'{quotes["spread"].std():.4f}',
        f'{quotes["spread"].min():.4f}',
        f'{quotes["spread"].max():.4f}',
        f'{quotes["mid_price"].mean():.4f}',
        f'{quotes["mid_price"].min():.4f} - {quotes["mid_price"].max():.4f}'
    ]
}

table_data = list(zip(summary_stats['Metric'], summary_stats['Value']))
table = ax10.table(cellText=table_data, colLabels=['Metric', 'Value'], 
                  cellLoc='left', loc='center', colWidths=[0.6, 0.4])
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 2)
ax10.set_title('Summary Statistics', fontsize=14, fontweight='bold', y=0.95)

plt.tight_layout()
plt.savefig('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-comprehensive-analysis.png', 
            dpi=300, bbox_inches='tight')
plt.close()

print("\n=== ANALYSIS COMPLETE ===")
print(f"Comprehensive analysis saved to: /mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-comprehensive-analysis.png")

# Also create individual focused plots
print("\nCreating additional focused plots...")

# High-resolution spread time series
fig, ax = plt.subplots(figsize=(16, 8))
quotes_sample = quotes[::20]  # Less aggressive sampling for detail
ax.plot(quotes_sample.index, quotes_sample['spread'], alpha=0.8, linewidth=0.8, color='red')
ax.set_title('DEBQ4_25 vs FRBQ4_25: Bid-Ask Spread Evolution', fontsize=16, fontweight='bold')
ax.set_ylabel('Spread (points)', fontsize=12)
ax.set_xlabel('Date', fontsize=12)
ax.grid(True, alpha=0.3)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

# Add statistical annotations
ax.axhline(y=quotes['spread'].mean(), color='orange', linestyle='--', alpha=0.8, 
          label=f'Mean: {quotes["spread"].mean():.3f}')
ax.axhline(y=quotes['spread'].median(), color='blue', linestyle='--', alpha=0.8, 
          label=f'Median: {quotes["spread"].median():.3f}')
ax.legend()

plt.tight_layout()
plt.savefig('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-spread-timeseries.png', 
            dpi=300, bbox_inches='tight')
plt.close()

# Price level analysis
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))

# Price levels over time
quotes_sample = quotes[::30]  # Sample for performance
ax1.plot(quotes_sample.index, quotes_sample['b_price'], alpha=0.8, linewidth=1, label='Bid', color='blue')
ax1.plot(quotes_sample.index, quotes_sample['a_price'], alpha=0.8, linewidth=1, label='Ask', color='red')
ax1.plot(quotes_sample.index, quotes_sample['mid_price'], alpha=0.8, linewidth=1, label='Mid', color='green')

if len(trades) > 0:
    trades_sample = trades[::5] if len(trades) > 100 else trades
    ax1.scatter(trades_sample.index, trades_sample['price'], alpha=0.7, s=15, color='black', label='Trades', zorder=5)

ax1.set_title('DEBQ4_25 vs FRBQ4_25: Price Levels Evolution', fontsize=16, fontweight='bold')
ax1.set_ylabel('Price', fontsize=12)
ax1.legend()
ax1.grid(True, alpha=0.3)
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

# Volume analysis (if trades available)
if len(trades) > 0:
    # Daily volume
    daily_volume = trades.resample('D')['volume'].sum()
    ax2.bar(daily_volume.index, daily_volume.values, alpha=0.7, color='brown', width=0.8)
    ax2.set_title('Daily Trading Volume', fontsize=14, fontweight='bold')
    ax2.set_ylabel('Volume', fontsize=12)
    ax2.set_xlabel('Date', fontsize=12)
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
else:
    ax2.text(0.5, 0.5, 'No trade volume data available', ha='center', va='center', 
             transform=ax2.transAxes, fontsize=14)
    ax2.set_title('Trading Volume Analysis (No Data)', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.savefig('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-price-volume.png', 
            dpi=300, bbox_inches='tight')
plt.close()

print(f"Additional plots saved:")
print(f"- Spread time series: /mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-spread-timeseries.png")
print(f"- Price & volume: /mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-price-volume.png")