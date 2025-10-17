#!/usr/bin/env python3
"""
Plot the reverted data to see the original behavior restored
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import sys
import os

# Load the reverted data
df = pd.read_parquet('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet')
df.index = pd.to_datetime(df.index)

# Separate by broker_id
df_datafetcher = df[df['broker_id'] == 1441.0]
df_spreadviewer = df[df['broker_id'] == 9999.0]

# Filter to actual price data (where price is not NaN)
trades_df = df_datafetcher[df_datafetcher['price'].notna()]
trades_sv = df_spreadviewer[df_spreadviewer['price'].notna()]

print(f"📊 REVERTED DATA VISUALIZATION")
print(f"=" * 35)
print(f"DataFetcher trades: {len(trades_df)}")
print(f"SpreadViewer trades: {len(trades_sv)}")

# Create the plot
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

# Plot 1: Price comparison
if len(trades_df) > 0:
    ax1.scatter(trades_df.index, trades_df['price'], alpha=0.7, s=20, 
                color='blue', label=f'DataFetcher (n={len(trades_df)})')

if len(trades_sv) > 0:
    ax1.scatter(trades_sv.index, trades_sv['price'], alpha=0.7, s=20, 
                color='red', label=f'SpreadViewer (n={len(trades_sv)})')

ax1.set_title('REVERTED Logic: DE/FR Q4 2025 Spread Prices\nJune 24 - July 1, 2025', fontsize=14, fontweight='bold')
ax1.set_ylabel('Price (€)', fontsize=12)
ax1.grid(True, alpha=0.3)
ax1.legend()
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

# Plot 2: Daily averages
daily_df = trades_df.groupby(trades_df.index.date)['price'].mean()
daily_sv = trades_sv.groupby(trades_sv.index.date)['price'].mean()

if len(daily_df) > 0:
    daily_dates_df = [pd.Timestamp(d) for d in daily_df.index]
    ax2.plot(daily_dates_df, daily_df.values, 'bo-', linewidth=2, markersize=8,
             label='DataFetcher Daily Avg', alpha=0.8)

if len(daily_sv) > 0:
    daily_dates_sv = [pd.Timestamp(d) for d in daily_sv.index]
    ax2.plot(daily_dates_sv, daily_sv.values, 'ro-', linewidth=2, markersize=8,
             label='SpreadViewer Daily Avg', alpha=0.8)

ax2.set_title('Daily Average Spread Prices', fontsize=12, fontweight='bold')
ax2.set_ylabel('Daily Average Price (€)', fontsize=12)
ax2.set_xlabel('Date', fontsize=12)
ax2.grid(True, alpha=0.3)
ax2.legend()
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
ax2.xaxis.set_major_locator(mdates.DayLocator())
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

# Add analysis text
analysis_text = f"""
REVERTED LOGIC ANALYSIS:
• DataFetcher: {len(trades_df)} trades
• SpreadViewer: {len(trades_sv)} trades
• Original €32 vs €20 difference: {'RESTORED' if len(trades_sv) == 0 else 'PARTIALLY RESTORED'}
• June 26 DataFetcher: {len(trades_df[trades_df.index.date == datetime(2025, 6, 26).date()])} trades
• June 26 SpreadViewer: {len(trades_sv[trades_sv.index.date == datetime(2025, 6, 26).date()])} trades
"""

plt.figtext(0.02, 0.02, analysis_text, fontsize=10, ha='left', va='bottom',
            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.8))

plt.tight_layout()
plt.subplots_adjust(bottom=0.2)

# Save the plot
output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/reverted_data_analysis.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight')
print(f"✅ Plot saved: {output_path}")

plt.show()