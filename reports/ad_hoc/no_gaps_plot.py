#!/usr/bin/env python3
"""
Continuous Plot with No Gaps
============================
Creates a continuous plot with no x-axis gaps where there are no y values.
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

print(f"📊 Loaded {len(df):,} records from {df.index.min()} to {df.index.max()}")

# Select a representative week from the middle of the dataset
middle_date = df.index.min() + (df.index.max() - df.index.min()) / 2
week_start = middle_date - timedelta(days=middle_date.weekday())  # Monday
week_end = week_start + timedelta(days=6)  # Sunday

print(f"📅 Selected week: {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")

# Filter data for the selected week
week_data = df[(df.index >= week_start) & (df.index <= week_end)].copy()
print(f"📈 Week data: {len(week_data):,} records")

# Remove rows with NaN values in key columns and sort by time
week_data = week_data.dropna(subset=['b_price', 'a_price']).sort_index()

# Calculate mid price and spread
week_data['mid_price'] = (week_data['b_price'] + week_data['a_price']) / 2
week_data['spread'] = week_data['a_price'] - week_data['b_price']

print(f"📊 Clean data: {len(week_data):,} records without gaps")

# Create the plot with NO GAPS - only plot actual data points connected by lines
plt.style.use('default')
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, 
                               gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.1})

# Main price plot - CONTINUOUS LINES with no gaps
ax1.plot(week_data.index, week_data['b_price'], 
         color='#1f77b4', linewidth=1.5, label='Bid Price', alpha=0.9)
ax1.plot(week_data.index, week_data['a_price'], 
         color='#d62728', linewidth=1.5, label='Ask Price', alpha=0.9)
ax1.plot(week_data.index, week_data['mid_price'], 
         color='#2ca02c', linewidth=2, label='Mid Price', alpha=0.9)

ax1.set_ylabel('Price (EUR/MWh)', fontsize=12, fontweight='bold')
ax1.set_title(f'DEBQ4_25 vs FRBQ4_25 Spread - Continuous (No Gaps)\n'
              f'{week_start.strftime("%B %d")} - {week_end.strftime("%B %d, %Y")}', 
              fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', fontsize=11)
ax1.grid(True, alpha=0.3)

# Add price range info
price_min = week_data[['b_price', 'a_price']].min().min()
price_max = week_data[['b_price', 'a_price']].max().max()
ax1.text(0.02, 0.98, f'Price Range: {price_min:.2f} - {price_max:.2f} EUR/MWh\nData Points: {len(week_data):,}', 
         transform=ax1.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Spread plot - CONTINUOUS LINE with no gaps
ax2.plot(week_data.index, week_data['spread'], 
         color='#ff7f0e', linewidth=1.5, label='Bid-Ask Spread', alpha=0.9)
ax2.fill_between(week_data.index, week_data['spread'], 
                 color='#ff7f0e', alpha=0.3)

ax2.set_ylabel('Spread\n(EUR/MWh)', fontsize=12, fontweight='bold')
ax2.set_xlabel('Time (Continuous - No Gaps)', fontsize=12, fontweight='bold')
ax2.legend(loc='upper left', fontsize=11)
ax2.grid(True, alpha=0.3)

# Add spread statistics
spread_mean = week_data['spread'].mean()
spread_std = week_data['spread'].std()
spread_median = week_data['spread'].median()
ax2.text(0.02, 0.98, f'Spread - Mean: {spread_mean:.3f}, Median: {spread_median:.3f}, Std: {spread_std:.3f}', 
         transform=ax2.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Format x-axis to show time progression without gaps
# Use adaptive formatting based on data density
total_hours = (week_data.index.max() - week_data.index.min()).total_seconds() / 3600
if total_hours > 48:  # More than 2 days
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=12))
    ax2.xaxis.set_minor_locator(mdates.HourLocator(interval=4))
else:  # Less than 2 days
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    ax2.xaxis.set_minor_locator(mdates.HourLocator(interval=2))

# Rotate x-axis labels
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Add day markers if we have multiple days
unique_days = week_data.index.date
unique_days = sorted(set(unique_days))

if len(unique_days) > 1:
    for day in unique_days[1:]:  # Skip first day
        day_start = pd.Timestamp(day)
        if week_data.index.min() <= day_start <= week_data.index.max():
            ax1.axvline(x=day_start, color='gray', linestyle='--', alpha=0.5, linewidth=1)
            # Add day label
            day_name = day_start.strftime('%A %m/%d')
            ax1.text(day_start, ax1.get_ylim()[1], day_name, 
                    rotation=90, ha='right', va='top', fontsize=9, alpha=0.7,
                    bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

# Adjust layout
plt.subplots_adjust(hspace=0.1)

# Create output directory
output_dir = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc"
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, "no-gaps-continuous-plot.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"📁 Plot saved: {output_path}")

# Display summary statistics
print(f"\n📊 Continuous Plot Summary ({week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}):")
print(f"   Total data points plotted: {len(week_data):,} (NO GAPS)")
print(f"   Time span: {(week_data.index.max() - week_data.index.min()).total_seconds()/3600:.1f} hours")
print(f"   Price range: {price_min:.2f} - {price_max:.2f} EUR/MWh ({price_max-price_min:.2f} range)")
print(f"   Average spread: {spread_mean:.3f} EUR/MWh (±{spread_std:.3f})")
print(f"   Spread range: {week_data['spread'].min():.3f} - {week_data['spread'].max():.3f}")
print(f"   Zero spreads: {(week_data['spread'] == 0).sum():,} ({(week_data['spread'] == 0).mean()*100:.1f}%)")

plt.show()