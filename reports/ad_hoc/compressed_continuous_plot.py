#!/usr/bin/env python3
"""
Compressed Continuous Plot - No Time Gaps
=========================================
Creates a plot where x-axis shows only data points with no gaps for missing times.
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

# Remove rows with NaN values and sort by time
week_data = week_data.dropna(subset=['b_price', 'a_price']).sort_index()

# Calculate mid price and spread
week_data['mid_price'] = (week_data['b_price'] + week_data['a_price']) / 2
week_data['spread'] = week_data['a_price'] - week_data['b_price']

print(f"📈 Clean data: {len(week_data):,} records")

# Create compressed x-axis: sequential integers instead of actual timestamps
# This removes all time gaps
x_positions = np.arange(len(week_data))

# Create the plot
plt.style.use('default')
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True, 
                               gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.1})

# Main price plot - using sequential x positions (NO GAPS)
ax1.plot(x_positions, week_data['b_price'].values, 
         color='#1f77b4', linewidth=1.5, label='Bid Price', alpha=0.9)
ax1.plot(x_positions, week_data['a_price'].values, 
         color='#d62728', linewidth=1.5, label='Ask Price', alpha=0.9)
ax1.plot(x_positions, week_data['mid_price'].values, 
         color='#2ca02c', linewidth=2, label='Mid Price', alpha=0.9)

ax1.set_ylabel('Price (EUR/MWh)', fontsize=12, fontweight='bold')
ax1.set_title(f'DEBQ4_25 vs FRBQ4_25 Spread - Compressed Continuous View\n'
              f'{week_start.strftime("%B %d")} - {week_end.strftime("%B %d, %Y")} '
              f'({len(week_data):,} data points)', 
              fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', fontsize=11)
ax1.grid(True, alpha=0.3)

# Price range info
price_min = week_data[['b_price', 'a_price']].min().min()
price_max = week_data[['b_price', 'a_price']].max().max()
ax1.text(0.02, 0.98, f'Price Range: {price_min:.2f} - {price_max:.2f} EUR/MWh\nNo Time Gaps', 
         transform=ax1.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Spread plot - using sequential x positions (NO GAPS)
ax2.plot(x_positions, week_data['spread'].values, 
         color='#ff7f0e', linewidth=1.5, label='Bid-Ask Spread', alpha=0.9)
ax2.fill_between(x_positions, week_data['spread'].values, 
                 color='#ff7f0e', alpha=0.3)

ax2.set_ylabel('Spread\n(EUR/MWh)', fontsize=12, fontweight='bold')
ax2.set_xlabel('Data Point Sequence (Time Gaps Removed)', fontsize=12, fontweight='bold')
ax2.legend(loc='upper left', fontsize=11)
ax2.grid(True, alpha=0.3)

# Spread statistics
spread_mean = week_data['spread'].mean()
spread_std = week_data['spread'].std()
spread_median = week_data['spread'].median()
ax2.text(0.02, 0.98, f'Spread - Mean: {spread_mean:.3f}, Median: {spread_median:.3f}, Std: {spread_std:.3f}', 
         transform=ax2.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Custom x-axis labels showing actual times at key positions
n_labels = 8  # Number of time labels to show
label_positions = np.linspace(0, len(week_data)-1, n_labels, dtype=int)
label_times = [week_data.index[pos].strftime('%m/%d %H:%M') for pos in label_positions]

ax2.set_xticks(label_positions)
ax2.set_xticklabels(label_times, rotation=45, ha='right')

# Add day boundary markers if we span multiple days
unique_days = sorted(set(week_data.index.date))
if len(unique_days) > 1:
    for day in unique_days[1:]:  # Skip first day
        day_start = pd.Timestamp(day)
        # Find the first data point on this day
        day_mask = week_data.index.date == day
        if day_mask.any():
            first_idx_of_day = np.where(day_mask)[0][0]
            ax1.axvline(x=first_idx_of_day, color='gray', linestyle='--', alpha=0.5, linewidth=1)
            day_name = day_start.strftime('%A')
            ax1.text(first_idx_of_day, ax1.get_ylim()[1], day_name, 
                    rotation=90, ha='right', va='top', fontsize=9, alpha=0.7)

# Adjust layout
plt.subplots_adjust(hspace=0.1)

# Create output directory
output_dir = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc"
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, "compressed-continuous-no-gaps.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"📁 Plot saved: {output_path}")

# Summary
print(f"\n📊 Compressed Continuous Plot Summary:")
print(f"   Data points: {len(week_data):,} (sequential, no time gaps)")
print(f"   Time span: {week_start.strftime('%Y-%m-%d %H:%M')} to {week_end.strftime('%Y-%m-%d %H:%M')}")
print(f"   Price range: {price_min:.2f} - {price_max:.2f} EUR/MWh")
print(f"   Average spread: {spread_mean:.3f} ± {spread_std:.3f} EUR/MWh")
print(f"   All time gaps removed - pure continuous data visualization")

# Set backend to Agg to avoid display issues
plt.ioff()  # Turn off interactive mode
plt.close()  # Close the figure to free memory

print("✅ Plot completed without display")