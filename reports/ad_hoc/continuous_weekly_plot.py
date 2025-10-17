#!/usr/bin/env python3
"""
Continuous Weekly Spread Plot
============================
Creates a continuous plot for one week of spread data without gaps.
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
# Find the middle date and select a week around it
middle_date = df.index.min() + (df.index.max() - df.index.min()) / 2
week_start = middle_date - timedelta(days=middle_date.weekday())  # Monday
week_end = week_start + timedelta(days=6)  # Sunday

print(f"📅 Selected week: {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")

# Filter data for the selected week
week_data = df[(df.index >= week_start) & (df.index <= week_end)].copy()
print(f"📈 Week data: {len(week_data):,} records")

# Calculate mid price and spread
week_data['mid_price'] = (week_data['b_price'] + week_data['a_price']) / 2
week_data['spread'] = week_data['a_price'] - week_data['b_price']

# Remove rows with NaN values in key columns
week_data = week_data.dropna(subset=['b_price', 'a_price'])

# Create continuous time series by resampling to regular intervals
# Use 5-minute intervals and forward fill to create continuous data
continuous_data = week_data.resample('5min').last().fillna(method='ffill')

print(f"📊 Continuous data: {len(continuous_data):,} 5-minute intervals")

# Create the plot
plt.style.use('default')
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True, 
                               gridspec_kw={'height_ratios': [3, 1], 'hspace': 0.1})

# Main price plot
ax1.plot(continuous_data.index, continuous_data['b_price'], 
         color='blue', linewidth=2, label='Bid Price', alpha=0.8)
ax1.plot(continuous_data.index, continuous_data['a_price'], 
         color='red', linewidth=2, label='Ask Price', alpha=0.8)
ax1.plot(continuous_data.index, continuous_data['mid_price'], 
         color='green', linewidth=2, label='Mid Price', alpha=0.9)

ax1.set_ylabel('Price (EUR/MWh)', fontsize=12, fontweight='bold')
ax1.set_title(f'DEBQ4_25 vs FRBQ4_25 Spread - Continuous Weekly View\n'
              f'{week_start.strftime("%B %d")} - {week_end.strftime("%B %d, %Y")}', 
              fontsize=14, fontweight='bold')
ax1.legend(loc='upper left', fontsize=11)
ax1.grid(True, alpha=0.3)

# Add price range info
price_min = continuous_data[['b_price', 'a_price']].min().min()
price_max = continuous_data[['b_price', 'a_price']].max().max()
ax1.text(0.02, 0.98, f'Price Range: {price_min:.2f} - {price_max:.2f} EUR/MWh', 
         transform=ax1.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Spread plot
ax2.plot(continuous_data.index, continuous_data['spread'], 
         color='orange', linewidth=2, label='Bid-Ask Spread', alpha=0.8)
ax2.fill_between(continuous_data.index, continuous_data['spread'], 
                 color='orange', alpha=0.3)

ax2.set_ylabel('Spread\n(EUR/MWh)', fontsize=12, fontweight='bold')
ax2.set_xlabel('Time', fontsize=12, fontweight='bold')
ax2.legend(loc='upper left', fontsize=11)
ax2.grid(True, alpha=0.3)

# Add spread statistics
spread_mean = continuous_data['spread'].mean()
spread_std = continuous_data['spread'].std()
ax2.text(0.02, 0.98, f'Avg Spread: {spread_mean:.3f} ± {spread_std:.3f}', 
         transform=ax2.transAxes, fontsize=10, verticalalignment='top',
         bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

# Format x-axis
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
ax2.xaxis.set_major_locator(mdates.HourLocator(interval=12))  # Every 12 hours
ax2.xaxis.set_minor_locator(mdates.HourLocator(interval=4))   # Every 4 hours

# Rotate x-axis labels for better readability
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

# Add weekday labels
for i in range(7):
    day_start = week_start + timedelta(days=i)
    if day_start <= continuous_data.index.max():
        ax1.axvline(x=day_start, color='gray', linestyle='--', alpha=0.5)
        ax1.text(day_start, ax1.get_ylim()[1], day_start.strftime('%A'), 
                rotation=90, ha='right', va='top', fontsize=9, alpha=0.7)

# Tight layout and save
plt.tight_layout()

# Create output directory
output_dir = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc"
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, "continuous-weekly-spread-plot.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"📁 Plot saved: {output_path}")

# Display summary statistics
print(f"\n📊 Weekly Summary ({week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}):")
print(f"   Data points: {len(continuous_data):,} (5-minute intervals)")
print(f"   Price range: {price_min:.2f} - {price_max:.2f} EUR/MWh ({price_max-price_min:.2f} range)")
print(f"   Average spread: {spread_mean:.3f} EUR/MWh")
print(f"   Spread range: {continuous_data['spread'].min():.3f} - {continuous_data['spread'].max():.3f}")
print(f"   Data coverage: {(len(continuous_data) / (7*24*12)*100):.1f}% (continuous 5-min intervals)")

plt.show()