#!/usr/bin/env python3
"""
Negative Bid-Ask Spread Visualization
Creates charts to visualize the temporal and distribution patterns of negative spreads
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('default')
sns.set_palette("husl")

# File path
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet"
output_dir = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc"

print("Loading data and calculating spreads...")
df = pd.read_parquet(data_path)

# Filter to records with both bid and ask
both_present = df[df['b_price'].notna() & df['a_price'].notna()].copy()
both_present['spread'] = both_present['a_price'] - both_present['b_price']
both_present['is_negative'] = both_present['spread'] < 0

print(f"Creating visualizations for {len(both_present):,} records...")

# Create figure with multiple subplots
fig = plt.figure(figsize=(20, 16))

# 1. Spread distribution histogram
plt.subplot(3, 3, 1)
plt.hist(both_present['spread'], bins=100, alpha=0.7, edgecolor='black')
plt.axvline(x=0, color='red', linestyle='--', label='Zero spread')
plt.xlabel('Spread (Ask - Bid)')
plt.ylabel('Frequency')
plt.title('Distribution of Bid-Ask Spreads')
plt.legend()
plt.grid(True, alpha=0.3)

# 2. Spread distribution (zoomed on negative)
plt.subplot(3, 3, 2)
negative_spreads = both_present[both_present['spread'] < 0]['spread']
if len(negative_spreads) > 0:
    plt.hist(negative_spreads, bins=50, alpha=0.7, color='red', edgecolor='black')
    plt.xlabel('Negative Spread (Ask - Bid)')
    plt.ylabel('Frequency')
    plt.title(f'Distribution of Negative Spreads\n({len(negative_spreads):,} records)')
    plt.grid(True, alpha=0.3)

# 3. Time series of negative spread percentage by hour
plt.subplot(3, 3, 3)
hourly_data = both_present.groupby(both_present.index.hour).agg({
    'is_negative': ['sum', 'count']
}).round(1)
hourly_data.columns = ['negative_count', 'total_count']
hourly_data['negative_pct'] = (hourly_data['negative_count'] / hourly_data['total_count'] * 100)

plt.bar(hourly_data.index, hourly_data['negative_pct'], alpha=0.7, color='orange')
plt.xlabel('Hour of Day')
plt.ylabel('Negative Spread %')
plt.title('Negative Spread Percentage by Hour')
plt.xticks(range(9, 18))
plt.grid(True, alpha=0.3)

# 4. Daily negative spread counts
plt.subplot(3, 3, 4)
daily_data = both_present.groupby(both_present.index.date).agg({
    'is_negative': ['sum', 'count']
})
daily_data.columns = ['negative_count', 'total_count']
daily_data['negative_pct'] = (daily_data['negative_count'] / daily_data['total_count'] * 100)

# Plot top 15 worst days
worst_days = daily_data.nlargest(15, 'negative_pct')
plt.bar(range(len(worst_days)), worst_days['negative_pct'], alpha=0.7, color='red')
plt.xlabel('Date (worst 15 days)')
plt.ylabel('Negative Spread %')
plt.title('Worst Days by Negative Spread %')
plt.xticks(range(len(worst_days)), [str(d) for d in worst_days.index], rotation=45)
plt.grid(True, alpha=0.3)

# 5. Scatter plot: bid vs ask prices (sample)
plt.subplot(3, 3, 5)
sample_size = min(10000, len(both_present))
sample = both_present.sample(sample_size)
negative_sample = sample[sample['spread'] < 0]
positive_sample = sample[sample['spread'] >= 0]

plt.scatter(positive_sample['b_price'], positive_sample['a_price'], 
           alpha=0.3, s=1, color='blue', label=f'Normal ({len(positive_sample)})')
if len(negative_sample) > 0:
    plt.scatter(negative_sample['b_price'], negative_sample['a_price'], 
               alpha=0.7, s=2, color='red', label=f'Negative ({len(negative_sample)})')

# Add diagonal line (where bid = ask)
min_price = min(sample['b_price'].min(), sample['a_price'].min())
max_price = max(sample['b_price'].max(), sample['a_price'].max())
plt.plot([min_price, max_price], [min_price, max_price], 'k--', alpha=0.5, label='Bid = Ask')

plt.xlabel('Bid Price')
plt.ylabel('Ask Price')
plt.title(f'Bid vs Ask Prices\n(Sample of {sample_size:,} records)')
plt.legend()
plt.grid(True, alpha=0.3)

# 6. Spread over time (daily average)
plt.subplot(3, 3, 6)
daily_spread = both_present.groupby(both_present.index.date)['spread'].mean()
plt.plot(daily_spread.index, daily_spread.values, marker='o', linewidth=1, markersize=3)
plt.axhline(y=0, color='red', linestyle='--', alpha=0.7)
plt.xlabel('Date')
plt.ylabel('Average Spread')
plt.title('Daily Average Spread Over Time')
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)

# 7. Heatmap of negative spreads by hour and date
plt.subplot(3, 3, 7)
# Create pivot table for heatmap
heatmap_data = both_present.copy()
heatmap_data['date'] = heatmap_data.index.date
heatmap_data['hour'] = heatmap_data.index.hour

pivot_data = heatmap_data.groupby(['date', 'hour'])['is_negative'].mean().unstack(fill_value=0)
# Select recent dates for better visibility
recent_dates = sorted(pivot_data.index)[-20:]  # Last 20 days
pivot_subset = pivot_data.loc[recent_dates] * 100  # Convert to percentage

sns.heatmap(pivot_subset, cmap='Reds', cbar_kws={'label': 'Negative Spread %'}, 
           fmt='.1f', linewidths=0.1)
plt.xlabel('Hour')
plt.ylabel('Date')
plt.title('Negative Spread % Heatmap\n(Last 20 days)')

# 8. Box plot of spreads by hour
plt.subplot(3, 3, 8)
hourly_spreads = [both_present[both_present.index.hour == h]['spread'].values 
                  for h in range(9, 18)]
hour_labels = [str(h) for h in range(9, 18)]

plt.boxplot(hourly_spreads, labels=hour_labels)
plt.axhline(y=0, color='red', linestyle='--', alpha=0.7)
plt.xlabel('Hour of Day')
plt.ylabel('Spread')
plt.title('Spread Distribution by Hour')
plt.grid(True, alpha=0.3)

# 9. Cumulative distribution of spreads
plt.subplot(3, 3, 9)
sorted_spreads = np.sort(both_present['spread'])
cumulative_pct = np.arange(1, len(sorted_spreads) + 1) / len(sorted_spreads) * 100

plt.plot(sorted_spreads, cumulative_pct, linewidth=2)
plt.axvline(x=0, color='red', linestyle='--', alpha=0.7, label='Zero spread')
plt.xlabel('Spread')
plt.ylabel('Cumulative %')
plt.title('Cumulative Distribution of Spreads')
plt.legend()
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(f"{output_dir}/20250817-negative-spread-analysis.png", dpi=150, bbox_inches='tight')
plt.savefig(f"{output_dir}/20250817-negative-spread-analysis.pdf", bbox_inches='tight')
print(f"Saved comprehensive analysis chart to {output_dir}/20250817-negative-spread-analysis.png")

# Create a focused chart on the worst day
worst_day = daily_data['negative_pct'].idxmax()
worst_day_data = both_present[both_present.index.date == worst_day].copy()

if len(worst_day_data) > 0:
    plt.figure(figsize=(15, 10))
    
    # Subplot 1: Spread over time for worst day
    plt.subplot(2, 3, 1)
    plt.scatter(worst_day_data.index.time, worst_day_data['spread'], 
               c=worst_day_data['is_negative'], cmap='RdYlBu_r', alpha=0.6, s=1)
    plt.axhline(y=0, color='red', linestyle='--')
    plt.xlabel('Time')
    plt.ylabel('Spread')
    plt.title(f'Spreads on Worst Day: {worst_day}')
    plt.xticks(rotation=45)
    plt.colorbar(label='Is Negative')
    
    # Subplot 2: Bid and ask prices over time
    plt.subplot(2, 3, 2)
    plt.plot(worst_day_data.index, worst_day_data['b_price'], label='Bid', alpha=0.7, linewidth=0.5)
    plt.plot(worst_day_data.index, worst_day_data['a_price'], label='Ask', alpha=0.7, linewidth=0.5)
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.title(f'Bid/Ask Prices on {worst_day}')
    plt.legend()
    plt.xticks(rotation=45)
    
    # Subplot 3: Distribution of spreads for worst day
    plt.subplot(2, 3, 3)
    plt.hist(worst_day_data['spread'], bins=50, alpha=0.7, edgecolor='black')
    plt.axvline(x=0, color='red', linestyle='--')
    plt.xlabel('Spread')
    plt.ylabel('Frequency')
    plt.title(f'Spread Distribution on {worst_day}')
    
    # Subplot 4: Hourly pattern for worst day
    plt.subplot(2, 3, 4)
    hourly_worst = worst_day_data.groupby(worst_day_data.index.hour)['is_negative'].agg(['sum', 'count'])
    hourly_worst['pct'] = hourly_worst['sum'] / hourly_worst['count'] * 100
    plt.bar(hourly_worst.index, hourly_worst['pct'], alpha=0.7, color='red')
    plt.xlabel('Hour')
    plt.ylabel('Negative Spread %')
    plt.title(f'Hourly Pattern on {worst_day}')
    
    # Subplot 5: Bid vs Ask scatter for worst day
    plt.subplot(2, 3, 5)
    negative_worst = worst_day_data[worst_day_data['spread'] < 0]
    positive_worst = worst_day_data[worst_day_data['spread'] >= 0]
    
    if len(positive_worst) > 0:
        plt.scatter(positive_worst['b_price'], positive_worst['a_price'], 
                   alpha=0.3, s=1, color='blue', label=f'Normal ({len(positive_worst)})')
    if len(negative_worst) > 0:
        plt.scatter(negative_worst['b_price'], negative_worst['a_price'], 
                   alpha=0.7, s=2, color='red', label=f'Negative ({len(negative_worst)})')
    
    min_p = min(worst_day_data['b_price'].min(), worst_day_data['a_price'].min())
    max_p = max(worst_day_data['b_price'].max(), worst_day_data['a_price'].max())
    plt.plot([min_p, max_p], [min_p, max_p], 'k--', alpha=0.5)
    plt.xlabel('Bid Price')
    plt.ylabel('Ask Price')
    plt.title(f'Bid vs Ask on {worst_day}')
    plt.legend()
    
    # Subplot 6: Summary stats
    plt.subplot(2, 3, 6)
    plt.text(0.1, 0.9, f'Worst Day Analysis: {worst_day}', fontsize=14, fontweight='bold')
    plt.text(0.1, 0.8, f'Total Records: {len(worst_day_data):,}', fontsize=12)
    plt.text(0.1, 0.7, f'Negative Spreads: {len(negative_worst):,}', fontsize=12)
    plt.text(0.1, 0.6, f'Negative Rate: {len(negative_worst)/len(worst_day_data)*100:.1f}%', fontsize=12)
    plt.text(0.1, 0.5, f'Min Spread: {worst_day_data["spread"].min():.3f}', fontsize=12)
    plt.text(0.1, 0.4, f'Max Spread: {worst_day_data["spread"].max():.3f}', fontsize=12)
    plt.text(0.1, 0.3, f'Mean Spread: {worst_day_data["spread"].mean():.3f}', fontsize=12)
    plt.xlim(0, 1)
    plt.ylim(0, 1)
    plt.axis('off')
    
    plt.tight_layout()
    plt.savefig(f"{output_dir}/20250817-worst-day-analysis.png", dpi=150, bbox_inches='tight')
    print(f"Saved worst day analysis to {output_dir}/20250817-worst-day-analysis.png")

print("Visualization complete!")