#!/usr/bin/env python3
"""
Weekly Continuous Spread Plot Analysis
Creates a professional continuous plot showing bid, ask, mid prices and spread
for the week with the best data coverage.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import os

def main():
    # Load and prepare data
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet'
    df = pd.read_parquet(file_path)
    df.index = pd.to_datetime(df.index)

    # Select the week with best coverage (Aug 4-10, 2025)
    start_date = '2025-08-04'
    end_date = '2025-08-11'
    week_data = df[start_date:end_date].copy()

    # Calculate prices and spread
    week_data['bid'] = week_data['b_price']
    week_data['ask'] = week_data['a_price'] 
    week_data['mid'] = week_data['0']
    week_data['spread'] = week_data['ask'] - week_data['bid']

    # Remove NaN values
    week_data = week_data.dropna(subset=['bid', 'ask', 'mid', 'spread'])

    # Create the plot
    plt.style.use('default')
    fig, ax1 = plt.subplots(figsize=(16, 10))

    # Plot price lines on primary y-axis
    ax1.plot(week_data.index, week_data['bid'], color='blue', linewidth=1.2, label='Bid Price', alpha=0.8)
    ax1.plot(week_data.index, week_data['ask'], color='red', linewidth=1.2, label='Ask Price', alpha=0.8)
    ax1.plot(week_data.index, week_data['mid'], color='green', linewidth=1.2, label='Mid Price', alpha=0.8)

    # Configure primary y-axis
    ax1.set_xlabel('Time', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Price ($)', fontsize=12, fontweight='bold', color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.grid(True, alpha=0.3)

    # Create secondary y-axis for spread
    ax2 = ax1.twinx()
    ax2.plot(week_data.index, week_data['spread'], color='orange', linewidth=1.2, label='Spread (Ask-Bid)', alpha=0.8)
    ax2.set_ylabel('Spread ($)', fontsize=12, fontweight='bold', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')

    # Format x-axis
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

    # Add title with specific week dates
    week_start = week_data.index.min().strftime('%Y-%m-%d')
    week_end = week_data.index.max().strftime('%Y-%m-%d')
    plt.title(f'Continuous Price and Spread Analysis\nWeek of {week_start} to {week_end}', 
              fontsize=14, fontweight='bold', pad=20)

    # Combine legends from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10, framealpha=0.9)

    # Improve layout
    plt.tight_layout()

    # Save the plot
    output_dir = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f'{output_dir}/weekly-continuous-spread-plot.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')

    print('Plot saved successfully!')
    print(f'Week analyzed: {week_start} to {week_end}')
    print(f'Data points plotted: {len(week_data):,}')
    print(f'File saved to: {output_path}')

    # Show some key statistics
    print(f'\nKey Statistics for the Week:')
    print(f'Average Bid: ${week_data["bid"].mean():.3f}')
    print(f'Average Ask: ${week_data["ask"].mean():.3f}')
    print(f'Average Mid: ${week_data["mid"].mean():.3f}')
    print(f'Average Spread: ${week_data["spread"].mean():.3f}')
    print(f'Max Spread: ${week_data["spread"].max():.3f}')
    print(f'Min Spread: ${week_data["spread"].min():.3f}')
    
    plt.show()

if __name__ == "__main__":
    main()