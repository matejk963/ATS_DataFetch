#!/usr/bin/env python3
"""
Negative Bid-Ask Spread Analysis - Version 2
Investigates instances where ask price < bid price in the spread data
Specifically looks at b_price (bid) and a_price (ask) columns
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# File path
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet"

print("Loading spread data...")
df = pd.read_parquet(data_path)

print(f"Dataset shape: {df.shape}")
print(f"Columns: {list(df.columns)}")

# Check for b_price and a_price columns
if 'b_price' not in df.columns or 'a_price' not in df.columns:
    print("ERROR: Expected b_price and a_price columns not found!")
    exit(1)

print(f"\nBid column: b_price")
print(f"Ask column: a_price")
print(f"Index type: {type(df.index)}")

# Basic info about bid and ask prices
print(f"\n=== BID PRICE (b_price) STATISTICS ===")
print(f"Non-null count: {df['b_price'].notna().sum():,}")
print(f"Null count: {df['b_price'].isna().sum():,}")
print(f"Min: {df['b_price'].min()}")
print(f"Max: {df['b_price'].max()}")
print(f"Mean: {df['b_price'].mean():.6f}")

print(f"\n=== ASK PRICE (a_price) STATISTICS ===")
print(f"Non-null count: {df['a_price'].notna().sum():,}")
print(f"Null count: {df['a_price'].isna().sum():,}")
print(f"Min: {df['a_price'].min()}")
print(f"Max: {df['a_price'].max()}")
print(f"Mean: {df['a_price'].mean():.6f}")

# Filter to records where both bid and ask are present
both_present = df[df['b_price'].notna() & df['a_price'].notna()].copy()
print(f"\nRecords with both bid and ask prices: {len(both_present):,}")

if len(both_present) == 0:
    print("ERROR: No records with both bid and ask prices!")
    exit(1)

# Calculate spread (ask - bid)
both_present['spread'] = both_present['a_price'] - both_present['b_price']

print(f"\n=== SPREAD ANALYSIS ===")
print(f"Total records with spreads: {len(both_present):,}")
print(f"Min spread: {both_present['spread'].min():.6f}")
print(f"Max spread: {both_present['spread'].max():.6f}")
print(f"Mean spread: {both_present['spread'].mean():.6f}")
print(f"Std spread: {both_present['spread'].std():.6f}")

# Find negative spreads (ask < bid)
negative_mask = both_present['spread'] < 0
negative_count = negative_mask.sum()

print(f"\n=== NEGATIVE SPREAD ANALYSIS ===")
print(f"Negative spreads (ask < bid): {negative_count:,}")
print(f"Percentage of records: {(negative_count / len(both_present) * 100):.2f}%")

if negative_count > 0:
    negative_records = both_present[negative_mask].copy()
    
    print(f"\nNegative spread statistics:")
    print(f"Most negative spread: {negative_records['spread'].min():.6f}")
    print(f"Least negative spread: {negative_records['spread'].max():.6f}")
    print(f"Mean negative spread: {negative_records['spread'].mean():.6f}")
    
    # Temporal analysis using the datetime index
    print(f"\n=== TEMPORAL PATTERNS ===")
    
    # Add time components
    negative_records['hour'] = negative_records.index.hour
    negative_records['date'] = negative_records.index.date
    negative_records['minute'] = negative_records.index.minute
    
    # By hour analysis
    print("\nNegative spreads by hour:")
    hourly_negative = negative_records.groupby('hour').size()
    hourly_total = both_present.groupby(both_present.index.hour).size()
    
    for hour in sorted(hourly_negative.index):
        total_hour = hourly_total.get(hour, 0)
        pct = (hourly_negative[hour] / total_hour * 100) if total_hour > 0 else 0
        print(f"  Hour {hour:02d}: {hourly_negative[hour]:,} negative / {total_hour:,} total ({pct:.1f}%)")
    
    # By date analysis
    print(f"\nNegative spreads by date:")
    daily_negative = negative_records.groupby('date').size()
    daily_total = both_present.groupby(both_present.index.date).size()
    
    print(f"Dates with negative spreads: {len(daily_negative)}")
    worst_dates = daily_negative.nlargest(10)
    print(f"\nTop 10 worst dates:")
    for date, count in worst_dates.items():
        total_day = daily_total.get(date, 0)
        pct = (count / total_day * 100) if total_day > 0 else 0
        print(f"  {date}: {count:,} negative / {total_day:,} total ({pct:.1f}%)")
    
    # Sample negative records
    print(f"\n=== SAMPLE NEGATIVE SPREAD RECORDS ===")
    sample_size = min(10, len(negative_records))
    sample_records = negative_records.nsmallest(sample_size, 'spread')
    
    print(f"Top {sample_size} most negative spreads:")
    display_cols = ['b_price', 'a_price', 'spread', 'price', 'volume']
    available_cols = [col for col in display_cols if col in sample_records.columns]
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(sample_records[available_cols].to_string())
    
    # Distribution analysis
    print(f"\n=== SPREAD DISTRIBUTION ===")
    
    # Percentiles
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    print("Spread percentiles:")
    for p in percentiles:
        value = np.percentile(both_present['spread'], p)
        print(f"  {p:2d}th percentile: {value:.6f}")
    
    # Zero spreads
    zero_spreads = (both_present['spread'] == 0).sum()
    print(f"\nZero spreads: {zero_spreads:,} ({zero_spreads/len(both_present)*100:.2f}%)")
    
    # Very small spreads
    small_spreads = (both_present['spread'].abs() < 0.001).sum()
    print(f"Spreads < 0.001: {small_spreads:,} ({small_spreads/len(both_present)*100:.2f}%)")

else:
    print("No negative spreads found!")
    
    # Still show distribution for context
    print(f"\n=== SPREAD DISTRIBUTION (ALL POSITIVE) ===")
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    print("Spread percentiles:")
    for p in percentiles:
        value = np.percentile(both_present['spread'], p)
        print(f"  {p:2d}th percentile: {value:.6f}")

print(f"\n=== SUMMARY ===")
print(f"Total records: {len(df):,}")
print(f"Records with both bid/ask: {len(both_present):,}")
print(f"Negative spreads: {negative_count:,}")
if len(both_present) > 0:
    print(f"Negative spread rate: {(negative_count/len(both_present)*100):.3f}%")