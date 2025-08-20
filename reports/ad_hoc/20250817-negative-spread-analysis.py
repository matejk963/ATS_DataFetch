#!/usr/bin/env python3
"""
Negative Bid-Ask Spread Analysis
Investigates instances where ask price < bid price in the spread data
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
print("\nFirst few rows:")
print(df.head())

print("\nDataset info:")
print(df.info())

# Identify bid and ask columns
bid_cols = [col for col in df.columns if 'bid' in col.lower()]
ask_cols = [col for col in df.columns if 'ask' in col.lower()]

print(f"\nBid columns: {bid_cols}")
print(f"Ask columns: {ask_cols}")

# Analyze spread structure
print("\nDataset structure analysis:")
print(f"Total records: {len(df):,}")

# Check for timestamp column
time_cols = [col for col in df.columns if any(t in col.lower() for t in ['time', 'date', 'timestamp'])]
print(f"Time columns: {time_cols}")

# Basic statistics
print("\nBasic statistics:")
print(df.describe())

# Check for negative spreads
negative_spreads = []
all_spreads = []

for bid_col in bid_cols:
    for ask_col in ask_cols:
        if bid_col in df.columns and ask_col in df.columns:
            # Calculate spread (ask - bid)
            spread = df[ask_col] - df[bid_col]
            spread_name = f"{ask_col}_minus_{bid_col}"
            
            # Find negative spreads (ask < bid)
            negative_mask = spread < 0
            negative_count = negative_mask.sum()
            
            if negative_count > 0:
                negative_spreads.append({
                    'bid_col': bid_col,
                    'ask_col': ask_col,
                    'spread_name': spread_name,
                    'negative_count': negative_count,
                    'total_count': len(df),
                    'negative_pct': (negative_count / len(df)) * 100,
                    'min_spread': spread.min(),
                    'max_spread': spread.max(),
                    'mean_spread': spread.mean(),
                    'std_spread': spread.std()
                })
                
                # Store spread data
                df[spread_name] = spread
                all_spreads.append(spread_name)
                
                print(f"\n=== {spread_name} ===")
                print(f"Negative spreads: {negative_count:,} ({(negative_count/len(df)*100):.2f}%)")
                print(f"Min spread: {spread.min():.6f}")
                print(f"Max spread: {spread.max():.6f}")
                print(f"Mean spread: {spread.mean():.6f}")

# Convert to DataFrame for easier analysis
if negative_spreads:
    negative_df = pd.DataFrame(negative_spreads)
    print("\n=== SUMMARY OF NEGATIVE SPREADS ===")
    print(negative_df)
    
    # Find worst offenders
    worst_spread = negative_df.loc[negative_df['negative_count'].idxmax()]
    print(f"\nWorst spread pair: {worst_spread['spread_name']}")
    print(f"Negative instances: {worst_spread['negative_count']:,} ({worst_spread['negative_pct']:.2f}%)")
    
    # Analyze temporal patterns if timestamp exists
    if time_cols:
        time_col = time_cols[0]
        print(f"\n=== TEMPORAL ANALYSIS using {time_col} ===")
        
        # Convert to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
            try:
                df[time_col] = pd.to_datetime(df[time_col])
                print(f"Converted {time_col} to datetime")
            except:
                print(f"Could not convert {time_col} to datetime")
        
        if pd.api.types.is_datetime64_any_dtype(df[time_col]):
            # Analyze by hour
            df['hour'] = df[time_col].dt.hour
            df['date'] = df[time_col].dt.date
            
            for spread_name in all_spreads:
                if spread_name in df.columns:
                    negative_mask = df[spread_name] < 0
                    
                    print(f"\n--- Temporal patterns for {spread_name} ---")
                    
                    # By hour
                    hourly_negative = df[negative_mask].groupby('hour').size()
                    hourly_total = df.groupby('hour').size()
                    hourly_pct = (hourly_negative / hourly_total * 100).fillna(0)
                    
                    print("Negative spreads by hour:")
                    for hour in range(24):
                        if hour in hourly_negative.index:
                            print(f"  Hour {hour:02d}: {hourly_negative[hour]:,} ({hourly_pct[hour]:.1f}%)")
                    
                    # By date
                    daily_negative = df[negative_mask].groupby('date').size()
                    daily_total = df.groupby('date').size()
                    daily_pct = (daily_negative / daily_total * 100).fillna(0)
                    
                    print(f"\nDaily summary:")
                    print(f"  Dates with negative spreads: {len(daily_negative)}")
                    print(f"  Total dates: {len(daily_total)}")
                    if len(daily_negative) > 0:
                        print(f"  Worst day: {daily_negative.idxmax()} ({daily_negative.max():,} negative spreads)")
    
    # Sample anomalous records
    print("\n=== SAMPLE ANOMALOUS RECORDS ===")
    for spread_name in all_spreads[:3]:  # Show first 3 spreads
        if spread_name in df.columns:
            negative_mask = df[spread_name] < 0
            if negative_mask.any():
                sample_records = df[negative_mask].head(5)
                print(f"\nSample records for {spread_name}:")
                
                # Show relevant columns
                relevant_cols = []
                if time_cols:
                    relevant_cols.extend(time_cols)
                relevant_cols.extend(bid_cols + ask_cols)
                relevant_cols.append(spread_name)
                
                display_cols = [col for col in relevant_cols if col in df.columns]
                print(sample_records[display_cols].to_string())

else:
    print("\nNo negative spreads found in the data!")

print(f"\n=== ANALYSIS COMPLETE ===")
print(f"Total records analyzed: {len(df):,}")
print(f"Spread combinations checked: {len(bid_cols) * len(ask_cols)}")
print(f"Negative spread patterns found: {len(negative_spreads)}")