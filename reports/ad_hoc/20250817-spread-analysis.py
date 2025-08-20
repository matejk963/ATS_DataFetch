#!/usr/bin/env python3
"""
Ad-hoc analysis: Check for negative bid-ask spreads in latest data
Date: 2025-08-17
Question: Do negative bid-ask spreads still exist after latest data fetch?
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# Load the data
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet"
print(f"Loading data from: {data_path}")

# Check file info
file_stats = os.stat(data_path)
file_time = datetime.fromtimestamp(file_stats.st_mtime)
print(f"File last modified: {file_time}")
print(f"File size: {file_stats.st_size:,} bytes")

# Load the data
df = pd.read_parquet(data_path)

print(f"\n=== DATA OVERVIEW ===")
print(f"Total records: {len(df):,}")
print(f"Columns: {list(df.columns)}")
print(f"Date range: {df.index.min()} to {df.index.max()}")

# Identify bid and ask columns
bid_cols = [col for col in df.columns if 'bid' in col.lower()]
ask_cols = [col for col in df.columns if 'ask' in col.lower()]

print(f"\nBid columns: {bid_cols}")
print(f"Ask columns: {ask_cols}")

# Calculate spreads and check for negative values
print(f"\n=== NEGATIVE SPREAD ANALYSIS ===")

total_negative_count = 0
total_valid_pairs = 0

for bid_col in bid_cols:
    for ask_col in ask_cols:
        # Only check matching instrument pairs
        if bid_col.replace('bid', '') == ask_col.replace('ask', ''):
            # Calculate spread (ask - bid)
            spread = df[ask_col] - df[bid_col]
            
            # Count valid (non-null) pairs
            valid_mask = df[bid_col].notna() & df[ask_col].notna()
            valid_pairs = valid_mask.sum()
            total_valid_pairs += valid_pairs
            
            # Count negative spreads (ask < bid)
            negative_mask = valid_mask & (spread < 0)
            negative_count = negative_mask.sum()
            total_negative_count += negative_count
            
            if negative_count > 0:
                print(f"\n{bid_col} vs {ask_col}:")
                print(f"  Valid pairs: {valid_pairs:,}")
                print(f"  Negative spreads: {negative_count:,}")
                print(f"  Percentage: {(negative_count/valid_pairs)*100:.2f}%")
                
                # Show sample of negative spreads
                negative_samples = df[negative_mask][[bid_col, ask_col]].head(5)
                if len(negative_samples) > 0:
                    print(f"  Sample negative spreads:")
                    for idx, row in negative_samples.iterrows():
                        spread_val = row[ask_col] - row[bid_col]
                        print(f"    {idx}: bid={row[bid_col]:.4f}, ask={row[ask_col]:.4f}, spread={spread_val:.4f}")

print(f"\n=== SUMMARY ===")
print(f"Total valid bid-ask pairs: {total_valid_pairs:,}")
print(f"Total negative spreads: {total_negative_count:,}")

if total_valid_pairs > 0:
    negative_percentage = (total_negative_count / total_valid_pairs) * 100
    print(f"Overall negative spread percentage: {negative_percentage:.4f}%")
else:
    negative_percentage = 0

# Final answer
if total_negative_count > 0:
    answer = "YES"
    print(f"\n🔴 ANSWER: {answer} - Negative bid-ask spreads still exist")
else:
    answer = "NO"
    print(f"\n🟢 ANSWER: {answer} - No negative bid-ask spreads found")

print(f"\nKey numbers:")
print(f"- File timestamp: {file_time}")
print(f"- Total records: {len(df):,}")
print(f"- Negative spreads: {total_negative_count:,}")
print(f"- Percentage: {negative_percentage:.4f}%")