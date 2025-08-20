#!/usr/bin/env python3
"""
Ad-hoc analysis: Check for negative bid-ask spreads in latest data (v2)
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

# Show sample data to understand structure
print(f"\n=== SAMPLE DATA ===")
print(df.head(10))

print(f"\n=== COLUMN ANALYSIS ===")
for col in df.columns:
    print(f"{col}: {df[col].dtype}, non-null: {df[col].notna().sum():,}")

# Check if b_price and a_price are bid and ask
if 'b_price' in df.columns and 'a_price' in df.columns:
    print(f"\n=== BID-ASK SPREAD ANALYSIS ===")
    print("Assuming b_price = bid, a_price = ask")
    
    # Filter out null values
    valid_mask = df['b_price'].notna() & df['a_price'].notna()
    valid_data = df[valid_mask]
    
    print(f"Valid bid-ask pairs: {len(valid_data):,}")
    
    if len(valid_data) > 0:
        # Calculate spread (ask - bid)
        spread = valid_data['a_price'] - valid_data['b_price']
        
        # Count negative spreads
        negative_mask = spread < 0
        negative_count = negative_mask.sum()
        
        print(f"Negative spreads (ask < bid): {negative_count:,}")
        print(f"Percentage: {(negative_count/len(valid_data))*100:.4f}%")
        
        if negative_count > 0:
            print(f"\n=== SAMPLE NEGATIVE SPREADS ===")
            negative_samples = valid_data[negative_mask][['b_price', 'a_price']].head(10)
            for idx, row in negative_samples.iterrows():
                spread_val = row['a_price'] - row['b_price']
                print(f"{idx}: bid={row['b_price']:.4f}, ask={row['a_price']:.4f}, spread={spread_val:.4f}")
        
        # Summary statistics
        print(f"\n=== SPREAD STATISTICS ===")
        print(f"Min spread: {spread.min():.4f}")
        print(f"Max spread: {spread.max():.4f}")
        print(f"Mean spread: {spread.mean():.4f}")
        print(f"Median spread: {spread.median():.4f}")
        
        # Final answer
        if negative_count > 0:
            answer = "YES"
            print(f"\n🔴 ANSWER: {answer} - Negative bid-ask spreads still exist")
        else:
            answer = "NO"
            print(f"\n🟢 ANSWER: {answer} - No negative bid-ask spreads found")
        
        print(f"\nKey numbers:")
        print(f"- File timestamp: {file_time}")
        print(f"- Total records: {len(df):,}")
        print(f"- Valid bid-ask pairs: {len(valid_data):,}")
        print(f"- Negative spreads: {negative_count:,}")
        print(f"- Percentage: {(negative_count/len(valid_data))*100:.4f}%")
    else:
        print("No valid bid-ask data found")
else:
    print("Could not identify bid and ask price columns")