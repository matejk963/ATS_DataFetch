#!/usr/bin/env python3
"""
Analysis script to verify BidAskValidator effectiveness
Checks if negative bid-ask spreads were eliminated from the latest data file
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os

# File path
data_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet"

print("=== BidAsk Validator Analysis ===")
print(f"Analyzing file: {data_file}")

# Check file modification time
file_stat = os.stat(data_file)
mod_time = datetime.fromtimestamp(file_stat.st_mtime)
print(f"File last modified: {mod_time}")
print(f"File size: {file_stat.st_size:,} bytes")

# Load the data
print("\nLoading data...")
try:
    df = pd.read_parquet(data_file)
    print(f"Successfully loaded {len(df):,} records")
    
    # Basic info about the dataset
    print(f"\nDataset Info:")
    print(f"Total records: {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    
    # Show sample of data
    print(f"\nFirst few rows:")
    print(df.head())
    
    # Use the correct column names
    bid_col = 'b_price'
    ask_col = 'a_price'
    
    print(f"\n=== NEGATIVE SPREAD ANALYSIS ===")
    print(f"Using columns: {bid_col} (bid), {ask_col} (ask)")
    
    # Check for null values first
    print(f"\nData quality check:")
    print(f"Null values in {bid_col}: {df[bid_col].isnull().sum():,}")
    print(f"Null values in {ask_col}: {df[ask_col].isnull().sum():,}")
    
    # Filter out rows where both bid and ask are available
    valid_data = df.dropna(subset=[bid_col, ask_col])
    print(f"Records with both bid and ask: {len(valid_data):,}")
    
    if len(valid_data) == 0:
        print("ERROR: No records with both bid and ask prices found!")
        exit(1)
    
    # Calculate spreads
    valid_data = valid_data.copy()
    valid_data['spread'] = valid_data[ask_col] - valid_data[bid_col]
    
    # Count negative spreads
    negative_spreads = valid_data[valid_data['spread'] < 0]
    total_valid_records = len(valid_data)
    negative_count = len(negative_spreads)
    negative_percentage = (negative_count / total_valid_records) * 100 if total_valid_records > 0 else 0
    
    print(f"\nSpread Analysis:")
    print(f"Valid records (with both bid & ask): {total_valid_records:,}")
    print(f"Negative spreads: {negative_count:,}")
    print(f"Negative spread percentage: {negative_percentage:.2f}%")
    
    # Previous analysis comparison
    previous_negative = 57968
    previous_percentage = 23.76
    
    print(f"\n=== COMPARISON WITH PREVIOUS ANALYSIS ===")
    print(f"Previous: {previous_negative:,} negative spreads ({previous_percentage:.2f}%)")
    print(f"Current:  {negative_count:,} negative spreads ({negative_percentage:.2f}%)")
    
    # Calculate improvement
    reduction_absolute = previous_negative - negative_count
    reduction_percentage = ((previous_negative - negative_count) / previous_negative) * 100 if previous_negative > 0 else 0
    
    print(f"\nImprovement:")
    print(f"Absolute reduction: {reduction_absolute:,} negative spreads")
    print(f"Percentage reduction: {reduction_percentage:.2f}%")
    
    # Final verdict
    print(f"\n=== VALIDATION RESULT ===")
    if negative_count == 0:
        print("✅ SUCCESS: BidAskValidator completely eliminated negative spreads!")
        validation_success = "YES"
    elif negative_count < previous_negative:
        print(f"⚠️  PARTIAL SUCCESS: BidAskValidator reduced negative spreads by {reduction_percentage:.2f}%")
        validation_success = "PARTIAL"
    else:
        print("❌ FAILED: BidAskValidator did not reduce negative spreads")
        validation_success = "NO"
    
    print(f"\nDid the BidAskValidator work? {validation_success}")
    
    # Show some examples of remaining negative spreads if any
    if negative_count > 0:
        print(f"\nSample of remaining negative spreads:")
        sample_negative = negative_spreads.head(10)[[bid_col, ask_col, 'spread']]
        print(sample_negative)
        
        # Statistics on negative spreads
        print(f"\nNegative spread statistics:")
        print(f"Min spread: {valid_data['spread'].min():.6f}")
        print(f"Max negative spread: {negative_spreads['spread'].max():.6f}")
        print(f"Mean negative spread: {negative_spreads['spread'].mean():.6f}")
    
    # Check data quality
    print(f"\n=== DATA QUALITY CHECKS ===")
    print(f"Zero values in {bid_col}: {(valid_data[bid_col] == 0).sum():,}")
    print(f"Zero values in {ask_col}: {(valid_data[ask_col] == 0).sum():,}")
    
    # Summary statistics
    print(f"\nBid/Ask Summary Statistics:")
    print(f"{bid_col} - Min: {valid_data[bid_col].min():.6f}, Max: {valid_data[bid_col].max():.6f}, Mean: {valid_data[bid_col].mean():.6f}")
    print(f"{ask_col} - Min: {valid_data[ask_col].min():.6f}, Max: {valid_data[ask_col].max():.6f}, Mean: {valid_data[ask_col].mean():.6f}")
    print(f"Spread - Min: {valid_data['spread'].min():.6f}, Max: {valid_data['spread'].max():.6f}, Mean: {valid_data['spread'].mean():.6f}")
    
    # Distribution of spreads
    positive_spreads = len(valid_data[valid_data['spread'] > 0])
    zero_spreads = len(valid_data[valid_data['spread'] == 0])
    
    print(f"\nSpread Distribution:")
    print(f"Positive spreads: {positive_spreads:,} ({(positive_spreads/total_valid_records)*100:.2f}%)")
    print(f"Zero spreads: {zero_spreads:,} ({(zero_spreads/total_valid_records)*100:.2f}%)")
    print(f"Negative spreads: {negative_count:,} ({negative_percentage:.2f}%)")

except Exception as e:
    print(f"Error loading or analyzing data: {e}")
    import traceback
    traceback.print_exc()