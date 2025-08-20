#!/usr/bin/env python3
"""
Test script to validate the BidAskValidator is working correctly
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime

# Setup paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

# Import the validator
sys.path.insert(0, os.path.join(project_root, 'engines'))
from data_fetch_engine import BidAskValidator

print("🧪 Testing BidAskValidator")
print("=" * 40)

# Create test data with known negative spreads
print("1. Creating test data with negative spreads...")

# Test data with some negative spreads
test_data = pd.DataFrame({
    'b_price': [30.0, 32.0, 35.0, 33.15, 28.0, 31.0],  # bid prices
    'a_price': [30.5, 31.8, 36.0, 30.0, 28.5, 30.5],  # ask prices (some negative spreads)
    'volume': [100, 200, 150, 300, 120, 180],
    'timestamp': pd.date_range('2025-07-01 10:00', periods=6, freq='1H')
})

test_data.set_index('timestamp', inplace=True)

print(f"   📊 Created test data: {len(test_data)} records")
print("   🔍 Test data:")
for idx, row in test_data.iterrows():
    spread = row['a_price'] - row['b_price']
    status = "❌ NEGATIVE" if spread < 0 else "✅ positive"
    print(f"      {idx}: bid={row['b_price']:.2f}, ask={row['a_price']:.2f}, spread={spread:.2f} {status}")

# Test the validator
print("\\n2. Testing BidAskValidator...")

validator = BidAskValidator(strict_mode=True, log_filtered=True)
filtered_data = validator.validate_orders(test_data, "TestData")

print(f"\\n3. Results:")
print(f"   📈 Original records: {len(test_data)}")
print(f"   📉 Filtered records: {len(filtered_data)}")
print(f"   🚫 Removed records: {len(test_data) - len(filtered_data)}")

stats = validator.get_stats()
print(f"   📊 Filter rate: {stats['filter_rate']:.1f}%")

print("\\n4. Filtered data:")
if len(filtered_data) > 0:
    for idx, row in filtered_data.iterrows():
        spread = row['a_price'] - row['b_price']
        print(f"      {idx}: bid={row['b_price']:.2f}, ask={row['a_price']:.2f}, spread={spread:.2f}")
else:
    print("      ⚠️  No records remaining after filtering")

# Test with the actual problematic data file if it exists
print("\\n5. Testing with actual data file...")
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet"

if os.path.exists(data_path):
    print(f"   📁 Loading real data: {data_path}")
    real_data = pd.read_parquet(data_path)
    
    # Filter to only order data (has bid/ask)
    order_data = real_data[real_data['b_price'].notna() & real_data['a_price'].notna()]
    
    print(f"   📊 Order records in file: {len(order_data)}")
    
    if len(order_data) > 0:
        # Check for negative spreads
        negative_mask = order_data['a_price'] < order_data['b_price']
        negative_count = negative_mask.sum()
        
        print(f"   🚫 Negative spreads found: {negative_count} ({negative_count/len(order_data)*100:.1f}%)")
        
        if negative_count > 0:
            print("   🔍 Sample negative spreads:")
            sample = order_data[negative_mask].head(3)
            for idx, row in sample.iterrows():
                spread = row['a_price'] - row['b_price']
                print(f"      {idx}: bid={row['b_price']:.3f}, ask={row['a_price']:.3f}, spread={spread:.3f}")
            
            # Test validator on real data
            print("\\n   🧪 Testing validator on real data sample...")
            validator2 = BidAskValidator(strict_mode=True, log_filtered=True)
            sample_data = order_data.head(1000)  # Test on first 1000 records
            filtered_sample = validator2.validate_orders(sample_data, "RealData")
            
            stats2 = validator2.get_stats()
            print(f"   📊 Real data filter results: {stats2['filtered_count']}/{stats2['total_processed']} filtered ({stats2['filter_rate']:.1f}%)")
        else:
            print("   ✅ No negative spreads found in current data")
    else:
        print("   ⚠️  No order data found in file")
else:
    print(f"   ❌ Data file not found: {data_path}")

print("\\n✅ BidAskValidator testing completed!")