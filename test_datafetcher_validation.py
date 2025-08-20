#!/usr/bin/env python3
"""
Test script to verify BidAskValidator integration in DataFetcher
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
import tempfile

# Setup paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

# Import the updated DataFetcher
from core.data_fetcher import DataFetcher, BidAskValidator

print("🧪 Testing DataFetcher BidAskValidator Integration")
print("=" * 50)

# Test 1: Direct BidAskValidator test
print("1. Testing BidAskValidator class...")

# Create test data with negative spreads
test_data = pd.DataFrame({
    'price': [30.0, 31.5, 35.0, 32.0, 28.0],  # trade prices
    'volume': [100, 200, 150, 300, 120],
    'b_price': [30.0, 32.0, 35.0, 33.15, 28.0],  # bid prices
    'a_price': [30.5, 31.8, 36.0, 30.0, 28.5],  # ask prices (one negative spread)
    'other_col': ['A', 'B', 'C', 'D', 'E']
})

test_data.index = pd.date_range('2025-07-01 10:00', periods=5, freq='1h')

print(f"   📊 Created test data: {len(test_data)} records")
print("   🔍 Spread analysis:")
for idx, row in test_data.iterrows():
    spread = row['a_price'] - row['b_price']
    status = "❌ NEGATIVE" if spread < 0 else "✅ positive" 
    print(f"      {idx}: bid={row['b_price']:.2f}, ask={row['a_price']:.2f}, spread={spread:.2f} {status}")

# Test the validator directly
validator = BidAskValidator(strict_mode=True, log_filtered=True)
filtered_data = validator.validate_merged_data(test_data, "TestData")

print(f"\\n   📈 Original records: {len(test_data)}")
print(f"   📉 Filtered records: {len(filtered_data)}")
print(f"   🚫 Removed records: {len(test_data) - len(filtered_data)}")

stats = validator.get_stats()
print(f"   📊 Filter rate: {stats['filter_rate']:.1f}%")

# Test 2: DataFetcher export_to_parquet method
print("\\n2. Testing DataFetcher export_to_parquet method...")

# Create mock contract data format
contract_data = {
    'debm09_25_frbm09_25': {
        'orders': test_data[['b_price', 'a_price', 'volume']].copy(),
        'trades': test_data[['price', 'volume']].copy()
    }
}

print(f"   📊 Mock contract data created: {list(contract_data.keys())}")

# Create temporary output directory
with tempfile.TemporaryDirectory() as temp_dir:
    print(f"   📁 Using temp directory: {temp_dir}")
    
    # Test DataFetcher export with validation
    fetcher = DataFetcher()
    
    try:
        print("   🚀 Calling export_to_parquet...")
        fetcher.export_to_parquet(contract_data, temp_dir)
        
        # Check if file was created
        expected_file = os.path.join(temp_dir, 'debm09_25_frbm09_25_tr_ba_data.parquet')
        if os.path.exists(expected_file):
            print(f"   ✅ File created: {os.path.basename(expected_file)}")
            
            # Load and verify the data
            saved_data = pd.read_parquet(expected_file)
            print(f"   📊 Saved data: {len(saved_data)} records")
            
            # Check for negative spreads in saved data
            if 'b_price' in saved_data.columns and 'a_price' in saved_data.columns:
                has_both = saved_data['b_price'].notna() & saved_data['a_price'].notna()
                valid_records = saved_data[has_both]
                
                if len(valid_records) > 0:
                    negative_spreads = (valid_records['a_price'] < valid_records['b_price']).sum()
                    print(f"   🔍 Negative spreads in saved data: {negative_spreads}")
                    
                    if negative_spreads == 0:
                        print("   ✅ SUCCESS: No negative spreads in saved data!")
                    else:
                        print(f"   ❌ FAILED: {negative_spreads} negative spreads remain in saved data")
                else:
                    print("   ⚠️  No bid/ask data found in saved file")
            else:
                print("   ⚠️  No bid/ask columns found in saved file")
                print(f"   📋 Available columns: {list(saved_data.columns)}")
        else:
            print(f"   ❌ File not created: {expected_file}")
    
    except Exception as e:
        print(f"   ❌ Error in export_to_parquet: {e}")
        import traceback
        traceback.print_exc()

print("\\n✅ DataFetcher validation integration testing completed!")