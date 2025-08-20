#!/usr/bin/env python3
"""
Test the pandas frequency fix for SpreadViewer tenor mapping
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime
from data_fetch_engine import calculate_synchronized_product_dates

print("🧪 TESTING PANDAS FREQUENCY FIX")
print("=" * 50)

# Test the exact scenario that failed
test_dates = pd.date_range('2025-06-24', '2025-07-01', freq='B')
tenors_list = ['q_1', 'q_1']  # This was causing the "Q_1S" invalid frequency error
tn1_list = [1, 1]
n_s = 3

print(f"📋 Test scenario (the one that failed):")
print(f"   📅 Dates: {test_dates[0]} to {test_dates[-1]} ({len(test_dates)} business days)")
print(f"   📊 Tenors: {tenors_list} (SpreadViewer format)")
print(f"   📊 Periods: {tn1_list}")
print(f"   🔧 n_s: {n_s}")

try:
    print(f"\n🔄 Running synchronized product_dates...")
    result = calculate_synchronized_product_dates(test_dates, tenors_list, tn1_list, n_s)
    
    print(f"\n✅ SUCCESS! No pandas frequency error")
    print(f"📊 Results: {len(result)} tenor/period combinations")
    
    for i, (tenor, tn) in enumerate(zip(tenors_list, tn1_list)):
        product_dates = result[i]
        if len(product_dates) > 0:
            sample_dates = product_dates[:min(3, len(product_dates))]
            print(f"   📊 {tenor}, period {tn}: {len(product_dates)} dates")
            print(f"      📅 Sample: {[d.strftime('%Y-%m-%d') for d in sample_dates]}")

except Exception as e:
    print(f"❌ STILL FAILING: {e}")
    import traceback
    traceback.print_exc()

print(f"\n🎯 If this works, SpreadViewer data generation should now succeed!")