#!/usr/bin/env python3
"""
Test the complete n_s synchronization fix addressing the SpreadViewer product_dates discrepancy
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, date
import pandas as pd
from data_fetch_engine import (
    calculate_synchronized_product_dates, 
    convert_absolute_to_relative_periods,
    parse_absolute_contract
)

print("🧪 TESTING COMPLETE N_S SYNCHRONIZATION FIX")
print("=" * 70)

# Test the exact scenario from the refetched data
start_date = datetime(2025, 6, 24)
end_date = datetime(2025, 7, 5)
n_s = 3

print(f"📋 Test Configuration:")
print(f"   📅 Period: {start_date.date()} to {end_date.date()}")
print(f"   🔧 n_s: {n_s}")
print()

print(f"🔧 STEP 1: Testing convert_absolute_to_relative_periods fix")
print("=" * 70)

# Parse contracts
contract1 = parse_absolute_contract('debq4_25')
contract2 = parse_absolute_contract('frbq4_25')

print(f"Testing contract mapping consistency...")

periods1 = convert_absolute_to_relative_periods(contract1, start_date, end_date, n_s)
print()
periods2 = convert_absolute_to_relative_periods(contract2, start_date, end_date, n_s)

print(f"\n✅ FIXED RELATIVE PERIOD MAPPING:")
print(f"Contract debq4_25: {len(periods1)} period(s)")
for i, (rel_period, p_start, p_end) in enumerate(periods1):
    print(f"   📊 Period {i+1}: q_{rel_period.relative_offset} ({p_start.date()} to {p_end.date()})")

print(f"\nContract frbq4_25: {len(periods2)} period(s)")
for i, (rel_period, p_start, p_end) in enumerate(periods2):
    print(f"   📊 Period {i+1}: q_{rel_period.relative_offset} ({p_start.date()} to {p_end.date()})")

print(f"\n🔧 STEP 2: Testing synchronized product_dates function")
print("=" * 70)

# Create test dates for the critical period
test_dates = pd.date_range('2025-06-26', '2025-06-27', freq='B')
tenors_list = ['q', 'q']
tn1_list = [1, 1]  # Both should map to q_1 now

print(f"Testing synchronized product_dates calculation...")
result = calculate_synchronized_product_dates(test_dates, tenors_list, tn1_list, n_s)

print(f"\n✅ SYNCHRONIZED PRODUCT DATES RESULT:")
for i, (tenor, tn) in enumerate(zip(tenors_list, tn1_list)):
    product_dates = result[i]
    print(f"   📊 Tenor {tenor}, period {tn}: {product_dates}")
    
    if len(product_dates) > 0:
        delivery_date = product_dates[0]
        quarter = ((delivery_date.month - 1) // 3) + 1
        print(f"      📦 Delivery: {delivery_date.strftime('%Y-%m-%d')} (Q{quarter} {delivery_date.year})")

print(f"\n🎯 CONSISTENCY CHECK:")
print("=" * 70)

# Verify both contracts now map to same relative period
if len(periods1) > 0 and len(periods2) > 0:
    offset1 = periods1[0][0].relative_offset
    offset2 = periods2[0][0].relative_offset
    
    print(f"Contract debq4_25 maps to: q_{offset1}")
    print(f"Contract frbq4_25 maps to: q_{offset2}")
    
    if offset1 == offset2:
        print(f"✅ CONSISTENT: Both contracts map to same relative period!")
        print(f"   This means both DataFetcher and SpreadViewer should query")
        print(f"   the same underlying quarterly contracts.")
    else:
        print(f"❌ INCONSISTENT: Contracts still map to different periods!")
        print(f"   This would still cause price discrepancies.")

print(f"\n🔍 EXPECTED BEHAVIOR:")
print("=" * 70)
print(f"For the period June 24 - July 5, 2025 with n_s=3:")
print(f"")
print(f"Both debq4_25 and frbq4_25 (Q4 2025 contracts) should:")
print(f"1. Map to the same relative period (q_1)")
print(f"2. Query the same quarterly contracts")
print(f"3. Result in consistent pricing between DataFetcher and SpreadViewer")
print(f"")
print(f"The middle date (June 29) is in Q2 2025 transition period,")
print(f"so both systems should use Q3 2025 perspective:")
print(f"   q_1 = Q3+1 = Q4 2025 → October 2025 delivery")

print(f"\n🚨 IF THIS FIX WORKS:")
print("=" * 70)
print(f"✅ Price discrepancies on June 26-27 should be eliminated")
print(f"✅ Both systems should show similar price ranges (€19-22 instead of €19-33)")
print(f"✅ No more mixing of Q1 2026 and Q4 2025 contract data")
print(f"✅ Consistent spread calculations across all transition periods")

print(f"\n🔄 Next step: Refetch data and verify price consistency!")