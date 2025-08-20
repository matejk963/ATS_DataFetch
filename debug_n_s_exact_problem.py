#!/usr/bin/env python3
"""
Debug the exact n_s problem for June 26, 2025 case
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta
from data_fetch_engine import calculate_synchronized_product_dates

print("🔍 DEBUGGING EXACT N_S PROBLEM - June 26, 2025")
print("=" * 60)

# Test the exact scenario from June 26, 2025
test_date = datetime(2025, 6, 26)
test_dates = pd.date_range('2025-06-26', '2025-06-26', freq='B')  # Single day

# Q4 2025 contracts (debq4_25, frbq4_25) should map to relative periods
print(f"📅 Test date: {test_date.strftime('%Y-%m-%d')} (%s)" % test_date.strftime('%A'))
print(f"📊 This is Q2 2025, day {test_date.timetuple().tm_yday} of year")

# Calculate quarter info
ref_quarter = ((test_date.month - 1) // 3) + 1
ref_year = test_date.year
print(f"📊 Reference quarter: Q{ref_quarter} {ref_year}")

# Find last business day of Q2 2025
quarter_end = datetime(2025, 6, 30)  # June 30, 2025
last_bday = quarter_end
while last_bday.weekday() > 4:  # Find last business day
    last_bday -= timedelta(days=1)

print(f"📅 Q2 2025 ends: {quarter_end.strftime('%Y-%m-%d')} (%s)" % quarter_end.strftime('%A'))
print(f"📅 Last business day of Q2: {last_bday.strftime('%Y-%m-%d')} (%s)" % last_bday.strftime('%A'))

# Calculate business days from June 26 to end of quarter
business_days = 0
current = test_date
while current <= last_bday:
    if current.weekday() < 5:  # Monday = 0, Sunday = 6
        business_days += 1
    current += timedelta(days=1)

print(f"📊 Business days from June 26 to Q2 end: {business_days}")
print(f"📊 n_s threshold: 3")
print(f"📊 In transition? {business_days <= 3}")

print("\n" + "=" * 60)
print("🔧 DATAFETCHER vs SPREADVIEWER LOGIC COMPARISON")
print("=" * 60)

# DataFetcher logic: If <= n_s business days to quarter end, use NEXT quarter perspective
if business_days <= 3:
    datafetcher_perspective = "Q3 2025"  # Next quarter
    print(f"🟢 DataFetcher: {business_days} <= 3, use NEXT quarter perspective: {datafetcher_perspective}")
else:
    datafetcher_perspective = "Q2 2025"  # Current quarter  
    print(f"🟢 DataFetcher: {business_days} > 3, use CURRENT quarter perspective: {datafetcher_perspective}")

# For Q4 2025 contracts from Q3 2025 perspective
if datafetcher_perspective == "Q3 2025":
    # Q4 2025 delivery from Q3 2025 perspective = 1 quarter ahead = q_1
    datafetcher_relative = "q_1"
else:
    # Q4 2025 delivery from Q2 2025 perspective = 2 quarters ahead = q_2
    datafetcher_relative = "q_2"

print(f"🟢 DataFetcher maps debq4_25/frbq4_25 to: {datafetcher_relative}")

# SpreadViewer logic: Forward shift approach
# (dates + n_s * dates.freq).shift(tn, freq='QS')
# This adds 3 business days forward, then shifts

print(f"\n🔵 SpreadViewer original logic:")
print(f"   📅 June 26 + 3 business days = ?")

forward_date = test_date
for _ in range(3):
    forward_date += timedelta(days=1)
    while forward_date.weekday() > 4:  # Skip weekends
        forward_date += timedelta(days=1)

print(f"   📅 June 26 + 3 business days = {forward_date.strftime('%Y-%m-%d')} (%s)" % forward_date.strftime('%A'))

forward_quarter = ((forward_date.month - 1) // 3) + 1
forward_year = forward_date.year
print(f"   📊 Forward date is in: Q{forward_quarter} {forward_year}")

# For Q4 2025 contracts from this forward perspective
if forward_quarter == 3 and forward_year == 2025:
    spreadviewer_relative = "q_1"  # Q4 from Q3 perspective
elif forward_quarter == 2 and forward_year == 2025:
    spreadviewer_relative = "q_2"  # Q4 from Q2 perspective
else:
    spreadviewer_relative = "q_?"

print(f"🔵 SpreadViewer maps debq4_25/frbq4_25 to: {spreadviewer_relative}")

print(f"\n🎯 SYNCHRONIZATION STATUS:")
if datafetcher_relative == spreadviewer_relative:
    print(f"✅ SYNCHRONIZED: Both use {datafetcher_relative}")
else:
    print(f"❌ MISMATCH: DataFetcher={datafetcher_relative}, SpreadViewer={spreadviewer_relative}")
    print(f"   This explains the €12.64 price discrepancy!")

print(f"\n🔧 Testing our synchronized function...")
try:
    tenors_list = ['q_1']  # Try the DataFetcher mapping
    tn1_list = [1]
    result = calculate_synchronized_product_dates(test_dates, tenors_list, tn1_list, n_s=3)
    print(f"✅ Synchronized function works for q_1")
    
    tenors_list = ['q_2']  # Try the alternative mapping
    tn1_list = [2] 
    result = calculate_synchronized_product_dates(test_dates, tenors_list, tn1_list, n_s=3)
    print(f"✅ Synchronized function works for q_2")
    
except Exception as e:
    print(f"❌ Synchronized function error: {e}")

print(f"\n🎯 NEXT STEPS:")
print(f"1. Verify which relative period SpreadViewer is actually using for June 26")
print(f"2. Ensure our synchronized function forces the correct DataFetcher mapping")
print(f"3. Test with actual data fetch to confirm price synchronization")