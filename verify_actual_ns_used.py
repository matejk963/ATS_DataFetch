#!/usr/bin/env python3
"""
Verify the ACTUAL n_s value used in our synchronized function
Be completely honest about what was calculated
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, date, timedelta
import pandas as pd

print("🔍 VERIFYING ACTUAL N_S VALUE USED")
print("=" * 50)

# Test June 26, 2025 with different n_s values
test_date = date(2025, 6, 26)
print(f"📅 Test date: {test_date}")

# Calculate business days to Q2 end manually
q2_end = date(2025, 6, 30)  # Monday
business_days_remaining = 0
current_date = test_date

print(f"\n📊 Manual business day calculation:")
print(f"   From: {test_date} ({test_date.strftime('%A')})")
print(f"   To: {q2_end} ({q2_end.strftime('%A')})")

while current_date <= q2_end:
    weekday = current_date.weekday()
    is_business_day = weekday < 5
    
    if is_business_day:
        business_days_remaining += 1
        
    print(f"   {current_date} ({current_date.strftime('%A')}): {'✅ BUSINESS' if is_business_day else '❌ WEEKEND'}")
    current_date += timedelta(days=1)

business_days_remaining -= 1  # Don't count the reference date itself
print(f"\n📊 Business days remaining after {test_date}: {business_days_remaining}")

print(f"\n🔧 Testing transition logic with different n_s values:")

for n_s_test in [1, 2, 3, 4, 5]:
    in_transition = business_days_remaining <= n_s_test
    result = "IN TRANSITION (use Q3 perspective)" if in_transition else "NOT in transition (use Q2 perspective)"
    symbol = "✅" if in_transition else "❌"
    print(f"   n_s = {n_s_test}: {business_days_remaining} ≤ {n_s_test} = {in_transition} {symbol} {result}")

print(f"\n🤔 HONEST ANALYSIS:")
print("=" * 50)
print(f"June 26, 2025 has {business_days_remaining} business days remaining to Q2 end")
print(f"")
print(f"If n_s = 2: {business_days_remaining} ≤ 2 = {business_days_remaining <= 2} → {'TRANSITION' if business_days_remaining <= 2 else 'NO TRANSITION'}")
print(f"If n_s = 3: {business_days_remaining} ≤ 3 = {business_days_remaining <= 3} → {'TRANSITION' if business_days_remaining <= 3 else 'NO TRANSITION'}")

print(f"\n🚨 CONFESSION:")
print("=" * 50)
if business_days_remaining == 2:
    print(f"You're right to question this!")
    print(f"June 26 has exactly {business_days_remaining} business days remaining")
    print(f"With n_s=3, it SHOULD be in transition: 2 ≤ 3 = True")
    print(f"But if I mistakenly used n_s=2 logic, then: 2 ≤ 2 = True (edge case)")
    print(f"")
    print(f"The fact that you suspect n_s=2 suggests you might have spotted")
    print(f"an edge case in my calculation or configuration!")

# Now let's check what the data_fetch_engine.py actually has configured
print(f"\n📋 CHECKING ACTUAL CONFIGURATION:")
print("=" * 50)

try:
    # Read the actual config from data_fetch_engine.py
    with open('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines/data_fetch_engine.py', 'r') as f:
        content = f.read()
        
    # Look for n_s configuration
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if "'n_s':" in line or "n_s =" in line or "n_s=" in line:
            print(f"   Line {i+1}: {line.strip()}")

except Exception as e:
    print(f"❌ Error reading config: {e}")

print(f"\n🎯 THE TRUTH:")
print("=" * 50)
print(f"I need to be honest: if the price discrepancies still exist,")
print(f"it means either:")
print(f"1. My fix wasn't actually applied correctly")
print(f"2. I made an error in the n_s calculation") 
print(f"3. There's a different root cause I missed")
print(f"4. The configuration used a different n_s value than claimed")
print(f"")
print(f"Your suspicion about n_s=2 might be correct - let me investigate!")