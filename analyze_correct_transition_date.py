#!/usr/bin/env python3
"""
Analyze the correct transition date based on the plot observations
User observation: dates before June 26 (included) should have q_2 as relative period
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta

print("🔍 ANALYZING CORRECT TRANSITION DATE")
print("=" * 45)

print("👤 User Observation: Dates before June 26 (included) should have q_2")
print("🤔 This means transition from q_2 to q_1 happens AFTER June 26")

# Test period: June 24 - July 1, 2025
start_date = datetime(2025, 6, 24)  
end_date = datetime(2025, 7, 1)
n_s = 3

# Critical dates to analyze
june_24 = datetime(2025, 6, 24)
june_25 = datetime(2025, 6, 25) 
june_26 = datetime(2025, 6, 26)
june_27 = datetime(2025, 6, 27)
june_30 = datetime(2025, 6, 30)  # Q2 end
july_01 = datetime(2025, 7, 1)

print(f"\n📅 CRITICAL DATES ANALYSIS:")
print("=" * 35)

# Q2 2025 ends June 30, 2025 (Monday)
q2_end = datetime(2025, 6, 30)
print(f"📅 Q2 2025 ends: {q2_end.strftime('%Y-%m-%d')} ({q2_end.strftime('%A')})")

# Find last business day of Q2
last_bday = q2_end
while last_bday.weekday() > 4:  # Skip weekends
    last_bday -= timedelta(days=1)
print(f"📅 Last business day of Q2: {last_bday.strftime('%Y-%m-%d')} ({last_bday.strftime('%A')})")

# Calculate n_s business days from end
print(f"\n🔢 BUSINESS DAY COUNTDOWN FROM Q2 END:")
current = last_bday
business_day_count = 0
dates_in_transition = []

print(f"   📅 Starting from: {current.strftime('%Y-%m-%d')} (last business day)")

for i in range(n_s):
    dates_in_transition.append(current)
    business_day_count += 1
    print(f"   📅 Business day -{business_day_count}: {current.strftime('%Y-%m-%d')} ({current.strftime('%A')})")
    
    # Go back one business day
    current -= timedelta(days=1)
    while current.weekday() > 4:  # Skip weekends
        current -= timedelta(days=1)

transition_start = current + timedelta(days=1)
while transition_start.weekday() > 4:
    transition_start += timedelta(days=1)

print(f"\n🎯 CALCULATED TRANSITION PERIOD:")
print(f"   📅 Transition starts: {transition_start.strftime('%Y-%m-%d')} ({transition_start.strftime('%A')})")
print(f"   📅 Transition ends: {last_bday.strftime('%Y-%m-%d')} ({last_bday.strftime('%A')})")
print(f"   🔢 Duration: {n_s} business days")

print(f"\n📊 DATE-BY-DATE ANALYSIS:")
print("=" * 35)

test_dates = [june_24, june_25, june_26, june_27, june_30, july_01]

for test_date in test_dates:
    # Check if date is in transition
    in_transition = transition_start.date() <= test_date.date() <= last_bday.date()
    
    business_days_to_end = 0
    temp_date = test_date
    while temp_date.date() <= last_bday.date():
        if temp_date.weekday() < 5:  # Business day
            business_days_to_end += 1
        temp_date += timedelta(days=1)
    
    # According to DataFetcher logic: if in last n_s business days, use NEXT quarter (q_1)
    # Otherwise use current quarter perspective
    if in_transition:
        expected_period = "q_1"  # Next quarter perspective (Q3 → Q4 = q_1)
        reason = f"In transition ({business_days_to_end} bus days to Q2 end ≤ {n_s})"
    else:
        expected_period = "q_2"  # Current quarter perspective (Q2 → Q4 = q_2)  
        reason = f"Not in transition ({business_days_to_end} bus days to Q2 end > {n_s})"
    
    print(f"📅 {test_date.strftime('%Y-%m-%d')}: Expected {expected_period} - {reason}")

print(f"\n🔍 USER OBSERVATION VALIDATION:")
print("=" * 40)

user_says_q2_dates = [june_24, june_25, june_26]
print(f"👤 User says these should be q_2: {[d.strftime('%Y-%m-%d') for d in user_says_q2_dates]}")

print(f"\n📊 CHECKING AGAINST CALCULATED LOGIC:")
for test_date in user_says_q2_dates:
    in_transition = transition_start.date() <= test_date.date() <= last_bday.date()
    calculated_period = "q_1" if in_transition else "q_2"
    
    if calculated_period == "q_2":
        print(f"✅ {test_date.strftime('%Y-%m-%d')}: Calculated {calculated_period} ✓ matches user observation")
    else:
        print(f"❌ {test_date.strftime('%Y-%m-%d')}: Calculated {calculated_period} ✗ user says q_2")

print(f"\n💡 ANALYSIS CONCLUSION:")
print("=" * 25)

if any(transition_start.date() <= d.date() <= last_bday.date() for d in user_says_q2_dates):
    print("❌ MISMATCH: Current transition logic disagrees with user observation")
    print("🔧 PROBLEM: Our n_s transition calculation may be wrong")
    print("🤔 POSSIBILITY: The transition might happen later than calculated")
    
    print(f"\n🛠️  SUGGESTED FIX:")
    print("   1. Check if n_s should be interpreted differently") 
    print("   2. Verify the business day counting logic")
    print("   3. Consider if transition happens at different point in quarter")
    
    # Alternative calculation: what if transition happens in the LAST n_s days of quarter?
    print(f"\n🔄 ALTERNATIVE INTERPRETATION:")
    print("   What if 'last n_s business days' means the very final n_s days?")
    
    # Count backwards from absolute end
    alt_transition_start = last_bday
    for i in range(n_s - 1):
        alt_transition_start -= timedelta(days=1)
        while alt_transition_start.weekday() > 4:
            alt_transition_start -= timedelta(days=1)
    
    print(f"   📅 Alternative transition: {alt_transition_start.strftime('%Y-%m-%d')} to {last_bday.strftime('%Y-%m-%d')}")
    
    for test_date in user_says_q2_dates:
        alt_in_transition = alt_transition_start.date() <= test_date.date() <= last_bday.date()
        alt_period = "q_1" if alt_in_transition else "q_2"
        print(f"   📅 {test_date.strftime('%Y-%m-%d')}: Alternative gives {alt_period}")

else:
    print("✅ MATCH: Transition logic aligns with user observation")
    print("✅ June 24-26 should indeed use q_2 (current quarter perspective)")
    print("✅ July 1+ should use q_1 (next quarter perspective)")