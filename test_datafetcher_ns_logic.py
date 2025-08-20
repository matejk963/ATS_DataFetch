#!/usr/bin/env python3
"""
Test DataFetcher's actual n_s transition logic for June 26, 2025
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, timedelta
from data_fetch_engine import calculate_last_business_day, calculate_transition_dates

print("🔍 TESTING DATAFETCHER'S ACTUAL N_S LOGIC")
print("=" * 60)

# Test June 2025 (the problematic month)
test_date = datetime(2025, 6, 26)  # Thursday
n_s = 3

print(f"📋 Test Configuration:")
print(f"   📅 Test date: {test_date.date()} ({test_date.strftime('%A')})")
print(f"   📅 Month: June 2025")
print(f"   🔧 n_s: {n_s}")
print()

# Step 1: Calculate last business day of June 2025
print("🔧 STEP 1: Find last business day of June 2025")
last_bday_june = calculate_last_business_day(2025, 6)
print(f"   📅 Last business day of June 2025: {last_bday_june.date()} ({last_bday_june.strftime('%A')})")
print()

# Step 2: Calculate transition start point (last_bday - n_s + 1 business days)
print(f"🔧 STEP 2: Calculate transition start (last_bday - {n_s} + 1 business days)")
transition_start = last_bday_june

# DataFetcher's logic: for _ in range(n_s - 1):
print(f"   📅 Starting from: {transition_start.date()} ({transition_start.strftime('%A')})")

for i in range(n_s - 1):  # n_s - 1 = 2 steps backward
    print(f"   🔄 Step {i+1}: Moving back 1 business day...")
    
    # Move back one day
    transition_start -= timedelta(days=1)
    print(f"      📅 After -1 day: {transition_start.date()} ({transition_start.strftime('%A')})")
    
    # Skip weekends
    while transition_start.weekday() > 4:  # Skip weekends
        print(f"      ⚠️  Weekend detected, skipping...")
        transition_start -= timedelta(days=1)
        print(f"      📅 After skipping weekend: {transition_start.date()} ({transition_start.strftime('%A')})")

print(f"   ✅ Final transition start: {transition_start.date()} ({transition_start.strftime('%A')})")
print()

# Step 3: Check if our test date falls in transition period
print(f"🔧 STEP 3: Check if {test_date.date()} falls in transition period")
month_end = datetime(2025, 6, 30)  # Last day of June

print(f"   📊 Transition period: {transition_start.date()} to {month_end.date()}")
print(f"   📅 Test date: {test_date.date()}")

is_in_transition = transition_start <= test_date <= month_end
print(f"   ⚡ Is {test_date.date()} in transition period: {is_in_transition}")
print()

# Step 4: Use DataFetcher's calculate_transition_dates function
print("🔧 STEP 4: Use DataFetcher's calculate_transition_dates function")
periods = calculate_transition_dates(test_date, test_date, n_s)

print(f"   📊 Periods returned: {len(periods)}")
for i, (period_start, period_end, is_transition) in enumerate(periods):
    period_type = "TRANSITION" if is_transition else "NORMAL"
    print(f"   📋 Period {i+1}: {period_start.date()} to {period_end.date()} ({period_type})")

print()

# Summary
print("🎯 SUMMARY:")
print("=" * 60)
print(f"✅ Last business day of June 2025: {last_bday_june.date()}")
print(f"✅ Transition starts: {transition_start.date()} (last {n_s} business days)")
print(f"✅ Test date {test_date.date()} is {'IN' if is_in_transition else 'NOT IN'} transition period")

if is_in_transition:
    print(f"   📊 This means DataFetcher would use JULY 2025 (Q3) perspective:")
    print(f"      q_1 = Q3+1 = Q4 2025 (Oct-Dec)")
    print(f"      q_2 = Q3+2 = Q1 2026 (Jan-Mar)")
else:
    print(f"   📊 This means DataFetcher would use JUNE 2025 (Q2) perspective:")
    print(f"      q_1 = Q2+1 = Q3 2025 (Jul-Sep)")
    print(f"      q_2 = Q2+2 = Q4 2025 (Oct-Dec)")

print()
print("🔧 IMPLICATION FOR OUR FIX:")
print("=" * 60)
print("We need to update our synchronized function to match DataFetcher's exact logic:")
print("1. Find last business day of current period (month/quarter)")
print(f"2. Count backwards {n_s-1} business days to find transition start")
print("3. Check if reference date falls in [transition_start, period_end]")
print("4. If yes, use next period perspective; if no, use current period perspective")