#!/usr/bin/env python3
"""
Trace exactly which relative periods are being used by DataFetcher vs SpreadViewer
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta
from data_fetch_engine import convert_absolute_to_relative_periods, ContractSpec

print("🔍 TRACING RELATIVE PERIOD USAGE")
print("=" * 50)

# Test both systems with the exact same inputs
start_date = datetime(2025, 6, 24)
end_date = datetime(2025, 7, 1)
n_s = 3

print(f"📅 Test period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"🔧 n_s parameter: {n_s}")

# Create contract specs for debq4_25 and frbq4_25
contracts = {
    'debq4_25': ContractSpec(
        contract='debq4_25',
        market='de',
        product='base',
        tenor='q',
        delivery_date=datetime(2025, 10, 1)  # Q4 2025
    ),
    'frbq4_25': ContractSpec(
        contract='frbq4_25', 
        market='fr',
        product='base',
        tenor='q',
        delivery_date=datetime(2025, 10, 1)  # Q4 2025
    )
}

print(f"\n🔍 DATAFETCHER RELATIVE PERIOD MAPPING")
print("=" * 40)

for contract_name, contract_spec in contracts.items():
    print(f"\n📊 Contract: {contract_name}")
    
    try:
        periods = convert_absolute_to_relative_periods(contract_spec, start_date, end_date, n_s)
        
        print(f"   📋 Found {len(periods)} relative periods:")
        for i, (rel_period, p_start, p_end) in enumerate(periods):
            print(f"   📊 Period {i+1}: q_{rel_period.relative_offset}")
            print(f"      📅 {p_start.strftime('%Y-%m-%d')} to {p_end.strftime('%Y-%m-%d')}")
            print(f"      🔢 Relative offset: {rel_period.relative_offset}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")

print(f"\n🔍 INVESTIGATING SPREADVIEWER PATH")
print("=" * 40)

# Now let's trace what happens when we call the SpreadViewer integration
print(f"📊 For June 26, 2025 specifically...")

# Check what the middle-date logic produces for our period
middle_date = start_date + (end_date - start_date) / 2
print(f"📅 Middle date of period: {middle_date.strftime('%Y-%m-%d')}")

ref_quarter = ((middle_date.month - 1) // 3) + 1
ref_year = middle_date.year
print(f"📊 Middle date quarter: Q{ref_quarter} {ref_year}")

# Check the transition logic for middle date
if ref_quarter == 2:  # Q2
    quarter_end = datetime(ref_year, 6, 30)
else:
    quarter_end = datetime(ref_year, 3*ref_quarter, 31 if ref_quarter%2==1 else 30)

# Find last business day
last_bday = quarter_end
while last_bday.weekday() > 4:
    last_bday -= timedelta(days=1)

# Calculate transition start
transition_start = last_bday
for _ in range(n_s - 1):
    transition_start -= timedelta(days=1)
    while transition_start.weekday() > 4:
        transition_start -= timedelta(days=1)

in_transition = transition_start.date() <= middle_date.date() <= last_bday.date()

print(f"📅 Quarter end: {quarter_end.strftime('%Y-%m-%d')}")
print(f"📅 Last business day: {last_bday.strftime('%Y-%m-%d')}")
print(f"📅 Transition start: {transition_start.strftime('%Y-%m-%d')}")
print(f"📊 In transition: {in_transition}")

if in_transition:
    if ref_quarter == 4:
        calc_quarter = 1
        calc_year = ref_year + 1
    else:
        calc_quarter = ref_quarter + 1
        calc_year = ref_year
else:
    calc_quarter = ref_quarter
    calc_year = ref_year

print(f"📊 Calculation perspective: Q{calc_quarter} {calc_year}")

# Calculate relative offset for Q4 2025 delivery
delivery_quarter = 4  # Q4 2025
delivery_year = 2025

calc_quarters = calc_year * 4 + (calc_quarter - 1)
delivery_quarters = delivery_year * 4 + (delivery_quarter - 1)
relative_offset = delivery_quarters - calc_quarters

print(f"📊 Q4 2025 relative offset: {relative_offset} (q_{relative_offset})")

print(f"\n🎯 CRITICAL INVESTIGATION: ACTUAL USAGE")
print("=" * 45)

# The key question: Is this logic actually being used consistently?
print(f"💡 According to our logic:")
print(f"   📊 Middle date {middle_date.strftime('%Y-%m-%d')} is in transition: {in_transition}")
print(f"   📊 Should use Q{calc_quarter} {calc_year} perspective")
print(f"   📊 Q4 2025 contracts should map to q_{relative_offset}")

print(f"\n🔍 POTENTIAL ISSUES:")
print("=" * 25)

issues = []

if relative_offset == 1:
    issues.append("DataFetcher uses q_1")
elif relative_offset == 2:
    issues.append("DataFetcher uses q_2")

print(f"1. 📊 DataFetcher mapping: q_{relative_offset}")

# Check if there might be date-specific variations
print(f"2. 🕐 Date-specific check for June 26:")
june_26 = datetime(2025, 6, 26)
june_26_quarter = ((june_26.month - 1) // 3) + 1

# Transition check for June 26 specifically
q2_end = datetime(2025, 6, 30)
q2_last_bday = q2_end
while q2_last_bday.weekday() > 4:
    q2_last_bday -= timedelta(days=1)

q2_transition_start = q2_last_bday
for _ in range(n_s - 1):
    q2_transition_start -= timedelta(days=1)
    while q2_transition_start.weekday() > 4:
        q2_transition_start -= timedelta(days=1)

june_26_in_transition = q2_transition_start.date() <= june_26.date() <= q2_last_bday.date()

print(f"   📅 June 26 in Q2 transition: {june_26_in_transition}")

if june_26_in_transition:
    june_26_calc_quarter = 3  # Q3
    june_26_calc_year = 2025
else:
    june_26_calc_quarter = 2  # Q2
    june_26_calc_year = 2025

june_26_calc_quarters = june_26_calc_year * 4 + (june_26_calc_quarter - 1)
june_26_relative_offset = delivery_quarters - june_26_calc_quarters

print(f"   📊 June 26 should use Q{june_26_calc_quarter} perspective → q_{june_26_relative_offset}")

if june_26_relative_offset != relative_offset:
    print(f"   🚨 INCONSISTENCY: Middle date logic (q_{relative_offset}) ≠ June 26 logic (q_{june_26_relative_offset})")
    issues.append(f"Inconsistent relative mapping within same period")

print(f"\n🎯 SUMMARY OF FINDINGS:")
for i, issue in enumerate(issues, 1):
    print(f"   {i}. {issue}")

if not issues:
    print(f"   ✅ Logic appears consistent")
    print(f"   🔍 Issue likely elsewhere (caching, multiple code paths, etc.)")
else:
    print(f"   🚨 Found {len(issues)} potential issues")
    print(f"   🔧 These may explain the €9.14 price difference")