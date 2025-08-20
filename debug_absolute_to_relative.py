#!/usr/bin/env python3
"""
Debug the absolute to relative conversion that happens BEFORE our n_s fix
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, date
from data_fetch_engine import convert_absolute_to_relative_periods, parse_absolute_contract

print("🔍 DEBUGGING ABSOLUTE TO RELATIVE CONVERSION")
print("=" * 60)

# Test the exact scenario from the refetched data
start_date = datetime(2025, 6, 24)  # Period start
end_date = datetime(2025, 7, 5)     # Period end  
n_s = 3

# Parse the contracts
contract1 = parse_absolute_contract('debq4_25')  # German base Q4 2025
contract2 = parse_absolute_contract('frbq4_25')  # French base Q4 2025

print(f"📋 Test Configuration:")
print(f"   📅 Period: {start_date.date()} to {end_date.date()}")
print(f"   🔧 n_s: {n_s}")
print(f"   📊 Contract 1: {contract1.market}{contract1.product[0]}{contract1.tenor}{contract1.contract}")
print(f"      📦 Delivery: {contract1.delivery_date.date()} (Q4 2025)")
print(f"   📊 Contract 2: {contract2.market}{contract2.product[0]}{contract2.tenor}{contract2.contract}")
print(f"      📦 Delivery: {contract2.delivery_date.date()} (Q4 2025)")
print()

print(f"🔄 CONVERTING CONTRACTS TO RELATIVE PERIODS:")
print("=" * 60)

# Convert contracts to relative periods
print(f"📊 Converting debq4_25 to relative periods...")
periods1 = convert_absolute_to_relative_periods(contract1, start_date, end_date, n_s)

print(f"\n📊 Converting frbq4_25 to relative periods...")
periods2 = convert_absolute_to_relative_periods(contract2, start_date, end_date, n_s)

print(f"\n✅ RELATIVE PERIOD RESULTS:")
print("=" * 60)

print(f"Contract debq4_25 (Q4 2025) maps to:")
for i, (rel_period, p_start, p_end) in enumerate(periods1):
    print(f"   📊 Period {i+1}: Relative offset {rel_period.relative_offset} ({p_start.date()} to {p_end.date()})")

print(f"\nContract frbq4_25 (Q4 2025) maps to:")
for i, (rel_period, p_start, p_end) in enumerate(periods2):
    print(f"   📊 Period {i+1}: Relative offset {rel_period.relative_offset} ({p_start.date()} to {p_end.date()})")

print(f"\n🤔 ANALYSIS:")
print("=" * 60)
print(f"Both contracts are Q4 2025 contracts (October delivery)")
print(f"For June 26, 2025 (in Q2 transition period):")
print(f"   - Expected: Both should map to q_1 (Q3+1 = Q4 2025)")
print(f"   - If one maps to q_2: That would query Q1 2026 contracts instead!")

print(f"\n🚨 ROOT CAUSE HYPOTHESIS:")
print("=" * 60)
print(f"The convert_absolute_to_relative_periods function might be:")
print(f"1. Using monthly transition logic instead of quarterly")
print(f"2. Calculating different relative periods for the same absolute contract")
print(f"3. Not properly handling the Q2→Q3 transition")
print(f"4. Creating different mappings for debq4_25 vs frbq4_25")

print(f"\n🎯 SOLUTION:")
print("=" * 60)
print(f"We need to ensure convert_absolute_to_relative_periods:")
print(f"1. Uses quarterly transition logic for quarterly contracts")
print(f"2. Maps both debq4_25 and frbq4_25 to the same relative period (q_1)")
print(f"3. Uses the same n_s transition logic as our synchronized function")