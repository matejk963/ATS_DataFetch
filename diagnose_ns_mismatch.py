#!/usr/bin/env python3
"""
Diagnostic script to check for n_s parameter mismatch between DataFetcher and SpreadViewer
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/src')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np

print("🔍 DIAGNOSING N_S PARAMETER MISMATCH")
print("=" * 60)

# Test the critical date - June 26, 2025
test_date = date(2025, 6, 26)
contracts = ['debq4_25', 'frbq4_25']
n_s = 3

print(f"📋 Test Configuration:")
print(f"   📅 Critical date: {test_date} (Thursday)")
print(f"   📊 Contracts: {contracts}")
print(f"   🔧 n_s parameter: {n_s}")
print(f"   ⚠️  This date showed €20-€33 price spikes (100% synthetic)")
print()

def parse_contract_info(contract_name):
    """Parse contract to extract market, tenor, delivery info"""
    # Format: debq4_25 = German base Q4 2025
    parts = contract_name.split('_')
    market_product = parts[0]  # e.g., 'debq4'
    year_suffix = parts[1]     # e.g., '25'
    
    market = market_product[:2]  # 'de'
    product = market_product[2:3]  # 'b' 
    tenor = market_product[3:4]   # 'q'
    period_num = int(market_product[4:])  # 4
    
    year = 2000 + int(year_suffix)  # 2025
    
    # Calculate delivery date based on quarter
    if tenor == 'q':
        delivery_month = (period_num - 1) * 3 + 1  # Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct
        delivery_date = datetime(year, delivery_month, 1).date()
    else:
        delivery_date = datetime(year, period_num, 1).date()  # For monthly
    
    return {
        'market': market,
        'product': product,
        'tenor': tenor,
        'period_num': period_num,
        'year': year,
        'delivery_date': delivery_date,
        'contract': contract_name
    }

def calculate_relative_period_datafetcher_style(reference_date, contract_info, n_s):
    """Calculate relative period using DataFetcher-style logic"""
    
    # Get the quarter of the reference date
    ref_month = reference_date.month
    ref_quarter = ((ref_month - 1) // 3) + 1
    ref_year = reference_date.year
    
    # Get target contract quarter info
    target_quarter = contract_info['period_num']
    target_year = contract_info['year']
    
    # Calculate quarter difference (corrected logic)
    ref_quarters = ref_year * 4 + (ref_quarter - 1)
    target_quarters = target_year * 4 + (target_quarter - 1)
    relative_quarters = target_quarters - ref_quarters
    
    return {
        'relative_period': f"q_{relative_quarters}",
        'reference_quarter': f"Q{ref_quarter} {ref_year}",
        'target_quarter': f"Q{target_quarter} {target_year}",
        'quarters_difference': relative_quarters,
        'method': 'DataFetcher-style'
    }

def calculate_business_days_to_period_end(reference_date, reference_quarter, reference_year):
    """Calculate business days from reference date to end of its quarter"""
    
    # Find end of quarter
    if reference_quarter == 1:
        quarter_end = date(reference_year, 3, 31)
    elif reference_quarter == 2:
        quarter_end = date(reference_year, 6, 30)
    elif reference_quarter == 3:
        quarter_end = date(reference_year, 9, 30)
    else:  # Q4
        quarter_end = date(reference_year, 12, 31)
    
    # Count business days
    business_days = 0
    current_date = reference_date
    
    while current_date <= quarter_end:
        if current_date.weekday() < 5:  # Monday=0, Friday=4
            business_days += 1
        current_date += timedelta(days=1)
    
    return business_days - 1  # Don't count the reference date itself

def calculate_relative_period_with_ns_transition(reference_date, contract_info, n_s):
    """Calculate relative period considering n_s transition logic"""
    
    # Get base calculation
    base_calc = calculate_relative_period_datafetcher_style(reference_date, contract_info, n_s)
    
    # Extract reference quarter info
    ref_month = reference_date.month
    ref_quarter = ((ref_month - 1) // 3) + 1
    ref_year = reference_date.year
    
    # Check if we're in transition period (within n_s business days of quarter end)
    business_days_to_end = calculate_business_days_to_period_end(reference_date, ref_quarter, ref_year)
    
    print(f"   📅 Reference date: {reference_date} (Q{ref_quarter} {ref_year})")
    print(f"   📊 Business days to Q{ref_quarter} end: {business_days_to_end}")
    print(f"   🔧 n_s transition threshold: {n_s} business days")
    
    # Determine if we should transition
    in_transition = business_days_to_end <= n_s
    
    if in_transition:
        # We should be looking at NEXT quarter's perspective
        if ref_quarter == 4:
            next_quarter = 1
            next_year = ref_year + 1
        else:
            next_quarter = ref_quarter + 1
            next_year = ref_year
            
        print(f"   ⚡ IN TRANSITION: Should use Q{next_quarter} {next_year} perspective")
        
        # Recalculate from next quarter's perspective
        next_ref_quarters = next_year * 4 + (next_quarter - 1)
        target_quarters = contract_info['year'] * 4 + (contract_info['period_num'] - 1)
        adjusted_relative = target_quarters - next_ref_quarters
        
        return {
            'relative_period': f"q_{adjusted_relative}",
            'reference_quarter': f"Q{next_quarter} {next_year} (transitioned from Q{ref_quarter} {ref_year})",
            'target_quarter': f"Q{contract_info['period_num']} {contract_info['year']}",
            'quarters_difference': adjusted_relative,
            'method': 'With n_s transition',
            'in_transition': True,
            'business_days_to_end': business_days_to_end
        }
    else:
        print(f"   ✅ NO TRANSITION: Stay with Q{ref_quarter} {ref_year} perspective")
        return {
            **base_calc,
            'method': 'With n_s transition',
            'in_transition': False,
            'business_days_to_end': business_days_to_end
        }

# Run the diagnostic
print(f"🔍 DIAGNOSTIC ANALYSIS FOR {test_date}:")
print("=" * 60)

for contract in contracts:
    print(f"\n📊 ANALYZING CONTRACT: {contract}")
    
    # Parse contract
    contract_info = parse_contract_info(contract)
    print(f"   📋 Parsed: {contract_info['market'].upper()} {contract_info['product'].upper()} Q{contract_info['period_num']} {contract_info['year']}")
    print(f"   📅 Delivery: {contract_info['delivery_date']}")
    
    # Method 1: Simple DataFetcher-style (no n_s transition)
    print(f"\n   🔧 METHOD 1: Simple Relative Calculation (No n_s)")
    simple_calc = calculate_relative_period_datafetcher_style(test_date, contract_info, n_s)
    print(f"      📈 Relative period: {simple_calc['relative_period']}")
    print(f"      📅 Reference: {simple_calc['reference_quarter']}")
    print(f"      📅 Target: {simple_calc['target_quarter']}")
    print(f"      📊 Difference: {simple_calc['quarters_difference']} quarters")
    
    # Method 2: With n_s transition logic
    print(f"\n   🔧 METHOD 2: With n_s Transition Logic")
    transition_calc = calculate_relative_period_with_ns_transition(test_date, contract_info, n_s)
    print(f"      📈 Relative period: {transition_calc['relative_period']}")
    print(f"      📅 Reference: {transition_calc['reference_quarter']}")
    print(f"      📅 Target: {transition_calc['target_quarter']}")
    print(f"      📊 Difference: {transition_calc['quarters_difference']} quarters")
    print(f"      ⚡ In transition: {transition_calc['in_transition']}")
    
    # Check for discrepancy
    if simple_calc['relative_period'] != transition_calc['relative_period']:
        print(f"\n   ⚠️  MISMATCH DETECTED!")
        print(f"      Simple method: {simple_calc['relative_period']}")
        print(f"      n_s method: {transition_calc['relative_period']}")
        print(f"      ➡️  This could explain price discrepancies!")
    else:
        print(f"\n   ✅ Both methods agree: {simple_calc['relative_period']}")

# Test a few more dates around the transition
print(f"\n" + "=" * 60)
print(f"🔍 TESTING MULTIPLE DATES AROUND Q3 2025 TRANSITION:")
print("=" * 60)

test_dates = [
    date(2025, 6, 25),  # Day before spike
    date(2025, 6, 26),  # Spike day
    date(2025, 6, 27),  # Day after spike
    date(2025, 9, 26),  # Near Q3 end
    date(2025, 9, 29),  # Very close to Q3 end
    date(2025, 9, 30),  # Last day of Q3
]

contract_info = parse_contract_info('debq4_25')

for test_dt in test_dates:
    print(f"\n📅 DATE: {test_dt} ({test_dt.strftime('%A')})")
    
    simple = calculate_relative_period_datafetcher_style(test_dt, contract_info, n_s)
    
    # Quick transition check
    ref_quarter = ((test_dt.month - 1) // 3) + 1
    business_days_to_end = calculate_business_days_to_period_end(test_dt, ref_quarter, test_dt.year)
    in_transition = business_days_to_end <= n_s
    
    print(f"   📊 Simple method: {simple['relative_period']}")
    print(f"   📊 Business days to Q{ref_quarter} end: {business_days_to_end}")
    print(f"   ⚡ Would transition: {in_transition}")
    
    if in_transition:
        print(f"   ⚠️  TRANSITION PERIOD - potential mismatch zone!")

print(f"\n" + "=" * 60)
print(f"🎯 CONCLUSION:")
print("=" * 60)
print(f"If DataFetcher and SpreadViewer use different n_s transition logic:")
print(f"• One system might be querying q_1 (Q4 2025) contracts")
print(f"• Other system might be querying q_2 (Q1 2026) contracts")
print(f"• This would explain €20 vs €33 price differences!")
print(f"• The fix requires synchronizing n_s transition logic between systems")
print(f"\n✅ Diagnostic completed - investigate the actual code implementations next!")