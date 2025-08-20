#!/usr/bin/env python3
"""
Test the corrected transition logic - June 26 should now be q_2
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta
from data_fetch_engine import convert_absolute_to_relative_periods, ContractSpec

print("🧪 TESTING CORRECTED TRANSITION LOGIC")
print("=" * 45)

# Test the critical June 26 case
start_date = datetime(2025, 6, 24)
end_date = datetime(2025, 7, 1)
n_s = 3

# Create Q4 2025 contracts
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

print(f"📅 Test period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"🔧 n_s: {n_s}")
print(f"🎯 Expected: June 26 should now map to q_2 (not q_1)")

print(f"\n🔍 TESTING CORRECTED LOGIC:")
print("-" * 35)

for contract_name, contract_spec in contracts.items():
    print(f"\n📊 Contract: {contract_name} (Q4 2025 delivery)")
    
    try:
        periods = convert_absolute_to_relative_periods(contract_spec, start_date, end_date, n_s)
        
        print(f"   ✅ Generated {len(periods)} relative periods:")
        for i, (rel_period, p_start, p_end) in enumerate(periods):
            print(f"   📊 Period {i+1}: q_{rel_period.relative_offset}")
            print(f"      📅 Date range: {p_start.strftime('%Y-%m-%d')} to {p_end.strftime('%Y-%m-%d')}")
            print(f"      🔢 Relative offset: {rel_period.relative_offset}")
            
            # Critical check
            if rel_period.relative_offset == 2:
                print(f"      ✅ CORRECT: Using q_2 (Q2 perspective: Q4 delivery = 2 quarters ahead)")
            elif rel_period.relative_offset == 1:
                print(f"      ❌ INCORRECT: Still using q_1 (should be q_2 for June 24-26)")
            else:
                print(f"      🤔 UNEXPECTED: q_{rel_period.relative_offset}")
                
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()

# Test specific dates to verify the boundary
print(f"\n🎯 BOUNDARY TESTING:")
print("-" * 25)

test_dates = [
    datetime(2025, 6, 24),  # Should be q_2
    datetime(2025, 6, 25),  # Should be q_2  
    datetime(2025, 6, 26),  # Should be q_2 (USER REQUIREMENT)
    datetime(2025, 6, 27),  # Should be q_1 (transition starts AFTER June 26)
    datetime(2025, 6, 30),  # Should be q_1
]

for test_date in test_dates:
    single_day_end = test_date + timedelta(hours=23, minutes=59)
    
    try:
        periods = convert_absolute_to_relative_periods(
            contracts['debq4_25'], test_date, single_day_end, n_s
        )
        
        if len(periods) > 0:
            rel_offset = periods[0][0].relative_offset
            expected = "q_2" if test_date.date() <= datetime(2025, 6, 26).date() else "q_1"
            
            if f"q_{rel_offset}" == expected:
                status = "✅ CORRECT"
            else:
                status = "❌ WRONG"
                
            print(f"📅 {test_date.strftime('%Y-%m-%d')}: q_{rel_offset} (expected {expected}) {status}")
        else:
            print(f"📅 {test_date.strftime('%Y-%m-%d')}: No periods generated")
            
    except Exception as e:
        print(f"📅 {test_date.strftime('%Y-%m-%d')}: Error - {e}")

print(f"\n🎯 SUMMARY:")
print("=" * 15)
print("✅ If June 26 shows q_2: Fix successful!")
print("❌ If June 26 shows q_1: Need further adjustment")
print("🔄 Expected pattern: June 24-26 = q_2, June 27+ = q_1")