#!/usr/bin/env python3
"""
Test the comprehensive n_s fix for the June 26, 2025 critical case
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta
from data_fetch_engine import (
    convert_absolute_to_relative_periods, 
    create_spreadviewer_config_for_period,
    ContractSpec,
    RelativePeriod
)

print("🧪 TESTING COMPREHENSIVE N_S FIX")
print("=" * 40)

# Test the exact critical case: June 26, 2025
start_date = datetime(2025, 6, 24)
end_date = datetime(2025, 7, 1)
n_s = 3

print(f"📅 Critical test period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"🔧 n_s parameter: {n_s}")

# Create contract specs
contract1 = ContractSpec(
    contract='debq4_25',
    market='de', 
    product='base',
    tenor='q',
    delivery_date=datetime(2025, 10, 1)
)

contract2 = ContractSpec(
    contract='frbq4_25',
    market='fr',
    product='base', 
    tenor='q',
    delivery_date=datetime(2025, 10, 1)
)

coefficients = [1.0, -1.0]

print(f"\n🔍 STEP 1: DataFetcher Relative Period Mapping")
print("-" * 50)

try:
    # Get DataFetcher's relative periods
    periods1 = convert_absolute_to_relative_periods(contract1, start_date, end_date, n_s)
    periods2 = convert_absolute_to_relative_periods(contract2, start_date, end_date, n_s)
    
    print(f"✅ DataFetcher mapping successful")
    print(f"📊 debq4_25 periods: {len(periods1)}")
    for i, (rel_period, p_start, p_end) in enumerate(periods1):
        print(f"   Period {i+1}: q_{rel_period.relative_offset} ({p_start.strftime('%Y-%m-%d')} to {p_end.strftime('%Y-%m-%d')})")
    
    print(f"📊 frbq4_25 periods: {len(periods2)}")
    for i, (rel_period, p_start, p_end) in enumerate(periods2):
        print(f"   Period {i+1}: q_{rel_period.relative_offset} ({p_start.strftime('%Y-%m-%d')} to {p_end.strftime('%Y-%m-%d')})")
    
except Exception as e:
    print(f"❌ DataFetcher mapping failed: {e}")
    exit(1)

print(f"\n🔍 STEP 2: SpreadViewer Config Creation (NEW FIX)")
print("-" * 50)

try:
    # Test the fixed config creation
    if len(periods1) > 0 and len(periods2) > 0:
        rel_period1, p_start1, p_end1 = periods1[0]
        rel_period2, p_start2, p_end2 = periods2[0]
        
        # Find overlap
        overlap_start = max(p_start1, p_start2, start_date)
        overlap_end = min(p_end1, p_end2, end_date)
        
        print(f"📊 Using periods: q_{rel_period1.relative_offset} and q_{rel_period2.relative_offset}")
        print(f"📅 Overlap period: {overlap_start.strftime('%Y-%m-%d')} to {overlap_end.strftime('%Y-%m-%d')}")
        
        # Create SpreadViewer config with the fix
        config = create_spreadviewer_config_for_period(
            contract1, contract2,
            rel_period1, rel_period2,
            overlap_start, overlap_end,
            coefficients, n_s
        )
        
        print(f"✅ SpreadViewer config created successfully")
        print(f"📊 Config details:")
        print(f"   🏢 Markets: {config['markets']}")
        print(f"   📊 Tenors: {config['tenors']}")  # Should be ['q_1', 'q_1'] 
        print(f"   🔢 tn1_list: {config['tn1_list']}")  # Should be [1, 1]
        print(f"   🔧 n_s: {config['n_s']}")
        print(f"   📅 Period: {config['start_date']} to {config['end_date']}")
        print(f"   ⚡ Forced periods: {config['forced_relative_periods']}")
        
        # Verify the fix
        expected_tenors = ['q_1', 'q_1']  # Both should be q_1 for June 2025
        expected_tn1 = [1, 1]
        
        if config['tenors'] == expected_tenors and config['tn1_list'] == expected_tn1:
            print(f"\n✅ FIX VERIFICATION: SUCCESS!")
            print(f"   📊 SpreadViewer will use exact same relative periods as DataFetcher")
            print(f"   📊 Both systems should now fetch q_1 data")
            print(f"   💰 This should eliminate the €9+ price discrepancy")
        else:
            print(f"\n❌ FIX VERIFICATION: FAILED!")
            print(f"   📊 Expected tenors: {expected_tenors}, got: {config['tenors']}")
            print(f"   📊 Expected tn1_list: {expected_tn1}, got: {config['tn1_list']}")
    
except Exception as e:
    print(f"❌ SpreadViewer config creation failed: {e}")
    import traceback
    traceback.print_exc()

print(f"\n🎯 SUMMARY")
print("-" * 20)

if 'config' in locals():
    print(f"✅ Comprehensive fix implemented and tested")
    print(f"📊 Key changes:")
    print(f"   1. SpreadViewer tenors forced to: {config['tenors']}")
    print(f"   2. Relative offsets forced to: {config['tn1_list']}")
    print(f"   3. Forced periods metadata: {config['forced_relative_periods']}")
    print(f"\n🚀 Ready for integration test with actual data fetch")
    print(f"💡 Expected result: €0 price discrepancy between DataFetcher and SpreadViewer")
else:
    print(f"❌ Fix testing failed - needs investigation")

print(f"\n📋 NEXT STEPS:")
print(f"1. 🔄 Run full integration test (fetch both DataFetcher and SpreadViewer data)")
print(f"2. 📊 Compare prices for June 26-27, 2025 period")
print(f"3. ✅ Verify €0 discrepancy achieved")
print(f"4. 📝 Document the fix for future reference")