#!/usr/bin/env python3
"""
Implement comprehensive n_s fix - ensure SpreadViewer uses exact same relative periods as DataFetcher
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta
from data_fetch_engine import convert_absolute_to_relative_periods, ContractSpec

print("🔧 IMPLEMENTING COMPREHENSIVE N_S FIX")
print("=" * 50)

def create_datafetcher_consistent_spreadviewer_config(contract1: ContractSpec, contract2: ContractSpec, 
                                                     start_date: datetime, end_date: datetime, 
                                                     coefficients: list, n_s: int = 3):
    """
    Create SpreadViewer config that uses EXACTLY the same relative periods as DataFetcher
    
    This is the core fix: Instead of letting SpreadViewer calculate its own relative periods,
    we force it to use DataFetcher's period mapping.
    """
    print(f"🔧 Creating DataFetcher-consistent SpreadViewer config")
    print(f"   📅 Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"   🔧 n_s: {n_s}")
    
    # Get DataFetcher's relative period mapping for both contracts
    periods1 = convert_absolute_to_relative_periods(contract1, start_date, end_date, n_s)
    periods2 = convert_absolute_to_relative_periods(contract2, start_date, end_date, n_s)
    
    print(f"   📊 {contract1.contract} DataFetcher periods: {len(periods1)}")
    for i, (rel_period, p_start, p_end) in enumerate(periods1):
        print(f"      Period {i+1}: q_{rel_period.relative_offset} ({p_start.strftime('%Y-%m-%d')} to {p_end.strftime('%Y-%m-%d')})")
    
    print(f"   📊 {contract2.contract} DataFetcher periods: {len(periods2)}")
    for i, (rel_period, p_start, p_end) in enumerate(periods2):
        print(f"      Period {i+1}: q_{rel_period.relative_offset} ({p_start.strftime('%Y-%m-%d')} to {p_end.strftime('%Y-%m-%d')})")
    
    # Create SpreadViewer configs that force these exact relative periods
    configs = []
    
    for (rel_period1, p_start1, p_end1) in periods1:
        for (rel_period2, p_start2, p_end2) in periods2:
            # Find overlapping period
            overlap_start = max(p_start1, p_start2, start_date)
            overlap_end = min(p_end1, p_end2, end_date)
            
            if overlap_start <= overlap_end:
                # THIS IS THE KEY FIX: Force SpreadViewer to use DataFetcher's relative offsets
                config = {
                    'markets': [contract1.market, contract2.market],
                    'tenors': [f'{contract1.tenor}_{rel_period1.relative_offset}', 
                              f'{contract2.tenor}_{rel_period2.relative_offset}'],  # Force relative periods
                    'tn1_list': [rel_period1.relative_offset, rel_period2.relative_offset],  # Explicit relative offsets
                    'tn2_list': [],
                    'coefficients': coefficients,
                    'n_s': n_s,
                    'start_date': overlap_start.strftime('%Y-%m-%d'),
                    'end_date': overlap_end.strftime('%Y-%m-%d'),
                    'forced_relative_periods': {
                        contract1.contract: rel_period1.relative_offset,
                        contract2.contract: rel_period2.relative_offset
                    }
                }
                
                configs.append(config)
                
                print(f"   ✅ Created config: {contract1.contract}=q_{rel_period1.relative_offset}, {contract2.contract}=q_{rel_period2.relative_offset}")
                print(f"      📅 Overlap: {overlap_start.strftime('%Y-%m-%d')} to {overlap_end.strftime('%Y-%m-%d')}")
    
    return configs

# Test with the critical June 26, 2025 case
print(f"\n🧪 TESTING WITH CRITICAL CASE")
print("=" * 35)

start_date = datetime(2025, 6, 24)
end_date = datetime(2025, 7, 1)
n_s = 3

contracts = {
    'debq4_25': ContractSpec(
        contract='debq4_25',
        market='de',
        product='base',
        tenor='q',
        delivery_date=datetime(2025, 10, 1)
    ),
    'frbq4_25': ContractSpec(
        contract='frbq4_25',
        market='fr',
        product='base',
        tenor='q',
        delivery_date=datetime(2025, 10, 1)
    )
}

coefficients = [1.0, -1.0]  # Standard spread

try:
    configs = create_datafetcher_consistent_spreadviewer_config(
        contracts['debq4_25'], 
        contracts['frbq4_25'],
        start_date, 
        end_date, 
        coefficients,
        n_s
    )
    
    print(f"\n✅ Generated {len(configs)} consistent configs")
    
    for i, config in enumerate(configs):
        print(f"\n📋 Config {i+1}:")
        print(f"   🏢 Markets: {config['markets']}")
        print(f"   📊 Tenors: {config['tenors']}")
        print(f"   🔢 tn1_list: {config['tn1_list']}")
        print(f"   🔧 n_s: {config['n_s']}")
        print(f"   📅 Period: {config['start_date']} to {config['end_date']}")
        print(f"   ⚡ Forced periods: {config['forced_relative_periods']}")
        
        # This config should now force SpreadViewer to use the same relative periods as DataFetcher
        
except Exception as e:
    print(f"❌ Error creating configs: {e}")
    import traceback
    traceback.print_exc()

print(f"\n🎯 IMPLEMENTATION PLAN")
print("=" * 25)

print(f"1. 🔧 Modify create_spreadviewer_config_for_period() to force relative periods")
print(f"2. 🔧 Update fetch_spreadviewer_for_period() to use forced periods")
print(f"3. 🔧 Ensure tenors_list construction respects DataFetcher mappings")
print(f"4. 🧪 Test with refetch to verify €0 price discrepancy")

print(f"\n💡 KEY INSIGHT:")
print(f"   The issue isn't in the n_s transition logic itself, but in ensuring")
print(f"   that SpreadViewer receives the EXACT relative period parameters that")
print(f"   DataFetcher calculated, rather than calculating its own.")

print(f"\n🚨 CRITICAL:")
print(f"   SpreadViewer must use q_1 for both debq4_25 and frbq4_25 during")
print(f"   June 24-July 1, 2025 period to match DataFetcher's calculation.")