#!/usr/bin/env python3
"""
Test the corrected relative tenor calculation for SpreadViewer
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/src')

from core.data_fetcher import DataFetcher

print("🧪 TESTING CORRECTED RELATIVE TENOR LOGIC")
print("=" * 60)

# Test with the same Q4 2025 scenario that was failing
contracts = ['debq4_25', 'frbq4_25']
period = {
    'start_date': '2025-07-01', 
    'end_date': '2025-07-02'  # Just 1-2 days to test quickly
}
coefficients = [1, -1]

print(f"📋 Test Configuration:")
print(f"   📅 Date range: {period['start_date']} to {period['end_date']}")
print(f"   📅 Date range quarter: Q3 2025 (July 2025)")  
print(f"   📊 Contracts: {contracts}")
print(f"   📊 Target quarters: Q4 2025 for both contracts")
print(f"   🔢 Expected relative tenors: [1, 1] (next quarter)")
print(f"   🔢 Expected database queries: ['q_1', 'q_1']")
print()

try:
    # Initialize DataFetcher
    fetcher = DataFetcher()
    
    # This should trigger the corrected relative tenor calculation
    print(f"🚀 Running SpreadViewer with corrected logic...")
    
    # Format input correctly for fetch_spread_data
    spread_config = {
        'contracts': [
            {'market': 'de', 'tenor': 'q', 'contract': 'q4_25'},
            {'market': 'fr', 'tenor': 'q', 'contract': 'q4_25'}
        ],
        'coefficients': coefficients,
        'period': period
    }
    
    # This will show our debug output
    result = fetcher.fetch_spread_data(spread_config)
    
    print(f"\n✅ Test completed - check the debug output above!")
    print(f"   The 'Final tn1_list' should show [1, 1] instead of [4, 4]")
    print(f"   The database queries should be for 'q_1' instead of 'q_3'")
    
except Exception as e:
    print(f"❌ Test failed with error: {e}")
    import traceback
    traceback.print_exc()