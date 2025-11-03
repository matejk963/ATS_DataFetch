#!/usr/bin/env python3
"""
Test Vanilla Spread Engine
===========================

Test the vanilla engine with strict combination rules
"""

import sys
import os

# Add paths
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

def test_vanilla_combinations():
    """Test vanilla engine combination rules"""
    
    print("🔍 TESTING VANILLA ENGINE COMBINATION RULES")
    print("=" * 50)
    
    try:
        from spread_fetch_vanilla_engine import generate_spread_combinations
        
        # Test 1: Cross-country spreads (same period and year)
        print("\n📋 TEST 1: Cross-country spreads")
        cross_country_contracts = ['debm5_25', 'frbm5_25', 'debm6_25', 'frbm6_25']
        combinations1 = generate_spread_combinations(cross_country_contracts, 
                                                    trading_months_back={'m': 2, 'q': 4, 'y': 6})
        
        # Test 2: Same country sequential monthly
        print("\n📋 TEST 2: Same country sequential monthly")
        monthly_contracts = ['debm1_25', 'debm2_25', 'debm3_25', 'debm5_25']
        combinations2 = generate_spread_combinations(monthly_contracts,
                                                    trading_months_back={'m': 2, 'q': 4, 'y': 6})
        
        # Test 3: Same country sequential quarterly
        print("\n📋 TEST 3: Same country sequential quarterly")
        quarterly_contracts = ['debq1_25', 'debq2_25', 'debq3_25', 'debq4_25', 'debq1_26']
        combinations3 = generate_spread_combinations(quarterly_contracts,
                                                    trading_months_back={'m': 2, 'q': 4, 'y': 6})
        
        # Test 4: Same country sequential yearly
        print("\n📋 TEST 4: Same country sequential yearly")
        yearly_contracts = ['deby1_25', 'deby1_26', 'deby1_27']
        combinations4 = generate_spread_combinations(yearly_contracts,
                                                    trading_months_back={'m': 2, 'q': 4, 'y': 6})
        
        # Test 5: Mixed tenors (should produce NO combinations)
        print("\n📋 TEST 5: Mixed tenors (should be empty)")
        mixed_contracts = ['debm1_25', 'debq1_25', 'deby1_25']
        combinations5 = generate_spread_combinations(mixed_contracts,
                                                    trading_months_back={'m': 2, 'q': 4, 'y': 6})
        
        print(f"\n🎯 RESULTS SUMMARY:")
        print(f"   Cross-country: {len(combinations1)} combinations")
        print(f"   Monthly: {len(combinations2)} combinations") 
        print(f"   Quarterly: {len(combinations3)} combinations")
        print(f"   Yearly: {len(combinations4)} combinations")
        print(f"   Mixed tenors: {len(combinations5)} combinations (should be 0)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    test_vanilla_combinations()

if __name__ == "__main__":
    main()