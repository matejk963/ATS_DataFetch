#!/usr/bin/env python3
"""
Test Sophisticated Combinations
================================

Test the new sophisticated spread combination logic.
"""

import sys
import os

# Add project paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

from engines.spread_fetch_engine import generate_spread_combinations

def test_sophisticated_combinations():
    """Test the new sophisticated combination rules"""
    
    print("🧪 TESTING SOPHISTICATED COMBINATION RULES")
    print("=" * 50)
    
    # Test case 1: Mixed contracts with different tenors and years
    test_contracts = [
        # Months 2025
        'debm1_25', 'debm2_25', 'debm3_25', 'debm4_25', 'debm5_25',
        # Quarters 2025
        'debq1_25', 'debq2_25', 'debq3_25',
        # Year 2025
        'deby25',
        # Some 2024 contracts for cross-year testing
        'debm12_24', 'debq4_24'
    ]
    
    print(f"\n📊 INPUT CONTRACTS ({len(test_contracts)}):")
    for contract in test_contracts:
        print(f"   • {contract}")
    
    total_possible = len(test_contracts) * (len(test_contracts) - 1) // 2
    print(f"\n📈 Total possible combinations: {total_possible}")
    
    try:
        combinations = generate_spread_combinations(
            contracts=test_contracts,
            coefficients=[1, -1],
            trading_months_back=2
        )
        
        print(f"\n✅ SUCCESS: Generated {len(combinations)} valid combinations")
        print(f"📊 Reduction: {total_possible - len(combinations)} invalid combinations filtered out")
        
        # Group by combination type
        same_tenor = 0
        month_quarter = 0
        quarter_year = 0
        other = 0
        
        for combo in combinations:
            tenor1 = combo.parsed_contract1.tenor
            tenor2 = combo.parsed_contract2.tenor
            
            if tenor1 == tenor2:
                same_tenor += 1
            elif (tenor1 == 'm' and tenor2 == 'q') or (tenor1 == 'q' and tenor2 == 'm'):
                month_quarter += 1
            elif (tenor1 == 'q' and tenor2 == 'y') or (tenor1 == 'y' and tenor2 == 'q'):
                quarter_year += 1
            else:
                other += 1
        
        print(f"\n📋 COMBINATION TYPES:")
        print(f"   • Same tenor (m-m, q-q, y-y): {same_tenor}")
        print(f"   • Month-Quarter: {month_quarter}")
        print(f"   • Quarter-Year: {quarter_year}")
        print(f"   • Other: {other}")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_sophisticated_combinations()