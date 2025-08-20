#!/usr/bin/env python3
"""
Test edge cases for the relative tenor calculation
"""

from datetime import datetime
import pandas as pd

def test_relative_tenor_edge_cases():
    """Test various edge cases for relative tenor calculation"""
    
    print("🧪 TESTING RELATIVE TENOR EDGE CASES")
    print("=" * 80)
    
    test_cases = [
        {
            'name': 'Q4 2024 → Q1 2025 (year boundary)',
            'date_range': ('2024-12-01', '2024-12-05'),
            'contracts': [{'market': 'de', 'tenor': 'q', 'contract': 'q1_25'}],
            'expected': [1]  # Q1 2025 is 1 quarter after Q4 2024
        },
        {
            'name': 'Q1 2025 → Q4 2024 (backward year boundary)', 
            'date_range': ('2025-01-15', '2025-01-16'),
            'contracts': [{'market': 'de', 'tenor': 'q', 'contract': 'q4_24'}],
            'expected': [-1]  # Q4 2024 is 1 quarter before Q1 2025
        },
        {
            'name': 'Q1 2025 → Q4 2025 (3 quarters ahead)',
            'date_range': ('2025-02-01', '2025-02-05'),
            'contracts': [{'market': 'de', 'tenor': 'q', 'contract': 'q4_25'}],
            'expected': [3]  # Q4 2025 is 3 quarters after Q1 2025
        },
        {
            'name': 'Same quarter (Q3 → Q3)',
            'date_range': ('2025-07-15', '2025-07-16'),
            'contracts': [{'market': 'de', 'tenor': 'q', 'contract': 'q3_25'}],
            'expected': [0]  # Same quarter should be 0
        },
        {
            'name': 'Multiple contracts, different relative positions',
            'date_range': ('2025-05-15', '2025-05-16'),  # Q2 2025
            'contracts': [
                {'market': 'de', 'tenor': 'q', 'contract': 'q1_25'},  # 1 quarter back
                {'market': 'fr', 'tenor': 'q', 'contract': 'q3_25'},  # 1 quarter ahead
                {'market': 'de', 'tenor': 'q', 'contract': 'q4_25'}   # 2 quarters ahead
            ],
            'expected': [-1, 1, 2]
        }
    ]
    
    all_passed = True
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n🔍 TEST CASE {i}: {test_case['name']}")
        print(f"   📅 Date range: {test_case['date_range'][0]} to {test_case['date_range'][1]}")
        print(f"   📊 Contracts: {[c['contract'] for c in test_case['contracts']]}")
        print(f"   📋 Expected: {test_case['expected']}")
        
        # Run the calculation
        start_date = datetime.strptime(test_case['date_range'][0], '%Y-%m-%d')
        reference_date = start_date
        reference_quarter = ((reference_date.month-1)//3) + 1
        reference_year = reference_date.year
        
        print(f"   📅 Reference: Q{reference_quarter} {reference_year}")
        
        tn1_list = []
        
        for contract in test_case['contracts']:
            contract_spec = contract['contract']
            
            # Extract quarter and year from contract (format: 'q4_25' = Q4 2025)
            parts = contract_spec.split('_')
            target_quarter = int(parts[0][1:])  # Extract '4' from 'q4'
            target_year = 2000 + int(parts[1])  # Convert '25' to 2025
            
            # Calculate quarter difference
            ref_quarters = reference_year * 4 + (reference_quarter - 1)
            target_quarters = target_year * 4 + (target_quarter - 1)
            relative_quarters = target_quarters - ref_quarters
            
            print(f"      Contract {contract_spec}: Q{target_quarter} {target_year} → Relative: {relative_quarters}")
            tn1_list.append(relative_quarters)
        
        # Verify results
        print(f"   📊 Calculated: {tn1_list}")
        
        if tn1_list == test_case['expected']:
            print(f"   ✅ PASSED")
        else:
            print(f"   ❌ FAILED: Expected {test_case['expected']}, got {tn1_list}")
            all_passed = False
    
    return all_passed

if __name__ == "__main__":
    success = test_relative_tenor_edge_cases()
    
    if success:
        print(f"\n🎉 ALL EDGE CASE TESTS PASSED!")
        print(f"   ✅ The relative tenor calculation handles all edge cases correctly")
        print(f"   ✅ Year boundaries work properly")  
        print(f"   ✅ Backward/forward quarters work properly")
        print(f"   ✅ Multiple quarters ahead/behind work properly")
    else:
        print(f"\n💥 SOME TESTS FAILED!")
        print(f"   ❌ The logic needs further adjustment for edge cases")