#!/usr/bin/env python3
"""
Test just the relative tenor calculation logic (isolated)
"""

from datetime import datetime
import pandas as pd

# Simulate the corrected logic from our fix
def test_relative_tenor_calculation():
    """Test the relative tenor calculation logic"""
    
    print("🧪 TESTING RELATIVE TENOR CALCULATION LOGIC")
    print("=" * 60)
    
    # Test data (same as the failing case)
    contracts = [
        {'market': 'de', 'tenor': 'q', 'contract': 'q4_25'},
        {'market': 'fr', 'tenor': 'q', 'contract': 'q4_25'}
    ]
    
    period = {
        'start_date': '2025-07-01',
        'end_date': '2025-07-02'
    }
    
    print(f"📋 Test Configuration:")
    print(f"   📅 Date range: {period['start_date']} to {period['end_date']}")
    print(f"   📊 Contracts: {[c['contract'] for c in contracts]}")
    print()
    
    # Convert period to date range (as in our fix)
    start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
    dates = pd.date_range(start_date, end_date, freq='B')
    
    # Calculate relative tenor offsets (our corrected logic)
    tn1_list = []
    reference_date = start_date
    
    print(f"🔍 RELATIVE TENOR CALCULATION:")
    print(f"   📅 Reference date: {reference_date}")
    print(f"   📅 Date range quarter: Q{((reference_date.month-1)//3)+1} {reference_date.year}")
    
    for contract in contracts:
        contract_spec = contract['contract']
        
        if contract['tenor'] == 'q':  # Quarterly
            # Extract quarter and year from contract (format: 'q4_25' = Q4 2025)
            parts = contract_spec.split('_')
            target_quarter = int(parts[0][1:])  # Extract '4' from 'q4'
            target_year = 2000 + int(parts[1])  # Convert '25' to 2025
            
            # Calculate current quarter from reference date
            reference_quarter = ((reference_date.month-1)//3) + 1
            reference_year = reference_date.year
            
            # Calculate quarter difference
            ref_quarters = reference_year * 4 + (reference_quarter - 1)
            target_quarters = target_year * 4 + (target_quarter - 1)
            relative_quarters = target_quarters - ref_quarters
            
            print(f"   📊 Contract: {contract_spec}")
            print(f"      Reference: Q{reference_quarter} {reference_year} = {ref_quarters} total quarters")
            print(f"      Target: Q{target_quarter} {target_year} = {target_quarters} total quarters")
            print(f"      Relative: {target_quarters} - {ref_quarters} = {relative_quarters} quarters")
            tn1_list.append(relative_quarters)
    
    print(f"\n   ✅ Final tn1_list (relative tenors): {tn1_list}")
    print(f"   📝 Expected database queries for: {['q_' + str(t) for t in tn1_list]}")
    
    # Verify results
    print(f"\n🎯 VERIFICATION:")
    print(f"   📋 Expected result: [1, 1] (Q4 2025 is 1 quarter ahead of Q3 2025)")
    print(f"   📋 Actual result: {tn1_list}")
    
    if tn1_list == [1, 1]:
        print(f"   ✅ SUCCESS: Relative tenor calculation is correct!")
        print(f"   ✅ SpreadViewer should now query for 'q_1' instead of 'q_3'")
        return True
    else:
        print(f"   ❌ FAILURE: Expected [1, 1] but got {tn1_list}")
        return False

if __name__ == "__main__":
    success = test_relative_tenor_calculation()
    if success:
        print(f"\n🎉 Test PASSED - the relative tenor fix works correctly!")
    else:
        print(f"\n💥 Test FAILED - the logic still needs adjustment!")