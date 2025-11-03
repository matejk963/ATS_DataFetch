#!/usr/bin/env python3
"""
Test Quarterly Fix
=================

Test the fixed quarterly transition logic
"""

import sys
import os
from datetime import datetime

# Add paths
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

def test_quarterly_fix():
    """Test the quarterly transition fix"""
    
    print("🔍 TESTING QUARTERLY TRANSITION FIX")
    print("=" * 50)
    
    try:
        from data_fetch_engine import calculate_quarterly_relative_offset, ContractSpec
        from data_fetcher.contracts import DeliveryDateCalculator
        
        # Test debq2_25 contract
        contract_spec = ContractSpec(
            market='de',
            product='b', 
            tenor='q',
            contract='2_25',
            delivery_date=DeliveryDateCalculator.calc_delivery_date('q', '2_25')
        )
        
        print(f"📊 Testing contract: {contract_spec.contract}")
        print(f"📅 Delivery date: {contract_spec.delivery_date}")
        
        # Test key dates
        test_dates = [
            datetime(2024, 12, 26),  # Before transition
            datetime(2024, 12, 27),  # Transition start
            datetime(2025, 2, 27),   # Should stay same (no transition)
            datetime(2025, 3, 26),   # Before Q1 transition  
            datetime(2025, 3, 27),   # Q1 transition start
        ]
        
        print(f"\n📋 RELATIVE OFFSET TESTS:")
        for test_date in test_dates:
            # Test normal perspective
            offset_normal = calculate_quarterly_relative_offset(
                test_date, contract_spec, n_s=3, use_next_quarter=False
            )
            
            # Test next quarter perspective  
            offset_next = calculate_quarterly_relative_offset(
                test_date, contract_spec, n_s=3, use_next_quarter=True
            )
            
            print(f"   {test_date.strftime('%Y-%m-%d')}: Normal=q_{offset_normal}, Next=q_{offset_next}")
        
        print(f"\n✅ Quarterly transition logic test completed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    test_quarterly_fix()

if __name__ == "__main__":
    main()