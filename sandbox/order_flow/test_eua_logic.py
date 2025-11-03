#!/usr/bin/env python3
"""
Test EUA Logic
==============

Test the new EUA market logic with Dec 1st transitions
"""

import sys
from datetime import datetime

# Add paths
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

def test_eua_logic():
    """Test EUA transition logic"""
    
    print("🔍 TESTING EUA LOGIC")
    print("=" * 50)
    
    try:
        from data_fetch_engine import calculate_eua_relative_offset, ContractSpec
        from data_fetcher.contracts import DeliveryDateCalculator
        
        # Test EUA contract
        contract_spec = ContractSpec(
            market='eua',
            product='',  # EUA doesn't have product
            tenor='y',
            contract='1_25',
            delivery_date=datetime(2025, 12, 15)  # December 2025 delivery
        )
        
        print(f"📊 Testing contract: eua{contract_spec.tenor}{contract_spec.contract}")
        print(f"📅 Delivery date: {contract_spec.delivery_date}")
        
        # Test key dates around Dec 1st transitions
        test_dates = [
            datetime(2024, 11, 30),  # Before Dec 1, 2024
            datetime(2024, 12, 1),   # Dec 1, 2024 transition
            datetime(2024, 12, 2),   # After Dec 1, 2024
            datetime(2025, 6, 15),   # Mid-year 2025
            datetime(2025, 11, 30),  # Before Dec 1, 2025
            datetime(2025, 12, 1),   # Dec 1, 2025 transition
            datetime(2025, 12, 2),   # After Dec 1, 2025
        ]
        
        print(f"\n📋 EUA RELATIVE OFFSET TESTS:")
        print(f"   Contract: euay1_25 (delivery Dec 2025)")
        print(f"   Expected: dec_0 from Dec 1, 2024 to Nov 30, 2025")
        print(f"   Expected: dec_-1 from Dec 1, 2025 onwards")
        
        for test_date in test_dates:
            # Test normal perspective
            offset_normal = calculate_eua_relative_offset(
                test_date, contract_spec, n_s=3, use_next_year=False
            )
            
            # Test next year perspective  
            offset_next = calculate_eua_relative_offset(
                test_date, contract_spec, n_s=3, use_next_year=True
            )
            
            print(f"   {test_date.strftime('%Y-%m-%d')}: Normal=dec_{offset_normal}, Next=dec_{offset_next}")
        
        print(f"\n✅ EUA logic test completed!")
        
        # Test tenor translation
        print(f"\n🔧 TESTING TENOR TRANSLATION:")
        
        def translate_tenor_for_spreadviewer(market: str, tenor: str) -> str:
            """Translate tenor for SpreadViewer - EUA 'y' becomes 'dec'"""
            if market == 'eua' and tenor == 'y':
                return 'dec'
            return tenor
        
        test_cases = [
            ('eua', 'y'),
            ('de', 'y'), 
            ('eua', 'm'),
            ('fr', 'q')
        ]
        
        for market, tenor in test_cases:
            translated = translate_tenor_for_spreadviewer(market, tenor)
            print(f"   {market}:{tenor} → {translated}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    test_eua_logic()

if __name__ == "__main__":
    main()