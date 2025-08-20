#!/usr/bin/env python3
"""
Test only the parsing logic without database connections
"""

import sys
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/integration_related')

from integration_script_v2 import parse_absolute_contract

def test_parsing():
    print("🧪 Testing Contract Parsing Logic")
    print("=" * 35)
    
    test_contracts = [
        'demb06_25',  # German base monthly June 2025
        'demp07_25',  # German peak monthly July 2025  
        'demb08_25',  # German base monthly August 2025
    ]
    
    for contract in test_contracts:
        try:
            parsed = parse_absolute_contract(contract)
            print(f"✅ {contract}:")
            print(f"   Market: {parsed.market}")
            print(f"   Tenor: {parsed.tenor}")
            print(f"   Product: {parsed.product}")
            print(f"   Contract: {parsed.contract}")
            print(f"   Delivery: {parsed.delivery_date.strftime('%Y-%m-%d')}")
            print()
        except Exception as e:
            print(f"❌ {contract}: {e}")
            print()

if __name__ == "__main__":
    test_parsing()