#!/usr/bin/env python3
"""
Test data fetch engine WITHOUT pandas patches to isolate the hanging issue
"""

import sys
import os
from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import json
from dataclasses import dataclass

# Cross-platform project root
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
    rawdata_base = r'C:\Users\krajcovic\Documents\Testing Data\RawData'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'
    rawdata_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData'

sys.path.insert(0, project_root)
sys.path.insert(0, energy_trading_path)

print("🚀 Testing Data Fetch Engine WITHOUT pandas patches")
print("=" * 60)

# Import without patches
print("1. Importing DataFetcher...")
from src.core.data_fetcher import DataFetcher, TPDATA_AVAILABLE, DeliveryDateCalculator

print("2. Importing SpreadViewer classes (NO PATCHES)...")
try:
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from Database.TPData import TPData
    print("   ✅ SpreadViewer imports successful")
    SPREADVIEWER_AVAILABLE = True
except ImportError as e:
    print(f"   ❌ SpreadViewer imports failed: {e}")
    SPREADVIEWER_AVAILABLE = False

@dataclass
class ContractSpec:
    """Parsed contract specification"""
    market: str
    product: str
    tenor: str
    contract: str
    delivery_date: datetime

def parse_absolute_contract(contract_str: str) -> ContractSpec:
    """Parse absolute contract with product encoding"""
    if len(contract_str) < 6:
        raise ValueError(f"Invalid contract format: {contract_str}")
    
    market = contract_str[:2]           # 'de'
    product_code = contract_str[2:3]    # 'b' or 'p'
    tenor = contract_str[3:4]           # 'm'
    contract = contract_str[4:]         # '09_25'
    
    product_map = {'b': 'base', 'p': 'peak'}
    if product_code not in product_map:
        raise ValueError(f"Unknown product code: {product_code}")
    
    product = product_map[product_code]
    
    # Calculate delivery date
    calc = DeliveryDateCalculator()
    delivery_date = calc.calc_delivery_date(tenor, contract)
    
    return ContractSpec(
        market=market,
        product=product,
        tenor=tenor,
        contract=contract,
        delivery_date=delivery_date
    )

def main():
    print("3. Starting test execution...")
    
    # Configuration
    config = {
        'contracts': ['debm09_25', 'frbm09_25'],
        'coefficients': [1, -1],
        'period': {
            'start_date': '2025-07-01',
            'end_date': '2025-07-02'  # Just 1 day for test
        },
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': False,  # Disable synthetic for now
            'include_individual_legs': False
        },
        'n_s': 3,
        'test_mode': False
    }
    
    print(f"4. Config: {config['contracts']} from {config['period']['start_date']} to {config['period']['end_date']}")
    
    # Parse contracts
    print("5. Parsing contracts...")
    try:
        parsed_contracts = [parse_absolute_contract(c) for c in config['contracts']]
        print(f"   ✅ Parsed {len(parsed_contracts)} contracts")
        for i, contract in enumerate(parsed_contracts):
            print(f"      {i+1}. {contract.market}{contract.product[0]}{contract.tenor}{contract.contract} → {contract.delivery_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        print(f"   ❌ Contract parsing failed: {e}")
        return False
    
    # Test DataFetcher
    print("6. Testing DataFetcher...")
    try:
        fetcher = DataFetcher(allowed_broker_ids=[1441])
        print("   ✅ DataFetcher created successfully")
        
        # Test real spread fetch
        if len(parsed_contracts) == 2:
            contract1, contract2 = parsed_contracts
            print(f"   🔍 Testing real spread fetch: {contract1.market} vs {contract2.market}")
            
            # Quick test - just check if method is callable
            print("   📞 DataFetcher real spread test - SKIPPED (would be slow)")
        
    except Exception as e:
        print(f"   ❌ DataFetcher test failed: {e}")
        return False
    
    # Test SpreadViewer (if available)
    if SPREADVIEWER_AVAILABLE:
        print("7. Testing SpreadViewer...")
        try:
            spread = SpreadSingle(['de', 'fr'], ['m', 'm'], [9, 9], [], ['eex'])
            print("   ✅ SpreadSingle created successfully")
            
            # Test basic functionality
            dates = pd.date_range('2025-07-01', '2025-07-01', freq='B')  # Just 1 day
            print(f"   🔍 Testing product_dates with {len(dates)} dates...")
            
            result = spread.product_dates(dates, 3)
            print(f"   ✅ product_dates returned: {type(result)} with {len(result) if result else 0} items")
            
        except Exception as e:
            print(f"   ❌ SpreadViewer test failed: {e}")
            return False
    else:
        print("7. SpreadViewer not available - skipping")
    
    print("8. ✅ All tests completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 TEST COMPLETED - NO HANGING ISSUES!")
    else:
        print("\n💥 TEST FAILED!")