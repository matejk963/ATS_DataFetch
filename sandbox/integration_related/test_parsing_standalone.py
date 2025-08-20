#!/usr/bin/env python3
"""
Test contract parsing logic standalone
"""

import sys
import os
from datetime import datetime
from dataclasses import dataclass

# Add project root to path
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.append(project_root)

@dataclass
class ContractSpec:
    """Parsed contract specification"""
    market: str
    product: str
    tenor: str
    contract: str
    delivery_date: datetime

def calc_delivery_date(tenor: str, contract: str) -> datetime:
    """Calculate first delivery date from tenor/contract specifications"""
    base_year = 2000
    
    if tenor.lower() == 'm':
        # Monthly: contract is MM_YY format
        month_str, year_str = contract.split('_')
        year = base_year + int(year_str) if int(year_str) < 50 else 1900 + int(year_str)
        return datetime(year, int(month_str), 1)
    else:
        raise ValueError(f"Unknown tenor: {tenor}")

def parse_absolute_contract(contract_str: str) -> ContractSpec:
    """
    Parse absolute contract with product encoding
    
    Examples:
        'demb06_25' → market='de', tenor='m', product='base', contract='06_25'
        'demp07_25' → market='de', tenor='m', product='peak', contract='07_25'
    """
    if len(contract_str) < 6:
        raise ValueError(f"Invalid contract format: {contract_str}")
    
    market = contract_str[:2]           # 'de'
    tenor = contract_str[2:3]           # 'm'
    product_code = contract_str[3:4]    # 'b' or 'p'  
    contract = contract_str[4:]         # '06_25'
    
    product_map = {'b': 'base', 'p': 'peak'}
    if product_code not in product_map:
        raise ValueError(f"Unknown product code: {product_code}")
    
    product = product_map[product_code]
    
    # Calculate delivery date
    delivery_date = calc_delivery_date(tenor, contract)
    
    return ContractSpec(
        market=market,
        product=product,
        tenor=tenor,
        contract=contract,
        delivery_date=delivery_date
    )

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