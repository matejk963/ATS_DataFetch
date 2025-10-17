"""
Contract Specifications and Parsing
===================================

Dataclasses and functions for contract handling.
"""

from dataclasses import dataclass
from datetime import datetime
import sys
import os

# Add path for DeliveryDateCalculator
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
from src.core.data_fetcher import DeliveryDateCalculator


@dataclass
class ContractSpec:
    """Parsed contract specification"""
    market: str
    product: str
    tenor: str
    contract: str
    delivery_date: datetime


@dataclass
class RelativePeriod:
    """Relative contract period with date range"""
    relative_offset: int  # M1, M2, M3, M4
    start_date: datetime
    end_date: datetime


def parse_absolute_contract(contract_str: str) -> ContractSpec:
    """
    Parse absolute contract with product encoding - supports 2-3 letter market codes
    
    Examples:
        'debm07_25' → market='de', product='base', tenor='m', contract='07_25'
        'depm07_25' → market='de', product='peak', tenor='m', contract='07_25'
        'ttfbm09_25' → market='ttf', product='base', tenor='m', contract='09_25'
    """
    if len(contract_str) < 6:
        raise ValueError(f"Invalid contract format: {contract_str}")
    
    # Known 3-letter market codes
    three_letter_markets = {'ttf', 'nbp', 'peg', 'zee', 'gas'}
    
    # Try 3-letter market code first
    if len(contract_str) >= 7 and contract_str[:3].lower() in three_letter_markets:
        market = contract_str[:3].lower()       # 'ttf'
        product_code = contract_str[3:4]        # 'b' or 'p'
        tenor = contract_str[4:5]              # 'm'
        contract = contract_str[5:]            # '09_25'
    else:
        # Default to 2-letter market code
        market = contract_str[:2].lower()       # 'de'
        product_code = contract_str[2:3]        # 'b' or 'p'
        tenor = contract_str[3:4]              # 'm'
        contract = contract_str[4:]            # '09_25'
    
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


def create_contract_config_from_spec(contract_spec: ContractSpec, period: dict) -> dict:
    """Convert ContractSpec to DataFetcher contract config format"""
    return {
        'market': contract_spec.market,
        'tenor': contract_spec.tenor,
        'contract': contract_spec.contract,
        'start_date': period['start_date'],
        'end_date': period['end_date'],
        'prod': contract_spec.product
    }