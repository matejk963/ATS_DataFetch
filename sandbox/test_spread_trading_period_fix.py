#!/usr/bin/env python3
"""
Test Spread Trading Period Fix
===============================

Test the new conservative spread trading period logic.
"""

import sys
import os

# Add project paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

from engines.spread_fetch_engine import (
    calculate_spread_trading_period,
    calculate_contract_trading_end,
    parse_absolute_contract
)

def test_spread_period_fix():
    """Test the new spread trading period calculation"""
    
    print("🧪 TESTING SPREAD TRADING PERIOD FIX")
    print("=" * 50)
    
    # Test case: DEBM5_25 vs FRBQ2_25
    contract1_str = "debm5_25"
    contract2_str = "frbq2_25"
    
    # Parse contracts
    contract1 = parse_absolute_contract(contract1_str)
    contract2 = parse_absolute_contract(contract2_str)
    
    print(f"\n📊 CONTRACTS:")
    print(f"   Contract 1: {contract1_str} -> {contract1.contract} (delivery: {contract1.delivery_date})")
    print(f"   Contract 2: {contract2_str} -> {contract2.contract} (delivery: {contract2.delivery_date})")
    
    # Calculate individual trading ends
    trading_end1 = calculate_contract_trading_end(contract1)
    trading_end2 = calculate_contract_trading_end(contract2)
    
    print(f"\n📅 INDIVIDUAL TRADING ENDS:")
    print(f"   {contract1_str}: {trading_end1.strftime('%Y-%m-%d')}")
    print(f"   {contract2_str}: {trading_end2.strftime('%Y-%m-%d')}")
    print(f"   Earliest: {min(trading_end1, trading_end2).strftime('%Y-%m-%d')}")
    
    # Calculate spread trading period (should use earliest end)
    print(f"\n🔄 SPREAD TRADING PERIOD CALCULATION:")
    spread_period = calculate_spread_trading_period(contract1, contract2, months_back=2)
    
    print(f"\n✅ RESULT:")
    print(f"   Spread period: {spread_period['start_date']} to {spread_period['end_date']}")
    
    # Verify the fix
    earliest_end = min(trading_end1, trading_end2)
    spread_end = spread_period['end_date']
    
    if spread_end == earliest_end.strftime('%Y-%m-%d'):
        print(f"   ✅ CORRECT: Spread ends with earliest contract ({earliest_end.strftime('%Y-%m-%d')})")
    else:
        print(f"   ❌ ERROR: Spread should end on {earliest_end.strftime('%Y-%m-%d')}, got {spread_end}")

if __name__ == "__main__":
    test_spread_period_fix()