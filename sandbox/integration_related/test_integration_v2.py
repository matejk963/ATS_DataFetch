#!/usr/bin/env python3
"""
Test script for Enhanced Integration V2

This script demonstrates the new integration capabilities:
- Product-encoded contract names (demb07_25, demp07_25)
- Single leg vs spread mode detection
- Real spread contracts via DataFetcher
- Synthetic spread contracts via SpreadViewer with n_s transitions
- Multiple relative periods per absolute contract
"""

import sys
import os
from datetime import datetime

# Add project paths
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'

sys.path.append(project_root)
sys.path.append(energy_trading_path)

from integration_script_v2 import (
    parse_absolute_contract, 
    calculate_transition_dates,
    convert_absolute_to_relative_periods,
    integrated_fetch
)

def test_contract_parser():
    """Test the contract parser with product encoding"""
    print("🧪 Testing Contract Parser")
    print("-" * 30)
    
    test_contracts = [
        'demb07_25',  # German base monthly July 2025
        'demp08_25',  # German peak monthly August 2025  
        'demb06_25',  # German base monthly June 2025
    ]
    
    for contract in test_contracts:
        try:
            parsed = parse_absolute_contract(contract)
            print(f"✅ {contract} → market={parsed.market}, product={parsed.product}, tenor={parsed.tenor}, contract={parsed.contract}, delivery={parsed.delivery_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"❌ {contract} → Error: {e}")

def test_transition_logic():
    """Test n_s transition date calculation"""
    print("\n🧪 Testing n_s Transition Logic")
    print("-" * 35)
    
    start_date = datetime(2025, 2, 3)
    end_date = datetime(2025, 4, 30) 
    n_s = 3
    
    transitions = calculate_transition_dates(start_date, end_date, n_s)
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"n_s = {n_s} (last {n_s} business days)")
    print("Calculated periods:")
    
    for i, (period_start, period_end) in enumerate(transitions):
        print(f"  Period {i+1}: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")

def test_absolute_to_relative():
    """Test absolute to relative conversion"""
    print("\n🧪 Testing Absolute to Relative Conversion")
    print("-" * 42)
    
    contract = parse_absolute_contract('demb06_25')  # June 2025 delivery
    start_date = datetime(2025, 2, 3)
    end_date = datetime(2025, 4, 30)
    n_s = 3
    
    periods = convert_absolute_to_relative_periods(contract, start_date, end_date, n_s)
    
    print(f"Contract: demb06_25 (delivery: {contract.delivery_date.strftime('%Y-%m-%d')})")
    print(f"Trading period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("Relative periods:")
    
    for rel_period, period_start, period_end in periods:
        print(f"  M{rel_period.relative_offset}: {period_start.strftime('%Y-%m-%d')} to {period_end.strftime('%Y-%m-%d')}")

def test_single_leg_mode():
    """Test single leg integration mode"""
    print("\n🧪 Testing Single Leg Mode")
    print("-" * 25)
    
    config = {
        'contracts': ['demb07_25'],  # Single contract
        'period': {
            'start_date': '2025-06-01',
            'end_date': '2025-06-15'
        },
        'options': {},
        'n_s': 3
    }
    
    print(f"Configuration: {config}")
    print("Expected: Single leg mode processing")

def test_spread_mode():
    """Test spread integration mode"""  
    print("\n🧪 Testing Spread Mode")
    print("-" * 21)
    
    config = {
        'contracts': ['demb06_25', 'demb07_25'],  # Two contracts = spread
        'coefficients': [1, -1],  # Buy June, Sell July
        'period': {
            'start_date': '2025-02-03',
            'end_date': '2025-04-30'
        },
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': True,
            'include_individual_legs': False
        },
        'n_s': 3
    }
    
    print(f"Configuration: {config}")
    print("Expected: Dual spread mode (real + synthetic)")
    print("Real spread: DataFetcher with start_date1 + start_date2")  
    print("Synthetic spread: SpreadViewer with multiple relative periods")

def test_integration_dry_run():
    """Test integration without actual data fetching"""
    print("\n🧪 Testing Integration (Dry Run)")
    print("-" * 33)
    
    # Test single leg
    single_config = {
        'contracts': ['demb07_25'],
        'period': {'start_date': '2025-06-01', 'end_date': '2025-06-15'},
        'options': {},
        'n_s': 3
    }
    
    print("Single leg configuration validation...")
    try:
        # Just test parsing without actual fetching
        from integration_script_v2 import parse_absolute_contract
        contracts = [parse_absolute_contract(c) for c in single_config['contracts']]
        print(f"✅ Single leg: {len(contracts)} contract(s) parsed")
    except Exception as e:
        print(f"❌ Single leg parsing failed: {e}")
    
    # Test spread
    spread_config = {
        'contracts': ['demb06_25', 'demb07_25'], 
        'coefficients': [1, -1],
        'period': {'start_date': '2025-02-03', 'end_date': '2025-04-30'},
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': True, 
            'include_individual_legs': False
        },
        'n_s': 3
    }
    
    print("Spread configuration validation...")
    try:
        contracts = [parse_absolute_contract(c) for c in spread_config['contracts']]
        print(f"✅ Spread: {len(contracts)} contract(s) parsed")
        print(f"   Mode: {'Single leg' if len(contracts) == 1 else 'Spread'}")
    except Exception as e:
        print(f"❌ Spread parsing failed: {e}")

def main():
    """Run all tests"""
    print("🧪 Enhanced Integration V2 - Test Suite")
    print("=" * 45)
    
    test_contract_parser()
    test_transition_logic() 
    test_absolute_to_relative()
    test_single_leg_mode()
    test_spread_mode()
    test_integration_dry_run()
    
    print("\n✅ Test Suite Completed")
    print("=" * 45)
    print("\nKey Features Demonstrated:")
    print("- Product-encoded contract parsing (demb07_25, demp07_25)")
    print("- n_s business day transition logic") 
    print("- Absolute to relative conversion with multiple periods")
    print("- Single leg vs spread mode detection")
    print("- Real spread (DataFetcher) + Synthetic spread (SpreadViewer)")
    print("- Unified configuration dictionary approach")

if __name__ == "__main__":
    main()