#!/usr/bin/env python3
"""
Test script to investigate why we're getting 0 order book records
"""

import sys
import os

# Add project paths
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

from src.core.data_fetcher import DataFetcher

def test_order_book_issue():
    print("🔍 INVESTIGATING ORDER BOOK DATA ISSUE")
    print("=" * 50)
    
    try:
        # Initialize DataFetcher
        fetcher = DataFetcher(allowed_broker_ids=[1441])
        
        # Test 1: Single contract (should work if order book data exists)
        print("\n📊 Test 1: Single Contract Order Book")
        print("-" * 40)
        single_config = {
            'market': 'de',
            'tenor': 'm',
            'contract': '06_25',
            'start_date': '2025-02-03',
            'end_date': '2025-02-10',  # Short range for testing
            'prod': 'base'
        }
        
        single_result = fetcher.fetch_contract_data(single_config, include_trades=True, include_orders=True)
        
        print(f"Single contract orders: {len(single_result.get('orders', []))}")
        print(f"Single contract trades: {len(single_result.get('trades', []))}")
        
        if 'orders' in single_result and len(single_result['orders']) > 0:
            print("✅ Single contract HAS order book data")
            print("Sample order columns:", list(single_result['orders'].columns))
            print("Sample order data:")
            print(single_result['orders'].head(3).to_string())
        else:
            print("❌ Single contract has NO order book data")
        
        # Test 2: Spread contract (current issue)
        print("\n📊 Test 2: Spread Contract Order Book")
        print("-" * 40)
        
        contract1_config = {
            'market': 'de',
            'tenor': 'm', 
            'contract': '06_25',
            'start_date': '2025-02-03',
            'end_date': '2025-02-10',
            'prod': 'base'
        }
        
        contract2_config = {
            'market': 'de',
            'tenor': 'm',
            'contract': '07_25', 
            'start_date': '2025-02-03',
            'end_date': '2025-02-10',
            'prod': 'base'
        }
        
        spread_result = fetcher.fetch_spread_contract_data(
            contract1_config, contract2_config,
            include_trades=True, include_orders=True
        )
        
        print(f"Spread contract orders: {len(spread_result.get('spread_orders', []))}")
        print(f"Spread contract trades: {len(spread_result.get('spread_trades', []))}")
        
        if 'spread_orders' in spread_result and len(spread_result['spread_orders']) > 0:
            print("✅ Spread contract HAS order book data")
            print("Sample spread order columns:", list(spread_result['spread_orders'].columns))
            print("Sample spread order data:")
            print(spread_result['spread_orders'].head(3).to_string())
        else:
            print("❌ Spread contract has NO order book data")
        
        print(f"\n📋 All spread result keys: {list(spread_result.keys())}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_order_book_issue()