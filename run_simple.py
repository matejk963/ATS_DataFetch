#!/usr/bin/env python3
"""
Simple data fetch test - no patches, minimal functionality
"""

import sys
import os

# Setup paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'

sys.path.insert(0, project_root)
sys.path.insert(0, energy_trading_path)

print("🚀 Simple Data Fetch Test")
print("=" * 40)

# Import DataFetcher only (skip SpreadViewer for now)
print("1. Importing DataFetcher...")
from src.core.data_fetcher import DataFetcher

print("2. Creating DataFetcher instance...")
fetcher = DataFetcher(allowed_broker_ids=[1441])

print("3. Testing simple contract fetch...")
config = {
    'market': 'de',
    'tenor': 'm',
    'contract': '09_25',
    'start_date': '2025-07-01',
    'end_date': '2025-07-02',
    'prod': 'base'
}

print(f"   Config: {config}")

try:
    print("4. Fetching contract data...")
    result = fetcher.fetch_contract_data(
        config,
        include_trades=True,
        include_orders=True
    )
    
    print("5. Results:")
    print(f"   Orders: {len(result.get('orders', []))} records")
    print(f"   Trades: {len(result.get('trades', []))} records")
    
    if len(result.get('orders', [])) > 0:
        orders_df = result['orders']
        print(f"   Order columns: {list(orders_df.columns)}")
        print(f"   Order sample:")
        print(orders_df.head(3))
    
    if len(result.get('trades', [])) > 0:
        trades_df = result['trades'] 
        print(f"   Trade columns: {list(trades_df.columns)}")
        print(f"   Trade sample:")
        print(trades_df.head(3))
        
    print("✅ SUCCESS: Data fetch completed!")
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()