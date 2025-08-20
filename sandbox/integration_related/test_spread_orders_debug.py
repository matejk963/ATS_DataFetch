#!/usr/bin/env python3

"""
Debug script to check if cross-market spread orders exist in database
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/src')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

from core.data_fetcher import DataFetcher
from datetime import datetime, time
import pandas as pd

def test_spread_orders():
    """Test if cross-market spread orders exist"""
    
    print("🔍 Testing cross-market spread order data availability...")
    
    try:
        fetcher = DataFetcher(allowed_broker_ids=[1441])
        
        # Test contracts
        contract1_config = {
            'market': 'de_fr',  # Cross-market combination
            'tenor': 'm',
            'contract': '08_25',
            'start_date': '2025-07-01',
            'end_date': '2025-07-05',
            'prod': 'base'
        }
        
        contract2_config = {
            'market': 'de_fr',  # Cross-market combination  
            'tenor': 'm',
            'contract': '08_25',
            'start_date': '2025-07-01',
            'end_date': '2025-07-05',
            'prod': 'base'
        }
        
        print(f"📋 Test configuration:")
        print(f"   Market: {contract1_config['market']}")
        print(f"   Period: {contract1_config['start_date']} to {contract1_config['end_date']}")
        
        # Test spread order fetch
        print(f"\n🔍 Testing spread order fetch...")
        spread_data = fetcher.fetch_spread_contract_data(
            contract1_config, contract2_config,
            include_trades=True, include_orders=True
        )
        
        print(f"\n📊 Results:")
        print(f"   Spread orders: {len(spread_data.get('spread_orders', pd.DataFrame()))}")
        print(f"   Spread trades: {len(spread_data.get('spread_trades', pd.DataFrame()))}")
        
        # Test individual market orders for comparison
        print(f"\n🔍 Testing individual market orders for comparison...")
        
        individual_markets = ['de', 'fr']
        for market in individual_markets:
            individual_config = contract1_config.copy()
            individual_config['market'] = market
            
            try:
                individual_data = fetcher.fetch_contract_data(
                    individual_config, include_trades=True, include_orders=True
                )
                
                orders_count = len(individual_data.get('orders', pd.DataFrame()))
                trades_count = len(individual_data.get('trades', pd.DataFrame()))
                
                print(f"   {market}: {orders_count} orders, {trades_count} trades")
                
            except Exception as e:
                print(f"   {market}: Error - {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_spread_orders()