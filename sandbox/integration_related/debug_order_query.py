#!/usr/bin/env python3

"""
Debug script to inspect database queries for cross-market spread orders
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/src')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

from Database.TPData import TPData
from datetime import datetime, time
import pandas as pd

def debug_order_queries():
    """Debug what happens with cross-market order queries"""
    
    print("🔍 Debugging cross-market order queries...")
    
    try:
        # Create TPData instance for PostgreSQL
        data_class = TPData()
        data_class.create_connection('PostgreSQL')
        
        # Test parameters
        market = 'de_fr'  # Cross-market
        tenor = 'm'
        venue_list = ['eex']
        start_date1 = datetime(2025, 8, 1)  # Product delivery date
        bT = datetime(2025, 7, 1, 9, 0)     # Query start time
        eT = datetime(2025, 7, 1, 17, 30)   # Query end time
        prod = 'base'
        
        print(f"📋 Query parameters:")
        print(f"   Market: {market}")
        print(f"   Tenor: {tenor}")
        print(f"   Product date: {start_date1}")
        print(f"   Time range: {bT} to {eT}")
        
        # Check what orders_params generates
        print(f"\n🔍 Checking orders_params...")
        try:
            params = data_class.orders_params(market, tenor, venue_list, prod, start_date1, None, bT, eT)
            print(f"   Generated parameters: {params}")
        except Exception as e:
            print(f"   ❌ orders_params failed: {e}")
        
        # Try the actual get_best_ob_data call
        print(f"\n🔍 Testing get_best_ob_data...")
        try:
            result = data_class.get_best_ob_data(market, tenor, venue_list, start_date1, bT, eT, prod)
            print(f"   ✅ Query successful: {len(result)} rows")
            if len(result) > 0:
                print(f"   Sample columns: {list(result.columns)}")
                print(f"   Sample data:\n{result.head()}")
            else:
                print(f"   ⚠️  No data returned for market '{market}'")
        except Exception as e:
            print(f"   ❌ get_best_ob_data failed: {e}")
            import traceback
            traceback.print_exc()
        
        # For comparison, test individual markets
        print(f"\n🔍 Testing individual markets for comparison...")
        individual_markets = ['de', 'fr']
        
        for test_market in individual_markets:
            try:
                result = data_class.get_best_ob_data(test_market, tenor, venue_list, start_date1, bT, eT, prod)
                print(f"   {test_market}: {len(result)} rows")
            except Exception as e:
                print(f"   {test_market}: Error - {e}")
        
        data_class.disconnect_from_database()
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_order_queries()