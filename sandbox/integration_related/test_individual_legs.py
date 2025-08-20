#!/usr/bin/env python3
"""
Test individual legs that SpreadViewer should use for synthetic spread creation
"""

import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

def test_individual_legs():
    """Test individual contract legs for the current SpreadViewer configuration"""
    print("🔍 TESTING INDIVIDUAL CONTRACT LEGS")
    print("=" * 60)
    
    try:
        from src.core.data_fetcher import DataFetcher
        
        # Initialize DataFetcher
        fetcher = DataFetcher()
        
        # Current config from integration script
        contracts = ['debm09_25', 'debm10_25']  # Sep/Oct 2025
        date_range = ('2025-07-01', '2025-07-03')  # July 2025 period
        
        print(f"📋 Testing individual legs for spread: {contracts[0]} vs {contracts[1]}")
        print(f"📅 Date range: {date_range[0]} to {date_range[1]}")
        print()
        
        for i, contract_name in enumerate(contracts):
            print(f"\\n📊 Testing Leg {i+1}: {contract_name}")
            print("-" * 40)
            
            try:
                # Build contract config - using the same format as DataFetcher expects
                contract_config = {
                    'market': 'de',
                    'prod': 'base', 
                    'tenor': 'm',
                    'contract': contract_name,
                    'start_date': date_range[0],
                    'end_date': date_range[1]
                }
                
                print(f"   🔧 Config: {contract_config}")
                
                # Fetch contract data
                data = fetcher.fetch_contract_data(contract_config, include_trades=True, include_orders=True)
                
                orders = data.get('orders', pd.DataFrame())
                trades = data.get('trades', pd.DataFrame())
                
                print(f"   📋 Orders: {len(orders)} records")
                print(f"   📈 Trades: {len(trades)} records")
                
                if len(orders) > 0:
                    print(f"   ✅ HAS ORDER DATA!")
                    print(f"   📋 Order sample:")
                    print(f"      {orders.head(2)}")
                    print(f"   📋 Order columns: {list(orders.columns)}")
                    print(f"   📋 Order index range: {orders.index.min()} to {orders.index.max()}")
                
                if len(trades) > 0:
                    print(f"   ✅ HAS TRADE DATA!")
                    print(f"   📈 Trade sample:")  
                    print(f"      {trades.head(2)}")
                    print(f"   📈 Trade columns: {list(trades.columns)}")
                    print(f"   📈 Trade index range: {trades.index.min()} to {trades.index.max()}")
                
                if len(orders) == 0 and len(trades) == 0:
                    print(f"   ⚠️  NO DATA for {contract_name}")
                    print(f"   🤔 This explains why SpreadViewer produces 0 synthetic spreads")
                
            except Exception as e:
                print(f"   ❌ Error fetching {contract_name}: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\\n🎯 ANALYSIS")
        print("=" * 40)
        print("   If both legs show 'NO DATA', then SpreadViewer correctly")  
        print("   returns 0 synthetic spreads - there's nothing to synthesize from.")
        print("   If legs have data but SpreadViewer still fails, there may be")
        print("   a data format issue in the SpreadViewer processing.")
        
    except Exception as e:
        print(f"❌ Individual leg test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_individual_legs()