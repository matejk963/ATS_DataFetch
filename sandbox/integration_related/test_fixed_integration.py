#!/usr/bin/env python3
"""
Test the fixed integration script with correct SpreadViewer parameter format
"""

import sys
import os
from datetime import datetime

# Set up paths
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def test_fixed_integration():
    """Test the integration script with corrected parameter format"""
    print("🔍 TESTING FIXED INTEGRATION SCRIPT")
    print("=" * 50)
    
    try:
        from integration_script_v2 import integrated_fetch
        
        # Test configuration matching the working SpreadViewer test
        test_config = {
            'contracts': ['debm09_25', 'debm10_25'],  # M3/M4 for July dates (Sep/Oct 2025 delivery)
            'period': {
                'start_date': '2024-12-02',
                'end_date': '2024-12-06'
            },
            'n_s': 3,
            'mode': 'spread'
        }
        
        print(f"📋 Test Configuration:")
        print(f"   Contracts: {test_config['contracts']}")
        print(f"   Period: {test_config['period']['start_date']} to {test_config['period']['end_date']}")
        print(f"   n_s: {test_config['n_s']}")
        print(f"   Mode: {test_config['mode']}")
        
        print(f"\n🚀 Running integration analysis...")
        result = integrated_fetch(test_config)
        
        print(f"\n📊 INTEGRATION RESULTS:")
        print("-" * 30)
        
        if result:
            print(f"✅ Integration completed successfully")
            
            # Check synthetic spread data
            if 'synthetic_spread' in result:
                synthetic = result['synthetic_spread']
                orders = synthetic.get('spread_orders', None)
                trades = synthetic.get('spread_trades', None)
                
                print(f"🔧 Synthetic Spread Data:")
                if orders is not None and not orders.empty:
                    print(f"   📊 Orders: {len(orders)} records")
                    print(f"   📋 Order columns: {list(orders.columns)}")
                    print(f"   📈 Sample orders:")
                    print(f"      {orders.head(2)}")
                else:
                    print(f"   ⚠️  No synthetic spread orders")
                
                if trades is not None and not trades.empty:
                    print(f"   💹 Trades: {len(trades)} records")
                else:
                    print(f"   ⚠️  No synthetic spread trades")
            
            # Check real spread data
            if 'real_spread' in result:
                real = result['real_spread']
                if real and 'data' in real and not real['data'].empty:
                    print(f"📡 Real Spread Data: {len(real['data'])} records")
                else:
                    print(f"⚠️  No real spread data")
            
            # Check merged data
            if 'merged_data' in result:
                merged = result['merged_data']
                orders = merged.get('spread_orders', None)
                trades = merged.get('spread_trades', None)
                
                print(f"🔄 Merged Data:")
                if orders is not None and not orders.empty:
                    print(f"   📊 Total orders: {len(orders)} records")
                else:
                    print(f"   ⚠️  No merged orders")
                
                if trades is not None and not trades.empty:
                    print(f"   💹 Total trades: {len(trades)} records")
                else:
                    print(f"   ⚠️  No merged trades")
        else:
            print(f"❌ Integration returned no results")
        
        # Check if database connectivity issues are resolved
        if result and 'synthetic_spread' in result:
            synthetic = result['synthetic_spread']
            if ('spread_orders' in synthetic and synthetic['spread_orders'].empty and
                'spread_trades' in synthetic and synthetic['spread_trades'].empty):
                print(f"\n🔍 DIAGNOSIS:")
                print(f"   💭 SpreadViewer still returns empty data")
                print(f"   🤔 This suggests database connectivity issues persist")
                print(f"   📡 The JSON parsing error may still be occurring")
            else:
                print(f"\n🎉 SUCCESS!")
                print(f"   ✅ SpreadViewer parameter format fixed")
                print(f"   ✅ Database connectivity working")
                print(f"   ✅ Synthetic spread data generated")
                
    except ImportError as e:
        print(f"❌ Import failed: {e}")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_fixed_integration()