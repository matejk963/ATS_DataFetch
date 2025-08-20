#!/usr/bin/env python3
"""
Test individual contract data availability to find working date ranges
"""

import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

def test_individual_contracts():
    """Test various individual contracts to find data availability"""
    print("🔍 TESTING INDIVIDUAL CONTRACT DATA AVAILABILITY")
    print("=" * 60)
    
    try:
        from src.core.data_fetcher import DataFetcher
        
        # Initialize DataFetcher
        fetcher = DataFetcher()
        
        # Test different contract combinations and date ranges
        test_cases = [
            {
                'name': 'January 2025 contracts',
                'contracts': [
                    {'market': 'de', 'prod': 'base', 'tenor': 'm', 'contract': 'debm01_25'},
                    {'market': 'de', 'prod': 'base', 'tenor': 'm', 'contract': 'debm03_25'}
                ],
                'date_range': ('2025-01-15', '2025-01-17')
            },
            {
                'name': 'December 2024 contracts (historical)',
                'contracts': [
                    {'market': 'de', 'prod': 'base', 'tenor': 'm', 'contract': 'debm12_24'},
                    {'market': 'de', 'prod': 'base', 'tenor': 'm', 'contract': 'debm01_25'}
                ],
                'date_range': ('2024-12-10', '2024-12-12')
            },
            {
                'name': 'November 2024 contracts (historical)',
                'contracts': [
                    {'market': 'de', 'prod': 'base', 'tenor': 'm', 'contract': 'debm11_24'},
                    {'market': 'de', 'prod': 'base', 'tenor': 'm', 'contract': 'debm12_24'}
                ],
                'date_range': ('2024-11-15', '2024-11-17')
            }
        ]
        
        for test_case in test_cases:
            print(f"\\n📊 Testing: {test_case['name']}")
            print(f"   Date range: {test_case['date_range'][0]} to {test_case['date_range'][1]}")
            print("-" * 50)
            
            has_data = False
            
            for i, contract in enumerate(test_case['contracts']):
                print(f"\\n   Contract {i+1}: {contract['contract']} ({contract['market']}-{contract['prod']}-{contract['tenor']})")
                
                try:
                    # Build contract config
                    contract_config = {
                        'market': contract['market'],
                        'prod': contract['prod'],
                        'tenor': contract['tenor'],
                        'contract': contract['contract'],
                        'start_date': test_case['date_range'][0],
                        'end_date': test_case['date_range'][1]
                    }
                    
                    # Fetch contract data
                    data = fetcher.fetch_contract_data(contract_config, include_trades=True, include_orders=True)
                    
                    orders = data.get('orders', pd.DataFrame())
                    trades = data.get('trades', pd.DataFrame())
                    
                    print(f"      📋 Orders: {len(orders)} records")
                    print(f"      📈 Trades: {len(trades)} records")
                    
                    if len(orders) > 0 or len(trades) > 0:
                        has_data = True
                        print(f"      ✅ HAS DATA!")
                        
                        if len(orders) > 0:
                            print(f"      📋 Order sample:")
                            print(f"         {orders.head(2)}")
                            
                        if len(trades) > 0:
                            print(f"      📈 Trade sample:")  
                            print(f"         {trades.head(2)}")
                    else:
                        print(f"      ⚠️  No data found")
                
                except Exception as e:
                    print(f"      ❌ Error: {e}")
            
            if has_data:
                print(f"\\n   🎉 {test_case['name']} - GOOD CANDIDATE FOR SPREADVIEWER TESTING!")
            else:
                print(f"\\n   💔 {test_case['name']} - No data found")
        
        print(f"\\n🎯 RECOMMENDATION")
        print("=" * 40)
        print("   Use the date range with the most individual contract data")
        print("   for SpreadViewer testing to ensure synthetic spread creation.")
                
    except Exception as e:
        print(f"❌ Individual contract test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'
    test_individual_contracts()