#!/usr/bin/env python3
"""
Simple demo of the spread data merging functionality
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/integration_related')

from integration_script_v2 import merge_spread_data

def demo():
    print("🚀 Spread Data Merging Demo")
    print("=" * 30)
    
    # Create sample real data (DataFetcher format)
    real_data = {
        'spread_orders': pd.DataFrame({
            'b_price': [45.2, 45.3, 45.4],
            'a_price': [45.8, 45.9, 46.0]
        }, index=pd.date_range('2025-02-03 09:00', periods=3, freq='10min')),
        'spread_trades': pd.DataFrame({
            'price': [45.5, 45.7],
            'volume': [100, 150]
        }, index=pd.date_range('2025-02-03 09:15', periods=2, freq='15min'))
    }
    
    # Create sample synthetic data (SpreadViewer format)  
    synthetic_data = {
        'spread_orders': pd.DataFrame({
            'bid': [45.1, 45.5, 45.3],     # Different column names!
            'ask': [45.7, 46.1, 45.9]
        }, index=pd.date_range('2025-02-03 09:05', periods=3, freq='12min')),
        'spread_trades': pd.DataFrame({
            'buy': [45.4, 45.6],
            'sell': [np.nan, np.nan],
            'volume': [75, 125]
        }, index=pd.date_range('2025-02-03 09:12', periods=2, freq='18min'))
    }
    
    print("📊 Before merging:")
    print(f"Real: {len(real_data['spread_orders'])} orders, {len(real_data['spread_trades'])} trades")
    print(f"Synthetic: {len(synthetic_data['spread_orders'])} orders, {len(synthetic_data['spread_trades'])} trades")
    
    # Merge the data
    merged = merge_spread_data(real_data, synthetic_data)
    
    print(f"\n🎉 After merging:")
    print(f"Merged: {len(merged['spread_orders'])} orders, {len(merged['spread_trades'])} trades")
    
    print(f"\n📈 Sample merged orders:")
    print(merged['spread_orders'].head())
    
    print(f"\n💰 Sample merged trades:")
    print(merged['spread_trades'].head())

if __name__ == "__main__":
    demo()