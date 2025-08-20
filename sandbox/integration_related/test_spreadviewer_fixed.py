#!/usr/bin/env python3
"""
Test SpreadViewer with pandas compatibility fix
"""

import sys
import os
import pandas as pd
from datetime import datetime, time

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

def test_fixed_product_dates():
    print("🔍 TESTING FIXED SPREADVIEWER PRODUCT_DATES")
    print("=" * 50)
    
    try:
        from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
        print("✅ SpreadViewer imports successful")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return
    
    # Test configuration
    markets = ['de']
    tenors = ['m']
    tn1_list = ['M4', 'M5']  # June=M4, July=M5 relative to Feb
    tn2_list = []
    n_s = 3
    
    # Create date range
    start_date = datetime(2025, 2, 6)
    end_date = datetime(2025, 2, 6)  # Single day
    dates = pd.date_range(start_date, end_date, freq='B')
    
    print(f"📅 Testing with dates: {dates}")
    print(f"📅 n_s parameter: {n_s}")
    
    # Initialize SpreadSingle
    spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
    
    print(f"📋 Tenor list: {spread_class.tenor_list}")
    print(f"📋 TN1 list: {spread_class.tn1_list}")
    
    # Test the problematic method with manual fix
    def fixed_product_dates(self, dates, n_s, tn_bool=True):
        """Fixed version of product_dates method"""
        if tn_bool:
            tn_list = self.tn1_list
        else:
            tn_list = self.tn2_list
        if not tn_list:
            return [None] * len(self.tenor_list)
        
        print(f"   🔧 Processing tenor_list: {self.tenor_list}")
        print(f"   🔧 Processing tn_list: {tn_list}")
        
        pd_list = []
        for t, tn in zip(self.tenor_list, tn_list):
            print(f"   🔧 Processing tenor '{t}' with tn '{tn}'")
            
            if (t == 'da') or (t == 'd'):
                result = dates.shift(1, freq='B')
            elif t == 'w':
                result = dates.shift(tn, freq='W-MON')
            elif t == 'dec':
                result = dates.shift(tn, freq='YS')
            elif t == 'm1q':
                result = dates.shift(tn, freq='QS')
            elif t in ['sum']:
                # Fixed: use periods instead of freq multiplication
                shifted_dates = dates.shift(periods=n_s, freq=dates.freq)
                result = shifted_dates.shift(tn, freq='AS-Apr')
            elif t in ['win']:
                # Fixed: use periods instead of freq multiplication  
                shifted_dates = dates.shift(periods=n_s, freq=dates.freq)
                result = shifted_dates.shift(tn, freq='AS-Oct')
            else:
                # Fixed: use periods instead of freq multiplication
                shifted_dates = dates.shift(periods=n_s, freq=dates.freq)
                result = shifted_dates.shift(tn, freq=t.upper() + 'S')
            
            pd_list.append(result)
            print(f"   ✅ Result for {t}/{tn}: {result}")
            
        return pd_list
    
    # Test the fixed method
    print(f"\n🔧 Testing fixed product_dates method...")
    try:
        # Monkey patch the method temporarily
        original_method = spread_class.product_dates
        spread_class.product_dates = lambda dates, n_s, tn_bool=True: fixed_product_dates(spread_class, dates, n_s, tn_bool)
        
        product_dates = spread_class.product_dates(dates, n_s)
        print(f"✅ Fixed product_dates successful!")
        print(f"📊 Product dates result: {product_dates}")
        
        # Restore original method
        spread_class.product_dates = original_method
        
        return True
        
    except Exception as e:
        print(f"❌ Fixed method also failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_fixed_product_dates()