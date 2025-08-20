#!/usr/bin/env python3
"""
Patch SpreadViewer to fix pandas compatibility issues
"""

import sys
import os
import pandas as pd
from datetime import datetime, time
import re

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

def patch_spreadviewer():
    """Apply patches to fix pandas compatibility"""
    
    try:
        from SynthSpread.spreadviewer_class import SpreadSingle
        print("✅ SpreadViewer imported successfully")
    except ImportError as e:
        print(f"❌ SpreadViewer import failed: {e}")
        return False
    
    def fixed_product_dates(self, dates, n_s, tn_bool=True):
        """Fixed version of product_dates method with proper pandas compatibility"""
        if tn_bool:
            tn_list = self.tn1_list
        else:
            tn_list = self.tn2_list
        if not tn_list:
            return [None] * len(self.tenor_list)
        
        print(f"   🔧 Processing with tenor_list: {self.tenor_list}, tn_list: {tn_list}")
        
        pd_list = []
        for t, tn in zip(self.tenor_list, tn_list):
            # Extract numeric value from tn (e.g., 'M4' -> 4)
            if isinstance(tn, str) and tn.startswith('M'):
                match = re.search(r'M(\d+)', tn)
                tn_numeric = int(match.group(1)) if match else 0
            else:
                tn_numeric = int(tn) if isinstance(tn, (int, float)) else 0
            
            print(f"   🔧 Processing tenor '{t}' with tn '{tn}' -> numeric: {tn_numeric}")
            
            if (t == 'da') or (t == 'd'):
                result = dates.shift(1, freq='B')
            elif t == 'w':
                result = dates.shift(tn_numeric, freq='W-MON')
            elif t == 'dec':
                result = dates.shift(tn_numeric, freq='YS') 
            elif t == 'm1q':
                result = dates.shift(tn_numeric, freq='QS')
            elif t in ['sum']:
                # Fixed approach for seasonal frequencies
                shifted_dates = dates.shift(periods=n_s, freq='B')  # Use business days
                result = shifted_dates.shift(tn_numeric, freq='AS-Apr')
            elif t in ['win']:
                # Fixed approach for seasonal frequencies
                shifted_dates = dates.shift(periods=n_s, freq='B')  # Use business days  
                result = shifted_dates.shift(tn_numeric, freq='AS-Oct')
            else:
                # For monthly 'm' and other standard frequencies
                shifted_dates = dates.shift(periods=n_s, freq='B')  # Use business days for n_s shift
                result = shifted_dates.shift(tn_numeric, freq=t.upper() + 'S')
            
            pd_list.append(result)
            print(f"   ✅ Result for {t}/{tn}: {result}")
            
        return pd_list
    
    # Apply the patch
    SpreadSingle.product_dates = fixed_product_dates
    print("✅ SpreadViewer patched successfully!")
    return True

def test_patched_spreadviewer():
    """Test the patched SpreadViewer"""
    print("\n🔍 TESTING PATCHED SPREADVIEWER")
    print("=" * 40)
    
    if not patch_spreadviewer():
        return False
    
    try:
        from SynthSpread.spreadviewer_class import SpreadSingle
        
        # Test configuration  
        markets = ['de']
        tenors = ['m']
        tn1_list = ['M4', 'M5']  # June=M4, July=M5 relative to Feb
        tn2_list = []
        n_s = 3
        
        # Single day test
        start_date = datetime(2025, 2, 6)
        end_date = datetime(2025, 2, 6)
        dates = pd.date_range(start_date, end_date, freq='B')
        
        print(f"📅 Testing with: {dates}")
        
        # Initialize SpreadSingle
        spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
        
        # Test product_dates (should now work)
        product_dates = spread_class.product_dates(dates, n_s)
        
        print(f"✅ product_dates successful!")
        print(f"📊 Product dates: {len(product_dates)} entries")
        for i, pd_entry in enumerate(product_dates):
            print(f"   {i}: {pd_entry}")
        
        return True
        
    except Exception as e:
        print(f"❌ Patched SpreadViewer test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_patched_spreadviewer()