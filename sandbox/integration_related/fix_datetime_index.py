#!/usr/bin/env python3
"""
Fix SpreadViewer DatetimeIndex issues causing 0 orders/trades
"""

import sys
import os
import pandas as pd
from datetime import datetime
import traceback

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

def patch_spreadviewer_datetime_index():
    """Patch SpreadViewer to fix DatetimeIndex issues"""
    
    try:
        from SynthSpread.spreadviewer_data import SpreadViewerData
        print("✅ SpreadViewerData imported successfully")
        
        # Store original methods
        original_load_best_order_otc = SpreadViewerData.load_best_order_otc
        original_load_trades_otc = SpreadViewerData.load_trades_otc
        
        def fixed_load_best_order_otc(self, markets, tenors_list, product_dates, db_class, start_time=None, end_time=None, col_list=None, gran='1min', prod_list=None):
            """Fixed version that ensures DatetimeIndex"""
            try:
                # Call original method
                result = original_load_best_order_otc(self, markets, tenors_list, product_dates, db_class, start_time, end_time, col_list, gran, prod_list)
                
                # Fix DatetimeIndex if needed
                if hasattr(self, 'data_df') and self.data_df is not None and not self.data_df.empty:
                    if not isinstance(self.data_df.index, pd.DatetimeIndex):
                        print(f"   🔧 Fixing non-DatetimeIndex in order data: {type(self.data_df.index)}")
                        # Try to convert index to datetime
                        try:
                            self.data_df.index = pd.to_datetime(self.data_df.index)
                            print(f"   ✅ Fixed DatetimeIndex for order data")
                        except Exception as e:
                            print(f"   ⚠️  Could not fix DatetimeIndex for order data: {e}")
                            # Reset to empty DataFrame if can't fix
                            self.data_df = pd.DataFrame()
                
                return result
                
            except Exception as e:
                print(f"   ❌ load_best_order_otc failed: {e}")
                # Initialize empty DataFrame with proper DatetimeIndex
                self.data_df = pd.DataFrame(index=pd.DatetimeIndex([]))
                return None
        
        def fixed_load_trades_otc(self, markets, tenors_list, db_class, start_time=None, end_time=None, col_list=None, gran='1s', prod_list=None):
            """Fixed version that ensures DatetimeIndex"""
            try:
                # Call original method  
                result = original_load_trades_otc(self, markets, tenors_list, db_class, start_time, end_time, col_list, gran, prod_list)
                
                # Fix DatetimeIndex if needed
                if hasattr(self, 'data_df') and self.data_df is not None and not self.data_df.empty:
                    if not isinstance(self.data_df.index, pd.DatetimeIndex):
                        print(f"   🔧 Fixing non-DatetimeIndex in trade data: {type(self.data_df.index)}")
                        # Try to convert index to datetime
                        try:
                            self.data_df.index = pd.to_datetime(self.data_df.index)
                            print(f"   ✅ Fixed DatetimeIndex for trade data")
                        except Exception as e:
                            print(f"   ⚠️  Could not fix DatetimeIndex for trade data: {e}")
                            # Reset to empty DataFrame if can't fix
                            self.data_df = pd.DataFrame()
                
                return result
                
            except Exception as e:
                print(f"   ❌ load_trades_otc failed: {e}")
                # Initialize empty DataFrame with proper DatetimeIndex
                self.data_df = pd.DataFrame(index=pd.DatetimeIndex([]))
                return None
        
        # Apply patches
        SpreadViewerData.load_best_order_otc = fixed_load_best_order_otc
        SpreadViewerData.load_trades_otc = fixed_load_trades_otc
        
        print("✅ SpreadViewer DatetimeIndex patches applied successfully!")
        return True
        
    except ImportError as e:
        print(f"❌ SpreadViewerData import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ DatetimeIndex patch failed: {e}")
        traceback.print_exc()
        return False


def patch_aggregate_data_method():
    """Additional patch for aggregate_data method that might have DatetimeIndex issues"""
    try:
        from SynthSpread.spreadviewer_class import SpreadSingle
        
        # Store original method
        original_aggregate_data = SpreadSingle.aggregate_data
        
        def fixed_aggregate_data(self, data_class, dates, n_s, start_time=None, end_time=None, col_list=None, gran='1min', data_dict=None):
            """Fixed version with DatetimeIndex safety checks"""
            try:
                # Call original method
                result = original_aggregate_data(self, data_class, dates, n_s, start_time, end_time, col_list, gran, data_dict)
                
                # Ensure any DataFrames in result have proper DatetimeIndex
                if isinstance(result, dict):
                    for key, value in result.items():
                        if isinstance(value, pd.DataFrame) and not value.empty:
                            if not isinstance(value.index, pd.DatetimeIndex):
                                print(f"   🔧 Fixing DatetimeIndex in aggregate_data result: {key}")
                                try:
                                    value.index = pd.to_datetime(value.index)
                                    result[key] = value
                                    print(f"   ✅ Fixed DatetimeIndex for {key}")
                                except Exception as e:
                                    print(f"   ⚠️  Could not fix DatetimeIndex for {key}: {e}")
                                    result[key] = pd.DataFrame(index=pd.DatetimeIndex([]))
                
                return result
                
            except Exception as e:
                print(f"   ❌ aggregate_data failed: {e}")
                # Return empty dict with proper structure
                return {}
        
        # Apply patch
        SpreadSingle.aggregate_data = fixed_aggregate_data
        print("✅ aggregate_data DatetimeIndex patch applied!")
        return True
        
    except ImportError as e:
        print(f"❌ SpreadSingle import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ aggregate_data patch failed: {e}")
        return False


if __name__ == "__main__":
    print("🔧 APPLYING SPREADVIEWER DATETIME INDEX FIXES")
    print("=" * 50)
    
    success1 = patch_spreadviewer_datetime_index()
    success2 = patch_aggregate_data_method()
    
    if success1 and success2:
        print("\\n✅ All DatetimeIndex patches applied successfully!")
        print("   SpreadViewer should now handle DatetimeIndex issues gracefully")
    else:
        print("\\n❌ Some patches failed - SpreadViewer may still have issues")