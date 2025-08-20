#!/usr/bin/env python3
"""
Debug SpreadViewer directly to see what's happening with individual contract data
"""

import sys
import os
import pandas as pd
from datetime import datetime, time
import traceback

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def debug_spreadviewer():
    """Debug SpreadViewer processing step by step"""
    print("🔍 DEBUGGING SPREADVIEWER STEP BY STEP")
    print("=" * 60)
    
    try:
        # Import required classes (matching integration script)
        from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
        from Database.TPData import TPData
        
        # Apply pandas compatibility patch for SpreadViewer (from integration script)
        import re
        def fixed_product_dates(self, dates, n_s, tn_bool=True):
            """Fixed version of product_dates method with proper pandas compatibility"""
            if tn_bool:
                tn_list = self.tn1_list
            else:
                tn_list = self.tn2_list
            if not tn_list:
                return [None] * len(self.tenor_list)
            
            pd_list = []
            for t, tn in zip(self.tenor_list, tn_list):
                # Extract numeric value from tn (e.g., 'M4' -> 4)
                if isinstance(tn, str) and tn.startswith('M'):
                    match = re.search(r'M(\d+)', tn)
                    tn_numeric = int(match.group(1)) if match else 0
                else:
                    tn_numeric = int(tn) if isinstance(tn, (int, float)) else 0
                
                if (t == 'da') or (t == 'd'):
                    result = dates.shift(1, freq='B')
                elif t == 'w':
                    result = dates.shift(tn_numeric, freq='W-MON')
                elif t == 'dec':
                    result = dates.shift(tn_numeric, freq='YS') 
                elif t == 'm1q':
                    result = dates.shift(tn_numeric, freq='QS')
                elif t in ['sum']:
                    shifted_dates = dates.shift(periods=n_s, freq='B')
                    result = shifted_dates.shift(tn_numeric, freq='AS-Apr')
                elif t in ['win']:
                    shifted_dates = dates.shift(periods=n_s, freq='B')  
                    result = shifted_dates.shift(tn_numeric, freq='AS-Oct')
                else:
                    shifted_dates = dates.shift(periods=n_s, freq='B')
                    result = shifted_dates.shift(tn_numeric, freq=t.upper() + 'S')
                
                pd_list.append(result)
                
            return pd_list
        
        # Apply the pandas compatibility patch
        SpreadSingle.product_dates = fixed_product_dates
        
        # Configuration matching your successful test
        markets = ['de', 'de']
        tenors = ['m', 'm']
        tn1_list = [3, 4]  # Numeric values for M3, M4 (Sep/Oct 2025)
        tn2_list = []
        coefficients = [1.0, -1.0]
        n_s = 3
        
        # Date range (using your successful range)
        start_date = datetime(2025, 7, 1)
        end_date = datetime(2025, 7, 10)
        dates = pd.date_range(start_date, end_date, freq='B')
        
        print(f"📋 Configuration:")
        print(f"   Markets: {markets}")
        print(f"   Tenors: {tenors}")
        print(f"   TN1 list: {tn1_list}")
        print(f"   Coefficients: {coefficients}")
        print(f"   Dates: {list(dates)}")
        print(f"   n_s: {n_s}")
        
        # Initialize classes
        spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
        data_class = SpreadViewerData()
        data_class_tr = SpreadViewerData()
        db_class = TPData()
        
        # Test product_dates calculation
        print(f"\n📊 STEP 1: Calculate product dates")
        print("-" * 40)
        try:
            product_dates = spread_class.product_dates(dates, n_s)
            print(f"✅ Product dates: {product_dates}")
        except Exception as e:
            print(f"❌ Product dates failed: {e}")
            traceback.print_exc()
            return
        
        # Check tenors list
        tenors_list = spread_class.tenors_list
        print(f"\n🔧 Tenors list: {tenors_list}")
        
        # Test data loading - orders
        start_time = time(9, 0, 0)
        end_time = time(17, 25, 0)
        
        print(f"\n📡 STEP 2: Load order book data")
        print("-" * 40)
        try:
            print(f"   Loading for:")
            print(f"     Markets: {markets}")
            print(f"     Tenors: {tenors_list}")
            print(f"     Product dates: {product_dates}")
            print(f"     Time: {start_time} - {end_time}")
            
            data_class.load_best_order_otc(
                markets, tenors_list,
                product_dates,
                db_class,
                start_time=start_time, end_time=end_time
            )
            
            print(f"   ✅ Order loading completed")
            
            # Check what was loaded
            if hasattr(data_class, 'data_df') and data_class.data_df is not None:
                print(f"   📊 Order data shape: {data_class.data_df.shape}")
                if not data_class.data_df.empty:
                    print(f"   📋 Order columns: {list(data_class.data_df.columns)}")
                    print(f"   📋 Order index type: {type(data_class.data_df.index)}")
                    print(f"   📋 Order sample:")
                    print(f"      {data_class.data_df.head(2)}")
                else:
                    print(f"   ⚠️  Order data DataFrame is EMPTY")
            else:
                print(f"   ⚠️  No data_df attribute or it's None")
                
        except Exception as e:
            print(f"   ❌ Order loading failed: {e}")
            traceback.print_exc()
        
        # Test data loading - trades
        print(f"\n📈 STEP 3: Load trade data")
        print("-" * 40)
        try:
            data_class_tr.load_trades_otc(
                markets, tenors_list, db_class,
                start_time=start_time, end_time=end_time
            )
            
            print(f"   ✅ Trade loading completed")
            
            # Check what was loaded
            if hasattr(data_class_tr, 'data_df') and data_class_tr.data_df is not None:
                print(f"   📊 Trade data shape: {data_class_tr.data_df.shape}")
                if not data_class_tr.data_df.empty:
                    print(f"   📈 Trade columns: {list(data_class_tr.data_df.columns)}")
                    print(f"   📈 Trade index type: {type(data_class_tr.data_df.index)}")
                    print(f"   📈 Trade sample:")
                    print(f"      {data_class_tr.data_df.head(2)}")
                else:
                    print(f"   ⚠️  Trade data DataFrame is EMPTY")
            else:
                print(f"   ⚠️  No data_df attribute or it's None")
                
        except Exception as e:
            print(f"   ❌ Trade loading failed: {e}")
            traceback.print_exc()
        
        # Test data aggregation for each day
        print(f"\n🔄 STEP 4: Data aggregation and spread creation")
        print("-" * 40)
        
        sm_all = pd.DataFrame()
        tm_all = pd.DataFrame()
        
        for i, d in enumerate(dates):
            print(f"\n   📅 Processing date {i+1}: {d}")
            d_range = pd.date_range(d, d)
            
            try:
                # Aggregate order book data
                print(f"      🔧 Aggregating order data...")
                data_dict = spread_class.aggregate_data(
                    data_class, d_range, n_s,
                    start_time=start_time, end_time=end_time
                )
                
                print(f"      ✅ Order aggregation completed")
                print(f"      📊 Data dict keys: {list(data_dict.keys()) if data_dict else 'None'}")
                
                if data_dict:
                    for key, value in data_dict.items():
                        if isinstance(value, pd.DataFrame):
                            print(f"        {key}: DataFrame {value.shape}")
                        else:
                            print(f"        {key}: {type(value)}")
                
                # Create spread orders
                print(f"      🎯 Creating spread orders...")
                sm = spread_class.spread_maker(data_dict, coefficients, trade_type=['cmb', 'cmb']).dropna()
                print(f"      📊 Spread orders created: {len(sm)} rows")
                
                if not sm.empty:
                    print(f"      ✅ SUCCESS! Non-empty spread orders for {d}")
                    print(f"      📋 Order columns: {list(sm.columns)}")
                    print(f"      📋 Sample orders:")
                    print(f"         {sm.head(2)}")
                    sm_all = pd.concat([sm_all, sm], axis=0)
                else:
                    print(f"      ⚠️  Empty spread orders for {d}")
                
            except Exception as e:
                print(f"      ❌ Processing failed for {d}: {e}")
                traceback.print_exc()
        
        print(f"\n🎯 FINAL RESULTS")
        print("=" * 40)
        print(f"   Total spread orders: {len(sm_all)} rows")
        print(f"   Total spread trades: {len(tm_all)} rows")
        
        if len(sm_all) > 0:
            print(f"   🎉 SUCCESS! SpreadViewer created synthetic spread data")
        else:
            print(f"   💔 SpreadViewer produced no synthetic spread data")
            print(f"   🤔 This suggests underlying individual contract data is missing or invalid")
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    debug_spreadviewer()