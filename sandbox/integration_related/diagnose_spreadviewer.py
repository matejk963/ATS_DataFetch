#!/usr/bin/env python3
"""
Diagnostic script to investigate SpreadViewer empty results issue
"""

import sys
import os
import pandas as pd
from datetime import datetime, time
import traceback

# Add paths
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

def diagnose_spreadviewer_data():
    """Diagnose why SpreadViewer produces 0 orders/trades"""
    print("🔍 DIAGNOSING SPREADVIEWER DATA AVAILABILITY")
    print("=" * 60)
    
    try:
        # Import required classes
        from SynthSpread.spreadviewer_class import SpreadSingle
        from SynthSpread.spreadviewer_data import SpreadViewerData
        from DBConfig.tpdata import TPData
        
        # Apply pandas compatibility patch
        from sandbox.integration_related.patch_spreadviewer import patch_spreadviewer
        patch_spreadviewer()
        
        # Test configuration matching the integration script
        markets = ['de', 'de']
        tenors = ['m', 'm']
        tn1_list = ['M6', 'M7']  # June=M6, July=M7 (absolute contracts demb06_25, demb07_25)
        tn2_list = []
        coefficients = [1.0, -1.0]
        n_s = 3
        
        # Small date range for testing
        start_date = datetime(2025, 2, 6)
        end_date = datetime(2025, 2, 6)  # Single day
        dates = pd.date_range(start_date, end_date, freq='B')
        
        print(f"📅 Testing with dates: {list(dates)}")
        print(f"🔧 Configuration:")
        print(f"   Markets: {markets}")
        print(f"   Tenors: {tenors}")
        print(f"   TN1 list: {tn1_list}")
        print(f"   Coefficients: {coefficients}")
        print(f"   n_s: {n_s}")
        
        # Initialize classes
        spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
        data_class = SpreadViewerData()
        data_class_tr = SpreadViewerData()
        db_class = TPData()
        
        # Test product_dates calculation
        print(f"\\n📊 TESTING PRODUCT_DATES")
        print("-" * 40)
        try:
            product_dates = spread_class.product_dates(dates, n_s)
            print(f"✅ Product dates calculated successfully: {len(product_dates)} entries")
            for i, pd_entry in enumerate(product_dates):
                print(f"   [{i}]: {pd_entry}")
        except Exception as e:
            print(f"❌ Product dates failed: {e}")
            traceback.print_exc()
            return
        
        # Check tenors list
        tenors_list = spread_class.tenors_list
        print(f"\\n🔧 Tenors list: {tenors_list}")
        
        # Test data loading with verbose output
        start_time = time(9, 0, 0)
        end_time = time(17, 25, 0)
        
        print(f"\\n📡 TESTING ORDER BOOK DATA LOADING")
        print("-" * 40)
        try:
            print(f"   Loading for markets: {markets}")
            print(f"   Tenors list: {tenors_list}")
            print(f"   Product dates: {product_dates}")
            print(f"   Time range: {start_time} - {end_time}")
            
            data_class.load_best_order_otc(
                markets, tenors_list,
                product_dates,
                db_class,
                start_time=start_time, end_time=end_time
            )
            
            # Check what data was loaded
            print(f"   ✅ Order book loading completed")
            if hasattr(data_class, 'data_df') and data_class.data_df is not None:
                print(f"   📊 Loaded order book data shape: {data_class.data_df.shape}")
                if not data_class.data_df.empty:
                    print(f"   📋 Columns: {list(data_class.data_df.columns)}")
                    print(f"   📋 Index: {data_class.data_df.index}")
                    print("   📋 Sample data:")
                    print(data_class.data_df.head())
                else:
                    print("   ⚠️  Order book data is EMPTY")
            else:
                print("   ⚠️  No data_df attribute found")
                
        except Exception as e:
            print(f"   ❌ Order book loading failed: {e}")
            traceback.print_exc()
        
        print(f"\\n📈 TESTING TRADE DATA LOADING")  
        print("-" * 40)
        try:
            data_class_tr.load_trades_otc(
                markets, tenors_list, db_class,
                start_time=start_time, end_time=end_time
            )
            
            print(f"   ✅ Trade data loading completed")
            if hasattr(data_class_tr, 'data_df') and data_class_tr.data_df is not None:
                print(f"   📊 Loaded trade data shape: {data_class_tr.data_df.shape}")
                if not data_class_tr.data_df.empty:
                    print(f"   📋 Columns: {list(data_class_tr.data_df.columns)}")
                    print(f"   📋 Index: {data_class_tr.data_df.index}")
                    print("   📋 Sample data:")
                    print(data_class_tr.data_df.head())
                else:
                    print("   ⚠️  Trade data is EMPTY")
            else:
                print("   ⚠️  No data_df attribute found")
                
        except Exception as e:
            print(f"   ❌ Trade data loading failed: {e}")
            traceback.print_exc()
        
        print(f"\\n🔄 TESTING DATA AGGREGATION")
        print("-" * 40)
        for d in dates:
            print(f"\\n   📅 Processing date: {d}")
            d_range = pd.date_range(d, d)
            
            try:
                # Aggregate order book data
                data_dict = spread_class.aggregate_data(
                    data_class, d_range, n_s,
                    start_time=start_time, end_time=end_time
                )
                
                print(f"   ✅ Data aggregation completed")
                print(f"   📊 Data dict keys: {list(data_dict.keys()) if data_dict else 'None'}")
                
                if data_dict:
                    for key, value in data_dict.items():
                        if isinstance(value, pd.DataFrame):
                            print(f"     {key}: DataFrame {value.shape}")
                            if not value.empty:
                                print(f"       Sample: {value.head(2)}")
                        else:
                            print(f"     {key}: {type(value)} - {value}")
                
                # Test spread creation
                print(f"   🎯 Testing spread creation...")
                sm = spread_class.spread_maker(data_dict, coefficients, trade_type=['cmb', 'cmb']).dropna()
                print(f"   📊 Spread orders created: {len(sm)} rows")
                
                if not sm.empty:
                    print(f"   ✅ Non-empty spread orders!")
                    print(f"   📋 Columns: {list(sm.columns)}")
                    print(f"   📋 Sample orders:")
                    print(sm.head())
                else:
                    print(f"   ⚠️  Empty spread orders")
                
                # Test trade creation if orders exist
                if not sm.empty:
                    print(f"   🔄 Testing trade creation...")
                    col_list = ['bid', 'ask', 'volume', 'broker_id']
                    trade_dict = spread_class.aggregate_data(
                        data_class_tr, d_range, n_s, gran='1s',
                        start_time=start_time, end_time=end_time,
                        col_list=col_list, data_dict=data_dict
                    )
                    
                    tm = spread_class.add_trades(data_dict, trade_dict, coefficients, [True, True])
                    print(f"   📊 Spread trades created: {len(tm)} rows")
                    
                    if not tm.empty:
                        print(f"   ✅ Non-empty spread trades!")
                        print(f"   📋 Sample trades:")
                        print(tm.head())
                    else:
                        print(f"   ⚠️  Empty spread trades")
                
            except Exception as e:
                print(f"   ❌ Data processing failed for {d}: {e}")
                print("   🔍 Exception details:")
                traceback.print_exc()
        
        print(f"\\n🎯 DIAGNOSIS SUMMARY")
        print("=" * 40)
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ Diagnosis failed: {e}")
        traceback.print_exc()


def check_underlying_contracts():
    """Check what individual contract data exists for the underlying contracts"""
    print("\\n🔍 CHECKING UNDERLYING CONTRACT DATA")
    print("=" * 50)
    
    try:
        sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
        from src.core.data_fetcher import DataFetcher
        
        # Initialize DataFetcher
        fetcher = DataFetcher()
        
        # Test individual contracts that should be used for synthetic spread
        contracts_to_test = [
            {
                'market': 'de',
                'product': 'base',
                'tenor': 'm',
                'delivery_date': '2025-06-01'  # demb06_25
            },
            {
                'market': 'de', 
                'product': 'base',
                'tenor': 'm',
                'delivery_date': '2025-07-01'  # demb07_25
            }
        ]
        
        start_date = '2025-02-06'
        end_date = '2025-02-07'
        
        for i, contract in enumerate(contracts_to_test):
            print(f"\\n   📊 Testing individual contract {i+1}: {contract}")
            
            try:
                # Test trades
                trades = fetcher.fetch_trades(
                    contract['market'], contract['product'], contract['tenor'],
                    contract['delivery_date'], start_date, end_date
                )
                print(f"   📈 Trades: {len(trades)} records")
                
                # Test orders
                orders = fetcher.fetch_orders(
                    contract['market'], contract['product'], contract['tenor'],
                    contract['delivery_date'], start_date, end_date  
                )
                print(f"   📋 Orders: {len(orders)} records")
                
                if not trades.empty:
                    print(f"   ✅ Has trade data - Sample:")
                    print(f"      {trades.head(2)}")
                    
                if not orders.empty:
                    print(f"   ✅ Has order data - Sample:")
                    print(f"      {orders.head(2)}")
                    
                if trades.empty and orders.empty:
                    print(f"   ⚠️  NO DATA for this individual contract")
            
            except Exception as e:
                print(f"   ❌ Contract {i+1} failed: {e}")
                
    except Exception as e:
        print(f"❌ Underlying contract check failed: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    diagnose_spreadviewer_data()
    check_underlying_contracts()