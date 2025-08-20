#!/usr/bin/env python3
"""
Test SpreadViewer order book loading to understand why it's failing
"""

import sys
import os
import pandas as pd
from datetime import datetime, time

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

try:
    from Utilities.SpreadViewer.SpreadSingle import SpreadSingle
    from Utilities.SpreadViewer.SpreadViewerData import SpreadViewerData
    from Database.TPData import TPData
    SPREADVIEWER_AVAILABLE = True
    print("✅ SpreadViewer imports successful")
except ImportError as e:
    SPREADVIEWER_AVAILABLE = False
    print(f"❌ SpreadViewer import failed: {e}")

def test_spreadviewer_orders():
    print("🔍 TESTING SPREADVIEWER ORDER BOOK LOADING")
    print("=" * 50)
    
    if not SPREADVIEWER_AVAILABLE:
        print("❌ Cannot test - SpreadViewer not available")
        return
    
    # Small test configuration  
    markets = ['de']
    tenors = ['m']
    tn1_list = ['M1']  # June relative to February
    tn2_list = ['M2']  # July relative to February
    coefficients = [1, -1]
    n_s = 3
    
    # Short date range for testing
    start_date = datetime(2025, 2, 6)
    end_date = datetime(2025, 2, 7)  # Just 2 days
    dates = pd.date_range(start_date, end_date, freq='B')
    
    print(f"📅 Test Configuration:")
    print(f"   Markets: {markets}")
    print(f"   Tenors: {tenors}")
    print(f"   TN1: {tn1_list}, TN2: {tn2_list}")
    print(f"   Dates: {len(dates)} business days ({start_date.date()} to {end_date.date()})")
    print()
    
    try:
        # Initialize SpreadViewer components
        print("🔧 Initializing SpreadViewer...")
        spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
        data_class = SpreadViewerData()
        db_class = TPData()
        
        print(f"✅ SpreadSingle created")
        print(f"✅ SpreadViewerData created")
        print(f"✅ TPData created")
        
        # Check tenors list
        tenors_list = spread_class.tenors_list
        print(f"📋 Tenors list: {tenors_list}")
        
        # Check product dates
        product_dates = spread_class.product_dates(dates, n_s)
        print(f"📋 Product dates: {len(product_dates)} entries")
        for i, pd_entry in enumerate(product_dates[:5]):  # Show first 5
            print(f"   {i}: {pd_entry}")
        
        # Trading hours
        start_time = time(9, 0, 0)
        end_time = time(10, 0, 0)  # Very short time window
        
        print(f"📋 Time window: {start_time} to {end_time}")
        print()
        
        # Test 1: Try loading order book data with timeout
        print("📊 Test 1: Loading Order Book Data (with timeout)")
        print("-" * 40)
        
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("SpreadViewer load_best_order_otc timed out")
        
        # Set 30-second timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(30)
        
        try:
            data_class.load_best_order_otc(
                markets, tenors_list,
                product_dates,
                db_class,
                start_time=start_time, 
                end_time=end_time
            )
            signal.alarm(0)  # Cancel timeout
            print("✅ Order book loading completed successfully")
            
            # Check what was loaded
            if hasattr(data_class, 'data') and data_class.data is not None:
                print(f"📊 Loaded data shape: {data_class.data.shape}")
                print(f"📊 Loaded data columns: {list(data_class.data.columns)}")
                print("📊 Sample data:")
                print(data_class.data.head(3).to_string())
            else:
                print("❌ No data was loaded")
                
        except TimeoutError:
            signal.alarm(0)
            print("❌ Order book loading timed out after 30 seconds")
            
        except Exception as e:
            signal.alarm(0)
            print(f"❌ Order book loading failed: {e}")
            import traceback
            traceback.print_exc()
        
    except Exception as e:
        print(f"❌ SpreadViewer initialization failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_spreadviewer_orders()