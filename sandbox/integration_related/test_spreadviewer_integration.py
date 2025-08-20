#!/usr/bin/env python3
"""
Test if SpreadViewer can actually fetch data in the integration setup
"""

import sys
import os
import pandas as pd
from datetime import datetime, time

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python')

def test_spreadviewer_integration():
    print("🔍 TESTING SPREADVIEWER INTEGRATION DATA FETCH")
    print("=" * 55)
    
    try:
        from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
        from Database.TPData import TPData
        print("✅ SpreadViewer imports successful")
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return
    
    # Use the exact same configuration as the integration script
    markets = ['de']
    tenors = ['m']
    tn1_list = ['M4', 'M5']  # June=M4, July=M5 relative to Feb
    tn2_list = []
    coefficients = [1, -1]
    n_s = 3
    
    # Test with very small date range first
    test_configs = [
        {
            'name': 'Single Day Test',
            'start_date': datetime(2025, 2, 6),
            'end_date': datetime(2025, 2, 6),
            'timeout': 60
        },
        {
            'name': 'One Week Test', 
            'start_date': datetime(2025, 2, 6),
            'end_date': datetime(2025, 2, 12),
            'timeout': 180
        },
        {
            'name': 'Full Range Test (if prev successful)',
            'start_date': datetime(2025, 2, 3),
            'end_date': datetime(2025, 4, 30),
            'timeout': 600
        }
    ]
    
    for config in test_configs:
        print(f"\n📊 {config['name']}")
        print("-" * 40)
        
        start_date = config['start_date']
        end_date = config['end_date']
        dates = pd.date_range(start_date, end_date, freq='B')
        
        print(f"   📅 Date range: {start_date.date()} to {end_date.date()}")
        print(f"   📅 Business days: {len(dates)}")
        print(f"   ⏰ Timeout: {config['timeout']}s")
        
        try:
            # Initialize components
            spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
            data_class = SpreadViewerData()
            data_class_tr = SpreadViewerData()
            db_class = TPData()
            
            # Get required parameters
            tenors_list = spread_class.tenors_list
            product_dates = spread_class.product_dates(dates, n_s)
            
            print(f"   📋 Tenors list: {tenors_list}")
            print(f"   📋 Product dates count: {len(product_dates)}")
            
            # Trading hours (shorter for testing)
            start_time = time(9, 0, 0)
            end_time = time(10, 0, 0)  # Just 1 hour
            
            # Set up timeout
            import signal
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Timed out after {config['timeout']}s")
            
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(config['timeout'])
            
            try:
                print(f"   🔄 Loading order book data...")
                data_class.load_best_order_otc(
                    markets, tenors_list,
                    product_dates,
                    db_class,
                    start_time=start_time, end_time=end_time
                )
                
                print(f"   🔄 Loading trade data...")
                data_class_tr.load_trades_otc(
                    markets, tenors_list, db_class,
                    start_time=start_time, end_time=end_time
                )
                
                signal.alarm(0)  # Cancel timeout
                print(f"   ✅ Data loading completed!")
                
                # Check what was loaded
                if hasattr(data_class, 'data') and len(data_class.data) > 0:
                    total_orders = sum(len(df) for df in data_class.data.values() if isinstance(df, pd.DataFrame))
                    print(f"   📊 Order book data: {total_orders} records across {len(data_class.data)} datasets")
                else:
                    print(f"   ❌ No order book data loaded")
                
                if hasattr(data_class_tr, 'data') and len(data_class_tr.data) > 0:
                    total_trades = sum(len(df) for df in data_class_tr.data.values() if isinstance(df, pd.DataFrame))
                    print(f"   📊 Trade data: {total_trades} records across {len(data_class_tr.data)} datasets")
                else:
                    print(f"   ❌ No trade data loaded")
                
                # If successful, try processing spread calculation
                if total_orders > 0 or total_trades > 0:
                    print(f"   🔄 Testing spread calculation...")
                    
                    # Test one day processing (simplified)
                    test_date_range = pd.date_range(start_date, start_date, freq='D')  # Just first day
                    
                    sm_all = pd.DataFrame()
                    tm_all = pd.DataFrame()
                    
                    for d in test_date_range:
                        d_range = pd.date_range(d, d)
                        
                        try:
                            # Test aggregate_data method
                            data_dict = spread_class.aggregate_data(
                                data_class, d_range, n_s,
                                start_time=start_time, end_time=end_time
                            )
                            
                            print(f"   📊 Aggregated data keys: {list(data_dict.keys())}")
                            
                            # Check if we got bid/ask data
                            for key, df in data_dict.items():
                                if isinstance(df, pd.DataFrame) and not df.empty:
                                    print(f"      {key}: {len(df)} records, columns: {list(df.columns)}")
                                    if len(df) > 0:
                                        sm_all = pd.concat([sm_all, df], ignore_index=True)
                            
                            break  # Just test first day
                            
                        except Exception as e:
                            print(f"   ❌ Aggregate data failed: {e}")
                    
                    if not sm_all.empty:
                        print(f"   ✅ Spread calculation successful: {len(sm_all)} spread records")
                        print(f"   📊 Spread columns: {list(sm_all.columns)}")
                        if 'bid' in sm_all.columns and 'ask' in sm_all.columns:
                            print(f"   💰 Bid range: {sm_all['bid'].min():.2f} to {sm_all['bid'].max():.2f}")
                            print(f"   💰 Ask range: {sm_all['ask'].min():.2f} to {sm_all['ask'].max():.2f}")
                        
                        # This would be our synthetic orders!
                        return {
                            'success': True,
                            'spread_orders': sm_all,
                            'spread_trades': tm_all,
                            'method': 'synthetic_spreadviewer'
                        }
                    else:
                        print(f"   ❌ No spread data calculated")
                
            except TimeoutError:
                signal.alarm(0)
                print(f"   ⏰ Timed out after {config['timeout']}s")
                if config['name'] == 'Single Day Test':
                    print(f"   ❌ Even single day fails - there may be a fundamental issue")
                    break
                continue
                
            except Exception as e:
                signal.alarm(0)
                print(f"   ❌ Failed with error: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        except Exception as e:
            print(f"   ❌ Setup failed: {e}")
            continue
        
        # If we got here and it was successful, continue to next test
        print(f"   ✅ {config['name']} completed successfully")

if __name__ == "__main__":
    test_spreadviewer_integration()