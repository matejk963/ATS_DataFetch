"""
SpreadViewer Integration
========================

Functions for integrating with the SpreadViewer synthetic spread calculation system.
"""

import sys
import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import Dict, List

from .contracts import ContractSpec, RelativePeriod
from .date_utils import convert_absolute_to_relative_periods, calculate_synchronized_product_dates

# Add paths for SpreadViewer imports
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

try:
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from Database.TPData import TPData
    SPREADVIEWER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SpreadViewer imports failed: {e}")
    SPREADVIEWER_AVAILABLE = False


def create_spreadviewer_config_for_period(contract1: ContractSpec, contract2: ContractSpec,
                                        rel_period1: RelativePeriod, rel_period2: RelativePeriod,
                                        start_date: datetime, end_date: datetime,
                                        coefficients: List[float], n_s: int) -> Dict:
    """Create SpreadViewer configuration for specific relative period"""
    return {
        'markets': [contract1.market, contract2.market],
        'tenors': [contract1.tenor, contract2.tenor],
        'tn1_list': [rel_period1.relative_offset, rel_period2.relative_offset],
        'tn2_list': [],
        'coefficients': coefficients,
        'brk_list': ['eex'],
        'n_s': n_s,
        'add_trades': True,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    }


def fetch_synthetic_spread_multiple_periods(contract1: ContractSpec, contract2: ContractSpec,
                                          start_date: datetime, end_date: datetime,
                                          coefficients: List[float], n_s: int = 3) -> Dict:
    """
    Fetch synthetic spread data across multiple relative periods using SpreadViewer
    """
    if not SPREADVIEWER_AVAILABLE:
        raise ImportError("SpreadViewer not available")
    
    print(f"🔄 Fetching synthetic spread: {contract1.market}{contract1.product[0]}{contract1.tenor}{contract1.contract} vs {contract2.market}{contract2.product[0]}{contract2.tenor}{contract2.contract}")
    
    # Get relative periods for both contracts
    periods1 = convert_absolute_to_relative_periods(contract1, start_date, end_date, n_s)
    periods2 = convert_absolute_to_relative_periods(contract2, start_date, end_date, n_s)
    
    # Find overlapping periods
    all_results = {}
    all_orders = pd.DataFrame()
    all_trades = pd.DataFrame()
    
    for (rel_period1, p_start1, p_end1) in periods1:
        for (rel_period2, p_start2, p_end2) in periods2:
            # Find overlap
            overlap_start = max(p_start1, p_start2)
            overlap_end = min(p_end1, p_end2)
            
            if overlap_start <= overlap_end:
                print(f"   📅 Period: {overlap_start.strftime('%Y-%m-%d')} to {overlap_end.strftime('%Y-%m-%d')} (M{rel_period1.relative_offset}/M{rel_period2.relative_offset})")
                
                # Create SpreadViewer configuration for this period
                spread_config = create_spreadviewer_config_for_period(
                    contract1, contract2, rel_period1, rel_period2, 
                    overlap_start, overlap_end, coefficients, n_s
                )
                
                # Fetch data for this period
                period_results = fetch_spreadviewer_for_period(spread_config)
                
                # Accumulate results
                if 'spread_orders' in period_results and not period_results['spread_orders'].empty:
                    all_orders = pd.concat([all_orders, period_results['spread_orders']], axis=0)
                
                if 'spread_trades' in period_results and not period_results['spread_trades'].empty:
                    all_trades = pd.concat([all_trades, period_results['spread_trades']], axis=0)
    
    # Sort by timestamp and remove duplicates
    if not all_orders.empty:
        all_orders = all_orders.sort_index().drop_duplicates()
    if not all_trades.empty:
        all_trades = all_trades.sort_index().drop_duplicates()
    
    return {
        'spread_orders': all_orders,
        'spread_trades': all_trades,
        'method': 'synthetic_spreadviewer',
        'periods_processed': len(periods1) * len(periods2)
    }


def fetch_spreadviewer_for_period(config: Dict) -> Dict:
    """Fetch SpreadViewer data for a specific period"""
    if not SPREADVIEWER_AVAILABLE:
        raise ImportError("SpreadViewer not available")
    
    markets = config['markets']
    tenors = config['tenors'] 
    tn1_list = config['tn1_list']
    tn2_list = config.get('tn2_list', [])
    coefficients = config['coefficients']
    n_s = config.get('n_s', 3)
    
    start_date = datetime.strptime(config['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(config['end_date'], '%Y-%m-%d')
    dates = pd.date_range(start_date, end_date, freq='B')
    
    if len(dates) == 0:
        return {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}
    
    # SpreadViewer setup
    spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
    data_class = SpreadViewerData()
    data_class_tr = SpreadViewerData()
    db_class = TPData()
    
    # Load data - use original tenors, not spread_class.tenors_list
    tenors_list = spread_class.tenors_list  # This should be ['q', 'q'] not ['q_1', 'q_1']
    start_time = time(9, 0, 0)
    end_time = time(17, 25, 0)
    
    try:
        # Load order book data - using synchronized product_dates for n_s consistency
        print(f"   🔍 Loading order data with:")
        print(f"     Markets: {markets}")
        print(f"     Tenors: {tenors_list}")
        
        # 🔧 FIX: Use synchronized product_dates instead of SpreadViewer's method
        # This ensures DataFetcher and SpreadViewer use the same n_s transition logic
        product_dates_result = calculate_synchronized_product_dates(dates, tenors_list, tn1_list, n_s)
        
        print(f"     Product dates: {product_dates_result}")
        print(f"     Date range: {dates[0]} to {dates[-1]}")
        
        data_class.load_best_order_otc(
            markets, tenors_list,
            product_dates_result,
            db_class,
            start_time=start_time, end_time=end_time
        )
        print(f"   ✅ Order data loading completed")
        
        # Load trade data - using correct parameter format from working test
        data_class_tr.load_trades_otc(
            markets, tenors_list, db_class,
            start_time=start_time, end_time=end_time
        )
        print(f"   ✅ Trade data loading completed")
        
        # Process daily data
        sm_all = pd.DataFrame()
        tm_all = pd.DataFrame()
        
        for d in dates:
            d_range = pd.date_range(d, d)
            print(f"   📅 Processing date: {d}")
            print(f"     Date range: {d_range}")
            
            # Aggregate order book data - using correct parameter format from working test
            print(f"   🔧 Calling aggregate_data for date {d}")
            
            data_dict = spread_class.aggregate_data(
                data_class, d_range, n_s, gran=None,
                start_time=start_time, end_time=end_time,
                col_list=['bid', 'ask']  # Explicitly pass the default col_list
            )
            
            print(f"   📊 aggregate_data returned: {type(data_dict)}")
            if data_dict:
                print(f"     Keys: {list(data_dict.keys())}")
                for key, value in data_dict.items():
                    if hasattr(value, 'shape'):
                        print(f"     {key}: {value.shape}")
                    else:
                        print(f"     {key}: {type(value)}")
            else:
                print(f"     ⚠️  Empty or None data_dict")
            
            # Create spread orders
            sm = spread_class.spread_maker(data_dict, coefficients, trade_type=['cmb', 'cmb']).dropna()
            sm_all = pd.concat([sm_all, sm], axis=0)
            
            # Create spread trades
            if not sm.empty:
                col_list = ['bid', 'ask', 'volume', 'broker_id']
                trade_dict = spread_class.aggregate_data(
                    data_class_tr, d_range, n_s, gran='1s',
                    start_time=start_time, end_time=end_time,
                    col_list=col_list, data_dict=data_dict
                )
                
                tm = spread_class.add_trades(data_dict, trade_dict, coefficients, [True, True])
                tm_all = pd.concat([tm_all, tm], axis=0)
        
        # Apply trade adjustment before returning - filter trades against spread bid/ask
        if not tm_all.empty and not sm_all.empty:
            print(f"   🔧 Applying trade adjustment filter...")
            tm_before = len(tm_all)
            tm_all = adjust_trds_(tm_all, sm_all)
            tm_after = len(tm_all)
            print(f"   📊 Trade filtering: {tm_before} → {tm_after} trades ({tm_before-tm_after} filtered)")
        
        return {
            'spread_orders': sm_all,
            'spread_trades': tm_all
        }
        
    except Exception as e:
        print(f"   ⚠️  SpreadViewer fetch failed: {e}")
        return {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}


def adjust_trds_(df_tr, df_sm):
    """
    Adjust trades against spread bid/ask with buffer - from SpreadViewer test
    Removes trades that are too close to current spread bid/ask prices
    """
    if df_tr.empty or df_sm.empty:
        return df_tr
    
    timestamp = df_tr.index
    ts_new = df_sm.index.union(timestamp)
    df_sm = df_sm.reindex(ts_new).ffill().reindex(timestamp)
    lb = df_sm.iloc[:, 0] + 0.001  # Lower bound + buffer (bid + buffer)
    ub = df_sm.iloc[:, 1] - 0.001  # Upper bound - buffer (ask - buffer)
    df_tr.loc[df_tr['buy'] >= ub, 'buy'] = np.nan    # Remove buys too close to ask
    df_tr.loc[df_tr['sell'] <= lb, 'sell'] = np.nan  # Remove sells too close to bid
    return df_tr.dropna(how='all')