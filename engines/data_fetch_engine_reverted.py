#!/usr/bin/env python3
"""
REVERTED Data Fetch Engine - Original Simple Logic
Removed all complex n_s synchronization modifications
"""

import pandas as pd
from datetime import datetime, timedelta, time
from typing import List, Tuple, Dict
from dataclasses import dataclass
import os
import sys

# Add paths for dependencies  
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'
rawdata_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData'

sys.path.append(energy_trading_path)
sys.path.append(project_root)

# Import data fetcher
try:
    from source_repos.ATS_3.main import DataFetcher
    DATAFETCHER_AVAILABLE = True
except ImportError as e:
    DATAFETCHER_AVAILABLE = False
    print(f"DataFetcher not available: {e}")

# Import SpreadViewer
try:
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from SynthSpread.tpdata import TPData
    SPREADVIEWER_AVAILABLE = True
except ImportError as e:
    SPREADVIEWER_AVAILABLE = False
    print(f"SpreadViewer not available: {e}")

@dataclass
class ContractSpec:
    contract: str
    market: str
    product: str
    tenor: str
    delivery_date: datetime

@dataclass
class RelativePeriod:
    relative_offset: int
    start_date: datetime
    end_date: datetime

def convert_absolute_to_relative_periods(contract_spec: ContractSpec, 
                                       start_date: datetime, 
                                       end_date: datetime,
                                       n_s: int = 3) -> List[Tuple[RelativePeriod, datetime, datetime]]:
    """
    Convert absolute contract to relative periods - REVERTED TO SIMPLE LOGIC
    
    Uses middle date to determine relative offset without complex transition handling.
    """
    periods = []
    
    # Use middle date to determine perspective
    middle_date = start_date + (end_date - start_date) / 2
    
    if contract_spec.tenor == 'q':
        # Get reference quarter for middle date
        ref_quarter = ((middle_date.month - 1) // 3) + 1
        ref_year = middle_date.year
        
        # Calculate relative offset from reference to delivery
        delivery_quarter = ((contract_spec.delivery_date.month - 1) // 3) + 1
        delivery_year = contract_spec.delivery_date.year
        
        # Simple quarters difference calculation
        ref_quarters = ref_year * 4 + (ref_quarter - 1)
        delivery_quarters = delivery_year * 4 + (delivery_quarter - 1)
        relative_offset = delivery_quarters - ref_quarters
        
        print(f"📊 {contract_spec.contract}: Q{ref_quarter} {ref_year} → Q{delivery_quarter} {delivery_year} = q_{relative_offset}")
        
        if relative_offset > 0:
            relative_period = RelativePeriod(
                relative_offset=relative_offset,
                start_date=start_date,
                end_date=end_date
            )
            periods.append((relative_period, start_date, end_date))
    
    return periods

def create_spreadviewer_config_for_period(contract1: ContractSpec, contract2: ContractSpec,
                                        rel_period1: RelativePeriod, rel_period2: RelativePeriod,
                                        start_date: datetime, end_date: datetime,
                                        coefficients: List[float], n_s: int) -> Dict:
    """Create SpreadViewer configuration for specific relative period - REVERTED TO ORIGINAL"""
    return {
        'markets': [contract1.market, contract2.market],
        'tenors': [contract1.tenor, contract2.tenor],  # Original simple tenors
        'tn1_list': [rel_period1.relative_offset, rel_period2.relative_offset],
        'tn2_list': [],
        'coefficients': coefficients,
        'brk_list': ['eex'],
        'n_s': n_s,
        'add_trades': True,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    }

def fetch_spreadviewer_for_period(config: Dict) -> Dict:
    """Fetch SpreadViewer data for a specific period - REVERTED TO ORIGINAL"""
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
    
    # SpreadViewer setup - ORIGINAL LOGIC
    spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
    data_class = SpreadViewerData()
    data_class_tr = SpreadViewerData()
    db_class = TPData()
    
    # Load data using ORIGINAL SpreadViewer logic
    tenors_list = spread_class.tenors_list
    start_time = time(9, 0, 0)
    end_time = time(17, 25, 0)
    
    try:
        print(f"   🔍 Loading order data with ORIGINAL logic:")
        print(f"     Markets: {markets}")
        print(f"     Tenors: {tenors_list}")
        
        # REVERTED: Use SpreadViewer's original product_dates method
        # This calls (dates + n_s * dates.freq).shift(tn, freq='QS')
        data_class.load_best_order_otc(
            markets, tenors_list,
            dates,  # Let SpreadViewer handle product_dates internally
            db_class,
            start_time=start_time, end_time=end_time
        )
        print(f"   ✅ Order data loading completed")
        
        # Load trade data
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
            
            data_dict = spread_class.aggregate_data(
                data_class, d_range, n_s, gran=None,
                start_time=start_time, end_time=end_time,
                col_list=['bid', 'ask']
            )
            
            if data_dict:
                spread_data = spread_class.spreads(data_dict, coefficients, True)
                if spread_data is not None and not spread_data.empty:
                    spread_data.index = pd.to_datetime(spread_data.index)
                    spread_data['broker_id'] = 9999.0  # SpreadViewer marker
                    sm_all = pd.concat([sm_all, spread_data], axis=0)
            
            # Process trades
            trade_dict = spread_class.aggregate_data(
                data_class_tr, d_range, n_s, gran=None,
                start_time=start_time, end_time=end_time,
                col_list=['price', 'volume']
            )
            
            if trade_dict:
                trade_spreads = spread_class.spreads(trade_dict, coefficients, True)
                if trade_spreads is not None and not trade_spreads.empty:
                    trade_spreads.index = pd.to_datetime(trade_spreads.index)
                    trade_spreads['broker_id'] = 9999.0  # SpreadViewer marker
                    tm_all = pd.concat([tm_all, trade_spreads], axis=0)
        
        # Sort and deduplicate
        if not sm_all.empty:
            sm_all = sm_all.sort_index().drop_duplicates()
        if not tm_all.empty:
            tm_all = tm_all.sort_index().drop_duplicates()
        
        return {
            'spread_orders': sm_all,
            'spread_trades': tm_all
        }
        
    except Exception as e:
        print(f"   ❌ SpreadViewer processing error: {e}")
        return {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}

def fetch_synthetic_spread_multiple_periods(contract1: ContractSpec, contract2: ContractSpec,
                                          start_date: datetime, end_date: datetime,
                                          coefficients: List[float], n_s: int = 3) -> Dict:
    """
    Fetch synthetic spread data - REVERTED TO ORIGINAL LOGIC
    """
    if not SPREADVIEWER_AVAILABLE:
        raise ImportError("SpreadViewer not available")
    
    print(f"🔄 Fetching synthetic spread: {contract1.contract} vs {contract2.contract}")
    
    # Get relative periods using SIMPLE logic
    periods1 = convert_absolute_to_relative_periods(contract1, start_date, end_date, n_s)
    periods2 = convert_absolute_to_relative_periods(contract2, start_date, end_date, n_s)
    
    all_orders = pd.DataFrame()
    all_trades = pd.DataFrame()
    
    for (rel_period1, p_start1, p_end1) in periods1:
        for (rel_period2, p_start2, p_end2) in periods2:
            # Find overlap
            overlap_start = max(p_start1, p_start2, start_date)
            overlap_end = min(p_end1, p_end2, end_date)
            
            if overlap_start <= overlap_end:
                print(f"   📊 Processing: q_{rel_period1.relative_offset} vs q_{rel_period2.relative_offset}")
                
                # Create configuration using ORIGINAL logic
                spread_config = create_spreadviewer_config_for_period(
                    contract1, contract2, rel_period1, rel_period2,
                    overlap_start, overlap_end, coefficients, n_s
                )
                
                period_results = fetch_spreadviewer_for_period(spread_config)
                
                if 'spread_orders' in period_results and not period_results['spread_orders'].empty:
                    all_orders = pd.concat([all_orders, period_results['spread_orders']], axis=0)
                
                if 'spread_trades' in period_results and not period_results['spread_trades'].empty:
                    all_trades = pd.concat([all_trades, period_results['spread_trades']], axis=0)
    
    # Sort and deduplicate
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

print("✅ REVERTED data_fetch_engine loaded with original simple logic")
print("🔄 All complex n_s synchronization modifications removed")
print("📊 SpreadViewer will use original (dates + n_s * dates.freq).shift() logic")