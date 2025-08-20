r"""
Enhanced SpreadViewer + DataFetcher Integration Script V2
======================================================

This script implements sophisticated integration between SpreadViewer and DataFetcher:
- Unified input dictionary with absolute contract naming (demb07_25, demp07_25)
- Single leg vs spread processing based on contract count
- Real spread contracts via DataFetcher (start_date1 + start_date2)
- Synthetic spread contracts via SpreadViewer with n_s transition logic
- Multiple SpreadViewer fetches per absolute contract across relative periods

Usage:
    python integration_script_v2.py

Key improvements:
- Product-encoded contract names (base/peak)
- n_s business day transition logic for relative contracts
- Dual spread sources: real (DataFetcher) + synthetic (SpreadViewer)
- Complex absolute-to-relative conversion with multiple periods
"""

import sys
import os
from datetime import datetime, time, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import json
from dataclasses import dataclass

# Cross-platform project root
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
    output_base = r'C:\Users\krajcovic\Documents\Testing Data\RawData\test'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'
    output_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'

sys.path.insert(0, project_root)  # Insert at beginning to take precedence
sys.path.insert(0, energy_trading_path)  # Insert at beginning to take precedence

from src.core.data_fetcher import DataFetcher, TPDATA_AVAILABLE, DeliveryDateCalculator

try:
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from Database.TPData import TPData
    
    # Verify which SpreadViewer we're using
    import SynthSpread.spreadviewer_class as svc_module
    print(f"📍 Using SpreadViewer from: {svc_module.__file__}")
    
    # Apply pandas compatibility patch for SpreadViewer
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
    
    # Apply DatetimeIndex fixes
    original_load_best_order_otc = SpreadViewerData.load_best_order_otc
    original_load_trades_otc = SpreadViewerData.load_trades_otc
    
    def fixed_load_best_order_otc(self, markets, tenors_list, product_dates, db_class, start_time=None, end_time=None):
        """Fixed version that ensures DatetimeIndex"""
        try:
            result = original_load_best_order_otc(self, markets, tenors_list, product_dates, db_class, start_time=start_time, end_time=end_time)
            
            # Fix DatetimeIndex if needed
            if hasattr(self, 'data_df') and self.data_df is not None and not self.data_df.empty:
                if not isinstance(self.data_df.index, pd.DatetimeIndex):
                    print(f"   🔧 Fixing non-DatetimeIndex in order data: {type(self.data_df.index)}")
                    try:
                        self.data_df.index = pd.to_datetime(self.data_df.index)
                        print(f"   ✅ Fixed DatetimeIndex for order data")
                    except Exception as e:
                        print(f"   ⚠️  Could not fix DatetimeIndex for order data: {e}")
                        self.data_df = pd.DataFrame()
            
            return result
        except Exception as e:
            print(f"   ❌ load_best_order_otc failed: {e}")
            self.data_df = pd.DataFrame(index=pd.DatetimeIndex([]))
            return None
    
    def fixed_load_trades_otc(self, markets, tenors_list, db_class, start_time=None, end_time=None):
        """Fixed version that ensures DatetimeIndex"""
        try:
            result = original_load_trades_otc(self, markets, tenors_list, db_class, start_time=start_time, end_time=end_time)
            
            # Fix DatetimeIndex if needed
            if hasattr(self, 'data_df') and self.data_df is not None and not self.data_df.empty:
                if not isinstance(self.data_df.index, pd.DatetimeIndex):
                    print(f"   🔧 Fixing non-DatetimeIndex in trade data: {type(self.data_df.index)}")
                    try:
                        self.data_df.index = pd.to_datetime(self.data_df.index)
                        print(f"   ✅ Fixed DatetimeIndex for trade data")
                    except Exception as e:
                        print(f"   ⚠️  Could not fix DatetimeIndex for trade data: {e}")
                        self.data_df = pd.DataFrame()
            
            return result
        except Exception as e:
            print(f"   ❌ load_trades_otc failed: {e}")
            self.data_df = pd.DataFrame(index=pd.DatetimeIndex([]))
            return None
    
    # Apply DatetimeIndex patches
    SpreadViewerData.load_best_order_otc = fixed_load_best_order_otc
    SpreadViewerData.load_trades_otc = fixed_load_trades_otc
    
    # Note: Removed aggregate_data patch to avoid parameter passing issues
    
    SPREADVIEWER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SpreadViewer imports failed: {e}")
    SPREADVIEWER_AVAILABLE = False


@dataclass
class ContractSpec:
    """Parsed contract specification"""
    market: str
    product: str
    tenor: str
    contract: str
    delivery_date: datetime


@dataclass
class RelativePeriod:
    """Relative contract period with date range"""
    relative_offset: int  # M1, M2, M3, M4
    start_date: datetime
    end_date: datetime


def parse_absolute_contract(contract_str: str) -> ContractSpec:
    """
    Parse absolute contract with product encoding
    
    Examples:
        'demb07_25' → market='de', product='base', tenor='m', contract='07_25'
        'demp07_25' → market='de', product='peak', tenor='m', contract='07_25'
    """
    if len(contract_str) < 6:
        raise ValueError(f"Invalid contract format: {contract_str}")
    
    market = contract_str[:2]           # 'de'
    product_code = contract_str[2:3]    # 'b' or 'p'
    tenor = contract_str[3:4]           # 'm'
    contract = contract_str[4:]         # '09_25'
    
    product_map = {'b': 'base', 'p': 'peak'}
    if product_code not in product_map:
        raise ValueError(f"Unknown product code: {product_code}")
    
    product = product_map[product_code]
    
    # Calculate delivery date
    calc = DeliveryDateCalculator()
    delivery_date = calc.calc_delivery_date(tenor, contract)
    
    return ContractSpec(
        market=market,
        product=product,
        tenor=tenor,
        contract=contract,
        delivery_date=delivery_date
    )


def calculate_last_business_day(year: int, month: int) -> datetime:
    """Calculate last business day of a month"""
    # Get last day of month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    last_day = next_month - timedelta(days=1)
    
    # Move backwards to find last business day
    while last_day.weekday() > 4:  # 5=Saturday, 6=Sunday
        last_day -= timedelta(days=1)
    
    return last_day


def calculate_transition_dates(start_date: datetime, end_date: datetime, n_s: int = 3) -> List[Tuple[datetime, datetime, bool]]:
    """
    Calculate transition dates for relative contract periods using n_s logic
    
    n_s = 3 means in the last 3 business days of each month,
    contracts transition to next month's relative numbering
    
    Returns list of (period_start, period_end, is_transition_period) tuples
    """
    periods = []
    current_date = start_date
    
    while current_date <= end_date:
        year, month = current_date.year, current_date.month
        
        # Calculate last business day of current month
        last_bday = calculate_last_business_day(year, month)
        
        # Calculate transition point (last_bday - n_s + 1 business days)
        transition_start = last_bday
        for _ in range(n_s - 1):
            transition_start -= timedelta(days=1)
            while transition_start.weekday() > 4:  # Skip weekends
                transition_start -= timedelta(days=1)
        
        # Calculate end of month 
        if month == 12:
            next_month_start = datetime(year + 1, 1, 1)
        else:
            next_month_start = datetime(year, month + 1, 1)
        month_end = next_month_start - timedelta(days=1)
        
        # Period 1: Early month (normal relative counting)
        early_period_end = min(transition_start - timedelta(days=1), end_date)
        if current_date <= early_period_end:
            periods.append((current_date, early_period_end, False))  # Not transition period
        
        # Period 2: Late month (next month's relative counting) 
        late_period_start = max(transition_start, current_date)
        late_period_end = min(month_end, end_date)
        if late_period_start <= late_period_end:
            periods.append((late_period_start, late_period_end, True))  # Is transition period
        
        # Move to next month
        current_date = next_month_start
        
        # If we've processed past end_date, break
        if current_date > end_date:
            break
    
    return periods


def convert_absolute_to_relative_periods(contract_spec: ContractSpec, 
                                       start_date: datetime, 
                                       end_date: datetime,
                                       n_s: int = 3) -> List[Tuple[RelativePeriod, datetime, datetime]]:
    """
    Convert absolute contract to relative periods with n_s transition logic
    
    Returns list of (RelativePeriod, period_start, period_end) tuples
    """
    periods = []
    transition_dates = calculate_transition_dates(start_date, end_date, n_s)
    
    for period_start, period_end, is_transition_period in transition_dates:
        # Determine the reference month for relative calculation
        if is_transition_period:
            # Late month period: count from NEXT month's perspective
            if period_start.month == 12:
                ref_year, ref_month = period_start.year + 1, 1
            else:
                ref_year, ref_month = period_start.year, period_start.month + 1
        else:
            # Early month period: count from current month's perspective
            ref_year, ref_month = period_start.year, period_start.month
        
        # Calculate relative offset from reference month to contract delivery
        months_diff = ((contract_spec.delivery_date.year - ref_year) * 12 + 
                      (contract_spec.delivery_date.month - ref_month))
        
        if months_diff > 0:  # Only include future contracts
            relative_period = RelativePeriod(
                relative_offset=months_diff,
                start_date=period_start,
                end_date=period_end
            )
            periods.append((relative_period, period_start, period_end))
    
    return periods


def create_contract_config_from_spec(contract_spec: ContractSpec, period: Dict) -> Dict:
    """Convert ContractSpec to DataFetcher contract config format"""
    return {
        'market': contract_spec.market,
        'tenor': contract_spec.tenor,
        'contract': contract_spec.contract,
        'start_date': period['start_date'],
        'end_date': period['end_date'],
        'prod': contract_spec.product
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
    tenors_list = spread_class.tenors_list  # This should be ['m', 'm'] not ['m_1', 'm_2']
    start_time = time(9, 0, 0)
    end_time = time(17, 25, 0)
    
    try:
        # Load order book data - using correct parameter format from working test
        print(f"   🔍 Loading order data with:")
        print(f"     Markets: {markets}")
        print(f"     Tenors: {tenors_list}")
        product_dates_result = spread_class.product_dates(dates, n_s)
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
        
        return {
            'spread_orders': sm_all,
            'spread_trades': tm_all
        }
        
    except Exception as e:
        print(f"   ⚠️  SpreadViewer fetch failed: {e}")
        return {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}


def transform_orders_to_target_format(orders_df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Transform order data to target format"""
    if orders_df.empty:
        return pd.DataFrame()
    
    # Ensure we have b_price/a_price columns
    if 'bid' in orders_df.columns and 'ask' in orders_df.columns:
        # SpreadViewer format
        orders_df = orders_df.rename(columns={'bid': 'b_price', 'ask': 'a_price'})
    
    # Create target format with NaN for trade-specific columns
    target_df = pd.DataFrame(index=orders_df.index)
    target_df['price'] = np.nan
    target_df['volume'] = np.nan  
    target_df['action'] = np.nan
    target_df['broker_id'] = np.nan
    target_df['count'] = np.nan
    target_df['tradeid'] = np.nan
    target_df['b_price'] = orders_df['b_price']
    target_df['a_price'] = orders_df['a_price']
    target_df['0'] = (orders_df['b_price'] + orders_df['a_price']) / 2  # Mid-price
    
    return target_df

def transform_trades_to_target_format(trades_df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Transform trade data to target format"""
    if trades_df.empty:
        return pd.DataFrame()
    
    target_df = pd.DataFrame(index=trades_df.index)
    
    if source == 'datafetcher':
        # DataFetcher format: ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid']
        target_df['price'] = trades_df.get('price', np.nan)
        target_df['volume'] = trades_df.get('volume', np.nan)
        target_df['action'] = trades_df.get('action', np.nan)
        target_df['broker_id'] = trades_df.get('broker_id', np.nan)
        target_df['count'] = trades_df.get('count', np.nan)
        target_df['tradeid'] = trades_df.get('tradeid', np.nan)
        target_df['0'] = trades_df.get('price', np.nan)  # Trade price
        
    elif source == 'spreadviewer':
        # SpreadViewer format: ['buy', 'sell']
        all_trades = []
        
        # Process buy trades
        if 'buy' in trades_df.columns:
            buy_trades = trades_df['buy'].dropna()
            for idx, price in buy_trades.items():
                all_trades.append({
                    'timestamp': idx,
                    'price': price,
                    'volume': 1,  # Default volume
                    'action': 1.0,  # Buy action
                    'broker_id': 9999,  # Synthetic broker ID
                    'count': 1,
                    'tradeid': f'synth_buy_{idx}',
                    '0': price
                })
        
        # Process sell trades  
        if 'sell' in trades_df.columns:
            sell_trades = trades_df['sell'].dropna()
            for idx, price in sell_trades.items():
                all_trades.append({
                    'timestamp': idx,
                    'price': price,
                    'volume': 1,  # Default volume
                    'action': -1.0,  # Sell action
                    'broker_id': 9999,  # Synthetic broker ID
                    'count': 1,
                    'tradeid': f'synth_sell_{idx}',
                    '0': price
                })
        
        if all_trades:
            target_df = pd.DataFrame(all_trades)
            target_df.set_index('timestamp', inplace=True)
            target_df.index.name = None
        else:
            # Empty case
            target_df = pd.DataFrame(columns=['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', '0'])
    
    # Add NaN for order-specific columns
    target_df['b_price'] = np.nan
    target_df['a_price'] = np.nan
    
    # Reorder columns to match target format
    target_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
    target_df = target_df.reindex(columns=target_columns)
    
    return target_df

def merge_spread_data(real_spread_data: Dict, synthetic_spread_data: Dict) -> Dict:
    """
    Enhanced three-stage unified DataFrame merging algorithm:
    1. Transform all data to target format
    2. Merge trades (union)  
    3. Merge orders (best bid/ask)
    4. Final union merge: trades + orders → single unified DataFrame
    """
    print("🔄 Merging real and synthetic spread data (unified pipeline)...")
    
    # Extract raw data
    real_orders = real_spread_data.get('spread_orders', pd.DataFrame())
    real_trades = real_spread_data.get('spread_trades', pd.DataFrame())
    synthetic_orders = synthetic_spread_data.get('spread_orders', pd.DataFrame())
    synthetic_trades = synthetic_spread_data.get('spread_trades', pd.DataFrame())
    
    # Stage 1: Transform all data to target format
    print("   🔄 Stage 1: Transforming data to target format")
    
    real_orders_formatted = transform_orders_to_target_format(real_orders, 'datafetcher')
    real_trades_formatted = transform_trades_to_target_format(real_trades, 'datafetcher')
    synthetic_orders_formatted = transform_orders_to_target_format(synthetic_orders, 'spreadviewer')
    synthetic_trades_formatted = transform_trades_to_target_format(synthetic_trades, 'spreadviewer')
    
    print(f"      ✅ Formatted: {len(real_orders_formatted)} real orders, {len(real_trades_formatted)} real trades")
    print(f"      ✅ Formatted: {len(synthetic_orders_formatted)} synthetic orders, {len(synthetic_trades_formatted)} synthetic trades")
    
    # Stage 2: Merge trades (simple union)
    print("   📊 Stage 2: Merging trades (union)")
    merged_trades = pd.DataFrame()
    
    if not real_trades_formatted.empty:
        merged_trades = pd.concat([merged_trades, real_trades_formatted], axis=0)
    if not synthetic_trades_formatted.empty:
        merged_trades = pd.concat([merged_trades, synthetic_trades_formatted], axis=0)
    
    if not merged_trades.empty:
        merged_trades = merged_trades.sort_index().drop_duplicates()
    
    print(f"      ✅ Merged trades: {len(merged_trades)} records")
    
    # Stage 3: Merge orders (best bid/ask selection)
    print("   🎯 Stage 3: Merging orders (best bid/ask selection)")
    merged_orders = pd.DataFrame()
    
    if not real_orders_formatted.empty or not synthetic_orders_formatted.empty:
        # Create union timeline
        union_timestamps = pd.Index([])
        if not real_orders_formatted.empty:
            union_timestamps = union_timestamps.union(real_orders_formatted.index)
        if not synthetic_orders_formatted.empty:
            union_timestamps = union_timestamps.union(synthetic_orders_formatted.index)
        
        if len(union_timestamps) > 0:
            # Resample and forward fill
            if not real_orders_formatted.empty:
                real_resampled = real_orders_formatted.reindex(union_timestamps).ffill()
            else:
                # Create empty DataFrame with target columns
                target_cols = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
                real_resampled = pd.DataFrame(index=union_timestamps, columns=target_cols)
                
            if not synthetic_orders_formatted.empty:
                synthetic_resampled = synthetic_orders_formatted.reindex(union_timestamps).ffill()
            else:
                # Create empty DataFrame with target columns
                target_cols = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
                synthetic_resampled = pd.DataFrame(index=union_timestamps, columns=target_cols)
            
            # Initialize merged orders DataFrame
            merged_orders = pd.DataFrame(index=union_timestamps)
            target_cols = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
            for col in target_cols:
                merged_orders[col] = np.nan
            
            # Best bid/ask selection
            if not real_resampled.empty and not synthetic_resampled.empty:
                # Both sources available - best bid/ask selection
                merged_orders['b_price'] = np.maximum(
                    real_resampled['b_price'].fillna(-np.inf),
                    synthetic_resampled['b_price'].fillna(-np.inf)
                )
                merged_orders['b_price'] = merged_orders['b_price'].replace(-np.inf, np.nan)
                
                merged_orders['a_price'] = np.minimum(
                    real_resampled['a_price'].fillna(np.inf),
                    synthetic_resampled['a_price'].fillna(np.inf)
                )
                merged_orders['a_price'] = merged_orders['a_price'].replace(np.inf, np.nan)
            elif not real_resampled.empty:
                # Only real data
                merged_orders['b_price'] = real_resampled['b_price']
                merged_orders['a_price'] = real_resampled['a_price']
            elif not synthetic_resampled.empty:
                # Only synthetic data
                merged_orders['b_price'] = synthetic_resampled['b_price']
                merged_orders['a_price'] = synthetic_resampled['a_price']
            
            # Calculate mid-price for '0' column
            merged_orders['0'] = (merged_orders['b_price'] + merged_orders['a_price']) / 2
            
            # Drop rows where both bid and ask are NaN
            merged_orders = merged_orders.dropna(subset=['b_price', 'a_price'], how='all')
    
    print(f"      ✅ Merged orders: {len(merged_orders)} records")
    
    # Stage 4: Final union merge (trades + orders → unified DataFrame)
    print("   🎉 Stage 4: Final union merge (trades + orders → unified DataFrame)")
    
    unified_data = pd.DataFrame()
    if not merged_trades.empty:
        unified_data = pd.concat([unified_data, merged_trades], axis=0)
    if not merged_orders.empty:
        unified_data = pd.concat([unified_data, merged_orders], axis=0)
    
    if not unified_data.empty:
        # Sort by timestamp and ensure target column order
        unified_data = unified_data.sort_index()
        target_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
        unified_data = unified_data.reindex(columns=target_columns)
    
    print(f"      ✅ Unified dataset: {len(unified_data)} total records (trades + orders)")
    
    # Create result with unified structure
    result = {
        'unified_spread_data': unified_data,
        'method': 'unified_real_synthetic_merged',
        'source_stats': {
            'real_trades': len(real_trades),
            'real_orders': len(real_orders),
            'synthetic_trades': len(synthetic_trades),
            'synthetic_orders': len(synthetic_orders),
            'merged_trades': len(merged_trades),
            'merged_orders': len(merged_orders),
            'unified_total': len(unified_data)
        }
    }
    
    print(f"   ✅ Unified spread dataset created: {len(unified_data)} total records")
    
    return result

def create_unified_spreadviewer_data(synthetic_spread_data: Dict) -> Dict:
    """Create unified DataFrame from SpreadViewer-only data"""
    print("🔄 Creating unified DataFrame from SpreadViewer data...")
    
    # Extract raw data
    synthetic_orders = synthetic_spread_data.get('spread_orders', pd.DataFrame())
    synthetic_trades = synthetic_spread_data.get('spread_trades', pd.DataFrame())
    
    # Transform to target format
    print("   🔄 Transforming SpreadViewer data to target format")
    synthetic_orders_formatted = transform_orders_to_target_format(synthetic_orders, 'spreadviewer')
    synthetic_trades_formatted = transform_trades_to_target_format(synthetic_trades, 'spreadviewer')
    
    print(f"      ✅ Formatted: {len(synthetic_orders_formatted)} orders, {len(synthetic_trades_formatted)} trades")
    
    # Union merge orders + trades
    print("   🎉 Final union merge (trades + orders → unified DataFrame)")
    unified_data = pd.DataFrame()
    
    if not synthetic_trades_formatted.empty:
        unified_data = pd.concat([unified_data, synthetic_trades_formatted], axis=0)
    if not synthetic_orders_formatted.empty:
        unified_data = pd.concat([unified_data, synthetic_orders_formatted], axis=0)
    
    if not unified_data.empty:
        # Sort by timestamp and ensure target column order
        unified_data = unified_data.sort_index()
        target_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
        unified_data = unified_data.reindex(columns=target_columns)
    
    print(f"      ✅ Unified dataset: {len(unified_data)} total records (trades + orders)")
    
    # Create result with unified structure
    result = {
        'unified_spread_data': unified_data,
        'method': 'unified_synthetic_only',
        'source_stats': {
            'synthetic_trades': len(synthetic_trades),
            'synthetic_orders': len(synthetic_orders),
            'unified_total': len(unified_data)
        }
    }
    
    print(f"   ✅ Unified SpreadViewer dataset created: {len(unified_data)} total records")
    
    return result


def create_unified_real_spread_data(real_spread_data: Dict) -> Dict:
    """Create unified DataFrame from DataFetcher-only data"""
    print("🔄 Creating unified DataFrame from real spread data...")
    
    # Extract raw data
    real_orders = real_spread_data.get('spread_orders', pd.DataFrame())
    real_trades = real_spread_data.get('spread_trades', pd.DataFrame())
    
    # Transform to target format
    print("   🔄 Transforming real spread data to target format")
    real_orders_formatted = transform_orders_to_target_format(real_orders, 'datafetcher')
    real_trades_formatted = transform_trades_to_target_format(real_trades, 'datafetcher')
    
    print(f"      ✅ Formatted: {len(real_orders_formatted)} orders, {len(real_trades_formatted)} trades")
    
    # Union merge orders + trades
    print("   🎉 Final union merge (trades + orders → unified DataFrame)")
    unified_data = pd.DataFrame()
    
    if not real_trades_formatted.empty:
        unified_data = pd.concat([unified_data, real_trades_formatted], axis=0)
    if not real_orders_formatted.empty:
        unified_data = pd.concat([unified_data, real_orders_formatted], axis=0)
    
    if not unified_data.empty:
        # Sort by timestamp and ensure target column order
        unified_data = unified_data.sort_index()
        target_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
        unified_data = unified_data.reindex(columns=target_columns)
    
    print(f"      ✅ Unified dataset: {len(unified_data)} total records (trades + orders)")
    
    # Create result with unified structure
    result = {
        'unified_spread_data': unified_data,
        'method': 'unified_real_only',
        'source_stats': {
            'real_trades': len(real_trades),
            'real_orders': len(real_orders),
            'unified_total': len(unified_data)
        }
    }
    
    print(f"   ✅ Unified real spread dataset created: {len(unified_data)} total records")
    
    return result


def save_unified_results(results: Dict, contracts: List[str], period: Dict, stage: str = 'unified') -> None:
    """Save unified spread data in both parquet and CSV formats - single file only"""
    output_dir = output_base
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Check for unified data in different possible keys
    unified_data = None
    data_source = 'unknown'
    
    if 'merged_spread_data' in results and 'unified_spread_data' in results['merged_spread_data']:
        unified_data = results['merged_spread_data']['unified_spread_data']
        data_source = 'merged'
    elif 'synthetic_spread_data' in results and 'unified_spread_data' in results['synthetic_spread_data']:
        unified_data = results['synthetic_spread_data']['unified_spread_data']  
        data_source = 'synthetic_only'
    elif 'real_spread_data' in results and 'unified_spread_data' in results['real_spread_data']:
        unified_data = results['real_spread_data']['unified_spread_data']
        data_source = 'real_only'
    
    if unified_data is None or (hasattr(unified_data, 'empty') and unified_data.empty):
        print(f"   ⚠️  No unified data to save")
        return
    
    # Generate filename based on number of contracts
    if len(contracts) == 1:
        # Single contract: contract_tr_ba_data
        filename = f"{contracts[0]}_tr_ba_data"
    elif len(contracts) == 2:
        # Spread: contract1_contract2_tr_ba_data  
        filename = f"{contracts[0]}_{contracts[1]}_tr_ba_data"
    else:
        # Fallback for multiple contracts
        contract_names = '_'.join(contracts)
        filename = f"{contract_names}_tr_ba_data"
    
    # Save as parquet
    parquet_path = os.path.join(output_dir, f'{filename}.parquet')
    unified_data.to_parquet(parquet_path)
    print(f"   📁 Saved unified spread data: {parquet_path}")
    
    # Save as CSV
    csv_path = os.path.join(output_dir, f'{filename}.csv')
    unified_data.to_csv(csv_path)
    print(f"   📁 Saved unified spread data: {csv_path}")
    
    # Save metadata
    metadata = {
        'stage': stage,
        'timestamp': timestamp,
        'contracts': contracts,
        'period': period,
        'n_s': results.get('metadata', {}).get('n_s', 3),
        'data_source': data_source,
        'unified_data_info': {
            'total_records': len(unified_data),
            'columns': list(unified_data.columns),
            'date_range': {
                'start': str(unified_data.index.min()) if not unified_data.empty else None,
                'end': str(unified_data.index.max()) if not unified_data.empty else None
            }
        }
    }
    
    # Add source statistics if available
    for key in ['merged_spread_data', 'synthetic_spread_data', 'real_spread_data']:
        if key in results and 'source_stats' in results[key]:
            metadata[f'{key}_stats'] = results[key]['source_stats']
    
    metadata_path = os.path.join(output_dir, f'{filename}_metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    print(f"   📁 Saved metadata: {metadata_path}")
    
    print(f"   ✅ Unified data summary: {len(unified_data):,} records, {unified_data.shape[1]} columns")
    print(f"   📊 Sample structure: {list(unified_data.columns)}")


def integrated_fetch(config: Dict) -> Dict:
    """
    Main integrated fetch function with unified input processing
    
    Config format:
    {
        'contracts': ['demb07_25', 'demb08_25'],  # 1=single, 2=spread
        'coefficients': [1, -1],  # Only for spreads
        'period': {
            'start_date': '2025-02-03',
            'end_date': '2025-06-02'
        },
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': True,
            'include_individual_legs': False
        },
        'n_s': 3
    }
    """
    contracts = config['contracts']
    period = config['period']
    options = config.get('options', {})
    coefficients = config.get('coefficients', [1, -1])
    n_s = config.get('n_s', 3)
    
    # Parse absolute contracts
    parsed_contracts = [parse_absolute_contract(c) for c in contracts]
    
    start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
    
    results = {
        'metadata': {
            'contracts': contracts,
            'parsed_contracts': [
                {
                    'contract': c.market + c.product[0] + c.tenor + c.contract,
                    'market': c.market, 'product': c.product, 'tenor': c.tenor,
                    'delivery_date': c.delivery_date.isoformat()
                } for c in parsed_contracts
            ],
            'period': period,
            'n_s': n_s,
            'mode': 'single_leg' if len(contracts) == 1 else 'spread'
        }
    }
    
    if len(parsed_contracts) == 1:
        # SINGLE LEG MODE
        print(f"🔍 SINGLE LEG MODE: {contracts[0]}")
        contract = parsed_contracts[0]
        
        # Use DataFetcher for individual contract
        fetcher = DataFetcher(allowed_broker_ids=[1441])
        
        # Create contract config for DataFetcher
        contract_config = {
            'market': contract.market,
            'tenor': contract.tenor,
            'contract': contract.contract,
            'start_date': period['start_date'],
            'end_date': period['end_date'],
            'prod': contract.product
        }
        
        single_data = fetcher.fetch_contract_data(
            contract_config,
            include_trades=True,
            include_orders=True
        )
        
        results['single_leg_data'] = single_data
        
    elif len(parsed_contracts) == 2:
        # SPREAD MODE
        print(f"🔍 SPREAD MODE: {contracts[0]} vs {contracts[1]}")
        contract1, contract2 = parsed_contracts
        
        # Real spread via DataFetcher (if requested)
        if options.get('include_real_spread', True):
            print("📈 Fetching real spread contract...")
            try:
                fetcher = DataFetcher(allowed_broker_ids=[1441])
                
                # Create contract configs for both legs
                contract1_config = create_contract_config_from_spec(contract1, period)
                contract2_config = create_contract_config_from_spec(contract2, period)
                
                # For cross-market spreads, DataFetcher needs combined market string
                if contract1.market != contract2.market:
                    combined_market = f"{contract1.market}_{contract2.market}"
                    print(f"   Cross-market spread detected: {contract1.market} + {contract2.market} → market='{combined_market}'")
                    contract1_config['market'] = combined_market
                    contract2_config['market'] = combined_market
                
                real_spread_data = fetcher.fetch_spread_contract_data(
                    contract1_config, contract2_config,
                    include_trades=True, include_orders=True
                )
                results['real_spread_data'] = real_spread_data
                print(f"   ✅ Real spread: {len(real_spread_data.get('spread_orders', pd.DataFrame()))} orders, {len(real_spread_data.get('spread_trades', pd.DataFrame()))} trades")
                
                # Process real spread data into unified format
                unified_real_data = create_unified_real_spread_data(real_spread_data)
                results['real_spread_data']['unified_spread_data'] = unified_real_data['unified_spread_data']
                
                # Save unified real spread data
                print("💾 Saving unified real spread data...")
                save_unified_results(results, config['contracts'], config['period'], 'real_only')
            except Exception as e:
                print(f"   ❌ Real spread failed: {e}")
                results['real_spread_error'] = str(e)
        
        # Synthetic spread via SpreadViewer (if requested) 
        if options.get('include_synthetic_spread', True):
            print("🔧 Fetching synthetic spread...")
            try:
                synthetic_spread_data = fetch_synthetic_spread_multiple_periods(
                    contract1, contract2, start_date, end_date, coefficients, n_s
                )
                # Store raw synthetic data first
                results['synthetic_spread_data'] = synthetic_spread_data
                
                # Create unified DataFrame from synthetic data
                unified_synthetic = create_unified_spreadviewer_data(synthetic_spread_data)
                results['synthetic_spread_data']['unified_spread_data'] = unified_synthetic['unified_spread_data']
                print(f"   ✅ Synthetic spread: {len(synthetic_spread_data.get('spread_orders', pd.DataFrame()))} orders, {len(synthetic_spread_data.get('spread_trades', pd.DataFrame()))} trades")
                print(f"   🎉 Unified synthetic data: {len(unified_synthetic.get('unified_spread_data', pd.DataFrame()))} total records")
                
                # Save unified synthetic spread data
                print("💾 Saving unified synthetic spread data...")
                save_unified_results(results, config['contracts'], config['period'], 'synthetic_only')
            except Exception as e:
                print(f"   ❌ Synthetic spread failed: {e}")
                results['synthetic_spread_error'] = str(e)
        
        # Merge real and synthetic spread data (if both are requested)
        if (options.get('include_real_spread', True) and 
            options.get('include_synthetic_spread', True) and
            'real_spread_data' in results and 
            'synthetic_spread_data' in results):
            print("🔗 Merging real and synthetic spread data...")
            try:
                merged_spread_data = merge_spread_data(
                    results['real_spread_data'],
                    results['synthetic_spread_data']
                )
                results['merged_spread_data'] = merged_spread_data
                print(f"   ✅ Merged spread: {len(merged_spread_data.get('unified_spread_data', pd.DataFrame()))} total records")
                
                # Save unified merged spread data
                print("💾 Saving unified merged spread data...")
                save_unified_results(results, config['contracts'], config['period'], 'merged')
            except Exception as e:
                print(f"   ❌ Spread merging failed: {e}")
                results['spread_merge_error'] = str(e)
        
        # Individual legs (if requested)
        if options.get('include_individual_legs', False):
            print("📊 Fetching individual leg data...")
            fetcher = DataFetcher(allowed_broker_ids=[1441])
            
            for i, contract in enumerate(parsed_contracts):
                contract_config = {
                    'market': contract.market,
                    'tenor': contract.tenor, 
                    'contract': contract.contract,
                    'start_date': period['start_date'],
                    'end_date': period['end_date'],
                    'prod': contract.product
                }
                
                try:
                    leg_data = fetcher.fetch_contract_data(
                        contract_config, include_trades=True, include_orders=True
                    )
                    results[f'leg_{i+1}_data'] = leg_data
                    print(f"   ✅ Leg {i+1}: {len(leg_data.get('orders', pd.DataFrame()))} orders, {len(leg_data.get('trades', pd.DataFrame()))} trades")
                except Exception as e:
                    print(f"   ❌ Leg {i+1} failed: {e}")
                    results[f'leg_{i+1}_error'] = str(e)
    else:
        raise ValueError("Only 1 or 2 contracts supported")
    
    return results


def main():
    """
    Main function with enhanced integration logic
    """
    print("🚀 Enhanced SpreadViewer + DataFetcher Integration Script V2")
    print("=" * 70)
    
    # Check dependencies
    if not SPREADVIEWER_AVAILABLE:
        print("❌ SpreadViewer not available - limited functionality")
    
    if not TPDATA_AVAILABLE:
        print("❌ TPData not available - cannot fetch data")
        return False
    
    # ⭐ ENHANCED UNIFIED CONFIGURATION ⭐
    config = {
        'contracts': ['debm08_25', 'frbm08_25'],  # German base monthly Aug2025/Sep2025
        'coefficients': [1, -1],  # Buy Jan, Sell Feb
        'period': {
            'start_date': '2025-07-01',  # Historical period with known data
            'end_date': '2025-07-05'     # Short range to test actual data fetching
        },
        'options': {
            'include_real_spread': True,      # DataFetcher real spread
            'include_synthetic_spread': True, # SpreadViewer synthetic spread
            'include_individual_legs': False   # Test individual contract data
        },
        'n_s': 3  # Last 3 business days transition
    }
    
    print(f"📋 Configuration:")
    print(f"   Contracts: {config['contracts']}")
    print(f"   Mode: {'Single Leg' if len(config['contracts']) == 1 else 'Spread'}")
    print(f"   Period: {config['period']['start_date']} to {config['period']['end_date']}")
    print(f"   n_s: {config['n_s']} (business day transition)")
    
    if len(config['contracts']) == 2:
        print(f"   Coefficients: {config['coefficients']}")
        print(f"   Options: {config['options']}")
    
    try:
        print(f"\n🔄 Starting integrated fetch...")
        results = integrated_fetch(config)
        
        print(f"\n✅ Integration completed successfully!")
        print(f"📊 Results summary:")
        
        metadata = results.get('metadata', {})
        print(f"   Mode: {metadata.get('mode', 'unknown')}")
        print(f"   Contracts processed: {len(metadata.get('parsed_contracts', []))}")
        
        # Single leg results
        if 'single_leg_data' in results:
            single_data = results['single_leg_data']
            print(f"   📈 Single leg: {len(single_data.get('orders', pd.DataFrame()))} orders, {len(single_data.get('trades', pd.DataFrame()))} trades")
        
        # Unified spread results
        if 'real_spread_data' in results:
            real_data = results['real_spread_data']
            if 'unified_spread_data' in real_data:
                print(f"   🎯 Real spread: {len(real_data['unified_spread_data'])} total records (unified)")
            else:
                print(f"   🎯 Real spread: {len(real_data.get('spread_orders', pd.DataFrame()))} orders, {len(real_data.get('spread_trades', pd.DataFrame()))} trades")
        
        if 'synthetic_spread_data' in results:
            synth_data = results['synthetic_spread_data']
            if 'unified_spread_data' in synth_data:
                print(f"   🔧 Synthetic spread: {len(synth_data['unified_spread_data'])} total records (unified)")
                print(f"       Method: {synth_data.get('method', 'unknown')}")
            else:
                print(f"   🔧 Synthetic spread: {len(synth_data.get('spread_orders', pd.DataFrame()))} orders, {len(synth_data.get('spread_trades', pd.DataFrame()))} trades")
        
        if 'merged_spread_data' in results:
            merged_data = results['merged_spread_data']
            if 'unified_spread_data' in merged_data:
                print(f"   🎉 Merged spread: {len(merged_data['unified_spread_data'])} total records (unified)")
                print(f"       Method: {merged_data.get('method', 'unknown')}")
            else:
                print(f"   🎉 Merged spread: {len(merged_data.get('spread_orders', pd.DataFrame()))} orders, {len(merged_data.get('spread_trades', pd.DataFrame()))} trades")
            if 'source_stats' in merged_data:
                stats = merged_data['source_stats']
                print(f"       Source breakdown: Real({stats['real_trades']}+{stats['real_orders']}) + Synthetic({stats['synthetic_trades']}+{stats['synthetic_orders']})")
        
        # Save results (optional)
        output_dir = output_base
        os.makedirs(output_dir, exist_ok=True)
        
        results_file = os.path.join(output_dir, 'integration_results_v2.json')
        
        # Convert DataFrames to dict for JSON serialization
        json_results = {}
        for key, value in results.items():
            if isinstance(value, dict):
                json_results[key] = {}
                for subkey, subvalue in value.items():
                    if isinstance(subvalue, pd.DataFrame):
                        json_results[key][subkey] = f"DataFrame with {len(subvalue)} rows"
                    else:
                        json_results[key][subkey] = subvalue
            else:
                json_results[key] = value
        
        with open(results_file, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        print(f"📁 Results summary saved: {results_file}")
        
        return True
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    
    if success:
        print("\n🎉 ENHANCED INTEGRATION COMPLETED SUCCESSFULLY!")
    else:
        print("\n💥 INTEGRATION FAILED!")
    
    print("=" * 70)