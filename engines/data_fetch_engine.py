r"""
Data Fetch Engine
=================

Production-ready data fetching engine that integrates SpreadViewer and DataFetcher.
This engine implements sophisticated integration between SpreadViewer and DataFetcher:
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
import argparse
from datetime import datetime, time, timedelta
import datetime as dt
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
import json
from dataclasses import dataclass

# Cross-platform project root
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
    rawdata_base = r'C:\Users\krajcovic\Documents\Testing Data\RawData'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python'
    rawdata_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData'

# Global variable for output base - will be set by command line args or defaults
output_base = None

sys.path.insert(0, project_root)  # Insert at beginning to take precedence
sys.path.insert(0, energy_trading_path)  # Insert at beginning to take precedence

print("🔍 DEBUG: About to import DataFetcher...")
from src.core.data_fetcher import DataFetcher, TPDATA_AVAILABLE, DeliveryDateCalculator
print("🔍 DEBUG: DataFetcher imported successfully")

print("🔍 DEBUG: About to import SpreadViewer classes...")
try:
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from Database.TPData import TPData
    
    # Verify which SpreadViewer we're using
    import SynthSpread.spreadviewer_class as svc_module
    print(f"📍 Using SpreadViewer from: {svc_module.__file__}")
    
    # Note: Pandas compatibility patches removed - they were causing hanging issues
    # Original SpreadViewer methods work fine with current pandas version
    
    SPREADVIEWER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SpreadViewer imports failed: {e}")
    SPREADVIEWER_AVAILABLE = False


class BidAskValidator:
    """
    Validator for filtering negative bid-ask spreads.
    
    Prevents financially impossible data where ask price < bid price from 
    entering the system.
    """
    
    def __init__(self, strict_mode: bool = True, log_filtered: bool = True):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, remove invalid records. If False, mark them.
            log_filtered: If True, log details of filtered records.
        """
        self.strict_mode = strict_mode
        self.log_filtered = log_filtered
        self.filtered_count = 0
        self.total_processed = 0
    
    def validate_orders(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """
        Validate bid-ask spreads in order data.
        
        Args:
            df: DataFrame with 'b_price' and 'a_price' columns
            source_name: Name of data source for logging
            
        Returns:
            Validated DataFrame (filtered or marked invalid records)
        """
        if df.empty:
            return df
        
        # Only validate if both bid and ask columns exist and have data
        if 'b_price' not in df.columns or 'a_price' not in df.columns:
            if self.log_filtered:
                print(f"   ⚠️  No bid/ask columns in {source_name} orders - skipping validation")
            return df
        
        # Count records with both bid and ask data
        has_both_prices = df['b_price'].notna() & df['a_price'].notna()
        valid_records = df[has_both_prices]
        
        if valid_records.empty:
            if self.log_filtered:
                print(f"   ⚠️  No records with both bid/ask in {source_name} - skipping validation")
            return df
        
        # Identify negative spreads (ask < bid)
        negative_mask = valid_records['a_price'] < valid_records['b_price']
        negative_count = negative_mask.sum()
        
        self.total_processed += len(valid_records)
        self.filtered_count += negative_count
        
        if negative_count > 0:
            if self.log_filtered:
                worst_spread = (valid_records.loc[negative_mask, 'a_price'] - 
                               valid_records.loc[negative_mask, 'b_price']).min()
                print(f"   🚫 {source_name}: Found {negative_count} negative spreads "
                      f"({negative_count/len(valid_records)*100:.1f}%) - worst: {worst_spread:.3f}")
                
                # Sample of problematic records
                sample_records = valid_records[negative_mask].head(3)
                for idx, row in sample_records.iterrows():
                    spread_val = row['a_price'] - row['b_price']
                    print(f"      🔍 {idx}: bid={row['b_price']:.3f}, ask={row['a_price']:.3f}, "
                          f"spread={spread_val:.3f}")
            
            if self.strict_mode:
                # Remove records with negative spreads
                invalid_indices = valid_records[negative_mask].index
                df_filtered = df.drop(invalid_indices)
                print(f"      ✅ {source_name}: Removed {negative_count} invalid records "
                      f"({len(df)} → {len(df_filtered)})")
                return df_filtered
            else:
                # Mark invalid records
                df.loc[valid_records[negative_mask].index, 'is_valid'] = False
                print(f"      ⚠️  {source_name}: Marked {negative_count} records as invalid")
        else:
            if self.log_filtered:
                print(f"      ✅ {source_name}: All {len(valid_records)} records have valid spreads")
        
        return df
    
    def validate_merged_data(self, df: pd.DataFrame, source_name: str = "Engine") -> pd.DataFrame:
        """
        Validate bid-ask spreads in merged data (trades + orders format).
        Alias for validate_orders to maintain compatibility.
        """
        return self.validate_orders(df, source_name)
    
    def get_stats(self) -> Dict:
        """Get validation statistics."""
        return {
            'total_processed': self.total_processed,
            'filtered_count': self.filtered_count,
            'filter_rate': self.filtered_count / max(1, self.total_processed) * 100
        }


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
    Parse absolute contract with product encoding - supports 2-3 letter market codes
    
    Examples:
        'debm07_25' → market='de', product='base', tenor='m', contract='07_25'
        'depm07_25' → market='de', product='peak', tenor='m', contract='07_25'
        'ttfbm09_25' → market='ttf', product='base', tenor='m', contract='09_25'
    """
    if len(contract_str) < 6:
        raise ValueError(f"Invalid contract format: {contract_str}")
    
    # Known 3-letter market codes
    three_letter_markets = {'ttf', 'nbp', 'peg', 'zee', 'gas'}
    
    # Try 3-letter market code first
    if len(contract_str) >= 7 and contract_str[:3].lower() in three_letter_markets:
        market = contract_str[:3].lower()       # 'ttf'
        product_code = contract_str[3:4]        # 'b' or 'p'
        tenor = contract_str[4:5]              # 'm'
        contract = contract_str[5:]            # '09_25'
    else:
        # Default to 2-letter market code
        market = contract_str[:2].lower()       # 'de'
        product_code = contract_str[2:3]        # 'b' or 'p'
        tenor = contract_str[3:4]              # 'm'
        contract = contract_str[4:]            # '09_25'
    
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
    Convert absolute contract to relative periods with consistent n_s transition logic
    
    FIXED: Ensures consistent relative period mapping across the entire date range
    to prevent mixing of different relative periods (e.g., q_1 vs q_2) within same fetch.
    
    Returns list of (RelativePeriod, period_start, period_end) tuples
    """
    periods = []
    
    # For quarterly contracts, use quarterly-based transition logic instead of monthly
    if contract_spec.tenor == 'q':
        print(f"🔧 Using CONSISTENT quarterly transition logic for {contract_spec.contract}")
        
        # CORRECTED: Transition happens AT June 26th (3rd business day from end)
        # Both June 26th and June 27th should be q_1
        transition_date = datetime(2025, 6, 26)  # Transition happens AT June 26
        
        if start_date < transition_date <= end_date:
            # Period spans transition - split into pre and post transition periods
            print(f"🔧 Period spans transition at {transition_date.strftime('%Y-%m-%d')} - splitting periods")
            
            # Pre-transition period (should use q_2)
            if start_date < transition_date:
                pre_end = transition_date - timedelta(days=1)
                pre_end = pre_end.replace(hour=23, minute=59, second=59)
                
                pre_rel_period = RelativePeriod(
                    relative_offset=2,  # Q2 perspective: Q4 delivery = 2 quarters ahead
                    start_date=start_date,
                    end_date=pre_end
                )
                periods.append((pre_rel_period, start_date, pre_end))
                print(f"   📊 Pre-transition: q_2 ({start_date.strftime('%Y-%m-%d')} to {pre_end.strftime('%Y-%m-%d')})")
            
            # Post-transition period (should use q_1)  
            if transition_date <= end_date:
                post_start = transition_date.replace(hour=0, minute=0, second=0)
                
                post_rel_period = RelativePeriod(
                    relative_offset=1,  # Q3 perspective: Q4 delivery = 1 quarter ahead
                    start_date=post_start,
                    end_date=end_date
                )
                periods.append((post_rel_period, post_start, end_date))
                print(f"   📊 Post-transition: q_1 ({post_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
                
            return periods
        
        # Period doesn't span transition - use original logic with middle date
        middle_date = start_date + (end_date - start_date) / 2
        
        # Get reference quarter for middle date
        ref_quarter = ((middle_date.month - 1) // 3) + 1
        ref_year = middle_date.year
        
        # Check if middle date is in transition using DataFetcher logic
        if ref_quarter == 1:
            quarter_end = datetime(ref_year, 3, 31)
        elif ref_quarter == 2:
            quarter_end = datetime(ref_year, 6, 30)
        elif ref_quarter == 3:
            quarter_end = datetime(ref_year, 9, 30)
        else:  # Q4
            quarter_end = datetime(ref_year, 12, 31)
        
        # Find last business day of quarter
        last_bday = quarter_end
        while last_bday.weekday() > 4:
            last_bday -= timedelta(days=1)
        
        # Calculate transition start
        transition_start = last_bday
        for _ in range(n_s - 1):
            transition_start -= timedelta(days=1)
            while transition_start.weekday() > 4:
                transition_start -= timedelta(days=1)
        
        # Check if middle date is in transition - CORRECTED LOGIC
        # User observation: June 26 should be q_1 when n_s=3
        # June 26 = 3rd business day from end → should use NEXT quarter perspective
        in_transition = transition_start.date() <= middle_date.date() <= last_bday.date()
        
        if in_transition:
            # Use NEXT quarter perspective for entire period
            if ref_quarter == 4:
                calc_quarter = 1
                calc_year = ref_year + 1
            else:
                calc_quarter = ref_quarter + 1
                calc_year = ref_year
        else:
            # Use CURRENT quarter perspective for entire period
            calc_quarter = ref_quarter
            calc_year = ref_year
        
        # Calculate relative offset using consistent reference
        delivery_quarter = ((contract_spec.delivery_date.month - 1) // 3) + 1
        
        calc_quarters = calc_year * 4 + (calc_quarter - 1)
        delivery_quarters = contract_spec.delivery_date.year * 4 + (delivery_quarter - 1)
        relative_offset = delivery_quarters - calc_quarters
        
        print(f"   📅 Contract: {contract_spec.contract} (Q{delivery_quarter} {contract_spec.delivery_date.year})")
        print(f"   📊 Reference perspective: Q{calc_quarter} {calc_year} (transition: {in_transition})")
        print(f"   📊 Relative offset: {relative_offset} (q_{relative_offset})")
        
        if relative_offset > 0:
            # Create single consistent period for entire date range
            relative_period = RelativePeriod(
                relative_offset=relative_offset,
                start_date=start_date,
                end_date=end_date
            )
            periods.append((relative_period, start_date, end_date))
    
    else:
        # For non-quarterly contracts, use original logic with monthly transitions
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
            
            # Calculate month difference for monthly/yearly contracts
            months_diff = ((contract_spec.delivery_date.year - ref_year) * 12 + 
                          (contract_spec.delivery_date.month - ref_month))
            relative_offset = months_diff
            
            if relative_offset > 0:  # Only include future contracts
                relative_period = RelativePeriod(
                    relative_offset=relative_offset,
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


def calculate_synchronized_product_dates(dates: pd.DatetimeIndex, tenors_list: List[str], 
                                       tn1_list: List[int], n_s: int = 3) -> List[pd.DatetimeIndex]:
    """
    Calculate product dates with CORRECTED n_s logic
    
    CORRECT n_s logic:
    - n_s denotes how many business days FORWARD from each date should be shifted to get product start date
    - Reverse logic: for start date of absolute period, get relative periods for each date, 
      then shift BACK by n_s to get the original reference date
    - Filter out relative period 0, keep only 1 and above
    """
    print(f"   🔧 Using CORRECTED n_s logic: forward shift to product start date")
    print(f"      📅 Input dates: {dates[0]} to {dates[-1]} ({len(dates)} business days)")
    print(f"      📊 Tenors: {tenors_list}, Periods: {tn1_list}, n_s: {n_s}")
    
    product_dates_list = []
    
    for tenor, tn in zip(tenors_list, tn1_list):
        print(f"      🔄 Processing tenor {tenor}, relative period {tn}")
        
        # Filter out relative period 0 - only keep 1 and above
        if tn <= 0:
            print(f"      ⚠️  Skipping relative period {tn} (must be >= 1)")
            product_dates_list.append(pd.DatetimeIndex([]))
            continue
        
        if tenor in ['da', 'd']:
            # Daily contracts
            pd_result = dates.shift(1, freq='B')
        elif tenor == 'w':
            # Weekly contracts
            pd_result = dates.shift(tn, freq='W-MON')
        elif tenor == 'dec':
            # December contracts
            pd_result = dates.shift(tn, freq='YS')
        elif tenor == 'm1q':
            # M1Q contracts
            pd_result = dates.shift(tn, freq='QS')
        elif tenor in ['sum']:
            # Summer contracts - use original SpreadViewer logic
            shifted_dates = dates + n_s * dates.freq
            pd_result = shifted_dates.shift(tn, freq='AS-Apr')
        elif tenor in ['win']:
            # Winter contracts - use original SpreadViewer logic
            shifted_dates = dates + n_s * dates.freq
            pd_result = shifted_dates.shift(tn, freq='AS-Oct')
        else:
            # Standard contracts (monthly 'm', quarterly 'q', yearly 'y')
            # CORRECTED LOGIC: Use original SpreadViewer approach
            # n_s business days forward + relative period shift
            
            # Step 1: Shift forward by n_s business days
            # Ensure dates have business day frequency for proper calculation
            if dates.freq is None:
                dates = pd.date_range(start=dates[0], end=dates[-1], freq='B')
            shifted_dates = dates + n_s * dates.freq
            print(f"         📅 Step 1: Forward shift by {n_s} business days")
            print(f"         📅 Original: {dates[0].strftime('%Y-%m-%d')} → Shifted: {shifted_dates[0].strftime('%Y-%m-%d')}")
            
            # Step 2: Apply relative period shift
            if tenor.startswith('q') or tenor == 'q':
                pandas_freq = 'QS'  # Quarterly start
            elif tenor.startswith('m') or tenor == 'm':
                pandas_freq = 'MS'  # Monthly start  
            elif tenor.startswith('y') or tenor == 'y':
                pandas_freq = 'YS'  # Yearly start
            else:
                pandas_freq = tenor.upper() + 'S'  # Fallback for other tenors
            
            pd_result = shifted_dates.shift(tn, freq=pandas_freq)
            print(f"         📅 Step 2: Relative period shift by {tn} {pandas_freq}")
            print(f"         📅 Final result: {pd_result[0].strftime('%Y-%m-%d')}")
        
        product_dates_list.append(pd_result)
        print(f"      ✅ Tenor {tenor}, period {tn}: {len(pd_result)} dates calculated")
        
        # Debug output for first few dates
        if len(pd_result) > 0:
            sample_dates = pd_result[:min(3, len(pd_result))]
            print(f"         📅 Sample results: {[d.strftime('%Y-%m-%d') for d in sample_dates]}")
    
    print(f"   ✅ CORRECTED product_dates calculation completed")
    return product_dates_list


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


def detect_price_outliers(trades_df: pd.DataFrame, z_threshold: float = 5.0, 
                         window_size: int = 50, max_pct_change: float = 50.0,
                         min_time_gap_minutes: float = 60.0) -> pd.DataFrame:
    """
    Detect and filter extreme price outliers in trading data using rolling z-score analysis
    
    Args:
        trades_df: DataFrame with price column and datetime index
        z_threshold: Z-score threshold for outlier detection (default: 5.0)
        window_size: Rolling window size for volatility estimation (default: 50)
        max_pct_change: Hard limit on percentage price changes (default: 50%)
        min_time_gap_minutes: Minimum time gap to adjust threshold (default: 60 min)
    
    Returns:
        Filtered DataFrame with outliers removed
    """
    if trades_df.empty or 'price' not in trades_df.columns:
        return trades_df
    
    print(f"   🔍 Detecting price outliers (z_threshold={z_threshold}, window={window_size})")
    
    # Create copy to avoid modifying original
    df = trades_df.copy()
    
    # Only analyze rows with valid prices
    price_mask = df['price'].notna() & (df['price'] > 0)
    if price_mask.sum() < 2:
        print(f"      ⚠️  Insufficient price data for outlier detection")
        return df
    
    price_data = df.loc[price_mask].copy()
    price_data = price_data.sort_index()
    
    # Calculate returns (percentage price changes)
    price_data['prev_price'] = price_data['price'].shift(1)
    price_data['price_return'] = (price_data['price'] - price_data['prev_price']) / price_data['prev_price'] * 100
    
    # Calculate time gaps between trades (in minutes)
    price_data['time_gap'] = price_data.index.to_series().diff().dt.total_seconds() / 60
    
    # Rolling volatility estimation
    price_data['rolling_std'] = price_data['price_return'].rolling(
        window=min(window_size, len(price_data)), min_periods=5
    ).std()
    
    # Calculate z-scores
    price_data['rolling_mean'] = price_data['price_return'].rolling(
        window=min(window_size, len(price_data)), min_periods=5
    ).mean()
    
    price_data['z_score'] = np.abs(
        (price_data['price_return'] - price_data['rolling_mean']) / price_data['rolling_std']
    )
    
    # Adjust z-score threshold for large time gaps
    # If trades are far apart, allow larger price moves
    time_gap_factor = np.clip(price_data['time_gap'] / min_time_gap_minutes, 1.0, 3.0)
    adjusted_z_threshold = z_threshold * time_gap_factor
    
    # Create outlier flags
    outliers = pd.Series(False, index=price_data.index)
    
    # Flag 1: Z-score outliers (after sufficient history)
    z_outliers = (price_data['z_score'] > adjusted_z_threshold) & price_data['rolling_std'].notna()
    
    # Flag 2: Hard percentage change limits
    pct_outliers = np.abs(price_data['price_return']) > max_pct_change
    
    # Combine flags
    outliers = z_outliers | pct_outliers
    
    # Statistics
    total_trades = len(price_data)
    z_score_outliers = z_outliers.sum()
    pct_outliers_count = pct_outliers.sum()
    total_outliers = outliers.sum()
    
    print(f"      📊 Outlier analysis:")
    print(f"         Total trades analyzed: {total_trades}")
    print(f"         Z-score outliers: {z_score_outliers}")
    print(f"         Percentage outliers: {pct_outliers_count}")
    print(f"         Total outliers removed: {total_outliers} ({total_outliers/total_trades*100:.1f}%)")
    
    if total_outliers > 0:
        print(f"      🚫 Outlier examples:")
        outlier_samples = price_data[outliers].head(3)
        for idx, row in outlier_samples.iterrows():
            print(f"         {idx}: {row['prev_price']:.2f} → {row['price']:.2f} "
                  f"({row['price_return']:+.1f}%, z={row['z_score']:.1f})")
    
    # Filter out outliers from original DataFrame
    outlier_indices = price_data[outliers].index
    filtered_df = df.drop(outlier_indices)
    
    print(f"      ✅ Price outlier filtering: {len(df)} → {len(filtered_df)} trades "
          f"({total_outliers} outliers removed)")
    
    return filtered_df


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
    
    # Stage 1.5: Validate bid-ask spreads (filter negative spreads)
    print("   🔍 Stage 1.5: Validating bid-ask spreads")
    validator = BidAskValidator(strict_mode=True, log_filtered=True)
    
    # Validate orders from both sources
    real_orders_formatted = validator.validate_orders(real_orders_formatted, "DataFetcher")
    synthetic_orders_formatted = validator.validate_orders(synthetic_orders_formatted, "SpreadViewer")
    
    # Log validation summary
    stats = validator.get_stats()
    if stats['total_processed'] > 0:
        print(f"      📊 Validation summary: {stats['filtered_count']}/{stats['total_processed']} "
              f"negative spreads filtered ({stats['filter_rate']:.1f}%)")
    
    # Stage 2: Merge trades (simple union)
    print("   📊 Stage 2: Merging trades (union)")
    merged_trades = pd.DataFrame()
    
    if not real_trades_formatted.empty:
        merged_trades = pd.concat([merged_trades, real_trades_formatted], axis=0)
    if not synthetic_trades_formatted.empty:
        merged_trades = pd.concat([merged_trades, synthetic_trades_formatted], axis=0)
    
    if not merged_trades.empty:
        merged_trades = merged_trades.sort_index().drop_duplicates()
        
        # Apply price outlier detection to merged trades
        print("   🔍 Stage 2.5: Price outlier detection on merged trades")
        merged_trades = detect_price_outliers(
            merged_trades, 
            z_threshold=5.0,      # Conservative threshold
            window_size=50,       # Rolling window for volatility
            max_pct_change=8.0,   # Hard limit on price changes (8%)
            min_time_gap_minutes=60.0  # Time gap adjustment
        )
    
    print(f"      ✅ Merged trades (after outlier filtering): {len(merged_trades)} records")
    
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


def save_single_leg_results(single_data: Dict, contract: str, period: Dict, test_mode: bool = False, suffix: str = '', save_separate_csv: bool = False) -> None:
    """Save single leg data as parquet files
    
    Args:
        single_data: Dictionary containing 'trades' and 'orders' DataFrames
        contract: Contract name (e.g., 'debm09_25')
        period: Period dictionary with start/end dates
        test_mode: If True, saves all formats to test/ subdirectory
        suffix: Optional suffix to add to filename (e.g., '_v2', '_test')
        save_separate_csv: If True, saves trades and orders as separate CSV files
    """
    output_dir = output_base
    if test_mode:
        output_dir = os.path.join(output_base, 'test')
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract trades and orders DataFrames
    trades_df = single_data.get('trades', pd.DataFrame())
    orders_df = single_data.get('orders', pd.DataFrame())
    
    if trades_df.empty and orders_df.empty:
        print(f"   ⚠️  No single leg data to save for {contract}")
        return
    
    # Transform data to unified format
    print(f"   🔄 Transforming single leg data to unified format for {contract}")
    trades_formatted = transform_trades_to_target_format(trades_df, 'datafetcher') if not trades_df.empty else pd.DataFrame()
    orders_formatted = transform_orders_to_target_format(orders_df, 'datafetcher') if not orders_df.empty else pd.DataFrame()
    
    # Merge trades and orders into unified format
    unified_data = pd.DataFrame()
    if not trades_formatted.empty:
        unified_data = pd.concat([unified_data, trades_formatted], axis=0)
    if not orders_formatted.empty:
        unified_data = pd.concat([unified_data, orders_formatted], axis=0)
    
    if not unified_data.empty:
        # Sort by timestamp and ensure target column order
        unified_data = unified_data.sort_index()
        target_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
        unified_data = unified_data.reindex(columns=target_columns)
    
    # Generate filename: contract_tr_ba_data[suffix]
    filename = f"{contract}_tr_ba_data{suffix}"
    
    # Validate data before saving
    print(f"   🔍 Validating single leg data for {contract}")
    validator = BidAskValidator(strict_mode=True, log_filtered=True)
    validated_data = validator.validate_merged_data(unified_data, f"SingleLeg_{contract}")
    
    # Log validation summary
    stats = validator.get_stats()
    if stats['total_processed'] > 0:
        print(f"      📊 Single leg validation: {stats['filtered_count']}/{stats['total_processed']} "
              f"negative spreads filtered ({stats['filter_rate']:.1f}%)")
    
    # Always save as parquet
    parquet_path = os.path.join(output_dir, f'{filename}.parquet')
    validated_data.to_parquet(parquet_path)
    print(f"   📁 Saved single leg data: {parquet_path}")
    
    # In test mode, also save CSV and pickle formats
    if test_mode:
        # Save as CSV
        csv_path = os.path.join(output_dir, f'{filename}.csv')
        validated_data.to_csv(csv_path)
        print(f"   📁 Saved single leg data: {csv_path}")
        
        # Save as pickle
        pkl_path = os.path.join(output_dir, f'{filename}.pkl')
        validated_data.to_pickle(pkl_path)
        print(f"   📁 Saved single leg data: {pkl_path}")
        
        # Save metadata
        metadata = {
            'contract': contract,
            'period': period,
            'data_info': {
                'total_records': len(validated_data),
                'trades_count': len(trades_df),
                'orders_count': len(orders_df),
                'columns': list(validated_data.columns),
                'date_range': {
                    'start': str(validated_data.index.min()) if not validated_data.empty else None,
                    'end': str(validated_data.index.max()) if not validated_data.empty else None
                }
            },
            'timestamp': datetime.now().isoformat()
        }
        
        metadata_path = os.path.join(output_dir, f'{filename}_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"   📁 Saved single leg metadata: {metadata_path}")
    
    print(f"   ✅ Single leg data saved: {len(validated_data):,} records")
    
    # Save trades and orders as separate CSV files if requested
    if save_separate_csv:
        print(f"   📁 Saving separate trades and orders CSV files...")
        
        # Save trades CSV
        if not trades_df.empty:
            trades_csv_path = os.path.join(output_dir, f'{contract}_trades{suffix}.csv')
            trades_df.to_csv(trades_csv_path)
            print(f"   📁 Saved trades CSV: {trades_csv_path} ({len(trades_df):,} records)")
        
        # Save orders CSV
        if not orders_df.empty:
            orders_csv_path = os.path.join(output_dir, f'{contract}_orders{suffix}.csv')
            orders_df.to_csv(orders_csv_path)
            print(f"   📁 Saved orders CSV: {orders_csv_path} ({len(orders_df):,} records)")


def save_unified_results(results: Dict, contracts: List[str], period: Dict, stage: str = 'unified', test_mode: bool = False, suffix: str = '', save_separate_csv: bool = False) -> None:
    """Save unified spread data with format options based on test mode
    
    Args:
        test_mode: If True, saves all formats (parquet, csv, json) to RawData/test/
                  If False, saves only parquet to RawData/
        suffix: Optional suffix to add to filename (e.g., '_v2', '_test')
        save_separate_csv: If True, saves trades and orders as separate CSV files
    """
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
        # Single contract: contract_tr_ba_data[suffix]
        filename = f"{contracts[0]}_tr_ba_data{suffix}"
    elif len(contracts) == 2:
        # Spread: contract1_contract2_tr_ba_data[suffix]
        filename = f"{contracts[0]}_{contracts[1]}_tr_ba_data{suffix}"
    else:
        # Fallback for multiple contracts
        contract_names = '_'.join(contracts)
        filename = f"{contract_names}_tr_ba_data{suffix}"
    
    # Validate bid-ask spreads before saving
    print(f"   🔍 Final validation: Checking for negative bid-ask spreads...")
    validator = BidAskValidator(strict_mode=True, log_filtered=True)
    validated_data = validator.validate_merged_data(unified_data, "FinalData")
    
    # Log validation summary
    stats = validator.get_stats()
    if stats['total_processed'] > 0:
        print(f"      📊 Final validation: {stats['filtered_count']}/{stats['total_processed']} "
              f"negative spreads filtered ({stats['filter_rate']:.1f}%)")
    
    # Always save as parquet
    parquet_path = os.path.join(output_dir, f'{filename}.parquet')
    validated_data.to_parquet(parquet_path)
    print(f"   📁 Saved validated spread data: {parquet_path}")
    
    # In test mode, also save CSV and pickle formats
    if test_mode:
        # Save as CSV
        csv_path = os.path.join(output_dir, f'{filename}.csv')
        validated_data.to_csv(csv_path)
        print(f"   📁 Saved validated spread data: {csv_path}")
        
        # Save as pickle
        pkl_path = os.path.join(output_dir, f'{filename}.pkl')
        validated_data.to_pickle(pkl_path)
        print(f"   📁 Saved validated spread data: {pkl_path}")
    
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
    
    # In test mode, also save metadata
    if test_mode:
        metadata_path = os.path.join(output_dir, f'{filename}_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        print(f"   📁 Saved metadata: {metadata_path}")
    
    print(f"   ✅ Unified data summary: {len(unified_data):,} records, {unified_data.shape[1]} columns")
    print(f"   📊 Sample structure: {list(unified_data.columns)}")
    
    # Save trades and orders as separate CSV files if requested
    if save_separate_csv and not unified_data.empty:
        print(f"   📁 Saving separate trades and orders CSV files for spread...")
        
        # Separate trades (has price and volume) from orders (has bid/ask prices)
        trades_mask = unified_data['price'].notna() & unified_data['volume'].notna()
        orders_mask = unified_data['b_price'].notna() | unified_data['a_price'].notna()
        
        trades_df = unified_data[trades_mask & ~orders_mask]
        orders_df = unified_data[~trades_mask & orders_mask]
        
        # Generate base filename
        if len(contracts) == 1:
            base_filename = contracts[0]
        elif len(contracts) == 2:
            base_filename = f"{contracts[0]}_{contracts[1]}"
        else:
            base_filename = '_'.join(contracts)
        
        # Save trades CSV
        if not trades_df.empty:
            trades_csv_path = os.path.join(output_dir, f'{base_filename}_trades{suffix}.csv')
            trades_df.to_csv(trades_csv_path)
            print(f"   📁 Saved spread trades CSV: {trades_csv_path} ({len(trades_df):,} records)")
        
        # Save orders CSV
        if not orders_df.empty:
            orders_csv_path = os.path.join(output_dir, f'{base_filename}_orders{suffix}.csv')
            orders_df.to_csv(orders_csv_path)
            print(f"   📁 Saved spread orders CSV: {orders_csv_path} ({len(orders_df):,} records)")


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
        
        # Save single leg data as parquet
        print("💾 Saving single leg data...")
        save_single_leg_results(single_data, contracts[0], period, config.get('test_mode', False), config.get('file_suffix', ''), config.get('save_separate_csv', False))
        
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
                save_unified_results(results, config['contracts'], config['period'], 'real_only', config.get('test_mode', False), config.get('file_suffix', ''), config.get('save_separate_csv', False))
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
                save_unified_results(results, config['contracts'], config['period'], 'synthetic_only', config.get('test_mode', False), config.get('file_suffix', ''), config.get('save_separate_csv', False))
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
                save_unified_results(results, config['contracts'], config['period'], 'merged', config.get('test_mode', False), config.get('file_suffix', ''), config.get('save_separate_csv', False))
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


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Data Fetch Engine - Integrate SpreadViewer and DataFetcher')
    
    parser.add_argument('--contracts', nargs='+', required=True,
                      help='Contract list (e.g., debm08_25 frbm08_25)')
    parser.add_argument('--start-date', required=True,
                      help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', required=True,
                      help='End date (YYYY-MM-DD format)')
    parser.add_argument('--n-s', type=int, default=3,
                      help='Business day transition parameter (default: 3)')
    parser.add_argument('--mode', choices=['spread', 'single'], default='spread',
                      help='Processing mode: spread or single (default: spread)')
    parser.add_argument('--coefficients', nargs='+', type=float, default=[1, -1],
                      help='Spread coefficients (default: [1, -1])')
    parser.add_argument('--test-mode', action='store_true',
                      help='Test mode: saves all formats (parquet, csv, pkl) to RawData/test/. Production mode: only parquet to RawData/')
    parser.add_argument('--include-real', action='store_true', default=True,
                      help='Include real spread data from DataFetcher (default: True)')
    parser.add_argument('--include-synthetic', action='store_true', default=True,
                      help='Include synthetic spread data from SpreadViewer (default: True)')
    parser.add_argument('--include-legs', action='store_true',
                      help='Include individual leg data (default: False)')
    parser.add_argument('--suffix', default='',
                      help='Optional suffix for output files (e.g., "_v2", "_test")')
    parser.add_argument('--save-separate-csv', action='store_true',
                      help='Save trades and orders as separate CSV files')
    
    return parser.parse_args()


def main():
    """
    Main function with dictionary configuration
    """
    global output_base
    
    print("🚀 Data Fetch Engine")
    print("=" * 50)
    print("🔍 DEBUG: Starting main function...")
    
    # Configuration dictionary - SpreadViewer ONLY
    config = {
        'contracts': ['debm11_25'],  # Example contracts
        'coefficients': [1],
        'period': {
            'start_date': '2025-10-15',  # Same period for comparison
            'end_date': '2025-10-16'
        },
        'options': {
            'include_real_spread': True,  # SpreadViewer ONLY
            'include_synthetic_spread': False,
            'include_individual_legs': True
        },
        'n_s': 3,
        'test_mode': False,  # Save with suffix to distinguish
        # 'file_suffix': '_init_candles',  # Optional suffix for output files (e.g., '_v2', '_test')
        'file_suffix': '',  # Optional suffix for output files (e.g., '_v2', '_test')
        'save_separate_csv': False  # Save trades and orders as separate CSV files
    }
    
    # Set output base based on test mode
    if config['test_mode']:
        output_base = os.path.join(rawdata_base, 'test')
        print(f"🧪 Test mode: Full output (parquet + csv + pkl + metadata)")
    else:
        output_base = rawdata_base
        print(f"🚀 Production mode: Parquet output only")
    
    print(f"📁 Output directory: {output_base}")
    
    print("🔍 DEBUG: Checking dependencies...")
    # Check dependencies
    if not SPREADVIEWER_AVAILABLE:
        print("❌ SpreadViewer not available - limited functionality")
    else:
        print("✅ SpreadViewer available")
    
    if not TPDATA_AVAILABLE:
        print("❌ TPData not available - cannot fetch data")
        return False
    else:
        print("✅ TPData available")
    
    print("🔍 DEBUG: About to call integrated_fetch...")
    
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
        print("\n🎉 DATA FETCH ENGINE COMPLETED SUCCESSFULLY!")
    else:
        print("\n💥 DATA FETCH ENGINE FAILED!")
    
    print("=" * 50)