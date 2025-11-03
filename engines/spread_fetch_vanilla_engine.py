r"""
Spread Fetch Vanilla Engine
===========================

VANILLA ENGINE with STRICT combination rules for spread generation.

This is a simplified version of spread_fetch_engine.py with much stricter rules:

STRICT COMBINATION RULES:
1. Cross-country spreads: ONLY same period and year (debm5_25 vs frbm5_25)
2. Same-country spreads: ONLY same period type AND sequential periods:
   - Monthly: jan/feb, apr/may, dec/jan, etc.
   - Quarterly: q1_25/q2_25, q4_25/q1_26, etc.
   - Yearly: y_25/y_26, y_26/y_27, etc.

Usage:
    python spread_fetch_vanilla_engine.py --contracts debm07_25 debm08_25 frbm07_25 --start-date 2025-02-03 --end-date 2025-06-02
"""

import sys
import os
import argparse
from datetime import datetime, time, timedelta
import datetime as dt
from typing import Dict, List, Tuple, Optional, Set
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import json
from itertools import combinations
from dataclasses import dataclass

# Cross-platform project root
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
    rawdata_base = r'C:\Users\krajcovic\Documents\Testing Data\RawData\spreads'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/EnergyTrading/Python'
    rawdata_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads'

# Global variable for output base - will be set by command line args or defaults
output_base = None

sys.path.insert(0, project_root)  # Insert at beginning to take precedence
sys.path.insert(0, energy_trading_path)  # Insert at beginning to take precedence

# Import core modules from data_fetch_engine
from engines.data_fetch_engine import (
    DataFetcher, TPDATA_AVAILABLE, DeliveryDateCalculator,
    BidAskValidator, ContractSpec, RelativePeriod,
    parse_absolute_contract, convert_absolute_to_relative_periods,
    fetch_synthetic_spread_multiple_periods, merge_spread_data,
    create_unified_spreadviewer_data, create_unified_real_spread_data,
    save_unified_results, detect_price_outliers,
    SPREADVIEWER_AVAILABLE
)
import engines.data_fetch_engine as dfe

# Additional imports for SpreadViewer if available
if SPREADVIEWER_AVAILABLE:
    try:
        from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
        from Database.TPData import TPData
    except ImportError as e:
        print(f"Warning: SpreadViewer imports failed: {e}")
        SPREADVIEWER_AVAILABLE = False


def calculate_contract_trading_end(contract_spec: ContractSpec) -> datetime:
    """
    Calculate when a contract stops trading based on its delivery date
    
    Args:
        contract_spec: Parsed contract specification
    
    Returns:
        datetime when trading ends
    """
    delivery_date = contract_spec.delivery_date
    
    # For monthly contracts, trading typically ends a few days before delivery
    # For quarterly contracts, trading might end at the end of the last month of the quarter
    if contract_spec.tenor == 'm':
        # Monthly contracts: trading ends ~3 days before delivery month starts
        trading_end = delivery_date - timedelta(days=3)
    elif contract_spec.tenor == 'q':
        # Quarterly contracts: trading ends at the end of the last month of the previous quarter
        if delivery_date.month in [1, 2, 3]:  # Q1 delivery
            trading_end = datetime(delivery_date.year - 1, 12, 31)
        elif delivery_date.month in [4, 5, 6]:  # Q2 delivery  
            trading_end = datetime(delivery_date.year, 3, 31)
        elif delivery_date.month in [7, 8, 9]:  # Q3 delivery
            trading_end = datetime(delivery_date.year, 6, 30)
        else:  # Q4 delivery
            trading_end = datetime(delivery_date.year, 9, 30)
    else:
        # Default: trading ends a few days before delivery
        trading_end = delivery_date - timedelta(days=3)
    
    return trading_end


def calculate_contract_trading_period(contract_spec: ContractSpec, months_back: int = 2) -> Dict[str, str]:
    """
    Calculate the last N months of trading for a contract ending at its delivery date
    
    Args:
        contract_spec: Parsed contract specification
        months_back: Number of months back from delivery date (default: 2)
    
    Returns:
        Dict with 'start_date' and 'end_date' strings in 'YYYY-MM-DD' format
    """
    trading_end = calculate_contract_trading_end(contract_spec)
    
    # Calculate start date: N months back from trading end
    trading_start = trading_end - relativedelta(months=months_back)
    
    return {
        'start_date': trading_start.strftime('%Y-%m-%d'),
        'end_date': trading_end.strftime('%Y-%m-%d')
    }


def calculate_spread_trading_period(contract1_spec: ContractSpec, contract2_spec: ContractSpec, months_back: int = 2) -> Dict[str, str]:
    """
    Calculate the trading period for a spread using conservative approach
    
    The spread can only be traded until the earliest contract stops trading.
    
    Args:
        contract1_spec: First contract specification
        contract2_spec: Second contract specification
        months_back: Number of months back from earliest trading end (default: 2)
    
    Returns:
        Dict with 'start_date' and 'end_date' strings in 'YYYY-MM-DD' format
    """
    # Get trading end dates for both contracts
    trading_end1 = calculate_contract_trading_end(contract1_spec)
    trading_end2 = calculate_contract_trading_end(contract2_spec)
    
    # Use the EARLIEST trading end (conservative approach)
    spread_trading_end = min(trading_end1, trading_end2)
    
    # Calculate start date: N months back from spread trading end
    spread_trading_start = spread_trading_end - relativedelta(months=months_back)
    
    print(f"   📅 SPREAD TRADING PERIOD CALCULATION:")
    print(f"      Contract 1 ({contract1_spec.contract}) ends: {trading_end1.strftime('%Y-%m-%d')}")
    print(f"      Contract 2 ({contract2_spec.contract}) ends: {trading_end2.strftime('%Y-%m-%d')}")
    print(f"      Spread ends: {spread_trading_end.strftime('%Y-%m-%d')} (earliest)")
    print(f"      Spread period: {spread_trading_start.strftime('%Y-%m-%d')} to {spread_trading_end.strftime('%Y-%m-%d')}")
    
    return {
        'start_date': spread_trading_start.strftime('%Y-%m-%d'),
        'end_date': spread_trading_end.strftime('%Y-%m-%d')
    }


@dataclass
class SpreadCombination:
    """Represents a spread combination between two contracts"""
    contract1: str
    contract2: str
    parsed_contract1: ContractSpec
    parsed_contract2: ContractSpec
    coefficients: List[float] = None
    trading_period: Dict[str, str] = None
    trading_months_back: int = 2
    
    def __post_init__(self):
        if self.coefficients is None:
            self.coefficients = [1, -1]
        
        if self.trading_period is None:
            # Calculate spread trading period using conservative approach
            self.trading_period = calculate_spread_trading_period(
                self.parsed_contract1, 
                self.parsed_contract2, 
                self.trading_months_back
            )
    
    @property
    def spread_name(self) -> str:
        """Generate spread name for output files"""
        return f"{self.contract1}_{self.contract2}"


def is_valid_spread_combination(contract1_parsed: ContractSpec, contract2_parsed: ContractSpec) -> bool:
    """
    VANILLA STRICT RULES for spread combinations
    
    STRICT RULES:
    1. Cross-country spreads: ONLY same period and year (debm5_25 vs frbm5_25)
    2. Same-country spreads: ONLY same period type AND sequential periods:
       - Monthly: jan/feb, apr/may, dec/jan (adjacent months only)
       - Quarterly: q1_25/q2_25, q4_25/q1_26 (adjacent quarters only)
       - Yearly: y_25/y_26, y_26/y_27 (adjacent years only)
    3. NO mixed tenor combinations (no month-quarter, quarter-year, etc.)
    
    Args:
        contract1_parsed: First contract specification
        contract2_parsed: Second contract specification
        
    Returns:
        bool: True if valid vanilla combination
    """
    # Extract contract details
    market1, market2 = contract1_parsed.market, contract2_parsed.market
    tenor1, tenor2 = contract1_parsed.tenor, contract2_parsed.tenor
    year1, year2 = contract1_parsed.delivery_date.year, contract2_parsed.delivery_date.year
    
    # Extract period numbers
    try:
        period1 = int(contract1_parsed.contract.split('_')[0])
        period2 = int(contract2_parsed.contract.split('_')[0])
    except (ValueError, IndexError):
        # Fallback for edge cases
        if tenor1 == 'm':
            period1 = contract1_parsed.delivery_date.month
        elif tenor1 == 'q':
            period1 = ((contract1_parsed.delivery_date.month - 1) // 3) + 1
        else:  # yearly
            period1 = year1 % 100
            
        if tenor2 == 'm':
            period2 = contract2_parsed.delivery_date.month
        elif tenor2 == 'q':
            period2 = ((contract2_parsed.delivery_date.month - 1) // 3) + 1
        else:  # yearly
            period2 = year2 % 100
    
    # RULE 1: Cross-country spreads - ONLY same period and year
    if market1 != market2:
        # Must be same tenor, same period, same year
        return (tenor1 == tenor2 and 
                period1 == period2 and 
                year1 == year2)
    
    # RULE 2: Same-country spreads - ONLY same tenor AND sequential periods
    if market1 == market2:
        # Must be same tenor
        if tenor1 != tenor2:
            return False
        
        # Sequential period checks by tenor type
        if tenor1 == 'm':  # Monthly: adjacent months only
            if year1 == year2:
                # Same year: adjacent months (1-2, 2-3, ..., 11-12)
                return abs(period1 - period2) == 1
            elif abs(year1 - year2) == 1:
                # Adjacent years: Dec-Jan only (12-1)
                return ((period1 == 12 and period2 == 1) or 
                        (period1 == 1 and period2 == 12))
            return False
            
        elif tenor1 == 'q':  # Quarterly: adjacent quarters only
            if year1 == year2:
                # Same year: adjacent quarters (1-2, 2-3, 3-4)
                return abs(period1 - period2) == 1
            elif abs(year1 - year2) == 1:
                # Adjacent years: Q4-Q1 only
                return ((period1 == 4 and period2 == 1) or 
                        (period1 == 1 and period2 == 4))
            return False
            
        elif tenor1 == 'y':  # Yearly: adjacent years only
            return abs(year1 - year2) == 1
    
    return False


def generate_spread_combinations(contracts: List[str], 
                               coefficients: Optional[List[float]] = None,
                               trading_months_back: Optional[Dict[str, int]] = None) -> List[SpreadCombination]:
    """
    Generate VANILLA STRICT spread combinations with period-specific trading lookback
    
    VANILLA STRICT RULES:
    1. Cross-country spreads: ONLY same period and year (debm5_25 vs frbm5_25)
    2. Same-country spreads: ONLY sequential periods:
       - Monthly: jan/feb, apr/may, dec/jan (adjacent only)
       - Quarterly: q1_25/q2_25, q4_25/q1_26 (adjacent only)  
       - Yearly: y_25/y_26, y_26/y_27 (adjacent only)
    3. NO mixed tenor combinations
    
    Args:
        contracts: List of absolute contract names (e.g., ['debm07_25', 'debm08_25', 'debm09_25'])
        coefficients: Optional coefficients for spreads (default: [1, -1])
        trading_months_back: Dict with period-specific lookback months:
                           {'monthly': 2, 'quarterly': 6, 'yearly': 12}
                           If not provided, defaults to {'monthly': 2, 'quarterly': 4, 'yearly': 6}
        
    Returns:
        List of SpreadCombination objects
    """
    if len(contracts) < 2:
        raise ValueError("At least 2 contracts required for spread combinations")
    
    # Set default trading months back for each period type
    if trading_months_back is None:
        trading_months_back = {
            'monthly': 2,      # 2 months back for monthly contracts
            'quarterly': 4,    # 4 months back for quarterly contracts  
            'yearly': 6        # 6 months back for yearly contracts
        }
    
    print(f"🔄 Generating VANILLA STRICT spread combinations from {len(contracts)} contracts...")
    print(f"   📋 VANILLA STRICT RULES:")
    print(f"      • Cross-country: ONLY same period and year (debm5_25 vs frbm5_25)")
    print(f"      • Same-country: ONLY sequential periods (jan/feb, q1/q2, y25/y26)")
    print(f"      • NO mixed tenors (no month-quarter, quarter-year combinations)")
    print(f"   📅 Period-specific trading lookback:")
    print(f"      • Monthly contracts: {trading_months_back.get('monthly', 2)} months back")
    print(f"      • Quarterly contracts: {trading_months_back.get('quarterly', 4)} months back")
    print(f"      • Yearly contracts: {trading_months_back.get('yearly', 6)} months back")
    
    # Parse all contracts first
    parsed_contracts = {}
    for contract in contracts:
        try:
            parsed_contracts[contract] = parse_absolute_contract(contract)
            parsed = parsed_contracts[contract]
            # Extract period for display
            try:
                period_num = int(parsed.contract.split('_')[0])
            except:
                period_num = parsed.delivery_date.month if parsed.tenor == 'm' else ((parsed.delivery_date.month - 1) // 3) + 1
            print(f"   ✅ Parsed {contract}: {parsed.market}/{parsed.product}/{parsed.tenor}{period_num}_{parsed.delivery_date.year}")
        except Exception as e:
            print(f"   ❌ Failed to parse {contract}: {e}")
            raise
    
    # Generate valid combinations based on sophisticated rules
    spread_combinations = []
    total_possible = len(contracts) * (len(contracts) - 1) // 2
    valid_count = 0
    
    for contract1, contract2 in combinations(contracts, 2):
        parsed1 = parsed_contracts[contract1]
        parsed2 = parsed_contracts[contract2]
        
        if is_valid_spread_combination(parsed1, parsed2):
            # Determine appropriate trading months back based on contract tenors
            # Use the higher lookback of the two contracts for conservative approach
            tenor1, tenor2 = parsed1.tenor, parsed2.tenor
            
            if tenor1 == 'm' and tenor2 == 'm':
                spread_months_back = trading_months_back.get('monthly', 2)
            elif tenor1 == 'q' and tenor2 == 'q':
                spread_months_back = trading_months_back.get('quarterly', 4)
            elif tenor1 == 'y' and tenor2 == 'y':
                spread_months_back = trading_months_back.get('yearly', 6)
            else:
                # Mixed tenors (shouldn't happen with vanilla rules, but just in case)
                spread_months_back = max(
                    trading_months_back.get('monthly', 2),
                    trading_months_back.get('quarterly', 4),
                    trading_months_back.get('yearly', 6)
                )
            
            combo = SpreadCombination(
                contract1=contract1,
                contract2=contract2,
                parsed_contract1=parsed1,
                parsed_contract2=parsed2,
                coefficients=coefficients or [1, -1],
                trading_months_back=spread_months_back
            )
            spread_combinations.append(combo)
            valid_count += 1
            # Extract periods for display
            try:
                p1 = int(parsed1.contract.split('_')[0])
                p2 = int(parsed2.contract.split('_')[0])
            except:
                p1 = parsed1.delivery_date.month if parsed1.tenor == 'm' else ((parsed1.delivery_date.month - 1) // 3) + 1
                p2 = parsed2.delivery_date.month if parsed2.tenor == 'm' else ((parsed2.delivery_date.month - 1) // 3) + 1
            print(f"   ✅ Valid spread: {combo.spread_name} ({parsed1.tenor}{p1}_{parsed1.delivery_date.year} vs {parsed2.tenor}{p2}_{parsed2.delivery_date.year})")
        else:
            # Extract periods for display
            try:
                p1 = int(parsed1.contract.split('_')[0])
                p2 = int(parsed2.contract.split('_')[0])
            except:
                p1 = parsed1.delivery_date.month if parsed1.tenor == 'm' else ((parsed1.delivery_date.month - 1) // 3) + 1
                p2 = parsed2.delivery_date.month if parsed2.tenor == 'm' else ((parsed2.delivery_date.month - 1) // 3) + 1
            print(f"   ❌ Invalid: {contract1} vs {contract2} ({parsed1.tenor}{p1}_{parsed1.delivery_date.year} vs {parsed2.tenor}{p2}_{parsed2.delivery_date.year})")
    
    print(f"   📊 SUMMARY: {valid_count}/{total_possible} combinations valid ({100*valid_count/total_possible:.1f}%)")
    print(f"   ✅ Generated {len(spread_combinations)} VANILLA STRICT spread combinations")
    return spread_combinations


def fetch_real_spread(combination: SpreadCombination, 
                     period: Dict,
                     allowed_broker_ids: Optional[List[int]] = None) -> Dict:
    """
    Fetch real spread data for a single combination using DataFetcher
    
    Args:
        combination: SpreadCombination object
        period: Period dictionary with start_date and end_date
        allowed_broker_ids: Optional list of allowed broker IDs
        
    Returns:
        Dictionary containing spread data and metadata
    """
    print(f"📈 Fetching real spread: {combination.spread_name}")
    
    try:
        fetcher = DataFetcher(allowed_broker_ids=allowed_broker_ids or [1441])
        
        # Create contract configs
        contract1_config = {
            'market': combination.parsed_contract1.market,
            'tenor': combination.parsed_contract1.tenor,
            'contract': combination.parsed_contract1.contract,
            'start_date': period['start_date'],
            'end_date': period['end_date'],
            'prod': combination.parsed_contract1.product
        }
        
        contract2_config = {
            'market': combination.parsed_contract2.market,
            'tenor': combination.parsed_contract2.tenor,
            'contract': combination.parsed_contract2.contract,
            'start_date': period['start_date'],
            'end_date': period['end_date'],
            'prod': combination.parsed_contract2.product
        }
        
        # Handle cross-market spreads
        if combination.parsed_contract1.market != combination.parsed_contract2.market:
            combined_market = f"{combination.parsed_contract1.market}_{combination.parsed_contract2.market}"
            print(f"   🌐 Cross-market spread: {combined_market}")
            contract1_config['market'] = combined_market
            contract2_config['market'] = combined_market
        
        # Fetch spread data
        spread_data = fetcher.fetch_spread_contract_data(
            contract1_config, contract2_config,
            include_trades=True, 
            include_orders=True
        )
        
        # Create unified format
        unified_data = create_unified_real_spread_data(spread_data)
        
        result = {
            'spread_name': combination.spread_name,
            'raw_data': spread_data,
            'unified_data': unified_data,
            'success': True,
            'error': None
        }
        
        orders_count = len(spread_data.get('spread_orders', pd.DataFrame()))
        trades_count = len(spread_data.get('spread_trades', pd.DataFrame()))
        unified_count = len(unified_data.get('unified_spread_data', pd.DataFrame()))
        
        print(f"   ✅ Real spread fetched: {orders_count} orders, {trades_count} trades, {unified_count} unified records")
        
        return result
        
    except Exception as e:
        print(f"   ❌ Real spread failed for {combination.spread_name}: {e}")
        return {
            'spread_name': combination.spread_name,
            'raw_data': {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()},
            'unified_data': {'unified_spread_data': pd.DataFrame()},
            'success': False,
            'error': str(e)
        }


def clean_merged_spread_outliers(unified_df: pd.DataFrame, 
                               spread_name: str,
                               z_threshold: float = 2.5,
                               iqr_multiplier: float = 2.0,
                               max_price_jump: float = 15.0) -> pd.DataFrame:
    """
    Enhanced outlier cleaning specifically for merged spread data
    
    This addresses outliers that appear when combining real and synthetic data
    but don't exist in individual components.
    
    Args:
        unified_df: Unified DataFrame with both trades and orders
        spread_name: Name of spread for logging
        z_threshold: Z-score threshold (more aggressive for merged data)
        iqr_multiplier: IQR multiplier (more aggressive for merged data)
        max_price_jump: Maximum allowed price jump
        
    Returns:
        Cleaned unified DataFrame
    """
    if unified_df.empty:
        return unified_df
    
    print(f"   🧹 Cleaning merged spread outliers for {spread_name}")
    
    original_count = len(unified_df)
    cleaned_df = unified_df.copy()
    
    # Clean trade prices
    trade_mask = cleaned_df['price'].notna()
    if trade_mask.sum() > 10:
        trade_df = cleaned_df[trade_mask].copy()
        cleaned_trade_df = clean_synthetic_spread_outliers(
            trade_df[['price']].rename(columns={'price': 'price'}),
            f"{spread_name}_merged_trades",
            z_threshold, iqr_multiplier, max_price_jump
        )
        
        # Remove outlier trades from unified data
        outlier_trade_indices = trade_df.index.difference(cleaned_trade_df.index)
        if len(outlier_trade_indices) > 0:
            print(f"      🚫 Removing {len(outlier_trade_indices)} outlier trades from merged data")
            cleaned_df = cleaned_df.drop(outlier_trade_indices)
    
    # Clean bid prices
    bid_mask = cleaned_df['b_price'].notna()
    if bid_mask.sum() > 10:
        bid_df = cleaned_df[bid_mask].copy()
        cleaned_bid_df = clean_synthetic_spread_outliers(
            bid_df[['b_price']].rename(columns={'b_price': 'price'}),
            f"{spread_name}_merged_bids",
            z_threshold, iqr_multiplier, max_price_jump
        )
        
        # Remove outlier bids from unified data
        outlier_bid_indices = bid_df.index.difference(cleaned_bid_df.index)
        if len(outlier_bid_indices) > 0:
            print(f"      🚫 Removing {len(outlier_bid_indices)} outlier bids from merged data")
            cleaned_df = cleaned_df.drop(outlier_bid_indices)
    
    # Clean ask prices
    ask_mask = cleaned_df['a_price'].notna()
    if ask_mask.sum() > 10:
        ask_df = cleaned_df[ask_mask].copy()
        cleaned_ask_df = clean_synthetic_spread_outliers(
            ask_df[['a_price']].rename(columns={'a_price': 'price'}),
            f"{spread_name}_merged_asks",
            z_threshold, iqr_multiplier, max_price_jump
        )
        
        # Remove outlier asks from unified data
        outlier_ask_indices = ask_df.index.difference(cleaned_ask_df.index)
        if len(outlier_ask_indices) > 0:
            print(f"      🚫 Removing {len(outlier_ask_indices)} outlier asks from merged data")
            cleaned_df = cleaned_df.drop(outlier_ask_indices)
    
    removed_count = original_count - len(cleaned_df)
    removal_pct = (removed_count / original_count) * 100 if original_count > 0 else 0
    
    print(f"      ✅ Merged outlier cleaning: {original_count:,} → {len(cleaned_df):,} records")
    print(f"      📊 Removed {removed_count:,} merged outliers ({removal_pct:.1f}%)")
    
    return cleaned_df


def clean_synthetic_spread_outliers(df: pd.DataFrame, 
                                   spread_name: str,
                                   z_threshold: float = 3.0,
                                   iqr_multiplier: float = 2.5,
                                   max_price_jump: float = 20.0) -> pd.DataFrame:
    """
    Enhanced outlier cleaning specifically for synthetic spread data
    
    Args:
        df: DataFrame with price data
        spread_name: Name of spread for logging
        z_threshold: Z-score threshold for outlier detection
        iqr_multiplier: IQR multiplier for outlier detection  
        max_price_jump: Maximum allowed price jump between consecutive points
        
    Returns:
        Cleaned DataFrame with outliers removed
    """
    if df.empty or 'price' not in df.columns:
        return df
    
    print(f"   🧹 Cleaning synthetic spread outliers for {spread_name}")
    
    original_count = len(df)
    cleaned_df = df.copy()
    
    # Only process rows with valid prices
    price_mask = cleaned_df['price'].notna()
    if price_mask.sum() < 10:
        print(f"      ⚠️  Insufficient price data for outlier cleaning")
        return cleaned_df
    
    price_data = cleaned_df[price_mask].copy().sort_index()
    
    # Method 1: Remove extreme price levels (Z-score based)
    prices = price_data['price']
    price_mean = prices.mean()
    price_std = prices.std()
    
    if price_std > 0:
        z_scores = np.abs((prices - price_mean) / price_std)
        extreme_outliers = z_scores > z_threshold
        
        if extreme_outliers.sum() > 0:
            print(f"      🚫 Z-score outliers: {extreme_outliers.sum()} points (threshold: {z_threshold})")
            price_data = price_data[~extreme_outliers]
    
    # Method 2: IQR-based outlier removal  
    if len(price_data) > 4:
        Q1 = price_data['price'].quantile(0.25)
        Q3 = price_data['price'].quantile(0.75)
        IQR = Q3 - Q1
        
        if IQR > 0:
            lower_bound = Q1 - iqr_multiplier * IQR
            upper_bound = Q3 + iqr_multiplier * IQR
            
            iqr_outliers = (price_data['price'] < lower_bound) | (price_data['price'] > upper_bound)
            
            if iqr_outliers.sum() > 0:
                print(f"      🚫 IQR outliers: {iqr_outliers.sum()} points (bounds: {lower_bound:.2f} - {upper_bound:.2f})")
                price_data = price_data[~iqr_outliers]
    
    # Method 3: Remove large price jumps
    if len(price_data) > 1:
        price_changes = price_data['price'].diff().abs()
        jump_outliers = price_changes > max_price_jump
        
        if jump_outliers.sum() > 0:
            print(f"      🚫 Price jump outliers: {jump_outliers.sum()} points (max jump: {max_price_jump})")
            price_data = price_data[~jump_outliers]
    
    # Method 4: Remove impossible negative prices for spreads (if applicable)
    if 'spread' in spread_name.lower():
        # For some spreads, extremely negative values might be unrealistic
        min_reasonable_price = price_data['price'].quantile(0.01) - 3 * price_data['price'].std()
        negative_outliers = price_data['price'] < min_reasonable_price
        
        if negative_outliers.sum() > 0:
            print(f"      🚫 Extreme negative outliers: {negative_outliers.sum()} points (below: {min_reasonable_price:.2f})")
            price_data = price_data[~negative_outliers]
    
    # Reconstruct the cleaned DataFrame
    cleaned_indices = price_data.index
    cleaned_df = cleaned_df.loc[cleaned_indices]
    
    removed_count = original_count - len(cleaned_df)
    removal_pct = (removed_count / original_count) * 100 if original_count > 0 else 0
    
    print(f"      ✅ Outlier cleaning: {original_count:,} → {len(cleaned_df):,} records")
    print(f"      📊 Removed {removed_count:,} outliers ({removal_pct:.1f}%)")
    
    return cleaned_df


def fetch_synthetic_spread_absolute_contracts(contract1: ContractSpec, contract2: ContractSpec,
                                             start_date: datetime, end_date: datetime,
                                             coefficients: List[float]) -> Dict:
    """
    Fetch synthetic spread data using ABSOLUTE contract specifications
    This avoids relative period transitions that cause price jumps
    """
    if not SPREADVIEWER_AVAILABLE:
        raise ImportError("SpreadViewer not available")
    
    print(f"🔄 Fetching synthetic spread (ABSOLUTE): {contract1.market}{contract1.product[0]}{contract1.tenor}{contract1.contract} vs {contract2.market}{contract2.product[0]}{contract2.tenor}{contract2.contract}")
    
    # Use SpreadViewer with ABSOLUTE contract names instead of relative periods
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from Database.TPData import TPData
    
    dates = pd.date_range(start_date, end_date, freq='B')
    if len(dates) == 0:
        return {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}
    
    # Create absolute contract names for SpreadViewer
    # Format: market + product + tenor + contract (e.g., "debm12_25")
    contract1_abs = f"{contract1.market}{contract1.product[0]}{contract1.tenor}{contract1.contract}"
    contract2_abs = f"{contract2.market}{contract2.product[0]}{contract2.tenor}{contract2.contract}"
    
    print(f"   📊 Using absolute contracts: {contract1_abs} vs {contract2_abs}")
    
    # Use SpreadViewer with absolute contract specifications
    # For absolute contracts, we still need to separate markets and tenors properly
    markets = [contract1.market, contract2.market]
    tenors = [f"{contract1.product[0]}{contract1.tenor}{contract1.contract}", 
             f"{contract2.product[0]}{contract2.tenor}{contract2.contract}"]  # Product + tenor + contract
    
    try:
        # Initialize SpreadViewer classes
        spread_class = SpreadSingle(markets, tenors, [], [], ['eex'])
        data_class = SpreadViewerData()
        data_class_tr = SpreadViewerData()
        db_class = TPData()
        
        start_time = time(9, 0, 0)
        end_time = time(17, 25, 0)
        
        # Load data using absolute contract names
        print(f"   🔍 Loading order data for absolute contracts...")
        
        # For absolute contracts, we don't need product_dates conversion
        # We directly use the contract names
        data_class.load_best_order_otc(
            markets, tenors,
            [dates, dates],  # Same date range for both contracts
            db_class,
            start_time=start_time, end_time=end_time
        )
        
        print(f"   ✅ Order data loading completed")
        
        # Load trade data
        data_class_tr.load_trades_otc(
            markets, tenors, db_class,
            start_time=start_time, end_time=end_time
        )
        print(f"   ✅ Trade data loading completed")
        
        # Process daily data
        sm_all = pd.DataFrame()
        tm_all = pd.DataFrame()
        
        for d in dates:
            d_range = pd.date_range(d, d)
            
            # Aggregate order book data
            data_dict = spread_class.aggregate_data(
                data_class, d_range, 0,  # n_s=0 for absolute contracts
                gran=None,
                start_time=start_time, end_time=end_time,
                col_list=['bid', 'ask']
            )
            
            # Create spread orders
            if data_dict:
                sm = spread_class.spread_maker(data_dict, coefficients, trade_type=['cmb', 'cmb']).dropna()
                sm_all = pd.concat([sm_all, sm], axis=0)
                
                # Create spread trades
                if not sm.empty:
                    col_list = ['bid', 'ask', 'volume', 'broker_id']
                    trade_dict = spread_class.aggregate_data(
                        data_class_tr, d_range, 0,  # n_s=0 for absolute contracts
                        gran='1s',
                        start_time=start_time, end_time=end_time,
                        col_list=col_list, data_dict=data_dict
                    )
                    
                    if trade_dict:
                        tm = spread_class.add_trades(data_dict, trade_dict, coefficients, [True, True])
                        tm_all = pd.concat([tm_all, tm], axis=0)
        
        # Apply trade adjustment
        if not tm_all.empty and not sm_all.empty:
            print(f"   🔧 Applying trade adjustment filter...")
            tm_before = len(tm_all)
            # Use the same trade adjustment as in the original
            from engines.data_fetch_engine import adjust_trds_
            tm_all = adjust_trds_(tm_all, sm_all)
            tm_after = len(tm_all)
            print(f"   📊 Trade filtering: {tm_before} → {tm_after} trades")
        
        return {
            'spread_orders': sm_all,
            'spread_trades': tm_all
        }
        
    except Exception as e:
        print(f"   ❌ Absolute synthetic spread fetch failed: {e}")
        return {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}


def fetch_synthetic_spread(combination: SpreadCombination,
                         period: Dict,
                         n_s: int = 3,
                         clean_outliers: bool = True,
                         use_absolute_contracts: bool = False) -> Dict:
    """
    Fetch synthetic spread data for a single combination using SpreadViewer
    
    Note: Absolute contracts implementation is temporarily disabled due to "list index out of range" 
    error in SpreadViewer initialization. The issue is in mapping absolute contract names
    (like 'debm12_25') to SpreadViewer's expected format of markets, tenors, and tn1_list.
    
    Args:
        combination: SpreadCombination object
        period: Period dictionary with start_date and end_date
        n_s: Business day transition parameter
        clean_outliers: Whether to clean outliers from synthetic data
        use_absolute_contracts: Whether to use absolute contract specs (avoids relative transitions)
        
    Returns:
        Dictionary containing spread data and metadata
    """
    print(f"🔧 Fetching synthetic spread: {combination.spread_name}")
    
    if not SPREADVIEWER_AVAILABLE:
        print(f"   ❌ SpreadViewer not available")
        return {
            'spread_name': combination.spread_name,
            'raw_data': {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()},
            'unified_data': {'unified_spread_data': pd.DataFrame()},
            'success': False,
            'error': 'SpreadViewer not available'
        }
    
    try:
        start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
        
        # Choose between absolute and relative contract fetching
        if use_absolute_contracts:
            print(f"   📊 Using ABSOLUTE contract specifications (prevents relative transitions)")
            spread_data = fetch_synthetic_spread_absolute_contracts(
                combination.parsed_contract1,
                combination.parsed_contract2,
                start_date,
                end_date,
                combination.coefficients
            )
        else:
            print(f"   📊 Using RELATIVE contract specifications (may cause transitions)")
            spread_data = fetch_synthetic_spread_multiple_periods(
                combination.parsed_contract1,
                combination.parsed_contract2,
                start_date,
                end_date,
                combination.coefficients,
                n_s
            )
        
        # Clean outliers from synthetic data if requested
        if clean_outliers:
            print(f"   🧹 Applying outlier cleaning to synthetic data...")
            
            # Clean orders data
            if 'spread_orders' in spread_data and not spread_data['spread_orders'].empty:
                original_orders = len(spread_data['spread_orders'])
                
                # Clean bid prices
                if 'bid' in spread_data['spread_orders'].columns:
                    bid_df = spread_data['spread_orders'][['bid']].rename(columns={'bid': 'price'})
                    bid_df = bid_df[bid_df['price'].notna()]
                    if not bid_df.empty:
                        cleaned_bid = clean_synthetic_spread_outliers(bid_df, f"{combination.spread_name}_bid")
                        # Update original data with cleaned indices
                        spread_data['spread_orders'] = spread_data['spread_orders'].loc[cleaned_bid.index]
                
                # Clean ask prices  
                if 'ask' in spread_data['spread_orders'].columns:
                    ask_df = spread_data['spread_orders'][['ask']].rename(columns={'ask': 'price'})
                    ask_df = ask_df[ask_df['price'].notna()]
                    if not ask_df.empty:
                        cleaned_ask = clean_synthetic_spread_outliers(ask_df, f"{combination.spread_name}_ask")
                        # Keep intersection of cleaned bid and ask indices
                        common_indices = spread_data['spread_orders'].index.intersection(cleaned_ask.index)
                        spread_data['spread_orders'] = spread_data['spread_orders'].loc[common_indices]
                
                cleaned_orders = len(spread_data['spread_orders'])
                print(f"      📊 Orders cleaned: {original_orders:,} → {cleaned_orders:,}")
            
            # Clean trades data
            if 'spread_trades' in spread_data and not spread_data['spread_trades'].empty:
                original_trades = len(spread_data['spread_trades'])
                
                # Clean buy trades
                if 'buy' in spread_data['spread_trades'].columns:
                    buy_df = spread_data['spread_trades'][['buy']].rename(columns={'buy': 'price'})
                    buy_df = buy_df[buy_df['price'].notna()]
                    if not buy_df.empty:
                        cleaned_buy = clean_synthetic_spread_outliers(buy_df, f"{combination.spread_name}_buy")
                        # Update buy column with cleaned data
                        spread_data['spread_trades'].loc[~spread_data['spread_trades'].index.isin(cleaned_buy.index), 'buy'] = np.nan
                
                # Clean sell trades
                if 'sell' in spread_data['spread_trades'].columns:
                    sell_df = spread_data['spread_trades'][['sell']].rename(columns={'sell': 'price'})
                    sell_df = sell_df[sell_df['price'].notna()]
                    if not sell_df.empty:
                        cleaned_sell = clean_synthetic_spread_outliers(sell_df, f"{combination.spread_name}_sell")
                        # Update sell column with cleaned data
                        spread_data['spread_trades'].loc[~spread_data['spread_trades'].index.isin(cleaned_sell.index), 'sell'] = np.nan
                
                # Remove rows where both buy and sell are NaN
                spread_data['spread_trades'] = spread_data['spread_trades'].dropna(subset=['buy', 'sell'], how='all')
                cleaned_trades = len(spread_data['spread_trades'])
                print(f"      📊 Trades cleaned: {original_trades:,} → {cleaned_trades:,}")
        
        # Create unified format
        unified_data = create_unified_spreadviewer_data(spread_data)
        
        result = {
            'spread_name': combination.spread_name,
            'raw_data': spread_data,
            'unified_data': unified_data,
            'success': True,
            'error': None
        }
        
        orders_count = len(spread_data.get('spread_orders', pd.DataFrame()))
        trades_count = len(spread_data.get('spread_trades', pd.DataFrame()))
        unified_count = len(unified_data.get('unified_spread_data', pd.DataFrame()))
        
        print(f"   ✅ Synthetic spread fetched: {orders_count} orders, {trades_count} trades, {unified_count} unified records")
        
        return result
        
    except Exception as e:
        print(f"   ❌ Synthetic spread failed for {combination.spread_name}: {e}")
        return {
            'spread_name': combination.spread_name,
            'raw_data': {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()},
            'unified_data': {'unified_spread_data': pd.DataFrame()},
            'success': False,
            'error': str(e)
        }


def fetch_all_spreads(contracts: List[str],
                     period: Optional[Dict] = None,
                     trading_months_back: Optional[Dict[str, int]] = None,
                     use_real_spreads: bool = True,
                     use_synthetic_spreads: bool = True,
                     coefficients: Optional[List[float]] = None,
                     n_s: int = 3,
                     allowed_broker_ids: Optional[List[int]] = None,
                     save_results: bool = True,
                     test_mode: bool = False,
                     file_suffix: str = '',
                     save_separate_csv: bool = False,
                     clean_synthetic_outliers: bool = True) -> Dict:
    """
    Main function to fetch spreads for all combinations
    
    Args:
        contracts: List of absolute contract names
        period: Period dictionary with start_date and end_date
        use_real_spreads: Whether to fetch real spreads
        use_synthetic_spreads: Whether to fetch synthetic spreads
        coefficients: Spread coefficients (default: [1, -1])
        n_s: Business day transition parameter
        allowed_broker_ids: Optional list of allowed broker IDs
        save_results: Whether to save results to files
        test_mode: Whether to save all formats (test mode) or just parquet
        file_suffix: Optional suffix for output files
        save_separate_csv: Whether to save trades and orders as separate CSV files
        clean_synthetic_outliers: Whether to clean outliers from synthetic data
        
    Returns:
        Dictionary containing all spread results
    """
    global output_base
    
    # Set output base for both this module and data_fetch_engine module
    if test_mode:
        output_base = os.path.join(rawdata_base, 'test')
        dfe.output_base = output_base
    else:
        output_base = rawdata_base
        dfe.output_base = output_base
    
    # Set default trading months back if not provided
    if trading_months_back is None:
        trading_months_back = {
            'monthly': 2,
            'quarterly': 4,
            'yearly': 6
        }
    
    print(f"🚀 Fetching all spreads for {len(contracts)} contracts")
    if period:
        print(f"   📅 Period: {period['start_date']} to {period['end_date']} (fixed)")
    else:
        print(f"   📅 Period: Individual trading periods per contract type:")
        print(f"      • Monthly: {trading_months_back.get('monthly', 2)} months back")
        print(f"      • Quarterly: {trading_months_back.get('quarterly', 4)} months back") 
        print(f"      • Yearly: {trading_months_back.get('yearly', 6)} months back")
    print(f"   🔧 Options: real={use_real_spreads}, synthetic={use_synthetic_spreads}, n_s={n_s}")
    
    # Generate all spread combinations with individual trading periods
    combinations = generate_spread_combinations(contracts, coefficients, trading_months_back)
    
    # Results storage
    results = {
        'metadata': {
            'contracts': contracts,
            'period': period,
            'n_s': n_s,
            'use_real_spreads': use_real_spreads,
            'use_synthetic_spreads': use_synthetic_spreads,
            'total_combinations': len(combinations),
            'timestamp': datetime.now().isoformat()
        },
        'spreads': {}
    }
    
    # Process each combination
    for i, combination in enumerate(combinations, 1):
        print(f"\n📊 Processing spread {i}/{len(combinations)}: {combination.spread_name}")
        print("=" * 60)
        
        spread_results = {
            'combination': {
                'contract1': combination.contract1,
                'contract2': combination.contract2,
                'coefficients': combination.coefficients
            }
        }
        
        # Fetch real spread if requested
        if use_real_spreads:
            real_result = fetch_real_spread(combination, combination.trading_period, allowed_broker_ids)
            spread_results['real_spread'] = real_result
        
        # Fetch synthetic spread if requested
        if use_synthetic_spreads:
            # Use the EXACT same approach as data_fetch_engine with individual trading periods
            start_date = datetime.strptime(combination.trading_period['start_date'], '%Y-%m-%d')
            end_date = datetime.strptime(combination.trading_period['end_date'], '%Y-%m-%d')
            
            synthetic_spread_data = fetch_synthetic_spread_multiple_periods(
                combination.parsed_contract1, combination.parsed_contract2, 
                start_date, end_date, combination.coefficients, n_s
            )
            
            # Create unified DataFrame from synthetic data
            unified_synthetic = create_unified_spreadviewer_data(synthetic_spread_data)
            
            synthetic_result = {
                'spread_orders': synthetic_spread_data.get('spread_orders', pd.DataFrame()),
                'spread_trades': synthetic_spread_data.get('spread_trades', pd.DataFrame()),
                'unified_data': {'unified_spread_data': unified_synthetic.get('unified_spread_data', pd.DataFrame())},
                'success': len(unified_synthetic.get('unified_spread_data', pd.DataFrame())) > 0,
                'metadata': {
                    'combination': f"{combination.contract1}_{combination.contract2}",
                    'method': 'data_fetch_engine_multiple_periods',
                    'coefficients': combination.coefficients,
                    'n_s': n_s,
                    'period': combination.trading_period
                }
            }
            
            # Apply outlier cleaning if requested
            if clean_synthetic_outliers and len(synthetic_result['unified_data']['unified_spread_data']) > 0:
                print(f"   🧹 Applying outlier cleaning to synthetic data...")
                synthetic_result['unified_data']['unified_spread_data'] = clean_synthetic_spread_outliers(
                    synthetic_result['unified_data']['unified_spread_data'], f"{combination.contract1}_{combination.contract2}"
                )
            
            spread_results['synthetic_spread'] = synthetic_result
        
        # Merge real and synthetic if both available
        if use_real_spreads and use_synthetic_spreads:
            real_data = spread_results.get('real_spread', {})
            synthetic_data = spread_results.get('synthetic_spread', {})
            
            if real_data.get('success') and synthetic_data.get('success'):
                print(f"🔗 Merging real and synthetic spreads for {combination.spread_name}...")
                try:
                    merged_data = merge_spread_data(
                        real_data['raw_data'],
                        synthetic_data['raw_data']
                    )
                    
                    # Clean outliers from merged data if requested
                    if clean_synthetic_outliers and 'unified_spread_data' in merged_data:
                        print(f"   🧹 Cleaning outliers from merged spread data...")
                        original_count = len(merged_data['unified_spread_data'])
                        merged_data['unified_spread_data'] = clean_merged_spread_outliers(
                            merged_data['unified_spread_data'], 
                            combination.spread_name
                        )
                        cleaned_count = len(merged_data['unified_spread_data'])
                        removed_count = original_count - cleaned_count
                        if removed_count > 0:
                            print(f"   🗑️  Removed {removed_count} outliers from merged data ({removed_count/original_count*100:.1f}%)")
                        else:
                            print(f"   ✅ No outliers found in merged data")
                    
                    spread_results['merged_spread'] = {
                        'spread_name': combination.spread_name,
                        'unified_data': merged_data,
                        'success': True,
                        'error': None
                    }
                    print(f"   ✅ Merged spread: {len(merged_data.get('unified_spread_data', pd.DataFrame()))} total records")
                except Exception as e:
                    print(f"   ❌ Merge failed: {e}")
                    spread_results['merged_spread'] = {
                        'spread_name': combination.spread_name,
                        'unified_data': {'unified_spread_data': pd.DataFrame()},
                        'success': False,
                        'error': str(e)
                    }
        
        # Save results if requested
        if save_results:
            print(f"💾 Saving results for {combination.spread_name}...")
            
            # Prepare config for save function
            save_config = {
                'contracts': [combination.contract1, combination.contract2],
                'period': combination.trading_period,
                'test_mode': test_mode,
                'file_suffix': file_suffix,
                'save_separate_csv': save_separate_csv
            }
            
            # Save real spread
            if use_real_spreads and spread_results.get('real_spread', {}).get('success'):
                save_results_dict = {
                    'real_spread_data': spread_results['real_spread']['unified_data']
                }
                save_unified_results(
                    save_results_dict, 
                    save_config['contracts'], 
                    save_config['period'], 
                    'real_only', 
                    test_mode,
                    file_suffix + '_real',
                    save_separate_csv
                )
            
            # Save synthetic spread
            if use_synthetic_spreads and spread_results.get('synthetic_spread', {}).get('success'):
                save_results_dict = {
                    'synthetic_spread_data': spread_results['synthetic_spread']['unified_data']
                }
                save_unified_results(
                    save_results_dict,
                    save_config['contracts'],
                    save_config['period'],
                    'synthetic_only',
                    test_mode,
                    file_suffix + '_synthetic',
                    save_separate_csv
                )
            
            # Save merged spread
            if 'merged_spread' in spread_results and spread_results['merged_spread'].get('success'):
                save_results_dict = {
                    'merged_spread_data': spread_results['merged_spread']['unified_data']
                }
                save_unified_results(
                    save_results_dict,
                    save_config['contracts'],
                    save_config['period'],
                    'merged',
                    test_mode,
                    file_suffix + '_merged',
                    save_separate_csv
                )
        
        # Store in results
        results['spreads'][combination.spread_name] = spread_results
    
    # Summary statistics
    total_real_success = sum(1 for s in results['spreads'].values() 
                           if s.get('real_spread', {}).get('success', False))
    total_synthetic_success = sum(1 for s in results['spreads'].values() 
                                if s.get('synthetic_spread', {}).get('success', False))
    total_merged_success = sum(1 for s in results['spreads'].values() 
                             if s.get('merged_spread', {}).get('success', False))
    
    results['summary'] = {
        'total_spreads': len(combinations),
        'real_success': total_real_success,
        'synthetic_success': total_synthetic_success,
        'merged_success': total_merged_success
    }
    
    print(f"\n🎉 Spread fetch completed!")
    print(f"   📊 Summary:")
    print(f"      Total spreads: {len(combinations)}")
    if use_real_spreads:
        print(f"      Real spreads success: {total_real_success}/{len(combinations)}")
    if use_synthetic_spreads:
        print(f"      Synthetic spreads success: {total_synthetic_success}/{len(combinations)}")
    if use_real_spreads and use_synthetic_spreads:
        print(f"      Merged spreads success: {total_merged_success}/{len(combinations)}")
    
    # Summary JSON saving disabled - only save parquet files
    
    return results


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Spread Fetch Engine - Fetch spreads for all combinations of contracts'
    )
    
    parser.add_argument('--contracts', nargs='+', required=True,
                      help='List of contracts (e.g., debm07_25 debm08_25 debm09_25)')
    parser.add_argument('--start-date', required=True,
                      help='Start date (YYYY-MM-DD format)')
    parser.add_argument('--end-date', required=True,
                      help='End date (YYYY-MM-DD format)')
    parser.add_argument('--n-s', type=int, default=3,
                      help='Business day transition parameter (default: 3)')
    parser.add_argument('--coefficients', nargs='+', type=float, default=[1, -1],
                      help='Spread coefficients (default: [1, -1])')
    parser.add_argument('--use-real', action='store_true', default=True,
                      dest='use_real', help='Fetch real spreads (default: True)')
    parser.add_argument('--no-real', action='store_false', 
                      dest='use_real', help='Skip real spreads')
    parser.add_argument('--use-synthetic', action='store_true', default=True,
                      dest='use_synthetic', help='Fetch synthetic spreads (default: True)')
    parser.add_argument('--no-synthetic', action='store_false',
                      dest='use_synthetic', help='Skip synthetic spreads')
    parser.add_argument('--test-mode', action='store_true',
                      help='Test mode: saves all formats to test/')
    parser.add_argument('--suffix', default='',
                      help='Optional suffix for output files')
    parser.add_argument('--save-separate-csv', action='store_true',
                      help='Save trades and orders as separate CSV files')
    parser.add_argument('--broker-ids', nargs='+', type=int, default=[1441],
                      help='Allowed broker IDs (default: [1441])')
    parser.add_argument('--no-save', action='store_true',
                      help='Skip saving results to files')
    parser.add_argument('--no-clean-outliers', action='store_false',
                      dest='clean_synthetic_outliers', default=True,
                      help='Disable outlier cleaning for synthetic spreads')
    
    return parser.parse_args()


def main():
    """Main function"""
    # Configuration - EDIT THESE VARIABLES AS NEEDED
    config = {
        # 'contracts': ['debm1_25', 'debq3_25'],
        'contracts': [
                      # DE monthly contracts 2024
                      'debm1_24', 'debm2_24', 'debm3_24', 'debm4_24', 'debm5_24', 'debm6_24',
                      'debm7_24', 'debm8_24', 'debm9_24', 'debm10_24', 'debm11_24', 'debm12_24',
                      # FR monthly contracts 2024
                      'frbm1_24', 'frbm2_24', 'frbm3_24', 'frbm4_24', 'frbm5_24', 'frbm6_24',
                      'frbm7_24', 'frbm8_24', 'frbm9_24', 'frbm10_24', 'frbm11_24', 'frbm12_24',
                      # DE quarterly contracts 2024
                      'debq1_24', 'debq2_24', 'debq3_24', 'debq4_24',
                      # FR quarterly contracts 2024
                      'frbq1_24', 'frbq2_24', 'frbq3_24', 'frbq4_24',
                      # DE yearly contracts 2024-2026
                      'deby1_24', 'deby1_25', 'deby1_26',
                      # FR yearly contracts 2024-2026
                      'frby1_24', 'frby1_25', 'frby1_26'
                      ],
        'period': None,  # Use individual trading periods per contract
        'trading_months_back': {  # Period-specific months back
            'm': 2,  # Monthly contracts: 2 months back
            'q': 4,  # Quarterly contracts: 4 months back  
            'y': 6   # Yearly contracts: 6 months back
        },
        'use_real_spreads': True,  # ONLY synthetic spreads
        'use_synthetic_spreads': True,  # Clean synthetic spreads only
        'coefficients': [1, -1],
        'n_s': 3,
        'allowed_broker_ids': [1441],
        'save_results': True,
        'test_mode': False,  # Save to test/ directory
        'file_suffix': '_data_fetch_engine_method',
        'save_separate_csv': False,
        'clean_synthetic_outliers': True
    }
    
    # Check dependencies silently
    if not TPDATA_AVAILABLE:
        return False
    
    if not SPREADVIEWER_AVAILABLE:
        config['use_synthetic_spreads'] = False
    
    # Execute spread fetching
    try:
        results = fetch_all_spreads(**config)
        return True
    except Exception as e:
        return False


if __name__ == "__main__":
    main()