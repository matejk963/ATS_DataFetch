"""
Data Merging and Unification
=============================

Advanced merging algorithms for combining real and synthetic spread data.
"""

import pandas as pd
import numpy as np
from typing import Dict

from .validators import BidAskValidator
from .data_transformers import (
    transform_orders_to_target_format,
    transform_trades_to_target_format,
    detect_price_outliers
)


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