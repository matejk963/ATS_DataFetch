"""
Data Transformation Components
==============================

Functions for transforming between different data formats and detecting outliers.
"""

import pandas as pd
import numpy as np
from typing import Dict


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