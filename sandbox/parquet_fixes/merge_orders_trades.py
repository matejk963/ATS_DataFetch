#!/usr/bin/env python3
"""
Merge spread orders and trades into the reference backtest format
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os

# File paths
data_dir = '/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test/parquet_files'
orders_file = 'DEM1-DEM2_orders_robust.parquet'
trades_file = 'DEM1-DEM2_trades_robust.parquet'

orders_path = os.path.join(data_dir, orders_file)
trades_path = os.path.join(data_dir, trades_file)

# Output path (matching reference format naming)
output_file = 'dem01-dem02_spread_tr_ba_data.parquet'
output_path = os.path.join(data_dir, output_file)

print("🔗 Merging Spread Orders & Trades into Reference Format")
print("=" * 60)
print(f"📁 Input directory: {data_dir}")
print(f"📊 Orders file: {orders_file}")
print(f"💹 Trades file: {trades_file}")
print(f"📦 Output file: {output_file}")

try:
    # Load the robust data files
    print(f"\n📂 Loading data...")
    orders_df = pd.read_parquet(orders_path, engine='pyarrow')
    trades_df = pd.read_parquet(trades_path, engine='pyarrow')
    
    print(f"✅ Orders loaded: {orders_df.shape}")
    print(f"✅ Trades loaded: {trades_df.shape}")
    print(f"📅 Orders period: {orders_df.index.min()} to {orders_df.index.max()}")
    print(f"📅 Trades period: {trades_df.index.min()} to {trades_df.index.max()}")
    
    # Process trades data first
    print(f"\n🔄 Processing trades data...")
    
    # Convert buy/sell columns to the reference format
    trades_processed = pd.DataFrame(index=trades_df.index)
    
    # Extract trade price and direction
    trades_processed['price'] = trades_df['buy'].fillna(trades_df['sell'])
    trades_processed['action'] = np.where(trades_df['buy'].notna(), 1.0, 
                                         np.where(trades_df['sell'].notna(), -1.0, np.nan))
    
    # Add trade metadata (using placeholder values similar to reference)
    trades_processed['volume'] = 1  # Default volume (could be enhanced)
    trades_processed['broker_id'] = 1441.0  # EEX broker ID
    trades_processed['count'] = 1  # Trade count
    trades_processed['tradeid'] = [f"spread_trade_{i}" for i in range(len(trades_processed))]
    
    # Remove any rows without valid trades
    trades_processed = trades_processed.dropna(subset=['price', 'action'])
    
    print(f"   ✅ Processed trades: {len(trades_processed)} valid executions")
    print(f"   📊 Buy trades: {(trades_processed['action'] == 1.0).sum()}")
    print(f"   📊 Sell trades: {(trades_processed['action'] == -1.0).sum()}")
    
    # Process orders data
    print(f"\n🔄 Processing orders data...")
    
    # Rename columns to match reference format
    orders_processed = orders_df.rename(columns={
        'bid': 'b_price',
        'ask': 'a_price'
    })
    
    # Calculate mid price (column '0')
    orders_processed['0'] = (orders_processed['b_price'] + orders_processed['a_price']) / 2
    
    print(f"   ✅ Processed orders: {len(orders_processed)} market data points")
    print(f"   📊 Mid price range: {orders_processed['0'].min():.3f} to {orders_processed['0'].max():.3f}")
    
    # Merge trades and orders on timestamp
    print(f"\n🔗 Merging trades and orders...")
    
    # Method 1: Use trades as base and forward-fill orders
    print(f"   🔄 Aligning order book data to trade timestamps...")
    
    # Get the union of all timestamps
    all_timestamps = orders_processed.index.union(trades_processed.index)
    
    # Reindex orders to all timestamps and forward-fill
    orders_aligned = orders_processed.reindex(all_timestamps).ffill()
    
    # Create the final merged DataFrame
    # Start with all timestamps that have either trades or orders
    merged_df = pd.DataFrame(index=all_timestamps)
    
    # Add trade data (will be NaN for non-trade timestamps)
    for col in ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid']:
        merged_df[col] = trades_processed.reindex(all_timestamps)[col]
    
    # Add order book data (forward-filled to all timestamps)
    for col in ['b_price', 'a_price', '0']:
        merged_df[col] = orders_aligned[col]
    
    # Fill trade metadata for non-trade rows with NaN (matching reference behavior)
    trade_cols = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid']
    for col in trade_cols:
        if col in ['volume', 'count']:
            merged_df[col] = merged_df[col].astype('Int64')  # Nullable integer
        elif col == 'tradeid':
            merged_df[col] = merged_df[col].astype('object')
        else:
            merged_df[col] = merged_df[col].astype('float64')
    
    # Ensure column order matches reference format
    reference_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
    merged_df = merged_df[reference_columns]
    
    # Remove any rows where order book data is missing (shouldn't happen with forward-fill)
    original_len = len(merged_df)
    merged_df = merged_df.dropna(subset=['b_price', 'a_price', '0'])
    
    if len(merged_df) < original_len:
        print(f"   🧹 Removed {original_len - len(merged_df)} rows with missing order data")
    
    print(f"   ✅ Final merged data: {merged_df.shape}")
    print(f"   📊 Trade rows: {merged_df['price'].notna().sum()}")
    print(f"   📊 Order-only rows: {merged_df['price'].isna().sum()}")
    
    # Verify data types match reference
    print(f"\n📊 Data type verification:")
    expected_types = {
        'price': 'float64',
        'volume': 'Int64', 
        'action': 'float64',
        'broker_id': 'float64',
        'count': 'Int64',
        'tradeid': 'object',
        'b_price': 'float64',
        'a_price': 'float64',
        '0': 'float64'
    }
    
    for col, expected_type in expected_types.items():
        actual_type = str(merged_df[col].dtype)
        print(f"   {col}: {actual_type} {'✅' if expected_type in actual_type or actual_type in expected_type else '⚠️'}")
    
    # Save the merged file
    print(f"\n💾 Saving merged file...")
    merged_df.to_parquet(output_path, engine='pyarrow', compression='snappy')
    
    # Verify the save
    test_df = pd.read_parquet(output_path, engine='pyarrow')
    print(f"✅ Saved and verified: {test_df.shape}")
    
    # Save CSV backup
    csv_path = output_path.replace('.parquet', '.csv')
    merged_df.head(5000).to_csv(csv_path)  # Save first 5000 rows as sample
    print(f"💾 CSV sample saved: {Path(csv_path).name}")
    
    # Show sample of final result
    print(f"\n📋 Sample of merged data:")
    print(merged_df.head(10))
    
    print(f"\n🎉 SUCCESS! Merged file created in reference format")
    print(f"📁 Output: {output_path}")
    print(f"📊 Format: {merged_df.shape} with {reference_columns}")
    print(f"🎯 Ready for backtest analysis!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()