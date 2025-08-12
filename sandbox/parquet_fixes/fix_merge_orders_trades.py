#!/usr/bin/env python3
"""
Fixed version: Merge spread orders and trades with correct absolute contract names and action handling
"""

import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime

# File paths
data_dir = '/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test/parquet_files'
orders_file = 'DEM1-DEM2_orders_robust.parquet'
trades_file = 'DEM1-DEM2_trades_robust.parquet'

orders_path = os.path.join(data_dir, orders_file)
trades_path = os.path.join(data_dir, trades_file)

print("🔧 FIXED: Merging Spread Orders & Trades with Correct Format")
print("=" * 70)

try:
    # Load data
    print(f"📂 Loading data...")
    orders_df = pd.read_parquet(orders_path, engine='pyarrow')
    trades_df = pd.read_parquet(trades_path, engine='pyarrow')
    
    print(f"✅ Orders loaded: {orders_df.shape}")
    print(f"✅ Trades loaded: {trades_df.shape}")
    
    # 1. DETERMINE ABSOLUTE CONTRACTS FROM DATE RANGE
    print(f"\\n🗓️  Determining absolute contracts from date range...")
    start_date = orders_df.index.min()
    end_date = orders_df.index.max()
    
    print(f"   Data period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # For M1M2 spread in July data, this would be July vs August contracts
    # M1 = Current month being traded = July 2025 = 07_25
    # M2 = Next month = August 2025 = 08_25
    leg1_contract = f"{start_date.month:02d}_{str(start_date.year)[2:]}"  # 07_25
    leg2_month = start_date.month + 1 if start_date.month < 12 else 1
    leg2_year = start_date.year if start_date.month < 12 else start_date.year + 1
    leg2_contract = f"{leg2_month:02d}_{str(leg2_year)[2:]}"  # 08_25
    
    print(f"   🎯 Inferred contracts:")
    print(f"      Leg 1 (M1): dem{leg1_contract}")
    print(f"      Leg 2 (M2): dem{leg2_contract}")
    
    # Generate correct filename
    output_file = f"dem{leg1_contract}-dem{leg2_contract}_spread_tr_ba_data.parquet"
    output_path = os.path.join(data_dir, output_file)
    
    print(f"   📁 Output filename: {output_file}")
    
    # 2. PROCESS TRADES DATA WITH CORRECT ACTION LOGIC
    print(f"\\n🔄 Processing trades data...")
    
    # Analyze the trade structure
    buy_trades = trades_df['buy'].notna().sum()
    sell_trades = trades_df['sell'].notna().sum()  
    both_filled = ((trades_df['buy'].notna()) & (trades_df['sell'].notna())).sum()
    
    print(f"   📊 Trade analysis:")
    print(f"      Buy trades: {buy_trades}")
    print(f"      Sell trades: {sell_trades}")
    print(f"      Both filled: {both_filled}")
    
    # Create trades processed DataFrame
    trades_processed = pd.DataFrame(index=trades_df.index)
    
    # CORRECT ACTION LOGIC:
    # If both buy and sell are filled -> this might be a spread quote, use buy as primary
    # If only buy filled -> buy trade (action = 1.0)
    # If only sell filled -> sell trade (action = -1.0)
    
    trades_processed['price'] = np.where(
        trades_df['buy'].notna(), 
        trades_df['buy'],           # Use buy price if available
        trades_df['sell']           # Otherwise use sell price
    )
    
    trades_processed['action'] = np.where(
        trades_df['buy'].notna() & trades_df['sell'].isna(), 1.0,    # Only buy -> +1.0
        np.where(trades_df['sell'].notna() & trades_df['buy'].isna(), -1.0,  # Only sell -> -1.0
                 np.where((trades_df['buy'].notna()) & (trades_df['sell'].notna()), 1.0,  # Both -> buy (1.0)
                          np.nan))  # Neither -> NaN
    )
    
    # Add trade metadata
    trades_processed['volume'] = 1  # Default volume
    trades_processed['broker_id'] = 1441.0  # EEX broker
    trades_processed['count'] = 1
    trades_processed['tradeid'] = [f"spread_trade_{i:06d}" for i in range(len(trades_processed))]
    
    # Remove invalid trades
    trades_processed = trades_processed.dropna(subset=['price', 'action'])
    
    print(f"   ✅ Processed trades: {len(trades_processed)} valid executions")
    print(f"      Buy actions: {(trades_processed['action'] == 1.0).sum()}")
    print(f"      Sell actions: {(trades_processed['action'] == -1.0).sum()}")
    
    # Show sample of processed trades
    print(f"\\n📋 Sample processed trades:")
    sample_trades = trades_processed.head(5)
    for idx, row in sample_trades.iterrows():
        print(f"      {idx}: price={row['price']:.3f}, action={row['action']}")
    
    # 3. PROCESS ORDERS DATA
    print(f"\\n🔄 Processing orders data...")
    orders_processed = orders_df.rename(columns={'bid': 'b_price', 'ask': 'a_price'})
    orders_processed['0'] = (orders_processed['b_price'] + orders_processed['a_price']) / 2
    
    print(f"   ✅ Processed orders: {len(orders_processed)} market data points")
    
    # 4. MERGE WITH UNION AND FORWARD-FILL
    print(f"\\n🔗 Merging with union and forward-fill...")
    
    # Create union of all timestamps
    all_timestamps = orders_processed.index.union(trades_processed.index)
    print(f"   📊 Total unique timestamps: {len(all_timestamps):,}")
    print(f"      Orders timestamps: {len(orders_processed):,}")
    print(f"      Trades timestamps: {len(trades_processed):,}")
    
    # Forward-fill orders to all timestamps
    orders_aligned = orders_processed.reindex(all_timestamps).ffill()
    
    # Create final merged DataFrame
    merged_df = pd.DataFrame(index=all_timestamps)
    
    # Add trade data (NaN for non-trade timestamps)
    for col in ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid']:
        merged_df[col] = trades_processed.reindex(all_timestamps)[col]
    
    # Add forward-filled order book data
    for col in ['b_price', 'a_price', '0']:
        merged_df[col] = orders_aligned[col]
    
    # Set proper column order
    reference_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
    merged_df = merged_df[reference_columns]
    
    # Remove any rows with missing order book data
    merged_df = merged_df.dropna(subset=['b_price', 'a_price', '0'])
    
    print(f"   ✅ Final merged data: {merged_df.shape}")
    print(f"      Trade rows: {merged_df['price'].notna().sum():,}")
    print(f"      Order-only rows: {merged_df['price'].isna().sum():,}")
    
    # 5. SAVE WITH CORRECT FILENAME
    print(f"\\n💾 Saving with correct filename...")
    merged_df.to_parquet(output_path, engine='pyarrow', compression='snappy')
    
    # Verify
    test_df = pd.read_parquet(output_path, engine='pyarrow')
    print(f"✅ Saved and verified: {test_df.shape}")
    
    # Save CSV sample
    csv_path = output_path.replace('.parquet', '.csv')
    merged_df.head(1000).to_csv(csv_path)
    print(f"💾 CSV sample: {Path(csv_path).name}")
    
    # Show final sample with both trade and order rows
    print(f"\\n📋 Final sample (trades + orders):")
    # Show a few trade rows
    trade_sample = merged_df[merged_df['price'].notna()].head(3)
    print("Trade rows:")
    for idx, row in trade_sample.iterrows():
        print(f"  {idx}: price={row['price']:.3f}, action={row['action']}, b_price={row['b_price']:.3f}, a_price={row['a_price']:.3f}")
    
    # Show a few order-only rows
    order_sample = merged_df[merged_df['price'].isna()].head(3)
    print("Order-only rows:")
    for idx, row in order_sample.iterrows():
        print(f"  {idx}: price=NaN, b_price={row['b_price']:.3f}, a_price={row['a_price']:.3f}, mid={row['0']:.3f}")
    
    print(f"\\n🎉 SUCCESS! Fixed merge completed:")
    print(f"📁 Filename: {output_file} (with absolute contracts)")
    print(f"🎯 Action logic: Buy/Sell properly handled")
    print(f"🔗 Merge method: Union + Forward-fill confirmed")
    print(f"📊 Format: {merged_df.shape} matching reference structure")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()