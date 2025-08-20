#!/usr/bin/env python3
"""
Save spread data in the exact structure as the reference file
"""

import sys
import os
import pandas as pd
import numpy as np

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def save_spread_structure():
    """Save spread data in the exact structure as reference file"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Creating Spread Data with Target Structure")
        print("=" * 45)
        
        # Fetch spread data (synthetic since we know real spread has issues)
        config = {
            'contracts': ['debm01_25', 'debm02_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 
            'mode': 'spread'
        }
        
        print("📡 Fetching spread data...")
        result = integrated_fetch(config)
        
        print(f"📊 Integration result keys: {list(result.keys())}")
        
        # Check what we have
        if 'synthetic_spread_data' in result:
            synthetic = result['synthetic_spread_data']
            spread_orders = synthetic.get('spread_orders', pd.DataFrame())
            spread_trades = synthetic.get('spread_trades', pd.DataFrame())
            
            print(f"📊 Synthetic spread data:")
            print(f"   Orders: {len(spread_orders):,} rows")
            print(f"   Trades: {len(spread_trades):,} rows")
            
            if spread_orders.empty and spread_trades.empty:
                print("⚠️  No synthetic spread data - falling back to individual legs calculation")
                return create_spread_from_legs()
        
        # If we get here and have no data, use individual legs
        if 'synthetic_spread_data' not in result or (spread_orders.empty and spread_trades.empty):
            print("⚠️  No spread data available - creating from individual legs")
            return create_spread_from_legs()
            
        # Process the spread data into target structure
        all_records = []
        
        # Process spread orders
        if not spread_orders.empty:
            print(f"📊 Processing {len(spread_orders):,} spread orders...")
            
            for idx, row in spread_orders.iterrows():
                record = {
                    'price': np.nan,  # Orders don't have trade price
                    'volume': np.nan,  # Orders don't have volume
                    'action': np.nan,  # Orders don't have action
                    'broker_id': np.nan,  # Orders don't have broker
                    'count': np.nan,  # Orders don't have count
                    'tradeid': np.nan,  # Orders don't have trade ID
                    'b_price': row.get('bid', row.get('b_price', np.nan)),
                    'a_price': row.get('ask', row.get('a_price', np.nan)),
                    '0': (row.get('bid', row.get('b_price', 0)) + row.get('ask', row.get('a_price', 0))) / 2  # Mid price
                }
                all_records.append((idx, record))
        
        # Process spread trades
        if not spread_trades.empty:
            print(f"📊 Processing {len(spread_trades):,} spread trades...")
            
            for idx, row in spread_trades.iterrows():
                record = {
                    'price': row.get('price', row.get('buy', row.get('sell', np.nan))),
                    'volume': row.get('volume', np.nan),
                    'action': row.get('action', np.nan),
                    'broker_id': row.get('broker_id', np.nan),
                    'count': row.get('count', 1),
                    'tradeid': row.get('tradeid', f'spread_trade_{idx}'),
                    'b_price': np.nan,  # Trades don't have bid/ask
                    'a_price': np.nan,
                    '0': row.get('price', row.get('buy', row.get('sell', np.nan)))  # Trade price as mid
                }
                all_records.append((idx, record))
        
        if not all_records:
            print("❌ No records to process")
            return None
        
        # Create dataframe with target structure
        timestamps, records = zip(*all_records)
        spread_df = pd.DataFrame(list(records), index=list(timestamps))
        spread_df.sort_index(inplace=True)
        
        # Ensure column order matches reference
        target_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
        spread_df = spread_df[target_columns]
        
        print(f"📊 Final spread dataframe: {spread_df.shape}")
        print(f"   Columns: {list(spread_df.columns)}")
        
        # Save with proper naming
        start_date = config['period']['start_date'].replace('-', '')
        end_date = config['period']['end_date'].replace('-', '')
        filename = f"debm01_25_debm02_25_spread_{start_date}_to_{end_date}"
        base_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'
        
        parquet_path = f"{base_path}/{filename}.parquet"
        csv_path = f"{base_path}/{filename}.csv"
        spread_df.to_parquet(parquet_path)
        spread_df.to_csv(csv_path)
        print(f"💾 Saved: {filename}.parquet")
        print(f"💾 Saved: {filename}.csv")
        
        # Show sample
        print(f"\n📋 Sample Data (first 10 rows):")
        print("=" * 120)
        print(spread_df.head(10))
        
        print(f"\n📊 Data Summary:")
        print(f"   Total records: {len(spread_df):,}")
        print(f"   Date range: {spread_df.index.min()} to {spread_df.index.max()}")
        print(f"   Structure: Matches reference dem07_25_tr_ba_data.parquet")
        
        return spread_df
        
    except Exception as e:
        print(f"❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def create_spread_from_legs():
    """Fallback: create synthetic spread from individual legs"""
    print("🔄 Creating synthetic spread from individual legs...")
    
    try:
        from integration_script_v2 import integrated_fetch
        
        # Get individual legs
        config1 = {'contracts': ['debm01_25'], 'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'}, 'n_s': 3, 'mode': 'individual'}
        config2 = {'contracts': ['debm02_25'], 'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'}, 'n_s': 3, 'mode': 'individual'}
        
        result1 = integrated_fetch(config1)
        result2 = integrated_fetch(config2)
        
        leg1_orders = result1['single_leg_data']['orders'].dropna()
        leg1_trades = result1['single_leg_data']['trades'].dropna()
        leg2_orders = result2['single_leg_data']['orders'].dropna() 
        leg2_trades = result2['single_leg_data']['trades'].dropna()
        
        print(f"📊 Individual legs: {len(leg1_orders):,}+{len(leg2_orders):,} orders, {len(leg1_trades):,}+{len(leg2_trades):,} trades")
        
        # Calculate synthetic spread orders (leg1 - leg2)
        all_times = leg1_orders.index.union(leg2_orders.index)
        leg1_aligned = leg1_orders.reindex(all_times, method='ffill')
        leg2_aligned = leg2_orders.reindex(all_times, method='ffill')
        
        valid_mask = ~(leg1_aligned.isna().any(axis=1) | leg2_aligned.isna().any(axis=1))
        spread_orders_data = []
        
        for idx in all_times[valid_mask]:
            leg1_mid = (leg1_aligned.loc[idx, 'b_price'] + leg1_aligned.loc[idx, 'a_price']) / 2
            leg2_mid = (leg2_aligned.loc[idx, 'b_price'] + leg2_aligned.loc[idx, 'a_price']) / 2
            spread_mid = leg1_mid - leg2_mid
            
            # Synthetic spread bid/ask (using average spread width)
            avg_spread = 0.15  # Typical spread width
            spread_bid = spread_mid - avg_spread/2
            spread_ask = spread_mid + avg_spread/2
            
            spread_orders_data.append((idx, {
                'price': np.nan,
                'volume': np.nan, 
                'action': np.nan,
                'broker_id': np.nan,
                'count': np.nan,
                'tradeid': np.nan,
                'b_price': spread_bid,
                'a_price': spread_ask,
                '0': spread_mid
            }))
        
        # Create synthetic spread trades from leg trades
        spread_trades_data = []
        all_trade_times = leg1_trades.index.union(leg2_trades.index)
        
        for idx in all_trade_times:
            leg1_price = leg1_trades.loc[idx, 'price'] if idx in leg1_trades.index else None
            leg2_price = leg2_trades.loc[idx, 'price'] if idx in leg2_trades.index else None
            
            if leg1_price is not None and leg2_price is not None:
                spread_price = leg1_price - leg2_price
                volume = min(
                    leg1_trades.loc[idx, 'volume'] if idx in leg1_trades.index else 0,
                    leg2_trades.loc[idx, 'volume'] if idx in leg2_trades.index else 0
                )
                
                spread_trades_data.append((idx, {
                    'price': spread_price,
                    'volume': volume,
                    'action': 0,  # Spread trade
                    'broker_id': 9999,  # Synthetic
                    'count': 1,
                    'tradeid': f'synthetic_spread_{idx}',
                    'b_price': np.nan,
                    'a_price': np.nan,
                    '0': spread_price
                }))
        
        # Combine all data
        all_data = spread_orders_data + spread_trades_data
        if not all_data:
            print("❌ No spread data could be created")
            return None
        
        timestamps, records = zip(*all_data)
        spread_df = pd.DataFrame(list(records), index=list(timestamps))
        spread_df.sort_index(inplace=True)
        
        # Save
        filename = "debm01_25_debm02_25_synthetic_spread_20241202_to_20241206"
        base_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'
        parquet_path = f"{base_path}/{filename}.parquet"
        csv_path = f"{base_path}/{filename}.csv"
        spread_df.to_parquet(parquet_path)
        spread_df.to_csv(csv_path)
        
        print(f"💾 Saved synthetic spread: {filename}.parquet")
        print(f"💾 Saved synthetic spread: {filename}.csv")
        print(f"📊 Shape: {spread_df.shape}")
        
        return spread_df
        
    except Exception as e:
        print(f"❌ Synthetic spread creation failed: {e}")
        return None

if __name__ == "__main__":
    df = save_spread_structure()