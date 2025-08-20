#!/usr/bin/env python3
"""
Save properly merged dataframe with trades included and no individual leg separation
"""

import sys
import os
import pandas as pd

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def save_proper_merged():
    """Save properly merged dataframe with trades and unified structure"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Creating Properly Merged DataFrame")
        print("=" * 40)
        
        # Fetch data
        config1 = {
            'contracts': ['debm01_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        config2 = {
            'contracts': ['debm02_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching contract data...")
        result1 = integrated_fetch(config1)
        result2 = integrated_fetch(config2)
        
        # Extract orders and trades
        leg1_orders = result1['single_leg_data']['orders'].dropna()
        leg1_trades = result1['single_leg_data']['trades'].dropna()
        leg2_orders = result2['single_leg_data']['orders'].dropna()
        leg2_trades = result2['single_leg_data']['trades'].dropna()
        
        print(f"📊 Raw data: {len(leg1_orders):,} + {len(leg2_orders):,} orders, {len(leg1_trades):,} + {len(leg2_trades):,} trades")
        
        # Combine all data into single unified dataframe
        all_data = []
        
        # Add orders data
        for idx, row in leg1_orders.iterrows():
            all_data.append({
                'datetime': idx,
                'contract': 'debm01_25',
                'type': 'order',
                'bid_price': row['b_price'],
                'ask_price': row['a_price'],
                'mid_price': (row['b_price'] + row['a_price']) / 2,
                'spread': row['a_price'] - row['b_price'],
                'trade_price': None,
                'volume': None,
                'action': None
            })
        
        for idx, row in leg2_orders.iterrows():
            all_data.append({
                'datetime': idx,
                'contract': 'debm02_25', 
                'type': 'order',
                'bid_price': row['b_price'],
                'ask_price': row['a_price'],
                'mid_price': (row['b_price'] + row['a_price']) / 2,
                'spread': row['a_price'] - row['b_price'],
                'trade_price': None,
                'volume': None,
                'action': None
            })
        
        # Add trades data
        for idx, row in leg1_trades.iterrows():
            all_data.append({
                'datetime': idx,
                'contract': 'debm01_25',
                'type': 'trade',
                'bid_price': None,
                'ask_price': None,
                'mid_price': None,
                'spread': None,
                'trade_price': row['price'],
                'volume': row['volume'],
                'action': row.get('action', None)
            })
        
        for idx, row in leg2_trades.iterrows():
            all_data.append({
                'datetime': idx,
                'contract': 'debm02_25',
                'type': 'trade', 
                'bid_price': None,
                'ask_price': None,
                'mid_price': None,
                'spread': None,
                'trade_price': row['price'],
                'volume': row['volume'],
                'action': row.get('action', None)
            })
        
        # Create unified dataframe
        unified_df = pd.DataFrame(all_data)
        unified_df.set_index('datetime', inplace=True)
        unified_df.sort_index(inplace=True)
        
        print(f"📊 Unified DataFrame: {len(unified_df):,} total records")
        print(f"   Orders: {len(unified_df[unified_df['type'] == 'order']):,}")
        print(f"   Trades: {len(unified_df[unified_df['type'] == 'trade']):,}")
        
        # Simple filename
        start_date = config1['period']['start_date'].replace('-', '')
        end_date = config1['period']['end_date'].replace('-', '')
        filename = f"debm01_25_debm02_25_{start_date}_to_{end_date}"
        base_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'
        
        # Save files
        parquet_path = f"{base_path}/{filename}.parquet"
        csv_path = f"{base_path}/{filename}.csv"
        
        unified_df.to_parquet(parquet_path)
        unified_df.to_csv(csv_path)
        
        print(f"💾 Files saved:")
        print(f"   📄 {filename}.parquet")
        print(f"   📄 {filename}.csv")
        
        # Show samples
        print(f"\n📋 Sample Orders:")
        orders_sample = unified_df[unified_df['type'] == 'order'].head(3)
        print(orders_sample[['contract', 'bid_price', 'ask_price', 'mid_price', 'spread']].round(3))
        
        print(f"\n📋 Sample Trades:")
        trades_sample = unified_df[unified_df['type'] == 'trade'].head(3)  
        print(trades_sample[['contract', 'trade_price', 'volume', 'action']].round(3))
        
        # Summary stats
        orders_df = unified_df[unified_df['type'] == 'order']
        trades_df = unified_df[unified_df['type'] == 'trade']
        
        print(f"\n📊 Summary:")
        print(f"   Total records: {len(unified_df):,}")
        print(f"   Orders: {len(orders_df):,}")
        print(f"   Trades: {len(trades_df):,}")
        print(f"   Date range: {unified_df.index.min()} to {unified_df.index.max()}")
        print(f"   Contracts: {unified_df['contract'].unique()}")
        
        if not trades_df.empty:
            print(f"   Total volume: {trades_df['volume'].sum():,.0f} MWh")
            print(f"   Avg trade size: {trades_df['volume'].mean():.1f} MWh")
        
        print(f"\n🎉 Properly merged dataframe with trades saved!")
        
    except Exception as e:
        print(f"❌ Save failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    save_proper_merged()