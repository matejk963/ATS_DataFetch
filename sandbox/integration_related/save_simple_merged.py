#!/usr/bin/env python3
"""
Save only the merged dataframe with simple naming
"""

import sys
import os
import pandas as pd

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def save_simple_merged():
    """Save only the merged dataframe with simple naming"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Saving Simple Merged DataFrame")
        print("=" * 35)
        
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
        
        # Extract and clean data
        leg1_orders = result1['single_leg_data']['orders'].dropna()
        leg2_orders = result2['single_leg_data']['orders'].dropna()
        
        # Calculate mid prices
        leg1_mid = (leg1_orders['b_price'] + leg1_orders['a_price']) / 2
        leg2_mid = (leg2_orders['b_price'] + leg2_orders['a_price']) / 2
        
        # Synchronize on common timeline
        all_times = leg1_orders.index.union(leg2_orders.index)
        
        leg1_orders_sync = leg1_orders.reindex(all_times, method='ffill')
        leg2_orders_sync = leg2_orders.reindex(all_times, method='ffill')
        leg1_mid_sync = leg1_mid.reindex(all_times, method='ffill')
        leg2_mid_sync = leg2_mid.reindex(all_times, method='ffill')
        
        # Remove invalid periods
        valid_mask = ~(leg1_mid_sync.isna() | leg2_mid_sync.isna())
        
        final_times = all_times[valid_mask]
        leg1_orders_final = leg1_orders_sync[valid_mask]
        leg2_orders_final = leg2_orders_sync[valid_mask]
        leg1_mid_final = leg1_mid_sync[valid_mask]
        leg2_mid_final = leg2_mid_sync[valid_mask]
        
        # Create simple merged dataframe
        merged_df = pd.DataFrame({
            'contract1_bid': leg1_orders_final['b_price'].values,
            'contract1_ask': leg1_orders_final['a_price'].values,
            'contract1_mid': leg1_mid_final.values,
            'contract2_bid': leg2_orders_final['b_price'].values,
            'contract2_ask': leg2_orders_final['a_price'].values,
            'contract2_mid': leg2_mid_final.values,
        }, index=final_times)
        
        # Add calculated fields
        merged_df['contract1_spread'] = merged_df['contract1_ask'] - merged_df['contract1_bid']
        merged_df['contract2_spread'] = merged_df['contract2_ask'] - merged_df['contract2_bid']
        merged_df['calendar_spread'] = merged_df['contract1_mid'] - merged_df['contract2_mid']
        
        print(f"📊 Merged DataFrame: {merged_df.shape[0]:,} rows, {merged_df.shape[1]} columns")
        
        # Simple filename: contract1_contract2_daterange
        start_date = config1['period']['start_date'].replace('-', '')
        end_date = config1['period']['end_date'].replace('-', '')
        filename = f"debm01_25_debm02_25_{start_date}_to_{end_date}"
        base_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'
        
        # Save in multiple formats
        parquet_path = f"{base_path}/{filename}.parquet"
        csv_path = f"{base_path}/{filename}.csv"
        pkl_path = f"{base_path}/{filename}.pkl"
        
        merged_df.to_parquet(parquet_path)
        merged_df.to_csv(csv_path)
        merged_df.to_pickle(pkl_path)
        
        print(f"💾 Files saved:")
        print(f"   📄 {filename}.parquet")
        print(f"   📄 {filename}.csv")
        print(f"   📄 {filename}.pkl")
        
        # Show sample
        print(f"\n📋 Sample Data (first 5 rows):")
        print("=" * 100)
        print(merged_df.head().round(3))
        
        print(f"\n📊 Summary:")
        print(f"   Records: {len(merged_df):,}")
        print(f"   Date range: {merged_df.index[0]} to {merged_df.index[-1]}")
        print(f"   Contract1 avg: {merged_df['contract1_mid'].mean():.2f} €/MWh")
        print(f"   Contract2 avg: {merged_df['contract2_mid'].mean():.2f} €/MWh")
        print(f"   Calendar spread avg: {merged_df['calendar_spread'].mean():.3f} €/MWh")
        print(f"   Correlation: {merged_df['contract1_mid'].corr(merged_df['contract2_mid']):.3f}")
        
        print(f"\n🎉 Simple merged dataframe saved!")
        
    except Exception as e:
        print(f"❌ Save failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    save_simple_merged()