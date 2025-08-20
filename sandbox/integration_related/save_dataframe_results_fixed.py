#!/usr/bin/env python3
"""
Save the resulting dataframe data - fixed version without Excel dependency
"""

import sys
import os
import pandas as pd

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def save_dataframe_results():
    """Save the synchronized dataframe data"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Saving DataFrame Results")
        print("=" * 30)
        
        # Fetch leg 1
        config1 = {
            'contracts': ['debm01_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching debm01_25 data...")
        result1 = integrated_fetch(config1)
        leg1_data = result1['single_leg_data']
        
        # Fetch leg 2
        config2 = {
            'contracts': ['debm02_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching debm02_25 data...")
        result2 = integrated_fetch(config2)
        leg2_data = result2['single_leg_data']
        
        # Extract data
        leg1_orders = leg1_data['orders'].dropna()
        leg1_trades = leg1_data['trades'].dropna()
        leg2_orders = leg2_data['orders'].dropna()  
        leg2_trades = leg2_data['trades'].dropna()
        
        print(f"📊 Raw Data:")
        print(f"   Leg1 orders: {len(leg1_orders):,} rows")
        print(f"   Leg1 trades: {len(leg1_trades):,} rows") 
        print(f"   Leg2 orders: {len(leg2_orders):,} rows")
        print(f"   Leg2 trades: {len(leg2_trades):,} rows")
        
        # Calculate mid prices
        leg1_mid = (leg1_orders['b_price'] + leg1_orders['a_price']) / 2
        leg2_mid = (leg2_orders['b_price'] + leg2_orders['a_price']) / 2
        
        # Create union timeline and synchronize
        all_times = leg1_orders.index.union(leg2_orders.index)
        
        # Align both contracts to same timeline
        leg1_orders_sync = leg1_orders.reindex(all_times, method='ffill')
        leg2_orders_sync = leg2_orders.reindex(all_times, method='ffill')
        leg1_mid_sync = leg1_mid.reindex(all_times, method='ffill')
        leg2_mid_sync = leg2_mid.reindex(all_times, method='ffill')
        
        # Remove periods before either contract starts
        valid_mask = ~(leg1_mid_sync.isna() | leg2_mid_sync.isna())
        
        final_times = all_times[valid_mask]
        leg1_orders_final = leg1_orders_sync[valid_mask]
        leg2_orders_final = leg2_orders_sync[valid_mask]
        leg1_mid_final = leg1_mid_sync[valid_mask]
        leg2_mid_final = leg2_mid_sync[valid_mask]
        
        print(f"📊 Synchronized Data: {len(final_times):,} aligned timestamps")
        
        # Create comprehensive synchronized dataframe
        synchronized_df = pd.DataFrame({
            'debm01_25_bid': leg1_orders_final['b_price'].values,
            'debm01_25_ask': leg1_orders_final['a_price'].values,
            'debm01_25_mid': leg1_mid_final.values,
            'debm02_25_bid': leg2_orders_final['b_price'].values,
            'debm02_25_ask': leg2_orders_final['a_price'].values,
            'debm02_25_mid': leg2_mid_final.values
        }, index=final_times)
        
        # Add calculated fields
        synchronized_df['debm01_25_spread'] = synchronized_df['debm01_25_ask'] - synchronized_df['debm01_25_bid']
        synchronized_df['debm02_25_spread'] = synchronized_df['debm02_25_ask'] - synchronized_df['debm02_25_bid']
        synchronized_df['calendar_spread'] = synchronized_df['debm01_25_mid'] - synchronized_df['debm02_25_mid']
        synchronized_df['price_correlation_1000'] = synchronized_df['debm01_25_mid'].rolling(1000).corr(synchronized_df['debm02_25_mid'])
        
        print(f"📊 Final DataFrame:")
        print(f"   Shape: {synchronized_df.shape}")
        print(f"   Columns: {list(synchronized_df.columns)}")
        print(f"   Memory usage: {synchronized_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        
        # Create filename with contracts and date range
        start_date = config1['period']['start_date'].replace('-', '')
        end_date = config1['period']['end_date'].replace('-', '')
        
        # Save as multiple formats
        base_filename = f"synchronized_data_debm01_25_vs_debm02_25_{start_date}_to_{end_date}"
        base_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'
        
        # Save as Parquet (most efficient)
        parquet_path = f"{base_path}/{base_filename}.parquet"
        synchronized_df.to_parquet(parquet_path)
        print(f"💾 Saved Parquet: {parquet_path}")
        
        # Save as CSV (most readable)
        csv_path = f"{base_path}/{base_filename}.csv"
        synchronized_df.to_csv(csv_path)
        print(f"💾 Saved CSV: {csv_path}")
        
        # Save as pickle (preserves all pandas features)
        pickle_path = f"{base_path}/{base_filename}.pkl"
        synchronized_df.to_pickle(pickle_path)
        print(f"💾 Saved Pickle: {pickle_path}")
        
        # Save individual legs as separate files
        leg1_orders_path = f"{base_path}/debm01_25_orders_{start_date}_to_{end_date}.parquet"
        leg1_orders.to_parquet(leg1_orders_path)
        print(f"💾 Saved Leg1 Orders: {leg1_orders_path}")
        
        leg2_orders_path = f"{base_path}/debm02_25_orders_{start_date}_to_{end_date}.parquet"
        leg2_orders.to_parquet(leg2_orders_path)
        print(f"💾 Saved Leg2 Orders: {leg2_orders_path}")
        
        # Save trades data
        leg1_trades_path = f"{base_path}/debm01_25_trades_{start_date}_to_{end_date}.parquet"
        leg1_trades.to_parquet(leg1_trades_path)
        print(f"💾 Saved Leg1 Trades: {leg1_trades_path}")
        
        leg2_trades_path = f"{base_path}/debm02_25_trades_{start_date}_to_{end_date}.parquet"
        leg2_trades.to_parquet(leg2_trades_path)
        print(f"💾 Saved Leg2 Trades: {leg2_trades_path}")
        
        # Create summary statistics file
        summary_stats = pd.DataFrame({
            'Metric': ['Count', 'Mean_Price', 'Std_Dev', 'Min_Price', 'Max_Price', 'Avg_Spread', 'First_Update', 'Last_Update'],
            'debm01_25': [
                len(leg1_orders),
                leg1_mid.mean(),
                leg1_mid.std(),
                leg1_mid.min(),
                leg1_mid.max(),
                (leg1_orders['a_price'] - leg1_orders['b_price']).mean(),
                str(leg1_orders.index[0]),
                str(leg1_orders.index[-1])
            ],
            'debm02_25': [
                len(leg2_orders),
                leg2_mid.mean(),
                leg2_mid.std(),
                leg2_mid.min(),
                leg2_mid.max(),
                (leg2_orders['a_price'] - leg2_orders['b_price']).mean(),
                str(leg2_orders.index[0]),
                str(leg2_orders.index[-1])
            ]
        })
        
        summary_path = f"{base_path}/summary_stats_{base_filename.split('_', 2)[-1]}.csv"
        summary_stats.to_csv(summary_path, index=False)
        print(f"💾 Saved Summary: {summary_path}")
        
        # Display sample data
        print(f"\n📋 SAMPLE DATA (First 10 rows):")
        print("=" * 120)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        print(synchronized_df.head(10).round(3))
        
        print(f"\n📋 SUMMARY STATISTICS:")
        print("=" * 80)
        print(synchronized_df.describe().round(3))
        
        print(f"\n📋 DATA QUALITY CHECK:")
        print("=" * 40)
        print(f"   Missing values: {synchronized_df.isnull().sum().sum()}")
        print(f"   Date range: {synchronized_df.index[0]} to {synchronized_df.index[-1]}")
        print(f"   Duration: {synchronized_df.index[-1] - synchronized_df.index[0]}")
        print(f"   Total timestamps: {len(synchronized_df):,}")
        print(f"   Avg calendar spread: {synchronized_df['calendar_spread'].mean():.3f} €/MWh")
        print(f"   Calendar spread range: {synchronized_df['calendar_spread'].min():.3f} to {synchronized_df['calendar_spread'].max():.3f} €/MWh")
        print(f"   Price correlation: {synchronized_df['debm01_25_mid'].corr(synchronized_df['debm02_25_mid']):.4f}")
        
        print(f"\n📋 TRADE SUMMARY:")
        print("=" * 30)
        print(f"   debm01_25 trades: {len(leg1_trades):,} ({leg1_trades['volume'].sum():,.0f} MWh)")
        print(f"   debm02_25 trades: {len(leg2_trades):,} ({leg2_trades['volume'].sum():,.0f} MWh)")
        print(f"   Total volume: {leg1_trades['volume'].sum() + leg2_trades['volume'].sum():,.0f} MWh")
        
        print(f"\n📋 FILES CREATED:")
        print("=" * 50)
        print(f"   📊 Main synchronized data:")
        print(f"     • {base_filename}.parquet (efficient)")
        print(f"     • {base_filename}.csv (readable)")
        print(f"     • {base_filename}.pkl (pandas native)")
        print(f"   📊 Individual contract data:")
        print(f"     • debm01_25_orders_{start_date}_to_{end_date}.parquet")
        print(f"     • debm01_25_trades_{start_date}_to_{end_date}.parquet")
        print(f"     • debm02_25_orders_{start_date}_to_{end_date}.parquet")
        print(f"     • debm02_25_trades_{start_date}_to_{end_date}.parquet")
        print(f"   📊 Summary statistics:")
        print(f"     • summary_stats_debm01_25_vs_debm02_25_{start_date}_to_{end_date}.csv")
        
        print(f"\n🎉 DataFrame results saved successfully!")
        print(f"💡 Use pd.read_parquet() for fastest loading")
        print(f"💡 Use pd.read_csv() for universal compatibility")
        
        return synchronized_df
        
    except Exception as e:
        print(f"❌ DataFrame saving failed: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    df = save_dataframe_results()