#!/usr/bin/env python3
"""
Save the resulting dataframe data instead of plotting
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
            'datetime': final_times,
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
        synchronized_df['price_correlation'] = synchronized_df['debm01_25_mid'].rolling(1000).corr(synchronized_df['debm02_25_mid'])
        
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
        
        # Save as Parquet (efficient for large data)
        parquet_path = f"{base_path}/{base_filename}.parquet"
        synchronized_df.to_parquet(parquet_path)
        print(f"💾 Saved Parquet: {parquet_path}")
        
        # Save as CSV (readable)
        csv_path = f"{base_path}/{base_filename}.csv"
        synchronized_df.to_csv(csv_path)
        print(f"💾 Saved CSV: {csv_path}")
        
        # Save as Excel (with multiple sheets)
        excel_path = f"{base_path}/{base_filename}.xlsx"
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Main synchronized data
            synchronized_df.to_excel(writer, sheet_name='Synchronized_Prices')
            
            # Individual legs data
            leg1_orders.to_excel(writer, sheet_name='debm01_25_Orders')
            leg1_trades.to_excel(writer, sheet_name='debm01_25_Trades')
            leg2_orders.to_excel(writer, sheet_name='debm02_25_Orders')
            leg2_trades.to_excel(writer, sheet_name='debm02_25_Trades')
            
            # Summary statistics
            summary_stats = pd.DataFrame({
                'Metric': ['Count', 'Mean Price', 'Std Dev', 'Min Price', 'Max Price', 'Avg Spread'],
                'debm01_25': [
                    len(synchronized_df),
                    synchronized_df['debm01_25_mid'].mean(),
                    synchronized_df['debm01_25_mid'].std(),
                    synchronized_df['debm01_25_mid'].min(),
                    synchronized_df['debm01_25_mid'].max(),
                    synchronized_df['debm01_25_spread'].mean()
                ],
                'debm02_25': [
                    len(synchronized_df),
                    synchronized_df['debm02_25_mid'].mean(),
                    synchronized_df['debm02_25_mid'].std(),
                    synchronized_df['debm02_25_mid'].min(),
                    synchronized_df['debm02_25_mid'].max(),
                    synchronized_df['debm02_25_spread'].mean()
                ]
            })
            summary_stats.to_excel(writer, sheet_name='Summary_Statistics', index=False)
            
        print(f"💾 Saved Excel: {excel_path}")
        
        # Save trades data separately
        trades_filename = f"trades_data_debm01_25_vs_debm02_25_{start_date}_to_{end_date}"
        
        # Combine trades with contract identifier
        leg1_trades_tagged = leg1_trades.copy()
        leg1_trades_tagged['contract'] = 'debm01_25'
        leg2_trades_tagged = leg2_trades.copy()
        leg2_trades_tagged['contract'] = 'debm02_25'
        
        all_trades = pd.concat([leg1_trades_tagged, leg2_trades_tagged], axis=0).sort_index()
        
        trades_parquet_path = f"{base_path}/{trades_filename}.parquet"
        all_trades.to_parquet(trades_parquet_path)
        print(f"💾 Saved Trades Parquet: {trades_parquet_path}")
        
        # Display sample data
        print(f"\n📋 SAMPLE DATA (First 10 rows):")
        print("=" * 80)
        print(synchronized_df.head(10).round(3))
        
        print(f"\n📋 SUMMARY STATISTICS:")
        print("=" * 50)
        print(synchronized_df.describe().round(3))
        
        print(f"\n📋 DATA QUALITY:")
        print("=" * 30)
        print(f"   No missing values: {synchronized_df.isnull().sum().sum() == 0}")
        print(f"   Date range: {synchronized_df.index[0]} to {synchronized_df.index[-1]}")
        print(f"   Duration: {synchronized_df.index[-1] - synchronized_df.index[0]}")
        print(f"   Avg calendar spread: {synchronized_df['calendar_spread'].mean():.3f} €/MWh")
        print(f"   Price correlation: {synchronized_df['debm01_25_mid'].corr(synchronized_df['debm02_25_mid']):.3f}")
        
        print(f"\n📋 FILES CREATED:")
        print("=" * 40)
        print(f"   📄 Parquet (efficient): {base_filename}.parquet")
        print(f"   📄 CSV (readable): {base_filename}.csv") 
        print(f"   📄 Excel (multi-sheet): {base_filename}.xlsx")
        print(f"   📄 Trades (separate): {trades_filename}.parquet")
        
        print(f"\n🎉 DataFrame results saved successfully!")
        
        # Return the dataframe for further use if needed
        return synchronized_df, all_trades
        
    except Exception as e:
        print(f"❌ DataFrame saving failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None


if __name__ == "__main__":
    df, trades_df = save_dataframe_results()