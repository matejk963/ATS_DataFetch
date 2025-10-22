#!/usr/bin/env python3
"""
Investigate Fake "Real" Trades
==============================

If there are no real trades for debm11_25 vs debq1_26 spread,
then where are the broker_id 9999.0 trades coming from?
This could be a serious data integrity issue.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

def investigate_fake_real_trades():
    """Investigate the source of fake real trades"""
    
    print("🚨 INVESTIGATING FAKE 'REAL' TRADES")
    print("=" * 50)
    
    # Load the merged dataset
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_test_merged.parquet'
    spread_data = pd.read_parquet(file_path)
    spread_data.index = pd.to_datetime(spread_data.index)
    spread_data = spread_data.sort_index()
    
    # Focus on trades only
    trades = spread_data[spread_data['action'].isin([1.0, -1.0])].copy()
    
    print(f"Total trade records: {len(trades):,}")
    
    # Analyze by broker_id (data source)
    print(f"\n📊 BROKER ID ANALYSIS:")
    print("-" * 30)
    
    broker_stats = trades.groupby('broker_id').agg({
        'price': ['count', 'mean', 'std', 'min', 'max'],
        'volume': ['mean', 'sum'],
        'tradeid': 'nunique'
    }).round(3)
    
    for broker_id in trades['broker_id'].unique():
        broker_trades = trades[trades['broker_id'] == broker_id]
        
        print(f"\nBroker ID: {broker_id}")
        print(f"  Count: {len(broker_trades):,} trades")
        print(f"  Price range: {broker_trades['price'].min():.3f} to {broker_trades['price'].max():.3f} EUR/MWh")
        print(f"  Average price: {broker_trades['price'].mean():.3f} EUR/MWh")
        print(f"  Price std: {broker_trades['price'].std():.3f} EUR/MWh")
        
        if 'volume' in broker_trades.columns:
            print(f"  Volume range: {broker_trades['volume'].min()} to {broker_trades['volume'].max()}")
            print(f"  Total volume: {broker_trades['volume'].sum()}")
        
        if 'tradeid' in broker_trades.columns:
            unique_trade_ids = broker_trades['tradeid'].nunique()
            print(f"  Unique trade IDs: {unique_trade_ids}")
            
            # Sample some trade IDs
            sample_ids = broker_trades['tradeid'].dropna().unique()[:10]
            print(f"  Sample trade IDs: {list(sample_ids)}")
        
        print(f"  Time range: {broker_trades.index.min()} to {broker_trades.index.max()}")
        
        # Check for suspicious patterns
        print(f"\n  🔍 SUSPICIOUS PATTERN ANALYSIS:")
        
        # Check if volumes are all the same (indicating synthetic data)
        if 'volume' in broker_trades.columns:
            volume_values = broker_trades['volume'].value_counts()
            if len(volume_values) <= 3:
                print(f"    ⚠️  Very few unique volumes: {dict(volume_values)}")
            
        # Check if trade IDs follow a pattern
        if 'tradeid' in broker_trades.columns:
            trade_ids = broker_trades['tradeid'].dropna()
            if len(trade_ids) > 0:
                # Check if trade IDs are sequential or have patterns
                if trade_ids.dtype in ['int64', 'float64']:
                    trade_id_diffs = trade_ids.diff().dropna()
                    if len(trade_id_diffs.unique()) <= 3:
                        print(f"    ⚠️  Trade IDs follow pattern, diffs: {list(trade_id_diffs.unique())}")
        
        # Check if prices cluster in specific ranges
        price_std = broker_trades['price'].std()
        price_range = broker_trades['price'].max() - broker_trades['price'].min()
        if price_std < 0.5 and price_range < 2.0:
            print(f"    ⚠️  Prices are very clustered (std: {price_std:.3f}, range: {price_range:.3f})")
    
    # Check the actual raw data sources to see what was supposed to be fetched
    print(f"\n🔍 CHECKING EXPECTED DATA SOURCES:")
    print("-" * 40)
    
    # Look for any real spread files that shouldn't exist
    raw_data_dir = Path('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads')
    
    if raw_data_dir.exists():
        # Look for real spread files
        real_files = list(raw_data_dir.glob('*real*'))
        synthetic_files = list(raw_data_dir.glob('*synthetic*'))
        
        print(f"Real spread files found: {len(real_files)}")
        for f in real_files[:5]:  # Show first 5
            print(f"  {f.name}")
            
        print(f"\nSynthetic spread files found: {len(synthetic_files)}")
        for f in synthetic_files[:5]:  # Show first 5
            print(f"  {f.name}")
        
        # Check if there's a real spread file for this combination
        target_real_file = raw_data_dir / 'debm11_25_debq1_26_tr_ba_dataanalysis_real.parquet'
        target_synthetic_file = raw_data_dir / 'debm11_25_debq1_26_tr_ba_dataanalysis_synthetic.parquet'
        
        print(f"\nTarget files:")
        print(f"  Real file exists: {target_real_file.exists()}")
        print(f"  Synthetic file exists: {target_synthetic_file.exists()}")
        
        if target_real_file.exists():
            print(f"\n🚨 ANALYZING SUSPICIOUS 'REAL' FILE:")
            real_data = pd.read_parquet(target_real_file)
            real_data.index = pd.to_datetime(real_data.index)
            
            real_trades = real_data[real_data['action'].isin([1.0, -1.0])] if len(real_data) > 0 else pd.DataFrame()
            
            print(f"  Records in 'real' file: {len(real_data):,}")
            print(f"  Trades in 'real' file: {len(real_trades):,}")
            
            if len(real_trades) > 0:
                print(f"  Price range: {real_trades['price'].min():.3f} to {real_trades['price'].max():.3f}")
                print(f"  Average price: {real_trades['price'].mean():.3f}")
                
                # This is the smoking gun - if this file contains data,
                # it means the DataFetcher is incorrectly returning data
                # for a spread that doesn't exist in the market
                
                print(f"\n  🔥 SMOKING GUN ANALYSIS:")
                print(f"     If this spread doesn't trade in the market,")
                print(f"     then this 'real' data is FAKE/GENERATED!")
                
                # Check metadata or source indicators
                if 'broker_id' in real_trades.columns:
                    print(f"     Broker IDs in 'real' file: {real_trades['broker_id'].unique()}")
                
                # Sample some records to see their structure
                print(f"\n  📋 SAMPLE 'REAL' RECORDS:")
                sample_trades = real_trades.head(3)
                for idx, row in sample_trades.iterrows():
                    print(f"    {idx}: price={row['price']:.3f}, vol={row.get('volume', 'N/A')}, "
                          f"broker={row.get('broker_id', 'N/A')}, trade_id={row.get('tradeid', 'N/A')}")

def main():
    """Main function"""
    investigate_fake_real_trades()

if __name__ == "__main__":
    main()