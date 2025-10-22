#!/usr/bin/env python3
"""
Investigate Synthetic Trade Generation
=====================================

If this spread has no real trades, where are the 5,718 synthetic "trades" 
coming from? They should be order book data, not trades.
"""

import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

def investigate_synthetic_trade_generation():
    """Investigate where synthetic trades are being incorrectly generated"""
    
    print("🔍 INVESTIGATING SYNTHETIC TRADE GENERATION")
    print("=" * 50)
    
    # Load the suspicious "real" file that shouldn't exist
    real_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_test_real.parquet'
    
    if Path(real_file).exists():
        print(f"🚨 CRITICAL: 'Real' file exists when it shouldn't!")
        print(f"File: {real_file}")
        
        real_data = pd.read_parquet(real_file)
        real_data.index = pd.to_datetime(real_data.index)
        
        print(f"\n📊 CONTENTS OF 'REAL' FILE:")
        print(f"Total records: {len(real_data):,}")
        print(f"Columns: {list(real_data.columns)}")
        
        # Separate trades vs orders
        trades = real_data[real_data['action'].isin([1.0, -1.0])] if len(real_data) > 0 else pd.DataFrame()
        orders = real_data[~real_data['action'].isin([1.0, -1.0])] if len(real_data) > 0 else pd.DataFrame()
        
        print(f"Trades: {len(trades):,}")
        print(f"Orders: {len(orders):,}")
        
        if len(trades) > 0:
            print(f"\n🔥 FAKE TRADES ANALYSIS:")
            print(f"Price range: {trades['price'].min():.3f} to {trades['price'].max():.3f}")
            print(f"Average price: {trades['price'].mean():.3f}")
            
            # Sample trade IDs to see the pattern
            sample_trade_ids = trades['tradeid'].head(10).tolist()
            print(f"Sample trade IDs:")
            for tid in sample_trade_ids:
                print(f"  {tid}")
            
            # Check broker IDs
            if 'broker_id' in trades.columns:
                broker_counts = trades['broker_id'].value_counts()
                print(f"\nBroker ID distribution:")
                for broker, count in broker_counts.items():
                    print(f"  Broker {broker}: {count:,} trades")
        
        # Check what the original SpreadViewer data should look like
        print(f"\n📈 EXPECTED vs ACTUAL DATA STRUCTURE:")
        print("SpreadViewer should produce:")
        print("  - Order book data (bids/asks)")
        print("  - NO trade data for non-trading spreads")
        print("  - action values like 0.0 (for orders)")
        
        print(f"\nBut this file contains:")
        if len(real_data) > 0:
            action_counts = real_data['action'].value_counts()
            print("  Action value distribution:")
            for action, count in action_counts.items():
                action_type = "TRADE" if action in [1.0, -1.0] else "ORDER"
                print(f"    {action}: {count:,} records ({action_type})")
    
    else:
        print(f"✅ Good: No 'real' file exists for this spread")
    
    # Check the synthetic file to see if it's being corrupted
    synthetic_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_test_synthetic.parquet'
    
    if Path(synthetic_file).exists():
        print(f"\n📊 ANALYZING SYNTHETIC FILE:")
        print(f"File: {synthetic_file}")
        
        synthetic_data = pd.read_parquet(synthetic_file)
        synthetic_data.index = pd.to_datetime(synthetic_data.index)
        
        print(f"Total records: {len(synthetic_data):,}")
        
        # Check if synthetic file has trades when it shouldn't
        synth_trades = synthetic_data[synthetic_data['action'].isin([1.0, -1.0])] if len(synthetic_data) > 0 else pd.DataFrame()
        synth_orders = synthetic_data[~synthetic_data['action'].isin([1.0, -1.0])] if len(synthetic_data) > 0 else pd.DataFrame()
        
        print(f"Synthetic trades: {len(synth_trades):,}")
        print(f"Synthetic orders: {len(synth_orders):,}")
        
        if len(synth_trades) > 0:
            print(f"\n🚨 PROBLEM: Synthetic file contains TRADES!")
            print("SpreadViewer should only generate order book data for non-trading spreads")
            
            # Sample synthetic trade data
            sample_synth_trades = synth_trades.head(5)
            print(f"\nSample synthetic trades:")
            for idx, row in sample_synth_trades.iterrows():
                print(f"  {idx}: price={row['price']:.3f}, action={row['action']}, "
                      f"broker={row.get('broker_id', 'N/A')}, trade_id={row.get('tradeid', 'N/A')}")
        
        if len(synth_orders) > 0:
            print(f"\n✅ Synthetic orders (expected):")
            print(f"Sample order actions: {synth_orders['action'].value_counts().head()}")
    
    else:
        print(f"❌ No synthetic file found")
    
    # Check where the transformation is happening
    print(f"\n🔍 TRACING THE TRANSFORMATION PIPELINE:")
    print("=" * 45)
    
    print("Expected flow:")
    print("1. SpreadViewer generates synthetic order book data")
    print("2. DataFetcher finds NO real spread data (correct)")
    print("3. Merger combines: empty real + synthetic orders = synthetic orders only")
    print("4. NO trades should exist in final dataset")
    
    print(f"\nActual problematic flow:")
    print("1. SpreadViewer somehow generates 'trades' from order book?")
    print("2. OR: transform_trades_to_target_format creates fake trades?")
    print("3. These fake trades get broker_id 9999 and trade IDs like 'synth_buy_*'")
    print("4. They end up in the merged dataset as 'real' trades")
    
    # The root cause investigation
    print(f"\n🎯 ROOT CAUSE INVESTIGATION:")
    print("=" * 30)
    print("QUESTION: Where in the pipeline are order book prices")
    print("          being converted to fake 'trades'?")
    print("")
    print("SUSPECTS:")
    print("1. SpreadViewer itself generating trades (incorrect)")
    print("2. transform_trades_to_target_format() creating trades from orders")
    print("3. Some other transformation step")
    
    return True

def main():
    """Main function"""
    investigate_synthetic_trade_generation()

if __name__ == "__main__":
    main()