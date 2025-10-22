#!/usr/bin/env python3
"""
Fix Synthetic Trade Generation
==============================

The transform_trades_to_target_format function is incorrectly creating
fake "trades" from SpreadViewer order book data. This fixes the issue
by ensuring synthetic spreads only produce order book data.
"""

import sys
import pandas as pd
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

def analyze_spreadviewer_output():
    """Analyze what SpreadViewer actually produces"""
    
    print("🔍 ANALYZING SPREADVIEWER RAW OUTPUT")
    print("=" * 50)
    
    # Check what the synthetic file originally contained before transformation
    synthetic_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_test_synthetic.parquet'
    
    if Path(synthetic_file).exists():
        synthetic_data = pd.read_parquet(synthetic_file)
        synthetic_data.index = pd.to_datetime(synthetic_data.index)
        
        print(f"Raw synthetic data structure:")
        print(f"  Total records: {len(synthetic_data):,}")
        print(f"  Columns: {list(synthetic_data.columns)}")
        
        # Check action types
        action_counts = synthetic_data['action'].value_counts()
        print(f"\nAction distribution:")
        for action, count in action_counts.items():
            action_type = "TRADE" if action in [1.0, -1.0] else "ORDER"
            print(f"  {action}: {count:,} ({action_type})")
        
        # The problem: SpreadViewer is producing action values 1.0 and -1.0
        # These get interpreted as "trades" by the system
        # But they should be order book mid-prices or spread calculations
        
        fake_trades = synthetic_data[synthetic_data['action'].isin([1.0, -1.0])]
        real_orders = synthetic_data[~synthetic_data['action'].isin([1.0, -1.0])]
        
        print(f"\nFAKE TRADES ANALYSIS:")
        if len(fake_trades) > 0:
            print(f"  Count: {len(fake_trades):,}")
            print(f"  Price range: {fake_trades['price'].min():.3f} to {fake_trades['price'].max():.3f}")
            
            # Sample some records
            sample = fake_trades.head(3)
            print(f"  Sample records:")
            for idx, row in sample.iterrows():
                print(f"    {idx}: price={row['price']:.3f}, action={row['action']}, broker_id={row.get('broker_id', 'N/A')}")
        
        print(f"\nORDER BOOK DATA:")
        if len(real_orders) > 0:
            print(f"  Count: {len(real_orders):,}")
            order_actions = real_orders['action'].value_counts()
            print(f"  Order action types: {dict(order_actions)}")
        
        return True
    else:
        print("❌ No synthetic file found")
        return False

def fix_synthetic_data_interpretation():
    """Propose fix for synthetic data interpretation"""
    
    print(f"\n🔧 PROPOSED FIX:")
    print("=" * 20)
    
    print("PROBLEM:")
    print("  SpreadViewer creates spread price calculations with action=1.0/-1.0")
    print("  These get interpreted as 'trades' by transform_trades_to_target_format()")
    print("  But they're actually calculated spread mid-prices, not real trades")
    
    print(f"\nSOLUTION:")
    print("  1. Modify transform_trades_to_target_format() to NOT create trades")
    print("     from SpreadViewer data when action=1.0/-1.0")
    print("  2. Instead, treat these as mid-price calculations")
    print("  3. Only create order book data (bids/asks) from SpreadViewer")
    
    print(f"\nEXPECTED RESULT:")
    print("  - Synthetic spreads = Order book data only")
    print("  - No fake 'trades' with synth_buy_*/synth_sell_* IDs")
    print("  - Clean synthetic spread prices without trade pollution")

def create_clean_synthetic_config():
    """Create a configuration for clean synthetic-only spreads"""
    
    print(f"\n📋 CLEAN SYNTHETIC-ONLY CONFIGURATION:")
    print("=" * 45)
    
    config = {
        'contracts': ['debm11_25', 'debm12_25', 'debq1_26'],
        'period': {
            'start_date': '2025-09-01',
            'end_date': '2025-10-16'
        },
        'use_real_spreads': False,  # NO real spreads
        'use_synthetic_spreads': True,  # ONLY synthetic
        'coefficients': [1, -1],
        'n_s': 3,
        'save_results': True,
        'test_mode': True,  # Save to test/ directory
        'file_suffix': '_synthetic_only_clean',
        'clean_synthetic_outliers': True
    }
    
    print("This configuration will:")
    print("  ✅ Generate all spread combinations from 3 contracts")
    print("  ✅ Fetch ONLY synthetic spreads (no real data)")
    print("  ✅ Apply outlier cleaning to synthetic data")
    print("  ✅ Save results with '_synthetic_only_clean' suffix")
    print("  ❌ NO fake trades will be created (once we fix the transform)")
    
    return config

def main():
    """Main function"""
    print("🔧 FIXING SYNTHETIC TRADE GENERATION ISSUE")
    print("=" * 60)
    
    # Analyze the current issue
    if analyze_spreadviewer_output():
        fix_synthetic_data_interpretation()
        create_clean_synthetic_config()
        
        print(f"\n🎯 NEXT STEPS:")
        print("1. Fix transform_trades_to_target_format() in data_transformers.py")
        print("2. Run spread_fetch_engine.py with synthetic-only config")
        print("3. Verify no fake trades are created")
        print("4. Get clean synthetic spread order book data only")

if __name__ == "__main__":
    main()