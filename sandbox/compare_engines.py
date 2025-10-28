#!/usr/bin/env python3
"""
Compare Data Fetch Engine vs Spread Fetch Engine
================================================

Compare the outputs from both engines to see why there's a price shift.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def run_data_fetch_engine():
    """Run the data fetch engine with same configuration"""
    
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    sys.path.insert(0, project_root)
    
    # Import and run data fetch engine
    from engines.data_fetch_engine import process_unified_data_request
    
    config = {
        'contracts': ['debm11_25', 'debq1_26'],
        'period': {
            'start_date': '2025-09-01',
            'end_date': '2025-10-20'
        },
        'options': {
            'use_spreadviewer': True,
            'use_datafetcher': False  # Only synthetic
        },
        'coefficients': [1, -1],
        'n_s': 3
    }
    
    print("🔄 Running data_fetch_engine with synthetic-only config...")
    try:
        results = process_unified_data_request(config)
        print("✅ Data fetch engine completed")
        return results
    except Exception as e:
        print(f"❌ Data fetch engine failed: {e}")
        return None

def compare_results():
    """Compare results from both engines"""
    
    # Check spread_fetch_engine output
    spread_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_synthetic_only_clean_synthetic.parquet'
    
    if Path(spread_file).exists():
        spread_data = pd.read_parquet(spread_file)
        spread_data.index = pd.to_datetime(spread_data.index)
        spread_trades = spread_data[spread_data['action'].isin([1.0, -1.0])]
        
        print(f"\n📊 SPREAD_FETCH_ENGINE RESULTS:")
        print(f"   Total records: {len(spread_data):,}")
        print(f"   Trade records: {len(spread_trades):,}")
        if len(spread_trades) > 0:
            print(f"   Price range: {spread_trades['price'].min():.3f} to {spread_trades['price'].max():.3f}")
            print(f"   Average price: {spread_trades['price'].mean():.3f}")
    else:
        print("❌ No spread_fetch_engine output found")
        spread_trades = None
    
    # Check data_fetch_engine output (typically in different location)
    # Look for any data_fetch_engine outputs
    data_files = list(Path('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData').glob('**/*debm11_25*debq1_26*.parquet'))
    
    print(f"\n📊 DATA_FETCH_ENGINE SEARCH:")
    print(f"   Found files: {len(data_files)}")
    for f in data_files:
        print(f"   - {f}")
    
    # Run data_fetch_engine now
    print(f"\n🔄 Running data_fetch_engine for comparison...")
    data_results = run_data_fetch_engine()
    
    return spread_trades, data_results

if __name__ == "__main__":
    compare_results()