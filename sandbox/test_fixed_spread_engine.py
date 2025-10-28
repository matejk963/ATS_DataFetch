#!/usr/bin/env python3
"""
Test Fixed Spread Engine
=========================

Generate spread with the new conservative trading period logic.
"""

import sys
import os

# Add project paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

from engines.spread_fetch_engine import fetch_all_spreads

def test_fixed_spread():
    """Test generating a spread with the fixed logic"""
    
    print("🧪 TESTING FIXED SPREAD ENGINE")
    print("=" * 40)
    
    # Test with DEBM5_25 vs FRBQ2_25 (the problematic combination)
    contracts = ['debm5_25', 'frbq2_25']
    
    try:
        results = fetch_all_spreads(
            contracts=contracts,
            period=None,  # Use automatic trading periods
            trading_months_back=2,
            use_real_spreads=False,  # Use synthetic only 
            use_synthetic_spreads=True,
            save_results=False,  # Don't save, just test
            test_mode=True  # Enable test mode
        )
        
        print(f"\n✅ SUCCESS: Generated {len(results)} spread combinations")
        
        for combination_name, data in results.items():
            if 'spread_trades' in data and not data['spread_trades'].empty:
                trades = data['spread_trades']
                print(f"\n📊 {combination_name}:")
                print(f"   Trade records: {len(trades)}")
                print(f"   Date range: {trades.index.min()} to {trades.index.max()}")
                print(f"   Price range: {trades['price'].min():.3f} to {trades['price'].max():.3f}")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_fixed_spread()