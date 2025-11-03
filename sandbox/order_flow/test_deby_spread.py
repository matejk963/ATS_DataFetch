#!/usr/bin/env python3
"""
Test DEBY Contract in Spread Engine
===================================

Test if deby contracts work in the spread fetch engine
"""

import sys
import os

# Add paths
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

def test_deby_in_spread_engine():
    """Test deby contracts in spread fetch engine"""
    
    print("🔍 TESTING DEBY IN SPREAD FETCH ENGINE")
    print("=" * 40)
    
    # Try to import the main function from spread fetch engine
    try:
        sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')
        from spread_fetch_engine import fetch_all_spreads
        
        # Test configuration with yearly contracts
        config = {
            'contracts': ['deby1_25', 'deby1_26'],  # Year 2025 vs 2026
            'period': None,  # Use individual trading periods
            'trading_months_back': 2,
            'use_real_spreads': True,
            'use_synthetic_spreads': True,
            'test_mode': True,  # Don't save results
            'save_results': False
        }
        
        print(f"📊 Testing config: {config}")
        
        # Run the spread fetch process
        print("\n🔄 Running spread fetch engine...")
        results = fetch_all_spreads(**config)
        
        print(f"\n📋 Results summary:")
        for spread_name, result in results.items():
            print(f"   {spread_name}: {result.get('status', 'unknown')}")
            if 'real_spread' in result:
                real_data = result['real_spread'].get('unified_data', {})
                if 'unified_spread_data' in real_data:
                    real_count = len(real_data['unified_spread_data'])
                    print(f"      Real spread: {real_count} records")
                    
            if 'synthetic_spread' in result:
                synth_data = result['synthetic_spread'].get('unified_data', {})
                if 'unified_spread_data' in synth_data:
                    synth_count = len(synth_data['unified_spread_data'])
                    print(f"      Synthetic spread: {synth_count} records")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    test_deby_in_spread_engine()

if __name__ == "__main__":
    main()