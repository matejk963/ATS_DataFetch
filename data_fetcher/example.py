"""
Data Fetcher Module - Example Usage
===================================

Example script demonstrating how to use the modular data fetcher.
"""

import sys
import os

# Add current directory to path for imports
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

from data_fetcher import DataFetchOrchestrator


def main():
    """Example usage of the DataFetchOrchestrator"""
    
    print("🚀 Data Fetcher Module - Example Usage")
    print("=" * 50)
    
    # Initialize orchestrator
    try:
        orchestrator = DataFetchOrchestrator()
        print("✅ DataFetchOrchestrator initialized successfully")
    except RuntimeError as e:
        print(f"❌ Failed to initialize orchestrator: {e}")
        return False
    
    # Example configuration - spread between German and French Q4 2025
    config = {
        'contracts': ['debq4_25', 'frbq4_25'],
        'coefficients': [1, -1],
        'period': {
            'start_date': '2025-06-24',
            'end_date': '2025-07-01'
        },
        'options': {
            'include_real_spread': True,      # DataFetcher real spread
            'include_synthetic_spread': True, # SpreadViewer synthetic spread
            'include_individual_legs': False   # Individual contract data
        },
        'n_s': 3,  # Business day transition parameter
        'test_mode': True  # Save all formats for testing
    }
    
    print(f"📋 Configuration:")
    print(f"   Contracts: {config['contracts']}")
    print(f"   Mode: {'Single Leg' if len(config['contracts']) == 1 else 'Spread'}")
    print(f"   Period: {config['period']['start_date']} to {config['period']['end_date']}")
    print(f"   Options: {config['options']}")
    print(f"   n_s: {config['n_s']}")
    
    try:
        print(f"\n🔄 Starting integrated fetch...")
        results = orchestrator.integrated_fetch(config)
        
        print(f"\n✅ Integration completed successfully!")
        print(f"📊 Results summary:")
        
        metadata = results.get('metadata', {})
        print(f"   Mode: {metadata.get('mode', 'unknown')}")
        print(f"   Contracts processed: {len(metadata.get('parsed_contracts', []))}")
        
        # Real spread results
        if 'real_spread_data' in results:
            real_data = results['real_spread_data']
            if 'unified_spread_data' in real_data:
                print(f"   🎯 Real spread: {len(real_data['unified_spread_data'])} total records (unified)")
            else:
                print(f"   🎯 Real spread: {len(real_data.get('spread_orders', []))} orders, {len(real_data.get('spread_trades', []))} trades")
        
        # Synthetic spread results
        if 'synthetic_spread_data' in results:
            synth_data = results['synthetic_spread_data']
            if 'unified_spread_data' in synth_data:
                print(f"   🔧 Synthetic spread: {len(synth_data['unified_spread_data'])} total records (unified)")
                print(f"       Method: {synth_data.get('method', 'unknown')}")
            else:
                print(f"   🔧 Synthetic spread: {len(synth_data.get('spread_orders', []))} orders, {len(synth_data.get('spread_trades', []))} trades")
        
        # Merged results
        if 'merged_spread_data' in results:
            merged_data = results['merged_spread_data']
            if 'unified_spread_data' in merged_data:
                print(f"   🎉 Merged spread: {len(merged_data['unified_spread_data'])} total records (unified)")
                print(f"       Method: {merged_data.get('method', 'unknown')}")
            else:
                print(f"   🎉 Merged spread: {len(merged_data.get('spread_orders', []))} orders, {len(merged_data.get('spread_trades', []))} trades")
            
            if 'source_stats' in merged_data:
                stats = merged_data['source_stats']
                print(f"       Source breakdown: Real({stats['real_trades']}+{stats['real_orders']}) + Synthetic({stats['synthetic_trades']}+{stats['synthetic_orders']})")
        
        print(f"\n🎉 MODULAR DATA FETCHER COMPLETED SUCCESSFULLY!")
        return True
        
    except Exception as e:
        print(f"❌ Integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    print("=" * 50)
    if not success:
        print("\n💥 EXAMPLE FAILED!")
        sys.exit(1)