"""
Example: Using Isolated Data Fetch Engine
========================================

This example demonstrates how to use the isolated data fetch engine with
the exact same input format as the original engines/data_fetch_engine.py.

The isolated engine provides identical functionality but can be moved
outside the project structure while maintaining full compatibility.
"""

import sys
import os
from datetime import datetime

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Import the isolated engine
from data_fetcher.data_fetch_engine import integrated_fetch


def example_single_contract():
    """Example: Single contract data fetching"""
    print("=" * 60)
    print("📈 EXAMPLE 1: Single Contract Fetch")
    print("=" * 60)
    
    # Configuration for single contract (same format as original)
    config = {
        'contracts': ['debq4_25'],  # German base Q4 2025
        'period': {
            'start_date': '2025-06-01',
            'end_date': '2025-07-01'
        },
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': False,
            'include_individual_legs': False
        },
        'n_s': 3,
        'test_mode': False
    }
    
    print("📋 Configuration:")
    print(f"   Contract: {config['contracts'][0]}")
    print(f"   Period: {config['period']['start_date']} to {config['period']['end_date']}")
    print(f"   Mode: Single Leg")
    
    try:
        results = integrated_fetch(config)
        
        print("\n✅ Single contract fetch completed!")
        
        # Display results
        if 'single_leg_data' in results:
            single_data = results['single_leg_data']
            orders_count = len(single_data.get('orders', []))
            trades_count = len(single_data.get('trades', []))
            print(f"📊 Results: {orders_count} orders, {trades_count} trades")
        
        return results
        
    except Exception as e:
        print(f"❌ Single contract fetch failed: {e}")
        return None


def example_spread_contract():
    """Example: Spread contract data fetching"""
    print("\n" + "=" * 60)
    print("🔄 EXAMPLE 2: Spread Contract Fetch")
    print("=" * 60)
    
    # Configuration for spread contract (same format as original)
    config = {
        'contracts': ['debq4_25', 'frbq4_25'],  # German vs French Q4 2025
        'coefficients': [1, -1],  # Buy German, Sell French
        'period': {
            'start_date': '2025-06-01',
            'end_date': '2025-07-01'
        },
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': True,
            'include_individual_legs': False
        },
        'n_s': 3,  # Business day transition
        'test_mode': False
    }
    
    print("📋 Configuration:")
    print(f"   Contracts: {config['contracts'][0]} vs {config['contracts'][1]}")
    print(f"   Coefficients: {config['coefficients']}")
    print(f"   Period: {config['period']['start_date']} to {config['period']['end_date']}")
    print(f"   Options: {config['options']}")
    print(f"   n_s: {config['n_s']} (business day transition)")
    
    try:
        results = integrated_fetch(config)
        
        print("\n✅ Spread contract fetch completed!")
        
        # Display results summary
        metadata = results.get('metadata', {})
        print(f"📊 Results Summary:")
        print(f"   Mode: {metadata.get('mode', 'unknown')}")
        print(f"   Contracts processed: {len(metadata.get('parsed_contracts', []))}")
        
        # Real spread results
        if 'real_spread_data' in results:
            real_data = results['real_spread_data']
            if 'unified_spread_data' in real_data:
                print(f"   🎯 Real spread: {len(real_data['unified_spread_data'])} total records (unified)")
            else:
                orders_count = len(real_data.get('spread_orders', []))
                trades_count = len(real_data.get('spread_trades', []))
                print(f"   🎯 Real spread: {orders_count} orders, {trades_count} trades")
        
        # Synthetic spread results
        if 'synthetic_spread_data' in results:
            synth_data = results['synthetic_spread_data']
            if 'unified_spread_data' in synth_data:
                print(f"   🔧 Synthetic spread: {len(synth_data['unified_spread_data'])} total records (unified)")
                print(f"       Method: {synth_data.get('method', 'unknown')}")
            else:
                orders_count = len(synth_data.get('spread_orders', []))
                trades_count = len(synth_data.get('spread_trades', []))
                print(f"   🔧 Synthetic spread: {orders_count} orders, {trades_count} trades")
        
        # Merged spread results
        if 'merged_spread_data' in results:
            merged_data = results['merged_spread_data']
            if 'unified_spread_data' in merged_data:
                print(f"   🎉 Merged spread: {len(merged_data['unified_spread_data'])} total records (unified)")
                print(f"       Method: {merged_data.get('method', 'unknown')}")
            
            if 'source_stats' in merged_data:
                stats = merged_data['source_stats']
                print(f"       Source breakdown: Real({stats['real_trades']}+{stats['real_orders']}) + Synthetic({stats['synthetic_trades']}+{stats['synthetic_orders']})")
        
        return results
        
    except Exception as e:
        print(f"❌ Spread contract fetch failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def example_custom_configuration():
    """Example: Custom configuration options"""
    print("\n" + "=" * 60)
    print("⚙️  EXAMPLE 3: Custom Configuration")
    print("=" * 60)
    
    # Custom configuration with all options
    config = {
        'contracts': ['ttfbm07_25', 'nbpbm07_25'],  # TTF vs NBP July 2025 base
        'coefficients': [1, -1],
        'period': {
            'start_date': '2025-06-15',
            'end_date': '2025-06-25'
        },
        'options': {
            'include_real_spread': True,
            'include_synthetic_spread': True,
            'include_individual_legs': True  # Include individual leg data
        },
        'n_s': 5,  # Different n_s value
        'test_mode': True  # Enable test mode for full output
    }
    
    print("📋 Custom Configuration:")
    print(f"   Contracts: {config['contracts'][0]} vs {config['contracts'][1]}")
    print(f"   Coefficients: {config['coefficients']}")
    print(f"   Period: {config['period']['start_date']} to {config['period']['end_date']}")
    print(f"   Include individual legs: {config['options']['include_individual_legs']}")
    print(f"   n_s: {config['n_s']} (custom business day transition)")
    print(f"   Test mode: {config['test_mode']}")
    
    try:
        results = integrated_fetch(config)
        
        print("\n✅ Custom configuration fetch completed!")
        
        # Display detailed results
        metadata = results.get('metadata', {})
        print(f"📊 Detailed Results:")
        
        for i, contract_info in enumerate(metadata.get('parsed_contracts', [])):
            print(f"   Contract {i+1}: {contract_info['original']}")
            print(f"      → Market: {contract_info['market']}, Product: {contract_info['product']}")
            print(f"      → Tenor: {contract_info['tenor']}, Contract: {contract_info['contract']}")
            print(f"      → Delivery: {contract_info['delivery_date']}")
        
        # Individual leg results (if requested)
        for leg_num in [1, 2]:
            leg_key = f'leg_{leg_num}_data'
            if leg_key in results:
                leg_data = results[leg_key]
                orders_count = len(leg_data.get('orders', []))
                trades_count = len(leg_data.get('trades', []))
                print(f"   📋 Leg {leg_num}: {orders_count} orders, {trades_count} trades")
            elif f'leg_{leg_num}_error' in results:
                print(f"   ❌ Leg {leg_num}: {results[f'leg_{leg_num}_error']}")
        
        return results
        
    except Exception as e:
        print(f"❌ Custom configuration fetch failed: {e}")
        return None


def main():
    """Run all examples"""
    print("🚀 Isolated Data Fetch Engine - Usage Examples")
    print("=" * 60)
    print("These examples show the exact same input format as the original engine.")
    print("The isolated engine can be moved outside the project while maintaining")
    print("full compatibility and functionality.")
    
    # Example 1: Single contract
    single_results = example_single_contract()
    
    # Example 2: Spread contract  
    spread_results = example_spread_contract()
    
    # Example 3: Custom configuration
    custom_results = example_custom_configuration()
    
    print("\n" + "=" * 60)
    print("🎉 ALL EXAMPLES COMPLETED!")
    print("=" * 60)
    print("📝 Summary:")
    print("   ✅ Single contract example:", "✅ Success" if single_results else "❌ Failed")
    print("   ✅ Spread contract example:", "✅ Success" if spread_results else "❌ Failed") 
    print("   ✅ Custom configuration example:", "✅ Success" if custom_results else "❌ Failed")
    
    print("\n🔧 USAGE IN YOUR CODE:")
    print("   from data_fetcher.data_fetch_engine import integrated_fetch")
    print("   results = integrated_fetch(config)  # Same config format as original")
    
    print("\n📦 TO MOVE THIS MODULE:")
    print("   1. Copy entire 'data_fetcher' directory to new location")
    print("   2. Update import paths for TPData/SpreadViewer if needed") 
    print("   3. Module works standalone with graceful degradation")
    print("   4. All functionality preserved for external use")


if __name__ == "__main__":
    main()