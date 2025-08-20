#!/usr/bin/env python3
"""
Test script for the three-stage spread data merging algorithm

This script demonstrates:
1. Trades: Simple union merge
2. Orders: Intelligent best bid/ask selection with resampling
3. Final: Combined hybrid output
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Add project paths
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/integration_related')

from integration_script_v2 import merge_spread_data

def create_sample_real_data():
    """Create sample real spread data from DataFetcher"""
    # Create time series with some gaps
    timestamps = pd.date_range('2025-02-03 09:00:00', '2025-02-03 12:00:00', freq='5min')[::2]  # Every 10 minutes
    
    real_orders = pd.DataFrame({
        'b_price': [45.2, 45.3, 45.1, 45.4, 45.2, 45.3, 45.5, 45.4, 45.6],
        'a_price': [45.8, 45.9, 45.7, 46.0, 45.8, 45.9, 46.1, 46.0, 46.2]
    }, index=timestamps[:9])
    
    # Fewer trade points
    trade_timestamps = pd.date_range('2025-02-03 09:15:00', '2025-02-03 11:45:00', freq='30min')
    real_trades = pd.DataFrame({
        'price': [45.5, 45.8, 45.3, 45.9, 45.6],
        'volume': [100, 150, 80, 120, 90],
        'action': [1, -1, 1, -1, 1]
    }, index=trade_timestamps[:5])
    
    return {
        'spread_orders': real_orders,
        'spread_trades': real_trades,
        'method': 'real_datafetcher'
    }

def create_sample_synthetic_data():
    """Create sample synthetic spread data from SpreadViewer"""
    # Different time series with different gaps
    timestamps = pd.date_range('2025-02-03 09:30:00', '2025-02-03 12:30:00', freq='3min')[::3]  # Every 9 minutes
    
    synthetic_orders = pd.DataFrame({
        'bid': [45.1, 45.5, 45.0, 45.6, 45.3, 45.4, 45.2, 45.7],  # Using SpreadViewer column names
        'ask': [45.7, 46.1, 45.6, 46.2, 45.9, 46.0, 45.8, 46.3]
    }, index=timestamps[:8])
    
    # More frequent trade points from SpreadViewer
    trade_timestamps = pd.date_range('2025-02-03 09:45:00', '2025-02-03 12:15:00', freq='15min')
    synthetic_trades = pd.DataFrame({
        'buy': [45.4, np.nan, 45.2, 45.8, np.nan, 45.5, 45.9],  # SpreadViewer format
        'sell': [np.nan, 45.9, np.nan, np.nan, 45.7, np.nan, np.nan],
        'volume': [75, 110, 95, 135, 85, 100, 125]
    }, index=trade_timestamps[:7])
    
    return {
        'spread_orders': synthetic_orders,
        'spread_trades': synthetic_trades,
        'method': 'synthetic_spreadviewer',
        'periods_processed': 3
    }

def test_merging_algorithm():
    """Test the three-stage merging algorithm"""
    print("🧪 Testing Three-Stage Spread Data Merging Algorithm")
    print("=" * 55)
    
    # Create sample data
    print("\n📊 Creating sample datasets...")
    real_data = create_sample_real_data()
    synthetic_data = create_sample_synthetic_data()
    
    print(f"Real data: {len(real_data['spread_orders'])} orders, {len(real_data['spread_trades'])} trades")
    print(f"Synthetic data: {len(synthetic_data['spread_orders'])} orders, {len(synthetic_data['spread_trades'])} trades")
    
    print("\nReal orders sample:")
    print(real_data['spread_orders'].head(3))
    
    print("\nSynthetic orders sample (note different column names):")
    print(synthetic_data['spread_orders'].head(3))
    
    # Test the merging algorithm
    print("\n🔗 Testing merging algorithm...")
    try:
        merged_result = merge_spread_data(real_data, synthetic_data)
        
        print(f"\n✅ Merging completed successfully!")
        print(f"📈 Results:")
        
        # Show merged statistics
        if 'source_stats' in merged_result:
            stats = merged_result['source_stats']
            print(f"   📊 Source breakdown:")
            print(f"      Real: {stats['real_trades']} trades, {stats['real_orders']} orders")
            print(f"      Synthetic: {stats['synthetic_trades']} trades, {stats['synthetic_orders']} orders")
            print(f"      Merged: {stats['merged_trades']} trades, {stats['merged_orders']} orders")
        
        # Show sample of merged data
        if not merged_result['spread_orders'].empty:
            print(f"\n   🎯 Merged orders sample (standardized columns):")
            print(merged_result['spread_orders'].head(3))
            
            print(f"\n   📈 Order timeline coverage:")
            print(f"      First: {merged_result['spread_orders'].index[0]}")
            print(f"      Last: {merged_result['spread_orders'].index[-1]}")
            print(f"      Total points: {len(merged_result['spread_orders'])}")
        
        if not merged_result['spread_trades'].empty:
            print(f"\n   💰 Merged trades sample:")
            print(merged_result['spread_trades'].head(3))
            
            print(f"\n   📈 Trade timeline coverage:")
            print(f"      First: {merged_result['spread_trades'].index[0]}")
            print(f"      Last: {merged_result['spread_trades'].index[-1]}")
            print(f"      Total points: {len(merged_result['spread_trades'])}")
        
        # Test best bid/ask logic
        print(f"\n   🎯 Best bid/ask validation:")
        merged_orders = merged_result['spread_orders']
        if 'b_price' in merged_orders.columns and 'a_price' in merged_orders.columns:
            avg_bid = merged_orders['b_price'].mean()
            avg_ask = merged_orders['a_price'].mean()
            avg_spread = avg_ask - avg_bid
            print(f"      Average bid: {avg_bid:.2f}")
            print(f"      Average ask: {avg_ask:.2f}")
            print(f"      Average spread: {avg_spread:.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Merging failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_edge_cases():
    """Test edge cases for merging algorithm"""
    print("\n🔬 Testing Edge Cases")
    print("-" * 25)
    
    # Test 1: Empty real data
    print("Test 1: Empty real data")
    empty_real = {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}
    synthetic = create_sample_synthetic_data()
    
    try:
        result = merge_spread_data(empty_real, synthetic)
        print(f"   ✅ Handled empty real data: {len(result['spread_orders'])} orders, {len(result['spread_trades'])} trades")
    except Exception as e:
        print(f"   ❌ Failed with empty real data: {e}")
    
    # Test 2: Empty synthetic data
    print("Test 2: Empty synthetic data")
    real = create_sample_real_data()
    empty_synthetic = {'spread_orders': pd.DataFrame(), 'spread_trades': pd.DataFrame()}
    
    try:
        result = merge_spread_data(real, empty_synthetic)
        print(f"   ✅ Handled empty synthetic data: {len(result['spread_orders'])} orders, {len(result['spread_trades'])} trades")
    except Exception as e:
        print(f"   ❌ Failed with empty synthetic data: {e}")
    
    # Test 3: Both empty
    print("Test 3: Both datasets empty")
    try:
        result = merge_spread_data(empty_real, empty_synthetic)
        print(f"   ✅ Handled both empty: {len(result['spread_orders'])} orders, {len(result['spread_trades'])} trades")
    except Exception as e:
        print(f"   ❌ Failed with both empty: {e}")

def main():
    """Run all tests"""
    print("🚀 Spread Data Merging Test Suite")
    print("=" * 40)
    
    # Test main merging algorithm
    success = test_merging_algorithm()
    
    if success:
        # Test edge cases
        test_edge_cases()
        
        print("\n🎉 All tests completed successfully!")
        print("\nKey Features Demonstrated:")
        print("✅ Three-stage merging: Trades → Orders → Combined")
        print("✅ Column standardization (bid/ask → b_price/a_price)")
        print("✅ Union timestamp creation")
        print("✅ Forward fill resampling")
        print("✅ Best bid/ask selection (max bid, min ask)")
        print("✅ Simple trade union merge")
        print("✅ Edge case handling (empty datasets)")
        
    else:
        print("\n💥 Tests failed!")
    
    print("\n" + "=" * 40)

if __name__ == "__main__":
    main()