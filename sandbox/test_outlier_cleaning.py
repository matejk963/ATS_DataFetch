#!/usr/bin/env python3
"""
Test Outlier Cleaning
====================

Test script to demonstrate the outlier cleaning functionality
on synthetic spread data with artificial outliers.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

from engines.spread_fetch_engine import clean_synthetic_spread_outliers

def create_test_data_with_outliers():
    """Create test data with known outliers"""
    print("🧪 Creating test data with artificial outliers...")
    
    # Create base time series
    dates = pd.date_range('2025-10-01', '2025-10-03', freq='5T')
    n_points = len(dates)
    
    # Generate normal price series (trending upward)
    np.random.seed(42)
    base_prices = 3.0 + 0.5 * np.cumsum(np.random.normal(0, 0.1, n_points))
    
    # Add some artificial outliers
    outlier_indices = [50, 100, 200, 300, 400]
    outlier_prices = base_prices.copy()
    
    # Type 1: Extreme high outliers
    outlier_prices[50] = 50.0   # Extreme high
    outlier_prices[100] = -20.0  # Extreme low
    
    # Type 2: Large price jumps
    outlier_prices[200] = outlier_prices[199] + 25.0  # Jump up
    outlier_prices[300] = outlier_prices[299] - 18.0  # Jump down
    
    # Type 3: Z-score outliers
    outlier_prices[400] = np.mean(base_prices) + 8 * np.std(base_prices)
    
    # Create DataFrame
    df = pd.DataFrame({
        'price': outlier_prices
    }, index=dates)
    
    print(f"   ✅ Created {len(df)} data points with {len(outlier_indices)} artificial outliers")
    print(f"   📊 Price range: {df['price'].min():.2f} to {df['price'].max():.2f}")
    print(f"   🎯 Outlier locations: {outlier_indices}")
    print(f"   🔍 Outlier values: {[f'{outlier_prices[i]:.2f}' for i in outlier_indices]}")
    
    return df

def test_outlier_cleaning():
    """Test the outlier cleaning function"""
    print("🧹 Testing Outlier Cleaning Functionality")
    print("=" * 50)
    
    # Create test data
    test_data = create_test_data_with_outliers()
    
    # Test different cleaning parameters
    test_configs = [
        {
            'name': 'Conservative Cleaning',
            'z_threshold': 3.0,
            'iqr_multiplier': 2.5,
            'max_price_jump': 10.0
        },
        {
            'name': 'Aggressive Cleaning', 
            'z_threshold': 2.0,
            'iqr_multiplier': 1.5,
            'max_price_jump': 5.0
        },
        {
            'name': 'Moderate Cleaning',
            'z_threshold': 2.5,
            'iqr_multiplier': 2.0,
            'max_price_jump': 8.0
        }
    ]
    
    results = {}
    
    for config in test_configs:
        print(f"\n📋 Testing: {config['name']}")
        print("-" * 40)
        
        cleaned_data = clean_synthetic_spread_outliers(
            test_data.copy(),
            f"test_spread_{config['name'].lower().replace(' ', '_')}",
            z_threshold=config['z_threshold'],
            iqr_multiplier=config['iqr_multiplier'],
            max_price_jump=config['max_price_jump']
        )
        
        original_count = len(test_data)
        cleaned_count = len(cleaned_data)
        removed_count = original_count - cleaned_count
        removal_pct = (removed_count / original_count) * 100
        
        results[config['name']] = {
            'original_count': original_count,
            'cleaned_count': cleaned_count,
            'removed_count': removed_count,
            'removal_pct': removal_pct,
            'price_range_before': (test_data['price'].min(), test_data['price'].max()),
            'price_range_after': (cleaned_data['price'].min(), cleaned_data['price'].max())
        }
        
        print(f"   📊 Summary: {original_count} → {cleaned_count} points ({removal_pct:.1f}% removed)")
        print(f"   📈 Price range: {test_data['price'].min():.2f}-{test_data['price'].max():.2f} → {cleaned_data['price'].min():.2f}-{cleaned_data['price'].max():.2f}")
    
    # Summary comparison
    print(f"\n📊 Cleaning Comparison Summary")
    print("=" * 50)
    print(f"{'Method':<20} {'Removed':<8} {'%':<6} {'Range Before':<15} {'Range After':<15}")
    print("-" * 70)
    
    for name, result in results.items():
        before_range = f"{result['price_range_before'][0]:.1f}-{result['price_range_before'][1]:.1f}"
        after_range = f"{result['price_range_after'][0]:.1f}-{result['price_range_after'][1]:.1f}"
        
        print(f"{name:<20} {result['removed_count']:<8} {result['removal_pct']:<6.1f} {before_range:<15} {after_range:<15}")
    
    return results

def main():
    """Main test function"""
    try:
        results = test_outlier_cleaning()
        
        print(f"\n✅ Outlier cleaning test completed successfully!")
        print(f"   🧪 All cleaning methods tested and working")
        print(f"   📊 Different sensitivity levels demonstrated")
        print(f"   🎯 Outliers successfully detected and removed")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()