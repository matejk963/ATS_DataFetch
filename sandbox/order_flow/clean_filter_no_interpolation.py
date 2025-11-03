#!/usr/bin/env python3
"""
Clean Filter - No Interpolation
==============================

Filter outliers by setting them to NaN, no interpolation
Keep the time series with natural gaps
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import sys

def clean_filter_no_interpolation():
    """Filter outliers without interpolation - just set to NaN"""
    
    print("🔧 CLEAN FILTERING - NO INTERPOLATION")
    print("=" * 50)
    
    # Load the original spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq2_25_frbq2_25_tr_ba_data.parquet"
    
    try:
        df = pd.read_parquet(spread_file)
        print(f"📊 Original data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        
        price_col = 'price'
        print(f"💰 Original price range: {df[price_col].min():.2f} to {df[price_col].max():.2f}")
        
        # Count existing NaNs
        original_nans = df[price_col].isna().sum()
        print(f"📊 Original NaN values: {original_nans}")
        
        # Create a copy for filtering
        df_clean = df.copy()
        
        # Calculate price changes
        df_clean['price_change'] = df_clean[price_col].diff()
        
        # Method 1: Simple threshold based on overall statistics
        price_std = df_clean[price_col].std()
        price_mean = df_clean[price_col].mean()
        
        # Define outlier thresholds
        # Use 3 standard deviations as a reasonable threshold
        lower_bound = price_mean - 3 * price_std
        upper_bound = price_mean + 3 * price_std
        
        print(f"\n🎯 OUTLIER DETECTION:")
        print(f"   Price mean: {price_mean:.2f}")
        print(f"   Price std: {price_std:.2f}")
        print(f"   Bounds: {lower_bound:.2f} to {upper_bound:.2f}")
        
        # Method 2: Also check for extreme price changes
        change_std = df_clean['price_change'].std()
        change_mean = df_clean['price_change'].mean()
        
        # Extreme change threshold - 5 std of price changes
        change_threshold = 5 * change_std
        
        print(f"   Change std: {change_std:.3f}")
        print(f"   Change threshold: ±{change_threshold:.2f}")
        
        # Identify outliers
        price_outliers = (df_clean[price_col] < lower_bound) | (df_clean[price_col] > upper_bound)
        change_outliers = abs(df_clean['price_change']) > change_threshold
        
        # Combine both criteria
        outliers = price_outliers | change_outliers
        
        print(f"\n⚠️  OUTLIERS FOUND:")
        print(f"   Price outliers: {price_outliers.sum()}")
        print(f"   Change outliers: {change_outliers.sum()}")
        print(f"   Total outliers: {outliers.sum()}")
        
        # Show the outliers
        outlier_data = df_clean[outliers]
        if not outlier_data.empty:
            print(f"\n📋 OUTLIER DETAILS:")
            for idx, row in outlier_data.iterrows():
                price = row[price_col]
                change = row.get('price_change', 0)
                print(f"   {idx}: price={price:.2f}, change={change:+.2f}")
        
        # REMOVE OUTLIERS - SET TO NaN (NO INTERPOLATION)
        df_clean.loc[outliers, price_col] = np.nan
        
        # Count final NaNs
        final_nans = df_clean[price_col].isna().sum()
        
        print(f"\n📈 CLEANING RESULTS:")
        print(f"   Original NaN values: {original_nans}")
        print(f"   Added NaN values: {final_nans - original_nans}")
        print(f"   Total NaN values: {final_nans}")
        print(f"   Valid data points: {len(df_clean) - final_nans}")
        print(f"   Data retention: {(len(df_clean) - final_nans) / len(df_clean) * 100:.2f}%")
        
        # Statistics on clean data (excluding NaNs)
        clean_prices = df_clean[price_col].dropna()
        
        print(f"\n📈 CLEAN DATA STATISTICS:")
        print(f"   Mean: {clean_prices.mean():.2f}")
        print(f"   Std:  {clean_prices.std():.2f}")
        print(f"   Min:  {clean_prices.min():.2f}")
        print(f"   Max:  {clean_prices.max():.2f}")
        
        # Calculate clean price changes
        df_clean['price_change_clean'] = df_clean[price_col].diff()
        clean_changes = df_clean['price_change_clean'].dropna()
        
        print(f"\n📈 CLEAN PRICE CHANGES:")
        print(f"   Max change: {clean_changes.max():.2f}")
        print(f"   Min change: {clean_changes.min():.2f}")
        print(f"   Std change: {clean_changes.std():.3f}")
        
        # Plot comparison
        plt.figure(figsize=(16, 12))
        
        # Original data
        plt.subplot(3, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=0.6, alpha=0.8, color='blue', label='Original')
        if not outlier_data.empty:
            plt.scatter(outlier_data.index, outlier_data[price_col], 
                       color='red', s=20, alpha=0.8, zorder=5, label='Outliers to Remove')
        plt.title('Original Data with Outliers Marked', fontsize=14, fontweight='bold')
        plt.ylabel('Price', fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Clean data (with gaps)
        plt.subplot(3, 1, 2)
        plt.plot(df_clean.index, df_clean[price_col], linewidth=0.8, alpha=0.9, color='green', label='Clean (with gaps)')
        plt.title('Clean Data - Outliers Removed (Natural Gaps)', fontsize=14, fontweight='bold')
        plt.ylabel('Price', fontsize=12)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Price changes comparison
        plt.subplot(3, 1, 3)
        plt.plot(df.index[1:], df[price_col].diff()[1:], linewidth=0.5, alpha=0.6, 
                color='blue', label='Original Changes')
        plt.plot(df_clean.index[1:], df_clean['price_change_clean'][1:], linewidth=0.6, alpha=0.8,
                color='green', label='Clean Changes')
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        plt.title('Price Changes Comparison', fontsize=12, fontweight='bold')
        plt.ylabel('Price Change', fontsize=10)
        plt.xlabel('Time', fontsize=10)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save plots
        save_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/clean_no_interpolation.png'
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"\n📊 Clean plot saved: {save_path}")
        
        # Save clean data (with NaNs, no interpolation)
        output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/debq2_25_frbq2_25_clean_gaps.parquet"
        
        # Keep only essential columns for the clean dataset
        df_output = df_clean[[price_col, 'volume', 'action', 'broker_id']].copy()
        df_output.to_parquet(output_file)
        print(f"💾 Clean data saved: {output_file}")
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    clean_filter_no_interpolation()

if __name__ == "__main__":
    main()