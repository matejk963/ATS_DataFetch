#!/usr/bin/env python3
"""
Filter Price Outliers
====================

Filter out price changes higher than 5 times hourly standard deviation
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import sys

def filter_price_outliers():
    """Filter price outliers based on hourly standard deviation"""
    
    print("🔧 FILTERING PRICE OUTLIERS")
    print("=" * 40)
    
    # Load the spread data
    spread_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq2_25_frbq2_25_tr_ba_data.parquet"
    
    try:
        df = pd.read_parquet(spread_file)
        print(f"📊 Original data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        
        price_col = 'price'
        print(f"💰 Price range: {df[price_col].min():.2f} to {df[price_col].max():.2f}")
        
        # Create a copy for filtering
        df_filtered = df.copy()
        
        # Calculate price changes
        df_filtered['price_change'] = df_filtered[price_col].diff()
        
        # Resample to hourly and calculate rolling standard deviation
        df_hourly = df_filtered.resample('1H')[price_col].agg(['mean', 'std', 'count']).fillna(method='ffill')
        
        # Calculate rolling hourly std (24-hour window)
        df_hourly['rolling_std'] = df_hourly['std'].rolling(window=24, min_periods=1).mean()
        
        # Merge back to original timeframe
        df_filtered = df_filtered.join(df_hourly[['rolling_std']], how='left')
        df_filtered['rolling_std'] = df_filtered['rolling_std'].fillna(method='ffill')
        
        # Define outlier threshold: 5 times hourly rolling std
        threshold_multiplier = 5
        df_filtered['outlier_threshold'] = threshold_multiplier * df_filtered['rolling_std']
        
        # Identify outliers
        df_filtered['is_outlier'] = (
            abs(df_filtered['price_change']) > df_filtered['outlier_threshold']
        )
        
        outliers = df_filtered[df_filtered['is_outlier']]
        print(f"\n⚠️  Found {len(outliers)} outliers:")
        
        if not outliers.empty:
            for idx, row in outliers.iterrows():
                change = row['price_change']
                threshold = row['outlier_threshold']
                std_val = row['rolling_std']
                print(f"   {idx}: change={change:+.2f}, threshold=±{threshold:.2f} (std={std_val:.3f})")
        
        # Filter out outliers by interpolating
        df_clean = df_filtered.copy()
        
        # For outliers, interpolate the price from surrounding values
        outlier_indices = df_clean['is_outlier']
        if outlier_indices.any():
            print(f"\n🔧 Interpolating {outlier_indices.sum()} outlier values...")
            
            # Set outlier prices to NaN and interpolate
            df_clean.loc[outlier_indices, price_col] = np.nan
            df_clean[price_col] = df_clean[price_col].interpolate(method='time', limit_direction='both')
            
            # Recalculate price changes after filtering
            df_clean['price_change_filtered'] = df_clean[price_col].diff()
        
        # Statistics
        print(f"\n📈 FILTERING RESULTS:")
        print(f"   Original records: {len(df)}")
        print(f"   Outliers found: {len(outliers)}")
        print(f"   Outlier percentage: {len(outliers)/len(df)*100:.2f}%")
        
        print(f"\n📈 PRICE STATISTICS (before/after):")
        print(f"   Mean: {df[price_col].mean():.2f} → {df_clean[price_col].mean():.2f}")
        print(f"   Std:  {df[price_col].std():.2f} → {df_clean[price_col].std():.2f}")
        print(f"   Min:  {df[price_col].min():.2f} → {df_clean[price_col].min():.2f}")
        print(f"   Max:  {df[price_col].max():.2f} → {df_clean[price_col].max():.2f}")
        
        # Plot comparison
        plt.figure(figsize=(15, 12))
        
        # Original data
        plt.subplot(3, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=0.6, alpha=0.8, color='blue', label='Original')
        if not outliers.empty:
            plt.scatter(outliers.index, outliers[price_col], 
                       color='red', s=30, alpha=0.8, zorder=5, label='Outliers')
        plt.title('Original Data with Outliers', fontsize=12, fontweight='bold')
        plt.ylabel('Price', fontsize=10)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Filtered data
        plt.subplot(3, 1, 2)
        plt.plot(df_clean.index, df_clean[price_col], linewidth=0.6, alpha=0.8, color='green', label='Filtered')
        plt.title('Filtered Data (Outliers Interpolated)', fontsize=12, fontweight='bold')
        plt.ylabel('Price', fontsize=10)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Price changes comparison
        plt.subplot(3, 1, 3)
        plt.plot(df.index[1:], df[price_col].diff()[1:], linewidth=0.5, alpha=0.7, 
                color='blue', label='Original Changes')
        plt.plot(df_clean.index[1:], df_clean['price_change_filtered'][1:], linewidth=0.5, alpha=0.7,
                color='green', label='Filtered Changes')
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        plt.title('Price Changes Comparison', fontsize=12, fontweight='bold')
        plt.ylabel('Price Change', fontsize=10)
        plt.xlabel('Time', fontsize=10)
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save plots
        save_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/filtered_spread_price.png'
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"\n📊 Filtered plot saved: {save_path}")
        
        # Save filtered data
        output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/debq2_25_frbq2_25_filtered.parquet"
        
        # Keep only essential columns for the filtered dataset
        df_output = df_clean[[price_col, 'volume', 'action', 'broker_id']].copy()
        df_output.to_parquet(output_file)
        print(f"💾 Filtered data saved: {output_file}")
        
        return df_clean
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    filter_price_outliers()

if __name__ == "__main__":
    main()