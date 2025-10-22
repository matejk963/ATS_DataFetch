#!/usr/bin/env python3
"""
Plot Corrected Data Comparisons
==============================

Compare before/after outlier cleaning to show the effect of corrections.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('default')
plt.rcParams['figure.figsize'] = [16, 12]

def plot_comparison():
    """Plot comparison of different data sources and cleaning stages"""
    
    print("🎨 Creating corrected data comparison plots...")
    
    # Define file paths
    base_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test"
    
    files = {
        'Real (Original)': f"{base_path}/debm12_25_debq1_26_tr_ba_datatest_merged_clean_real.parquet",
        'Merged (Corrected)': f"{base_path}/debm12_25_debq1_26_tr_ba_datatest_merged_clean_merged.parquet"
    }
    
    # Load data
    data = {}
    for name, filepath in files.items():
        if os.path.exists(filepath):
            df = pd.read_parquet(filepath)
            df.index = pd.to_datetime(df.index)
            data[name] = df
            print(f"   ✅ Loaded {name}: {len(df)} records")
        else:
            print(f"   ⚠️  File not found: {name}")
    
    if not data:
        print("❌ No data files found")
        return
    
    # Create comprehensive comparison plot
    fig, axes = plt.subplots(3, 2, figsize=(16, 12))
    fig.suptitle('Spread Data: Before vs After Outlier Cleaning', fontsize=16, fontweight='bold')
    
    colors = ['blue', 'red', 'green', 'orange']
    
    # Plot 1: Price scatter plots (datetime)
    for i, (name, df) in enumerate(data.items()):
        ax = axes[0, i]
        if len(df) > 0:
            ax.scatter(df.index, df['price'], alpha=0.6, s=15, c=colors[i], label=name)
            ax.set_title(f'{name} - Price Scatter')
            ax.set_ylabel('Price')
            ax.set_xlabel('DateTime')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            # Add statistics
            stats_text = f'Mean: {df["price"].mean():.3f}\\nStd: {df["price"].std():.3f}\\nRange: {df["price"].min():.3f} to {df["price"].max():.3f}'
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                   verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # Plot 2: Price distributions
    for i, (name, df) in enumerate(data.items()):
        ax = axes[1, i]
        if len(df) > 0:
            ax.hist(df['price'], bins=30, alpha=0.7, color=colors[i], edgecolor='black')
            ax.set_title(f'{name} - Price Distribution')
            ax.set_xlabel('Price')
            ax.set_ylabel('Frequency')
            ax.grid(True, alpha=0.3)
            
            # Add vertical lines for mean and std
            mean_price = df['price'].mean()
            std_price = df['price'].std()
            ax.axvline(mean_price, color='red', linestyle='--', alpha=0.8, label=f'Mean: {mean_price:.3f}')
            ax.axvline(mean_price + std_price, color='orange', linestyle=':', alpha=0.6, label=f'+1σ: {mean_price + std_price:.3f}')
            ax.axvline(mean_price - std_price, color='orange', linestyle=':', alpha=0.6, label=f'-1σ: {mean_price - std_price:.3f}')
            ax.legend(fontsize=8)
    
    # Plot 3: Volume analysis (if available)
    for i, (name, df) in enumerate(data.items()):
        ax = axes[2, i]
        if len(df) > 0 and 'volume' in df.columns:
            ax.bar(df.index, df['volume'], alpha=0.7, color=colors[i], width=pd.Timedelta(hours=1))
            ax.set_title(f'{name} - Volume')
            ax.set_xlabel('DateTime')
            ax.set_ylabel('Volume')
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            # Add volume statistics
            vol_stats = f'Total: {df["volume"].sum():.0f}\\nMean: {df["volume"].mean():.2f}\\nMax: {df["volume"].max():.0f}'
            ax.text(0.02, 0.98, vol_stats, transform=ax.transAxes, 
                   verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        else:
            ax.text(0.5, 0.5, 'No volume data', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=12)
            ax.set_title(f'{name} - Volume (N/A)')
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'debm12_25_debq1_26_corrected_comparison.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Comparison plot saved: {plot_path}")
    
    # Print summary statistics
    print(f"\\n📊 Summary Statistics:")
    print(f"{'Source':<20} {'Records':<8} {'Price Range':<20} {'Mean ± Std':<15}")
    print("-" * 70)
    
    for name, df in data.items():
        if len(df) > 0:
            price_range = f"{df['price'].min():.3f} to {df['price'].max():.3f}"
            mean_std = f"{df['price'].mean():.3f} ± {df['price'].std():.3f}"
            print(f"{name:<20} {len(df):<8} {price_range:<20} {mean_std:<15}")

def main():
    """Main function"""
    plot_comparison()

if __name__ == "__main__":
    main()