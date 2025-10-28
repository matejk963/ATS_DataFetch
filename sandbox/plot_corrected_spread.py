#!/usr/bin/env python3
"""
Plot Corrected Spread Data
==========================

Plot the corrected spread data using data_fetch_engine method.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def plot_corrected_spread():
    """Plot the corrected spread data"""
    
    # Load the corrected spread data
    corrected_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_data_fetch_engine_method_synthetic.parquet'
    old_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_synthetic_only_clean_synthetic.parquet'
    
    if not Path(corrected_file).exists():
        print(f"❌ Corrected file not found: {corrected_file}")
        return
    
    # Load both datasets for comparison
    corrected_data = pd.read_parquet(corrected_file)
    corrected_data.index = pd.to_datetime(corrected_data.index)
    
    old_data = pd.read_parquet(old_file) if Path(old_file).exists() else None
    if old_data is not None:
        old_data.index = pd.to_datetime(old_data.index)
    
    # Filter only trades (prices)
    corrected_trades = corrected_data[corrected_data['action'].isin([1.0, -1.0])]
    old_trades = old_data[old_data['action'].isin([1.0, -1.0])] if old_data is not None else pd.DataFrame()
    
    # Create plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot corrected data
    if len(corrected_trades) > 0:
        sequential_idx = range(len(corrected_trades))
        ax1.plot(sequential_idx, corrected_trades['price'], linewidth=0.8, alpha=0.7, color='green', label='Corrected (data_fetch_engine method)')
        ax1.set_title('CORRECTED: DEBM11_25 vs DEBQ1_26 Spread (Data Fetch Engine Method)', fontsize=14)
        ax1.set_ylabel('Price', fontsize=12)
        ax1.grid(True, alpha=0.3)
        
        # Add stats
        ax1.text(0.02, 0.98, f'Records: {len(corrected_trades):,}\nPrice range: {corrected_trades["price"].min():.3f} - {corrected_trades["price"].max():.3f}\nStd: {corrected_trades["price"].std():.3f}', 
                 transform=ax1.transAxes, verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))
        
        print(f"✅ Corrected data: {len(corrected_trades):,} trades")
        print(f"   Price range: {corrected_trades['price'].min():.3f} to {corrected_trades['price'].max():.3f}")
        print(f"   Standard deviation: {corrected_trades['price'].std():.3f}")
    else:
        ax1.text(0.5, 0.5, 'No corrected trade data', transform=ax1.transAxes, ha='center')
    
    # Plot old data for comparison
    if len(old_trades) > 0:
        sequential_idx_old = range(len(old_trades))
        ax2.plot(sequential_idx_old, old_trades['price'], linewidth=0.8, alpha=0.7, color='red', label='Old (spread_fetch_engine method)')
        ax2.set_title('OLD: DEBM11_25 vs DEBQ1_26 Spread (Original Method with Price Shifts)', fontsize=14)
        ax2.set_xlabel('Sequential Index', fontsize=12)
        ax2.set_ylabel('Price', fontsize=12)
        ax2.grid(True, alpha=0.3)
        
        # Add stats
        ax2.text(0.02, 0.98, f'Records: {len(old_trades):,}\nPrice range: {old_trades["price"].min():.3f} - {old_trades["price"].max():.3f}\nStd: {old_trades["price"].std():.3f}', 
                 transform=ax2.transAxes, verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.8))
        
        print(f"📊 Old data: {len(old_trades):,} trades")
        print(f"   Price range: {old_trades['price'].min():.3f} to {old_trades['price'].max():.3f}")
        print(f"   Standard deviation: {old_trades['price'].std():.3f}")
    else:
        ax2.text(0.5, 0.5, 'No old trade data for comparison', transform=ax2.transAxes, ha='center')
    
    plt.tight_layout()
    plt.show()
    
    # Compare statistics
    if len(corrected_trades) > 0 and len(old_trades) > 0:
        print(f"\n📈 COMPARISON:")
        print(f"   Price range reduction: {old_trades['price'].max() - old_trades['price'].min():.3f} → {corrected_trades['price'].max() - corrected_trades['price'].min():.3f}")
        print(f"   Volatility reduction: {old_trades['price'].std():.3f} → {corrected_trades['price'].std():.3f}")
        improvement = ((old_trades['price'].std() - corrected_trades['price'].std()) / old_trades['price'].std()) * 100
        print(f"   Volatility improvement: {improvement:.1f}%")

if __name__ == "__main__":
    plot_corrected_spread()