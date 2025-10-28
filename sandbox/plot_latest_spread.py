#!/usr/bin/env python3
"""
Plot Latest Fetched Spread
==========================

Plot the latest synthetic spread data without gaps.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

def plot_spread():
    """Plot the latest spread data"""
    
    # Load the spread data
    spread_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_synthetic_only_clean_synthetic.parquet'
    
    if not Path(spread_file).exists():
        print(f"❌ Spread file not found: {spread_file}")
        return
    
    # Load data
    data = pd.read_parquet(spread_file)
    data.index = pd.to_datetime(data.index)
    
    # Filter only trades (prices)
    trades = data[data['action'].isin([1.0, -1.0])]
    
    # Create sequential index to remove gaps
    sequential_idx = range(len(trades))
    
    # Create plot
    plt.figure(figsize=(14, 8))
    plt.plot(sequential_idx, trades['price'], linewidth=0.8, alpha=0.7)
    plt.title('DEBM11_25 vs DEBQ1_26 Spread (Synthetic)', fontsize=14)
    plt.xlabel('Sequential Index', fontsize=12)
    plt.ylabel('Price', fontsize=12)
    plt.grid(True, alpha=0.3)
    
    # Add some stats
    plt.text(0.02, 0.98, f'Records: {len(trades):,}\nPrice range: {trades["price"].min():.3f} - {trades["price"].max():.3f}', 
             transform=plt.gca().transAxes, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.show()
    
    print(f"✅ Plotted {len(trades):,} trade records")

if __name__ == "__main__":
    plot_spread()