#!/usr/bin/env python3
"""
Simple Spread Price Plotter
============================

Plots price time series from spread data
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def plot_spread_data(file_path: str):
    """Load and plot spread price data"""
    
    print(f"📊 Loading spread data from: {Path(file_path).name}")
    
    # Load data
    data = pd.read_parquet(file_path)
    data.index = pd.to_datetime(data.index)
    
    print(f"   📈 Data shape: {data.shape}")
    print(f"   📅 Date range: {data.index.min()} to {data.index.max()}")
    print(f"   💰 Price range: {data['price'].min():.2f} to {data['price'].max():.2f}")
    
    # Create plot
    fig, ax = plt.subplots(figsize=(15, 8))
    
    # Plot price over time
    ax.plot(data.index, data['price'], linewidth=1.2, color='blue', alpha=0.8)
    
    # Formatting
    spread_name = Path(file_path).stem.replace('_tr_ba_data_data_fetch_engine_method_synthetic', '')
    ax.set_title(f'Spread Price - {spread_name}', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12)
    ax.grid(True, alpha=0.3)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45)
    
    # Tight layout
    plt.tight_layout()
    
    # Save plot
    output_path = f'/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/{spread_name}_price_plot.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved plot: {output_path}")
    
    plt.show()
    
    return data

def main():
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm1_25_frbq2_25_tr_ba_data_data_fetch_engine_method_synthetic.parquet'
    data = plot_spread_data(file_path)

if __name__ == "__main__":
    main()