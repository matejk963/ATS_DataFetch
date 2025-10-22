#!/usr/bin/env python3
"""
Plot Corrected Spread Data
=========================

Simple script to plot the corrected merged spread data with outlier cleaning.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

def plot_corrected_spread_data(file_path: str):
    """Plot the corrected spread data"""
    
    print(f"🎨 Plotting corrected spread data from: {file_path}")
    
    # Load data
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return
    
    df = pd.read_parquet(file_path)
    print(f"📊 Loaded {len(df)} records")
    
    # Parse filename for spread name
    filename = os.path.basename(file_path)
    spread_name = filename.split('_tr_ba_data')[0]
    print(f"📈 Spread: {spread_name}")
    
    # Convert timestamp to datetime if needed
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        timestamp_col = 'timestamp'
    elif hasattr(df.index, 'dtype') and 'datetime' in str(df.index.dtype):
        timestamp_col = 'index'
    else:
        # Use index as timestamp
        df.index = pd.to_datetime(df.index)
        timestamp_col = 'index'
        print(f"📅 Using index as timestamp")
    
    # Check action values and separate data accordingly
    action_values = df['action'].unique()
    print(f"   📊 Action values: {action_values}")
    
    # If action is numeric (1, -1), treat all as trades
    # If action is string ('trade', 'bid', 'ask'), separate accordingly
    if all(isinstance(val, (int, float)) for val in action_values):
        trades_df = df.copy()  # All records are trades with buy/sell actions
        orders_df = pd.DataFrame()  # No separate orders
        print(f"   📊 All records treated as trades (numeric actions)")
    else:
        trades_df = df[df['action'] == 'trade'].copy()
        orders_df = df[df['action'] != 'trade'].copy()
        print(f"   📊 Separated by string actions")
    
    print(f"   📊 Trades: {len(trades_df)}")
    print(f"   📊 Orders: {len(orders_df)}")
    
    # Create plot
    fig, axes = plt.subplots(2, 1, figsize=(15, 10))
    fig.suptitle(f'Corrected Spread Data: {spread_name}', fontsize=16, fontweight='bold')
    
    # Plot 1: Price timeline
    ax1 = axes[0]
    
    if len(trades_df) > 0:
        # Use datetime x-axis
        ax1.scatter(trades_df.index, trades_df['price'], alpha=0.7, s=20, c='blue', label='Trade Prices')
        ax1.set_ylabel('Price')
        ax1.set_title(f'Trade Prices (DateTime Timeline - {len(trades_df)} trades)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Format x-axis for better datetime display
        ax1.tick_params(axis='x', rotation=45)
        
        # Add price trend line using datetime
        if len(trades_df) > 1:
            # Convert datetime to numeric for trend calculation
            x_numeric = mdates.date2num(trades_df.index)
            z = np.polyfit(x_numeric, trades_df['price'], 1)
            p = np.poly1d(z)
            ax1.plot(trades_df.index, p(x_numeric), "r--", alpha=0.8, label='Trend Line')
            ax1.legend()
    
    # Plot 2: Volume over time (if available)
    ax2 = axes[1]
    
    if len(trades_df) > 0 and 'volume' in trades_df.columns:
        ax2.bar(trades_df.index, trades_df['volume'], alpha=0.7, color='green', width=pd.Timedelta(hours=1))
        ax2.set_ylabel('Volume')
        ax2.set_xlabel('DateTime')
        ax2.set_title('Trade Volume (DateTime Timeline)')
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
    else:
        ax2.text(0.5, 0.5, 'No volume data available', ha='center', va='center', 
                transform=ax2.transAxes, fontsize=14)
        ax2.set_title('Volume Data Not Available')
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_filename = f"{spread_name}_corrected_data.png"
    plot_path = output_dir / plot_filename
    
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Plot saved: {plot_path}")
    
    # Show statistics
    if len(trades_df) > 0:
        print(f"\n📊 Corrected Data Statistics:")
        print(f"   Price range: {trades_df['price'].min():.4f} - {trades_df['price'].max():.4f}")
        print(f"   Price mean: {trades_df['price'].mean():.4f}")
        print(f"   Price std: {trades_df['price'].std():.4f}")
        if 'volume' in trades_df.columns:
            print(f"   Total volume: {trades_df['volume'].sum():.0f}")
            print(f"   Average volume: {trades_df['volume'].mean():.2f}")
    
    # Don't show plot in headless environment
    # plt.show()

def main():
    """Main function"""
    
    # Path to the corrected merged data
    corrected_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test/debm12_25_debq1_26_tr_ba_datatest_merged_clean_merged.parquet"
    
    plot_corrected_spread_data(corrected_file)

if __name__ == "__main__":
    main()