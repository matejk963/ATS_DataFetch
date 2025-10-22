#!/usr/bin/env python3
"""
Spread Data Visualization Script
===============================

Comprehensive visualization of spread data fetched by the spread_fetch_engine.
Supports multiple data sources (real, synthetic, merged) and provides:
- Price time series (trades and bid/ask)
- Volume analysis
- Spread statistics
- Data source comparison

Usage:
    python sandbox/spread_visualization.py
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import seaborn as sns
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
data_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test'

sys.path.insert(0, project_root)

# Configure matplotlib for better plots
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [15, 10]
plt.rcParams['font.size'] = 10
sns.set_palette("husl")

def load_spread_data(spread_name, data_types=['synthetic', 'real', 'merged']):
    """
    Load spread data from parquet files
    
    Args:
        spread_name: Name of the spread (e.g., 'debm11_25_debm12_25')
        data_types: List of data types to load ['synthetic', 'real', 'merged']
    
    Returns:
        Dictionary with loaded DataFrames
    """
    data = {}
    
    for data_type in data_types:
        file_path = os.path.join(data_path, f"{spread_name}_tr_ba_data_test_{data_type}.parquet")
        
        if os.path.exists(file_path):
            print(f"📁 Loading {data_type} data: {file_path}")
            try:
                df = pd.read_parquet(file_path)
                print(f"   ✅ Loaded {len(df):,} records")
                
                # Ensure datetime index
                if not isinstance(df.index, pd.DatetimeIndex):
                    df.index = pd.to_datetime(df.index)
                
                data[data_type] = df
                
                # Basic data info
                trades_count = df['price'].notna().sum()
                orders_count = (df['b_price'].notna() | df['a_price'].notna()).sum()
                print(f"   📊 Trades: {trades_count:,}, Orders: {orders_count:,}")
                
            except Exception as e:
                print(f"   ❌ Failed to load {data_type}: {e}")
        else:
            print(f"   ⚠️  File not found: {file_path}")
    
    return data

def plot_price_timeline(data_dict, spread_name):
    """
    Plot price timeline with trades and bid/ask spreads - no gaps, clean x-axis
    """
    fig, axes = plt.subplots(len(data_dict), 1, figsize=(16, 6 * len(data_dict)), sharex=True)
    if len(data_dict) == 1:
        axes = [axes]
    
    for i, (data_type, df) in enumerate(data_dict.items()):
        ax = axes[i]
        
        # Remove rows where all price data is NaN to eliminate gaps
        df_clean = df.dropna(subset=['price', 'b_price', 'a_price'], how='all')
        
        if df_clean.empty:
            ax.text(0.5, 0.5, f'No price data available for {data_type}', 
                   ha='center', va='center', transform=ax.transAxes, fontsize=12)
            ax.set_title(f'{spread_name} - {data_type.capitalize()} Data', fontsize=14, fontweight='bold')
            continue
        
        # Create sequential x-axis (remove time gaps)
        x_vals = np.arange(len(df_clean))
        
        # Separate trades and orders
        trades_mask = df_clean['price'].notna()
        orders_mask = (df_clean['b_price'].notna() | df_clean['a_price'].notna()) & ~trades_mask
        
        trades_df = df_clean[trades_mask]
        orders_df = df_clean[orders_mask]
        
        # Get x positions for trades and orders
        if not orders_df.empty:
            orders_x = x_vals[orders_mask]
            
            # Plot bid/ask spreads
            ax.fill_between(orders_x, 
                           orders_df['b_price'], 
                           orders_df['a_price'], 
                           alpha=0.3, 
                           color='lightblue', 
                           label='Bid-Ask Spread')
            
            ax.plot(orders_x, orders_df['b_price'], 
                   color='green', linewidth=1, alpha=0.7, label='Bid Price')
            ax.plot(orders_x, orders_df['a_price'], 
                   color='red', linewidth=1, alpha=0.7, label='Ask Price')
        
        # Plot trades
        if not trades_df.empty:
            trades_x = x_vals[trades_mask]
            
            # Separate buy/sell trades
            buy_mask = trades_df['action'] > 0
            sell_mask = trades_df['action'] < 0
            
            if buy_mask.any():
                buy_x = trades_x[buy_mask]
                buy_prices = trades_df[buy_mask]['price']
                ax.scatter(buy_x, buy_prices, 
                          color='darkgreen', s=25, alpha=0.8, 
                          marker='^', label='Buy Trades', zorder=5)
            
            if sell_mask.any():
                sell_x = trades_x[sell_mask]
                sell_prices = trades_df[sell_mask]['price']
                ax.scatter(sell_x, sell_prices, 
                          color='darkred', s=25, alpha=0.8, 
                          marker='v', label='Sell Trades', zorder=5)
        
        ax.set_title(f'{spread_name} - {data_type.capitalize()} Data', fontsize=14, fontweight='bold')
        ax.set_ylabel('Price', fontsize=12)
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)
        
        # Clean x-axis: show only date labels at key intervals
        # Select representative time points for labeling
        n_points = len(df_clean)
        if n_points > 10:
            label_indices = np.linspace(0, n_points-1, min(8, n_points), dtype=int)
            label_times = [df_clean.index[idx].strftime('%m-%d %H:%M') for idx in label_indices]
            ax.set_xticks(label_indices)
            ax.set_xticklabels(label_times, rotation=45)
        else:
            # For small datasets, show all points
            ax.set_xticks(x_vals[::max(1, len(x_vals)//8)])
            ax.set_xticklabels([df_clean.index[i].strftime('%m-%d %H:%M') 
                               for i in range(0, len(df_clean), max(1, len(df_clean)//8))], 
                              rotation=45)
        
        # Add data statistics as text
        stats_text = f"Records: {len(df_clean):,}"
        if not trades_df.empty:
            stats_text += f" | Trades: {len(trades_df):,}"
        if not orders_df.empty:
            stats_text += f" | Orders: {len(orders_df):,}"
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
                verticalalignment='top', fontsize=10)
    
    plt.xlabel('Time', fontsize=12)
    plt.tight_layout()
    return fig

def plot_volume_analysis(data_dict, spread_name):
    """
    Plot volume analysis
    """
    fig, axes = plt.subplots(2, len(data_dict), figsize=(6 * len(data_dict), 10))
    if len(data_dict) == 1:
        axes = axes.reshape(-1, 1)
    
    for i, (data_type, df) in enumerate(data_dict.items()):
        # Volume over time
        ax1 = axes[0, i]
        trades_df = df[df['price'].notna()]
        
        if not trades_df.empty and 'volume' in trades_df.columns:
            # Hourly volume aggregation
            hourly_volume = trades_df.resample('1H')['volume'].sum().dropna()
            
            if not hourly_volume.empty:
                ax1.bar(hourly_volume.index, hourly_volume.values, 
                       width=timedelta(hours=0.8), alpha=0.7)
                ax1.set_title(f'{data_type.capitalize()} - Hourly Volume')
                ax1.set_ylabel('Volume')
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                ax1.tick_params(axis='x', rotation=45)
        else:
            ax1.text(0.5, 0.5, 'No volume data', ha='center', va='center', 
                    transform=ax1.transAxes, fontsize=12)
            ax1.set_title(f'{data_type.capitalize()} - No Volume Data')
        
        # Volume distribution
        ax2 = axes[1, i]
        if not trades_df.empty and 'volume' in trades_df.columns and trades_df['volume'].notna().sum() > 0:
            volume_data = trades_df['volume'].dropna()
            ax2.hist(volume_data, bins=30, alpha=0.7, edgecolor='black')
            ax2.set_title(f'{data_type.capitalize()} - Volume Distribution')
            ax2.set_xlabel('Volume')
            ax2.set_ylabel('Frequency')
            
            # Add statistics
            stats_text = f"Mean: {volume_data.mean():.2f}\nStd: {volume_data.std():.2f}\nCount: {len(volume_data):,}"
            ax2.text(0.7, 0.7, stats_text, transform=ax2.transAxes, 
                    bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8))
        else:
            ax2.text(0.5, 0.5, 'No volume data', ha='center', va='center', 
                    transform=ax2.transAxes, fontsize=12)
            ax2.set_title(f'{data_type.capitalize()} - No Volume Data')
    
    plt.tight_layout()
    return fig

def plot_spread_statistics(data_dict, spread_name):
    """
    Plot spread statistics and comparisons
    """
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Collect statistics for all data types
    stats_summary = []
    price_data = {}
    
    for data_type, df in data_dict.items():
        trades_df = df[df['price'].notna()]
        orders_df = df[(df['b_price'].notna()) | (df['a_price'].notna())]
        
        # Calculate bid-ask spread
        if not orders_df.empty:
            bid_ask_spread = orders_df['a_price'] - orders_df['b_price']
            bid_ask_spread = bid_ask_spread.dropna()
        else:
            bid_ask_spread = pd.Series(dtype=float)
        
        # Price statistics
        if not trades_df.empty:
            price_stats = {
                'data_type': data_type,
                'trade_count': len(trades_df),
                'order_count': len(orders_df),
                'price_mean': trades_df['price'].mean(),
                'price_std': trades_df['price'].std(),
                'price_min': trades_df['price'].min(),
                'price_max': trades_df['price'].max(),
                'bid_ask_spread_mean': bid_ask_spread.mean() if not bid_ask_spread.empty else np.nan,
                'bid_ask_spread_std': bid_ask_spread.std() if not bid_ask_spread.empty else np.nan
            }
            stats_summary.append(price_stats)
            price_data[data_type] = trades_df['price'].dropna()
    
    # Plot 1: Price comparison (no gaps)
    ax1 = axes[0, 0]
    for data_type, prices in price_data.items():
        if not prices.empty:
            # Remove gaps by using sequential x-axis
            x_seq = np.arange(len(prices))
            ax1.plot(x_seq, prices.values, label=data_type.capitalize(), alpha=0.8, linewidth=2)
    ax1.set_title('Price Comparison Across Data Sources')
    ax1.set_ylabel('Price')
    ax1.set_xlabel('Sequential Time Points')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Price distribution comparison
    ax2 = axes[0, 1]
    for data_type, prices in price_data.items():
        if not prices.empty:
            ax2.hist(prices.values, bins=30, alpha=0.6, label=data_type.capitalize())
    ax2.set_title('Price Distribution Comparison')
    ax2.set_xlabel('Price')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    
    # Plot 3: Bid-Ask Spread Analysis
    ax3 = axes[1, 0]
    spread_data = {}
    for data_type, df in data_dict.items():
        orders_df = df[(df['b_price'].notna()) & (df['a_price'].notna())]
        if not orders_df.empty:
            bid_ask_spread = orders_df['a_price'] - orders_df['b_price']
            spread_data[data_type] = bid_ask_spread.dropna()
    
    if spread_data:
        box_data = [spread_data[dt] for dt in spread_data.keys()]
        box_labels = list(spread_data.keys())
        ax3.boxplot(box_data, labels=box_labels)
        ax3.set_title('Bid-Ask Spread Distribution')
        ax3.set_ylabel('Spread (Ask - Bid)')
    else:
        ax3.text(0.5, 0.5, 'No bid-ask data', ha='center', va='center', 
                transform=ax3.transAxes, fontsize=12)
        ax3.set_title('Bid-Ask Spread Distribution - No Data')
    
    # Plot 4: Summary statistics table
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    if stats_summary:
        stats_df = pd.DataFrame(stats_summary)
        
        # Create table text
        table_text = "SUMMARY STATISTICS\n" + "="*50 + "\n\n"
        for _, row in stats_df.iterrows():
            table_text += f"{row['data_type'].upper()}:\n"
            table_text += f"  Trades: {row['trade_count']:,}\n"
            table_text += f"  Orders: {row['order_count']:,}\n"
            table_text += f"  Price Mean: {row['price_mean']:.4f}\n"
            table_text += f"  Price Std: {row['price_std']:.4f}\n"
            table_text += f"  Price Range: {row['price_min']:.4f} - {row['price_max']:.4f}\n"
            if not pd.isna(row['bid_ask_spread_mean']):
                table_text += f"  Avg Bid-Ask Spread: {row['bid_ask_spread_mean']:.4f}\n"
            table_text += "\n"
        
        ax4.text(0.05, 0.95, table_text, transform=ax4.transAxes, 
                fontfamily='monospace', fontsize=10, verticalalignment='top',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.8))
    
    plt.suptitle(f'{spread_name} - Comprehensive Analysis', fontsize=16, fontweight='bold')
    plt.tight_layout()
    return fig

def main():
    """Main visualization function"""
    print("🎨 Spread Data Visualization")
    print("=" * 50)
    
    # Configuration
    spread_name = 'debm11_25_debm12_25'
    data_types = ['synthetic', 'real', 'merged']
    
    print(f"📊 Analyzing spread: {spread_name}")
    print(f"📁 Data path: {data_path}")
    
    # Load data
    data_dict = load_spread_data(spread_name, data_types)
    
    if not data_dict:
        print("❌ No data loaded! Check file paths.")
        return
    
    print(f"\n✅ Loaded {len(data_dict)} data sources: {list(data_dict.keys())}")
    
    # Create output directory
    output_dir = os.path.join(project_root, 'sandbox', 'plots')
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate plots
    plots_generated = []
    
    # 1. Price timeline plot
    print("\n📈 Generating price timeline plot...")
    try:
        fig1 = plot_price_timeline(data_dict, spread_name)
        plot_path1 = os.path.join(output_dir, f'{spread_name}_price_timeline.png')
        fig1.savefig(plot_path1, dpi=300, bbox_inches='tight')
        plots_generated.append(plot_path1)
        print(f"   ✅ Saved: {plot_path1}")
        plt.close(fig1)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # 2. Volume analysis plot
    print("\n📊 Generating volume analysis plot...")
    try:
        fig2 = plot_volume_analysis(data_dict, spread_name)
        plot_path2 = os.path.join(output_dir, f'{spread_name}_volume_analysis.png')
        fig2.savefig(plot_path2, dpi=300, bbox_inches='tight')
        plots_generated.append(plot_path2)
        print(f"   ✅ Saved: {plot_path2}")
        plt.close(fig2)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # 3. Comprehensive statistics plot
    print("\n📈 Generating comprehensive analysis plot...")
    try:
        fig3 = plot_spread_statistics(data_dict, spread_name)
        plot_path3 = os.path.join(output_dir, f'{spread_name}_comprehensive_analysis.png')
        fig3.savefig(plot_path3, dpi=300, bbox_inches='tight')
        plots_generated.append(plot_path3)
        print(f"   ✅ Saved: {plot_path3}")
        plt.close(fig3)
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # Summary
    print(f"\n🎉 Visualization completed!")
    print(f"   📊 Generated {len(plots_generated)} plots:")
    for plot_path in plots_generated:
        print(f"      📁 {os.path.basename(plot_path)}")
    
    print(f"\n📁 All plots saved in: {output_dir}")
    
    # Show basic data summary
    print(f"\n📋 Data Summary for {spread_name}:")
    for data_type, df in data_dict.items():
        trades_count = df['price'].notna().sum()
        orders_count = (df['b_price'].notna() | df['a_price'].notna()).sum()
        date_range = f"{df.index.min()} to {df.index.max()}"
        print(f"   {data_type.upper()}: {len(df):,} records ({trades_count:,} trades, {orders_count:,} orders)")
        print(f"      📅 Date range: {date_range}")

if __name__ == "__main__":
    main()