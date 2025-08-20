#!/usr/bin/env python3
"""
Simple script to load and visualize saved real market data
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
from datetime import datetime
import os

def load_and_inspect_data():
    """Load saved data and create simple visualizations"""
    
    # Data paths
    data_dir = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test"
    
    print("🔍 Loading Saved Real Market Data")
    print("=" * 40)
    
    # Find the latest files
    files = os.listdir(data_dir)
    trade_files = [f for f in files if f.startswith('real_spread_trades_') and f.endswith('.parquet')]
    metadata_files = [f for f in files if f.startswith('metadata_') and f.endswith('.json')]
    
    if not trade_files:
        print("❌ No trade data files found!")
        return
    
    # Use the latest file
    latest_trade_file = sorted(trade_files)[-1]
    latest_metadata_file = sorted(metadata_files)[-1] if metadata_files else None
    
    print(f"📁 Loading: {latest_trade_file}")
    
    # Load metadata
    if latest_metadata_file:
        with open(os.path.join(data_dir, latest_metadata_file), 'r') as f:
            metadata = json.load(f)
        print(f"📊 Contracts: {' vs '.join(metadata['contracts'])}")
        print(f"📅 Period: {metadata['period']['start_date']} to {metadata['period']['end_date']}")
        print(f"📈 Trade count: {metadata['data_counts']['real_spread']['trades']}")
        print()
    
    # Load trade data
    trades_df = pd.read_parquet(os.path.join(data_dir, latest_trade_file))
    
    print(f"✅ Loaded {len(trades_df)} trades")
    print(f"📊 Columns: {list(trades_df.columns)}")
    print(f"📅 Date range: {trades_df.index[0]} to {trades_df.index[-1]}")
    print()
    
    # Basic statistics
    print("📈 Trade Statistics:")
    print(f"   Price range: {trades_df['price'].min():.2f} to {trades_df['price'].max():.2f}")
    print(f"   Average price: {trades_df['price'].mean():.2f}")
    print(f"   Total volume: {trades_df['volume'].sum()}")
    print(f"   Average volume: {trades_df['volume'].mean():.2f}")
    print()
    
    # Sample data
    print("📋 Sample Trades:")
    print(trades_df.head(5)[['price', 'volume', 'action']].to_string())
    print()
    
    # Create visualizations
    print("📊 Creating Visualizations...")
    
    # Set up the plot - 2 panels vertically
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    fig.suptitle('Real Market Data - Spread Trading', fontsize=16, fontweight='bold')
    
    # Plot 1: Price with orders (trades) over time
    ax1.plot(trades_df.index, trades_df['price'], 'b-', alpha=0.7, linewidth=1, label='Price')
    # Add order points
    colors = ['red' if action == -1 else 'green' for action in trades_df['action']]
    ax1.scatter(trades_df.index, trades_df['price'], c=colors, alpha=0.8, s=30, edgecolors='black', linewidth=0.5)
    ax1.set_title('Spread Price with Orders')
    ax1.set_ylabel('Price (EUR)')
    ax1.grid(True, alpha=0.3)
    ax1.legend(['Price Line', 'Sell Orders', 'Buy Orders'], loc='upper right')
    
    # Plot 2: Volume over time
    ax2.bar(trades_df.index, trades_df['volume'], 
           color=['red' if action == -1 else 'green' for action in trades_df['action']], 
           alpha=0.7, width=0.01)
    ax2.set_title('Trade Volume')
    ax2.set_ylabel('Volume')
    ax2.set_xlabel('Date')
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax2.tick_params(axis='x', rotation=45)
    
    # Adjust layout
    plt.tight_layout()
    
    # Save plot
    plot_path = os.path.join(data_dir, f'market_data_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"💾 Plot saved: {plot_path}")
    
    # Show plot
    plt.show()
    
    # Daily summary
    print("\n📅 Daily Trading Summary:")
    daily_stats = trades_df.groupby(trades_df.index.date).agg({
        'price': ['mean', 'min', 'max'],
        'volume': ['sum', 'count']
    }).round(2)
    
    # Flatten column names
    daily_stats.columns = ['_'.join(col) for col in daily_stats.columns]
    daily_stats = daily_stats.rename(columns={
        'price_mean': 'Avg_Price',
        'price_min': 'Min_Price', 
        'price_max': 'Max_Price',
        'volume_sum': 'Total_Volume',
        'volume_count': 'Trade_Count'
    })
    
    print(daily_stats.to_string())
    
    print(f"\n✅ Analysis complete! Plot saved to {plot_path}")

if __name__ == "__main__":
    load_and_inspect_data()