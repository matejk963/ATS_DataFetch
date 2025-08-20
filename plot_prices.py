#!/usr/bin/env python3
"""
Simple price plotting script for fetched spread data
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# Set up paths
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet"

print("📊 Simple Price Plot")
print("=" * 40)

try:
    # Load the data
    print(f"📁 Loading data from: {os.path.basename(data_path)}")
    df = pd.read_parquet(data_path)
    
    print(f"📈 Data loaded: {len(df)} records")
    print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Check data structure
    trade_data = df[df['price'].notna()]
    order_data = df[df['b_price'].notna() | df['a_price'].notna()]
    
    print(f"💰 Trade records: {len(trade_data)}")
    print(f"📋 Order records: {len(order_data)}")
    
    if len(trade_data) == 0 and len(order_data) == 0:
        print("❌ No price data found!")
        exit(1)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Plot trades if available
    if len(trade_data) > 0:
        trade_prices = trade_data['price']
        ax.scatter(trade_data.index, trade_prices, 
                  c='red', s=50, alpha=0.7, label=f'Trades ({len(trade_data)})', marker='o')
        
        print(f"📊 Trade price range: {trade_prices.min():.2f} - {trade_prices.max():.2f}")
    
    # Plot order book if available  
    if len(order_data) > 0:
        bid_data = order_data[order_data['b_price'].notna()]
        ask_data = order_data[order_data['a_price'].notna()]
        
        if len(bid_data) > 0:
            ax.plot(bid_data.index, bid_data['b_price'], 
                   color='green', alpha=0.6, linewidth=1, label=f'Bid ({len(bid_data)})')
        
        if len(ask_data) > 0:
            ax.plot(ask_data.index, ask_data['a_price'], 
                   color='blue', alpha=0.6, linewidth=1, label=f'Ask ({len(ask_data)})')
        
        if len(bid_data) > 0:
            print(f"💚 Bid price range: {bid_data['b_price'].min():.2f} - {bid_data['b_price'].max():.2f}")
        if len(ask_data) > 0:
            print(f"💙 Ask price range: {ask_data['a_price'].min():.2f} - {ask_data['a_price'].max():.2f}")
    
    # Format the plot
    ax.set_title('DE-FR Spread Prices (debm09_25 vs frbm09_25)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Format time axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.xticks(rotation=45)
    
    # Tight layout
    plt.tight_layout()
    
    # Save the plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/spread_prices.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"💾 Plot saved: {plot_path}")
    
    # Display statistics
    print("\n📊 Data Summary:")
    if len(trade_data) > 0:
        print(f"   Trade Statistics:")
        print(f"     Count: {len(trade_data)}")
        print(f"     Mean: {trade_data['price'].mean():.2f}")
        print(f"     Median: {trade_data['price'].median():.2f}")
        print(f"     Std: {trade_data['price'].std():.2f}")
    
    if len(order_data) > 0:
        print(f"   Order Book Statistics:")
        if len(bid_data) > 0:
            print(f"     Bid Mean: {bid_data['b_price'].mean():.2f}")
        if len(ask_data) > 0:
            print(f"     Ask Mean: {ask_data['a_price'].mean():.2f}")
        if len(bid_data) > 0 and len(ask_data) > 0:
            # Calculate spread
            spreads = ask_data['a_price'] - bid_data['b_price'].reindex(ask_data.index).ffill()
            spreads = spreads.dropna()
            if len(spreads) > 0:
                print(f"     Bid-Ask Spread Mean: {spreads.mean():.2f}")
    
    print("✅ Plot completed successfully!")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()