#!/usr/bin/env python3
"""
Price plotting script for quarterly spread data (Q4 2025)
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# Set up paths
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"

print("📊 Quarterly Spread Price Plot - Q4 2025")
print("=" * 50)

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
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot trades if available
    if len(trade_data) > 0:
        trade_prices = trade_data['price']
        ax.scatter(trade_data.index, trade_prices, 
                  c='red', s=40, alpha=0.8, label=f'Trades ({len(trade_data)})', marker='o')
        
        print(f"📊 Trade price range: €{trade_prices.min():.2f} - €{trade_prices.max():.2f}")
    
    # Plot order book if available  
    if len(order_data) > 0:
        bid_data = order_data[order_data['b_price'].notna()]
        ask_data = order_data[order_data['a_price'].notna()]
        
        if len(bid_data) > 0:
            ax.plot(bid_data.index, bid_data['b_price'], 
                   color='green', alpha=0.7, linewidth=1.5, label=f'Bid ({len(bid_data)})')
        
        if len(ask_data) > 0:
            ax.plot(ask_data.index, ask_data['a_price'], 
                   color='blue', alpha=0.7, linewidth=1.5, label=f'Ask ({len(ask_data)})')
        
        if len(bid_data) > 0:
            print(f"💚 Bid price range: €{bid_data['b_price'].min():.2f} - €{bid_data['b_price'].max():.2f}")
        if len(ask_data) > 0:
            print(f"💙 Ask price range: €{ask_data['a_price'].min():.2f} - €{ask_data['a_price'].max():.2f}")
    
    # Format the plot
    ax.set_title('DE-FR Q4 2025 Spread Prices (debq4_25 vs frbq4_25)', fontsize=16, fontweight='bold')
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12)
    ax.legend(loc='upper right')
    ax.grid(True, alpha=0.3)
    
    # Format time axis - adjust based on data range
    time_span = df.index.max() - df.index.min()
    if time_span.days > 30:
        # For longer periods, show daily ticks
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, time_span.days//20)))
    else:
        # For shorter periods, show hourly ticks
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    
    plt.xticks(rotation=45)
    
    # Tight layout
    plt.tight_layout()
    
    # Save the plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/quarterly_spread_prices.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"💾 Plot saved: {plot_path}")
    
    # Display statistics
    print("\\n📊 Q4 2025 Spread Data Summary:")
    if len(trade_data) > 0:
        print(f"   💰 Trade Statistics:")
        print(f"     Count: {len(trade_data):,}")
        print(f"     Mean: €{trade_data['price'].mean():.2f}")
        print(f"     Median: €{trade_data['price'].median():.2f}")
        print(f"     Std Dev: €{trade_data['price'].std():.2f}")
        print(f"     Min: €{trade_data['price'].min():.2f}")
        print(f"     Max: €{trade_data['price'].max():.2f}")
    
    if len(order_data) > 0:
        print(f"   📋 Order Book Statistics:")
        if len(bid_data) > 0:
            print(f"     Bid Mean: €{bid_data['b_price'].mean():.2f}")
            print(f"     Bid Count: {len(bid_data):,}")
        if len(ask_data) > 0:
            print(f"     Ask Mean: €{ask_data['a_price'].mean():.2f}")
            print(f"     Ask Count: {len(ask_data):,}")
        if len(bid_data) > 0 and len(ask_data) > 0:
            # Calculate spread where we have both bid and ask
            common_times = bid_data.index.intersection(ask_data.index)
            if len(common_times) > 0:
                spreads = ask_data.loc[common_times, 'a_price'] - bid_data.loc[common_times, 'b_price']
                spreads = spreads.dropna()
                if len(spreads) > 0:
                    print(f"     Bid-Ask Spread Mean: €{spreads.mean():.3f}")
                    print(f"     Bid-Ask Spread Std: €{spreads.std():.3f}")
    
    # Check for negative spreads
    if len(order_data) > 0:
        has_both_prices = order_data['b_price'].notna() & order_data['a_price'].notna()
        valid_orders = order_data[has_both_prices]
        if len(valid_orders) > 0:
            negative_spreads = (valid_orders['a_price'] < valid_orders['b_price']).sum()
            if negative_spreads > 0:
                print(f"   ⚠️  Negative spreads detected: {negative_spreads} ({negative_spreads/len(valid_orders)*100:.1f}%)")
            else:
                print(f"   ✅ Data quality: No negative spreads detected")
    
    print("\\n✅ Quarterly spread plot completed successfully!")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()