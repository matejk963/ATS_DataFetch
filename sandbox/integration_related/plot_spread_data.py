#!/usr/bin/env python3

"""
Plot trades and orders from the merged spread data
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

def plot_spread_data():
    """Plot trades and orders from the saved data files"""
    
    print("📊 Loading and plotting spread data...")
    
    try:
        # Load the merged data
        data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debm08_25_frbm08_25_tr_ba_data.parquet"
        
        print(f"📁 Loading data from: {data_path}")
        df = pd.read_parquet(data_path)
        
        print(f"✅ Loaded {len(df):,} total records")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Date range: {df.index.min()} to {df.index.max()}")
        
        # Separate trades and orders
        # Trades have price, volume, and tradeid filled
        # Orders have b_price and/or a_price filled
        trades_mask = (~df['price'].isna()) & (~df['volume'].isna()) & (~df['tradeid'].isna())
        orders_mask = (~df['b_price'].isna()) | (~df['a_price'].isna())
        
        trades_df = df[trades_mask].copy()
        orders_df = df[orders_mask].copy()
        
        print(f"📈 Trades: {len(trades_df):,} records")
        print(f"📋 Orders: {len(orders_df):,} records")
        
        if len(trades_df) == 0 and len(orders_df) == 0:
            print("⚠️  No trade or order data to plot!")
            return
        
        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)
        
        # Plot 1: Order Book (Bid/Ask)
        if len(orders_df) > 0:
            # Drop NaN values for continuous plotting
            bid_data = orders_df['b_price'].dropna()
            ask_data = orders_df['a_price'].dropna()
            
            if len(bid_data) > 0:
                ax1.plot(bid_data.index, bid_data.values, 'b-', alpha=0.7, linewidth=1, label=f'Bid ({len(bid_data):,} points)')
            
            if len(ask_data) > 0:
                ax1.plot(ask_data.index, ask_data.values, 'r-', alpha=0.7, linewidth=1, label=f'Ask ({len(ask_data):,} points)')
            
            # Fill between bid and ask to show spread
            if len(bid_data) > 0 and len(ask_data) > 0:
                # Align timestamps for fill_between
                common_times = bid_data.index.intersection(ask_data.index)
                if len(common_times) > 0:
                    bid_aligned = bid_data.reindex(common_times)
                    ask_aligned = ask_data.reindex(common_times)
                    ax1.fill_between(common_times, bid_aligned, ask_aligned, alpha=0.2, color='gray', label='Bid-Ask Spread')
            
            ax1.set_ylabel('Price (EUR/MWh)', fontsize=12)
            ax1.set_title('DE-FR Spread Order Book (Bid/Ask Prices)', fontsize=14, fontweight='bold')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        else:
            ax1.text(0.5, 0.5, 'No Order Data Available', ha='center', va='center', transform=ax1.transAxes, fontsize=14)
            ax1.set_title('DE-FR Spread Order Book (No Data)', fontsize=14, fontweight='bold')
        
        # Plot 2: Trades
        if len(trades_df) > 0:
            # Plot trades as scatter points with size based on volume
            trade_prices = trades_df['price'].dropna()
            trade_volumes = trades_df['volume'].dropna()
            
            if len(trade_prices) > 0:
                # Normalize volume for point sizes (min=10, max=100)
                if len(trade_volumes) > 0:
                    vol_norm = (trade_volumes - trade_volumes.min()) / (trade_volumes.max() - trade_volumes.min() + 1e-6)
                    sizes = 10 + vol_norm * 90
                    
                    scatter = ax2.scatter(trade_prices.index, trade_prices.values, 
                                        s=sizes, alpha=0.6, c='green', 
                                        edgecolors='darkgreen', linewidth=0.5,
                                        label=f'Trades ({len(trade_prices):,})')
                    
                    # Add colorbar for volume
                    cbar = plt.colorbar(scatter, ax=ax2, pad=0.01)
                    cbar.set_label('Volume (MWh)', rotation=270, labelpad=15)
                else:
                    ax2.scatter(trade_prices.index, trade_prices.values, 
                              s=20, alpha=0.6, c='green', label=f'Trades ({len(trade_prices):,})')
            
            ax2.set_ylabel('Price (EUR/MWh)', fontsize=12)
            ax2.set_xlabel('Time', fontsize=12)
            ax2.set_title('DE-FR Spread Executed Trades', fontsize=14, fontweight='bold')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        else:
            ax2.text(0.5, 0.5, 'No Trade Data Available', ha='center', va='center', transform=ax2.transAxes, fontsize=14)
            ax2.set_title('DE-FR Spread Executed Trades (No Data)', fontsize=14, fontweight='bold')
        
        # Format x-axis
        if len(df) > 0:
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
            ax2.xaxis.set_major_locator(mdates.HourLocator(interval=6))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        # Overall title
        fig.suptitle(f'DE-FR Cross-Market Spread Data\n'
                    f'{df.index.min().strftime("%Y-%m-%d")} to {df.index.max().strftime("%Y-%m-%d")} '
                    f'({len(df):,} total records)', 
                    fontsize=16, fontweight='bold', y=0.98)
        
        plt.tight_layout()
        
        # Save the plot
        plot_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/spread_data_plot.png"
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        print(f"💾 Plot saved: {plot_path}")
        
        # Show statistics
        print(f"\n📊 Data Summary:")
        if len(trades_df) > 0:
            print(f"   Trades - Price range: €{trades_df['price'].min():.2f} - €{trades_df['price'].max():.2f}")
            print(f"   Trades - Total volume: {trades_df['volume'].sum():,.0f} MWh")
        
        if len(orders_df) > 0:
            bid_range = f"€{orders_df['b_price'].min():.2f} - €{orders_df['b_price'].max():.2f}" if not orders_df['b_price'].isna().all() else "N/A"
            ask_range = f"€{orders_df['a_price'].min():.2f} - €{orders_df['a_price'].max():.2f}" if not orders_df['a_price'].isna().all() else "N/A"
            print(f"   Orders - Bid range: {bid_range}")
            print(f"   Orders - Ask range: {ask_range}")
        
        plt.show()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    plot_spread_data()