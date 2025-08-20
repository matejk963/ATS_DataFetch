#!/usr/bin/env python3
"""
Plot detailed prices for June 26, 2025 - the spike day
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, date

print("📊 PLOTTING JUNE 26, 2025 PRICES")
print("=" * 40)

# Load the data
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.pkl"

try:
    # Load the data
    print(f"📁 Loading data...")
    df = pd.read_pickle(data_path)
    
    # Filter for June 26, 2025
    target_date = date(2025, 6, 26)
    trades = df[df['price'].notna()].copy()
    orders = df[df['price'].isna()].copy()
    
    # Filter for June 26
    june_26_trades = trades[trades.index.date == target_date].copy()
    june_26_orders = orders[orders.index.date == target_date].copy()
    
    print(f"   ✅ June 26 trades: {len(june_26_trades)}")
    print(f"   ✅ June 26 orders: {len(june_26_orders)}")
    
    if len(june_26_trades) == 0:
        print(f"   ❌ No trades found for June 26, 2025")
        exit()
    
    # Basic stats
    print(f"   💰 Price range: €{june_26_trades['price'].min():.2f} - €{june_26_trades['price'].max():.2f}")
    print(f"   💰 Average price: €{june_26_trades['price'].mean():.2f}")
    print(f"   ⏰ Time range: {june_26_trades.index.min().time()} to {june_26_trades.index.max().time()}")
    
    # Separate bids and asks for June 26
    june_26_bids = june_26_orders[june_26_orders['b_price'].notna()].copy()
    june_26_asks = june_26_orders[june_26_orders['a_price'].notna()].copy()
    
    print(f"   📉 Bids: {len(june_26_bids)}")
    print(f"   📈 Asks: {len(june_26_asks)}")
    
    # Create detailed plot
    print(f"\n📈 Creating June 26 detailed plot...")
    
    fig, axes = plt.subplots(3, 2, figsize=(20, 16))
    
    # Plot 1: All prices over time (trades + orders)
    ax1 = axes[0, 0]
    
    # Plot bids and asks first (background)
    if len(june_26_bids) > 0:
        ax1.scatter(june_26_bids.index, june_26_bids['b_price'], 
                   alpha=0.3, s=8, color='red', label=f'Bids ({len(june_26_bids):,})')
    
    if len(june_26_asks) > 0:
        ax1.scatter(june_26_asks.index, june_26_asks['a_price'], 
                   alpha=0.3, s=8, color='blue', label=f'Asks ({len(june_26_asks):,})')
    
    # Plot trades on top
    ax1.scatter(june_26_trades.index, june_26_trades['price'], 
               alpha=0.8, s=25, color='green', label=f'Trades ({len(june_26_trades)})')
    
    ax1.set_title('June 26, 2025 - All Prices (Trades + Orders)')
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Price (EUR/MWh)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Trades only - detailed
    ax2 = axes[0, 1]
    scatter = ax2.scatter(june_26_trades.index, june_26_trades['price'], 
                         c=june_26_trades['price'], cmap='coolwarm', s=40, alpha=0.8)
    plt.colorbar(scatter, ax=ax2, label='Price (EUR/MWh)')
    
    ax2.set_title('June 26, 2025 - Trades Only (Color by Price)')
    ax2.set_xlabel('Time')
    ax2.set_ylabel('Price (EUR/MWh)')
    ax2.grid(True, alpha=0.3)
    
    # Add price level lines
    ax2.axhline(y=30, color='orange', linestyle='--', alpha=0.7, label='€30')
    ax2.axhline(y=32, color='red', linestyle='--', alpha=0.7, label='€32')
    ax2.axhline(y=june_26_trades['price'].max(), color='darkred', linestyle=':', alpha=0.7, 
               label=f'Peak: €{june_26_trades["price"].max():.2f}')
    ax2.legend(loc='upper right')
    
    # Plot 3: Hourly price statistics
    ax3 = axes[1, 0]
    june_26_trades['hour'] = june_26_trades.index.hour
    hourly_stats = june_26_trades.groupby('hour')['price'].agg(['min', 'max', 'mean', 'count'])
    
    hours = hourly_stats.index
    ax3.fill_between(hours, hourly_stats['min'], hourly_stats['max'], 
                    alpha=0.3, color='gray', label='Price Range')
    ax3.plot(hours, hourly_stats['mean'], 'ro-', linewidth=2, markersize=8, label='Hourly Average')
    
    # Add count as text annotations
    for hour, stats in hourly_stats.iterrows():
        ax3.text(hour, stats['max'] + 0.2, f"{int(stats['count'])}", 
                ha='center', va='bottom', fontsize=9, color='blue')
    
    ax3.set_title('June 26, 2025 - Hourly Price Statistics')
    ax3.set_xlabel('Hour of Day')
    ax3.set_ylabel('Price (EUR/MWh)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.set_xticks(range(9, 18))
    
    # Plot 4: Price distribution
    ax4 = axes[1, 1]
    ax4.hist(june_26_trades['price'], bins=20, alpha=0.7, color='green', edgecolor='black')
    ax4.axvline(june_26_trades['price'].mean(), color='red', linestyle='--', 
               label=f'Mean: €{june_26_trades["price"].mean():.2f}')
    ax4.axvline(june_26_trades['price'].median(), color='blue', linestyle='--', 
               label=f'Median: €{june_26_trades["price"].median():.2f}')
    
    ax4.set_title('June 26, 2025 - Price Distribution')
    ax4.set_xlabel('Price (EUR/MWh)')
    ax4.set_ylabel('Frequency')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    # Plot 5: Moving average and volatility
    ax5 = axes[2, 0]
    june_26_trades_sorted = june_26_trades.sort_index()
    
    # Calculate rolling statistics (30-minute windows)
    june_26_trades_sorted['price_ma'] = june_26_trades_sorted['price'].rolling('30min').mean()
    june_26_trades_sorted['price_std'] = june_26_trades_sorted['price'].rolling('30min').std()
    
    ax5.scatter(june_26_trades_sorted.index, june_26_trades_sorted['price'], 
               alpha=0.6, s=20, color='gray', label='Trades')
    ax5.plot(june_26_trades_sorted.index, june_26_trades_sorted['price_ma'], 
            color='red', linewidth=2, label='30-min Moving Average')
    
    # Add volatility bands
    upper_band = june_26_trades_sorted['price_ma'] + june_26_trades_sorted['price_std']
    lower_band = june_26_trades_sorted['price_ma'] - june_26_trades_sorted['price_std']
    ax5.fill_between(june_26_trades_sorted.index, lower_band, upper_band, 
                    alpha=0.2, color='red', label='±1 Std Dev')
    
    ax5.set_title('June 26, 2025 - Moving Average & Volatility')
    ax5.set_xlabel('Time')
    ax5.set_ylabel('Price (EUR/MWh)')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    
    # Plot 6: Cumulative price change
    ax6 = axes[2, 1]
    june_26_trades_sorted['price_change'] = june_26_trades_sorted['price'].diff()
    june_26_trades_sorted['cumulative_change'] = june_26_trades_sorted['price_change'].cumsum()
    
    ax6.plot(june_26_trades_sorted.index, june_26_trades_sorted['cumulative_change'], 
            color='purple', linewidth=2)
    ax6.axhline(y=0, color='black', linestyle='-', alpha=0.5)
    ax6.fill_between(june_26_trades_sorted.index, 0, june_26_trades_sorted['cumulative_change'], 
                    alpha=0.3, color='purple')
    
    ax6.set_title('June 26, 2025 - Cumulative Price Change from Start')
    ax6.set_xlabel('Time')
    ax6.set_ylabel('Cumulative Change (EUR/MWh)')
    ax6.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/june_26_detailed_prices.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"   💾 June 26 detailed plot saved: {plot_path}")
    
    # Print detailed statistics
    print(f"\n📊 JUNE 26, 2025 DETAILED STATISTICS:")
    print(f"   📅 Date: Thursday, June 26, 2025")
    print(f"   📈 Total trades: {len(june_26_trades)}")
    print(f"   💰 Price statistics:")
    print(f"      • Min: €{june_26_trades['price'].min():.2f}")
    print(f"      • Max: €{june_26_trades['price'].max():.2f}")
    print(f"      • Mean: €{june_26_trades['price'].mean():.2f}")
    print(f"      • Median: €{june_26_trades['price'].median():.2f}")
    print(f"      • Std Dev: €{june_26_trades['price'].std():.2f}")
    
    # Time analysis
    print(f"   ⏰ Trading times:")
    print(f"      • First trade: {june_26_trades.index.min().time()}")
    print(f"      • Last trade: {june_26_trades.index.max().time()}")
    print(f"      • Most active hour: {hourly_stats['count'].idxmax()}:00 ({int(hourly_stats['count'].max())} trades)")
    
    # Peak analysis
    peak_trade = june_26_trades.loc[june_26_trades['price'].idxmax()]
    print(f"   🎯 Peak trade:")
    print(f"      • Time: {peak_trade.name}")
    print(f"      • Price: €{peak_trade['price']:.2f}")
    print(f"      • Volume: {peak_trade['volume']:.1f}")
    
    print(f"\n✅ June 26 detailed price analysis completed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()