#!/usr/bin/env python3
"""
Simple plot of trades and orders from the latest refetched data
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json

print("📊 SIMPLE PLOT: TRADES AND ORDERS")
print("=" * 40)

# Load the latest data
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.pkl"

try:
    # Load the data
    print(f"📁 Loading data...")
    df = pd.read_pickle(data_path)
    print(f"   ✅ Loaded: {len(df):,} total records")
    
    # Separate trades and orders
    trades = df[df['price'].notna()].copy()
    orders = df[df['price'].isna()].copy()
    
    print(f"   📈 Trades: {len(trades):,}")
    print(f"   📋 Orders: {len(orders):,}")
    
    # Separate orders into bids and asks
    bids = orders[orders['b_price'].notna()].copy()
    asks = orders[orders['a_price'].notna()].copy()
    
    print(f"   📉 Bids: {len(bids):,}")
    print(f"   📈 Asks: {len(asks):,}")
    
    # Create simple 2x2 plot
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # Plot 1: All Trades
    ax1 = axes[0, 0]
    if len(trades) > 0:
        ax1.scatter(trades.index, trades['price'], alpha=0.6, s=20, color='green')
        ax1.set_title(f'Trades ({len(trades):,})')
        ax1.set_xlabel('Time')
        ax1.set_ylabel('Price (EUR/MWh)')
        ax1.grid(True, alpha=0.3)
        
        # Add price stats
        price_range = f"€{trades['price'].min():.1f} - €{trades['price'].max():.1f}"
        price_mean = f"Mean: €{trades['price'].mean():.1f}"
        ax1.text(0.02, 0.98, f"{price_range}\n{price_mean}", 
                transform=ax1.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax1.text(0.5, 0.5, 'No Trades', transform=ax1.transAxes, ha='center', va='center')
        ax1.set_title('Trades (No Data)')
    
    # Plot 2: Bid Orders
    ax2 = axes[0, 1]
    if len(bids) > 0:
        # Sample if too many points
        if len(bids) > 5000:
            bid_sample = bids.sample(5000).sort_index()
        else:
            bid_sample = bids
            
        ax2.scatter(bid_sample.index, bid_sample['b_price'], alpha=0.4, s=10, color='red')
        ax2.set_title(f'Bid Orders ({len(bids):,})')
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Bid Price (EUR/MWh)')
        ax2.grid(True, alpha=0.3)
        
        # Add bid stats
        bid_range = f"€{bids['b_price'].min():.1f} - €{bids['b_price'].max():.1f}"
        bid_mean = f"Mean: €{bids['b_price'].mean():.1f}"
        ax2.text(0.02, 0.98, f"{bid_range}\n{bid_mean}", 
                transform=ax2.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax2.text(0.5, 0.5, 'No Bids', transform=ax2.transAxes, ha='center', va='center')
        ax2.set_title('Bid Orders (No Data)')
    
    # Plot 3: Ask Orders
    ax3 = axes[1, 0]
    if len(asks) > 0:
        # Sample if too many points
        if len(asks) > 5000:
            ask_sample = asks.sample(5000).sort_index()
        else:
            ask_sample = asks
            
        ax3.scatter(ask_sample.index, ask_sample['a_price'], alpha=0.4, s=10, color='blue')
        ax3.set_title(f'Ask Orders ({len(asks):,})')
        ax3.set_xlabel('Time')
        ax3.set_ylabel('Ask Price (EUR/MWh)')
        ax3.grid(True, alpha=0.3)
        
        # Add ask stats
        ask_range = f"€{asks['a_price'].min():.1f} - €{asks['a_price'].max():.1f}"
        ask_mean = f"Mean: €{asks['a_price'].mean():.1f}"
        ax3.text(0.02, 0.98, f"{ask_range}\n{ask_mean}", 
                transform=ax3.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax3.text(0.5, 0.5, 'No Asks', transform=ax3.transAxes, ha='center', va='center')
        ax3.set_title('Ask Orders (No Data)')
    
    # Plot 4: Combined View (Trades + Bids + Asks)
    ax4 = axes[1, 1]
    
    # Plot a sample of bids and asks first (background)
    if len(bids) > 0:
        bid_sample = bids.sample(min(1000, len(bids))).sort_index()
        ax4.scatter(bid_sample.index, bid_sample['b_price'], 
                   alpha=0.3, s=5, color='red', label=f'Bids ({len(bids):,})')
    
    if len(asks) > 0:
        ask_sample = asks.sample(min(1000, len(asks))).sort_index()
        ax4.scatter(ask_sample.index, ask_sample['a_price'], 
                   alpha=0.3, s=5, color='blue', label=f'Asks ({len(asks):,})')
    
    # Plot trades on top (foreground)
    if len(trades) > 0:
        ax4.scatter(trades.index, trades['price'], 
                   alpha=0.8, s=15, color='green', label=f'Trades ({len(trades):,})')
    
    ax4.set_title('Combined View: Trades + Orders')
    ax4.set_xlabel('Time')
    ax4.set_ylabel('Price (EUR/MWh)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/simple_trades_orders_plot.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"   💾 Simple plot saved: {plot_path}")
    
    # Print simple summary
    print(f"\n📊 SUMMARY:")
    if len(trades) > 0:
        print(f"   📈 Trades: {len(trades):,} | Price: €{trades['price'].min():.1f}-€{trades['price'].max():.1f} | Mean: €{trades['price'].mean():.1f}")
    if len(bids) > 0:
        print(f"   📉 Bids: {len(bids):,} | Price: €{bids['b_price'].min():.1f}-€{bids['b_price'].max():.1f} | Mean: €{bids['b_price'].mean():.1f}")
    if len(asks) > 0:
        print(f"   📈 Asks: {len(asks):,} | Price: €{asks['a_price'].min():.1f}-€{asks['a_price'].max():.1f} | Mean: €{asks['a_price'].mean():.1f}")
    
    if len(bids) > 0 and len(asks) > 0:
        avg_spread = asks['a_price'].mean() - bids['b_price'].mean()
        print(f"   💰 Average Bid-Ask Spread: €{avg_spread:.2f}")
    
    print(f"\n✅ Simple trades and orders plot completed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()