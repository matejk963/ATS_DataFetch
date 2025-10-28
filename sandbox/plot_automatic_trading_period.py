#!/usr/bin/env python3
"""
Plot Automatic Trading Period Data
==================================

Plot spread data from the automatic trading period feature.
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def plot_automatic_period_spread():
    """Plot spread data with automatic trading periods"""
    
    # Load one of the new spread files
    spread_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm1_25_debq1_25_tr_ba_data_data_fetch_engine_method_synthetic.parquet'
    
    if not Path(spread_file).exists():
        print(f"❌ Spread file not found: {spread_file}")
        return
    
    # Load data
    data = pd.read_parquet(spread_file)
    data.index = pd.to_datetime(data.index)
    
    print(f"📊 LOADED DATA - DEBM1_25 vs DEBQ1_25 (Automatic Trading Period):")
    print(f"   Total records: {len(data):,}")
    print(f"   Date range: {data.index.min()} to {data.index.max()}")
    print(f"   Columns: {list(data.columns)}")
    
    # Check action types
    action_counts = data['action'].value_counts()
    print(f"\n📈 ACTION DISTRIBUTION:")
    for action, count in action_counts.items():
        action_type = "TRADE" if action in [1.0, -1.0] else "ORDER"
        print(f"   {action}: {count:,} ({action_type})")
    
    # Filter only trades (prices)
    trades = data[data['action'].isin([1.0, -1.0])]
    orders = data[~data['action'].isin([1.0, -1.0])]
    
    # Create plot with datetime x-axis
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))
    
    # Plot trades with datetime x-axis
    if len(trades) > 0:
        ax1.plot(trades.index, trades['price'], linewidth=0.5, alpha=0.7, color='blue', marker='o', markersize=2)
        ax1.set_title('DEBM1_25 vs DEBQ1_25 Spread - Trade Prices (Automatic Trading Period)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price (EUR/MWh)', fontsize=12)
        ax1.grid(True, alpha=0.3)
        ax1.tick_params(axis='x', rotation=45)
        
        # Add stats
        ax1.text(0.02, 0.98, f'Trade Records: {len(trades):,}\nPeriod: {trades.index.min().strftime("%Y-%m-%d")} to {trades.index.max().strftime("%Y-%m-%d")}\nPrice range: {trades["price"].min():.3f} - {trades["price"].max():.3f}\nMean: {trades["price"].mean():.3f}\nStd: {trades["price"].std():.3f}', 
                 transform=ax1.transAxes, verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        print(f"\n💰 TRADE PRICES:")
        print(f"   Records: {len(trades):,}")
        print(f"   Period: {trades.index.min().strftime('%Y-%m-%d')} to {trades.index.max().strftime('%Y-%m-%d')}")
        print(f"   Price range: {trades['price'].min():.3f} to {trades['price'].max():.3f} EUR/MWh")
        print(f"   Mean price: {trades['price'].mean():.3f} EUR/MWh")
        print(f"   Standard deviation: {trades['price'].std():.3f}")
    else:
        ax1.text(0.5, 0.5, 'No trade data found', transform=ax1.transAxes, ha='center', fontsize=14)
        print(f"\n❌ No trade data found")
    
    # Plot orders (if any) with datetime x-axis  
    if len(orders) > 0:
        # Sample orders to avoid overcrowding
        sample_orders = orders.sample(min(1000, len(orders))) if len(orders) > 1000 else orders
        ax2.scatter(sample_orders.index, sample_orders['price'], s=1, alpha=0.5, color='green')
        ax2.set_title('DEBM1_25 vs DEBQ1_25 Spread - Order Prices (Sample)', fontsize=14, fontweight='bold')
        ax2.set_xlabel('DateTime', fontsize=12)
        ax2.set_ylabel('Price (EUR/MWh)', fontsize=12)
        ax2.grid(True, alpha=0.3)
        ax2.tick_params(axis='x', rotation=45)
        
        # Add stats
        ax2.text(0.02, 0.98, f'Order Records: {len(orders):,} (showing {len(sample_orders):,})\nPrice range: {orders["price"].min():.3f} - {orders["price"].max():.3f}\nMean: {orders["price"].mean():.3f}', 
                 transform=ax2.transAxes, verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))
        
        print(f"\n📋 ORDER PRICES:")
        print(f"   Records: {len(orders):,}")
        print(f"   Price range: {orders['price'].min():.3f} to {orders['price'].max():.3f} EUR/MWh")
        print(f"   Mean price: {orders['price'].mean():.3f} EUR/MWh")
    else:
        ax2.text(0.5, 0.5, 'No order data found', transform=ax2.transAxes, ha='center', fontsize=14)
        print(f"\n❌ No order data found")
    
    plt.tight_layout()
    plt.show()
    
    # Calculate trading period info
    if len(data) > 0:
        period_days = (data.index.max() - data.index.min()).days
        print(f"\n📅 AUTOMATIC TRADING PERIOD:")
        print(f"   Start: {data.index.min().strftime('%Y-%m-%d %H:%M')}")
        print(f"   End: {data.index.max().strftime('%Y-%m-%d %H:%M')}")
        print(f"   Duration: {period_days} days")
        print(f"   ✅ This represents the last 2 months before contract delivery!")

if __name__ == "__main__":
    plot_automatic_period_spread()