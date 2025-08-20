#!/usr/bin/env python3
"""
Analyze the price spike around €32.5 to identify when it occurred
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

print("🔍 ANALYZING PRICE SPIKE AROUND €32.5")
print("=" * 50)

# Load the data
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.pkl"

try:
    # Load the data
    print(f"📁 Loading data...")
    df = pd.read_pickle(data_path)
    
    # Get trades only
    trades = df[df['price'].notna()].copy()
    print(f"   ✅ Total trades: {len(trades):,}")
    print(f"   💰 Overall price range: €{trades['price'].min():.2f} - €{trades['price'].max():.2f}")
    
    # Find high-price trades (above €30)
    high_price_trades = trades[trades['price'] > 30.0].copy()
    print(f"\n🔥 HIGH PRICE TRADES (>€30):")
    print(f"   📈 Count: {len(high_price_trades)} trades")
    
    if len(high_price_trades) > 0:
        print(f"   💰 Price range: €{high_price_trades['price'].min():.2f} - €{high_price_trades['price'].max():.2f}")
        print(f"   📅 Time range: {high_price_trades.index.min()} to {high_price_trades.index.max()}")
        
        # Find the exact spike around €32.5
        spike_trades = trades[trades['price'] > 32.0].copy()
        print(f"\n⚡ SPIKE TRADES (>€32):")
        print(f"   📈 Count: {len(spike_trades)} trades")
        
        if len(spike_trades) > 0:
            print(f"   💰 Price range: €{spike_trades['price'].min():.2f} - €{spike_trades['price'].max():.2f}")
            print(f"   📅 Time range: {spike_trades.index.min()} to {spike_trades.index.max()}")
            
            # Show details of the highest price trades
            highest_trades = spike_trades.nlargest(10, 'price')
            print(f"\n🎯 TOP 10 HIGHEST PRICE TRADES:")
            for i, (timestamp, row) in enumerate(highest_trades.iterrows(), 1):
                broker_type = "Real" if row['broker_id'] == 1441.0 else "Synthetic"
                print(f"   {i:2}. {timestamp} | €{row['price']:.2f} | Vol: {row['volume']:.1f} | {broker_type}")
            
            # Analyze the time period of the spike
            spike_start = spike_trades.index.min()
            spike_end = spike_trades.index.max()
            spike_duration = spike_end - spike_start
            
            print(f"\n⏰ SPIKE TIMING ANALYSIS:")
            print(f"   📅 Spike start: {spike_start}")
            print(f"   📅 Spike end: {spike_end}")
            print(f"   ⏱️  Duration: {spike_duration}")
            print(f"   📊 Day of week: {spike_start.strftime('%A')}")
            print(f"   🕐 Time of day: {spike_start.strftime('%H:%M:%S')}")
            
            # Check what was happening around that time
            spike_date = spike_start.date()
            daily_trades = trades[trades.index.date == spike_date].copy()
            
            print(f"\n📅 ACTIVITY ON SPIKE DAY ({spike_date}):")
            print(f"   📈 Total trades that day: {len(daily_trades)}")
            print(f"   💰 Price range that day: €{daily_trades['price'].min():.2f} - €{daily_trades['price'].max():.2f}")
            print(f"   💰 Average price that day: €{daily_trades['price'].mean():.2f}")
            
            # Hourly breakdown for the spike day
            daily_trades['hour'] = daily_trades.index.hour
            hourly_stats = daily_trades.groupby('hour').agg({
                'price': ['count', 'min', 'max', 'mean']
            }).round(2)
            hourly_stats.columns = ['Count', 'Min_Price', 'Max_Price', 'Avg_Price']
            
            print(f"\n⏰ HOURLY BREAKDOWN FOR SPIKE DAY:")
            print(hourly_stats)
            
            # Create focused plot of the spike
            print(f"\n📊 Creating spike analysis plot...")
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
            
            # Plot 1: Full time series with spike highlighted
            ax1.scatter(trades.index, trades['price'], alpha=0.6, s=15, color='gray', label='All Trades')
            ax1.scatter(high_price_trades.index, high_price_trades['price'], 
                       alpha=0.8, s=30, color='orange', label='High Price (>€30)')
            ax1.scatter(spike_trades.index, spike_trades['price'], 
                       alpha=1.0, s=50, color='red', label='Spike (>€32)')
            
            ax1.axhline(y=30, color='orange', linestyle='--', alpha=0.7, label='€30 threshold')
            ax1.axhline(y=32, color='red', linestyle='--', alpha=0.7, label='€32 threshold')
            
            ax1.set_title('Price Spike Analysis - Full Timeline')
            ax1.set_xlabel('Time')
            ax1.set_ylabel('Price (EUR/MWh)')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # Plot 2: Zoomed in on spike day
            if len(daily_trades) > 0:
                ax2.scatter(daily_trades.index, daily_trades['price'], 
                           alpha=0.7, s=20, color='blue', label=f'All trades {spike_date}')
                ax2.scatter(spike_trades.index, spike_trades['price'], 
                           alpha=1.0, s=60, color='red', label='Spike trades')
                
                ax2.set_title(f'Spike Day Detail - {spike_date} ({spike_start.strftime("%A")})')
                ax2.set_xlabel('Time')
                ax2.set_ylabel('Price (EUR/MWh)')
                ax2.legend()
                ax2.grid(True, alpha=0.3)
                
                # Add annotations for highest spikes
                for timestamp, row in highest_trades.head(3).iterrows():
                    ax2.annotate(f'€{row["price"]:.2f}', 
                               xy=(timestamp, row['price']), 
                               xytext=(10, 10), textcoords='offset points',
                               bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                               arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
            
            plt.tight_layout()
            
            # Save plot
            plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/price_spike_analysis.png"
            plt.savefig(plot_path, dpi=150, bbox_inches='tight')
            print(f"   💾 Spike analysis plot saved: {plot_path}")
            
            # Check if this was real or synthetic data
            spike_sources = spike_trades['broker_id'].value_counts()
            print(f"\n🔍 SPIKE DATA SOURCE:")
            for broker_id, count in spike_sources.items():
                source = "Real (DataFetcher)" if broker_id == 1441.0 else "Synthetic (SpreadViewer)"
                print(f"   📊 {source}: {count} trades")
            
        else:
            print(f"   ❌ No trades found above €32")
    else:
        print(f"   ❌ No trades found above €30")
    
    print(f"\n✅ Price spike analysis completed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()