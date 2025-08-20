#!/usr/bin/env python3
"""
Plot the latest refetched data - trades and orders separately
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import json

print("📊 PLOTTING LATEST REFETCHED DATA (TRADES & ORDERS)")
print("=" * 70)

# Load the latest data
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.pkl"
metadata_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data_metadata.json"

try:
    # Load metadata
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    print(f"📋 Latest Data Information:")
    print(f"   📅 Timestamp: {metadata['timestamp']}")
    print(f"   📊 Data source: {metadata['data_source']}")
    print(f"   📈 Total records: {metadata['unified_data_info']['total_records']:,}")
    print(f"   📅 Date range: {metadata['unified_data_info']['date_range']['start']} to {metadata['unified_data_info']['date_range']['end']}")
    print(f"   📊 Contracts: {metadata['contracts']}")
    print(f"   📊 Period: {metadata['period']['start_date']} to {metadata['period']['end_date']}")
    
    if 'merged_spread_data_stats' in metadata:
        stats = metadata['merged_spread_data_stats']
        print(f"   📈 Real trades: {stats['real_trades']:,}")
        print(f"   📈 Synthetic trades: {stats['synthetic_trades']:,}")
        print(f"   📈 Total trades: {stats['merged_trades']:,}")
        print(f"   📋 Real orders: {stats['real_orders']:,}")
        print(f"   📋 Synthetic orders: {stats['synthetic_orders']:,}")
        print(f"   📋 Total orders: {stats['merged_orders']:,}")
    
    # Load the actual data
    print(f"\n📁 Loading refetched data...")
    df = pd.read_pickle(data_path)
    print(f"   ✅ Loaded: {len(df):,} total records")
    
    # Separate trades and orders
    trades = df[df['price'].notna()].copy()
    orders = df[df['price'].isna()].copy()
    
    print(f"   📈 Trades: {len(trades):,}")
    print(f"   📋 Orders: {len(orders):,}")
    
    # Separate by data source (broker_id)
    real_trades = trades[trades['broker_id'] == 1441.0].copy() if 'broker_id' in trades.columns else pd.DataFrame()
    synthetic_trades = trades[trades['broker_id'] == 9999.0].copy() if 'broker_id' in trades.columns else pd.DataFrame()
    
    real_orders = orders[orders['broker_id'] == 1441.0].copy() if 'broker_id' in orders.columns else pd.DataFrame()
    synthetic_orders = orders[orders['broker_id'] == 9999.0].copy() if 'broker_id' in orders.columns else pd.DataFrame()
    
    print(f"\n📊 DATA BREAKDOWN:")
    print(f"   📈 Real trades (DataFetcher): {len(real_trades):,}")
    print(f"   📈 Synthetic trades (SpreadViewer): {len(synthetic_trades):,}")
    print(f"   📋 Real orders (DataFetcher): {len(real_orders):,}")
    print(f"   📋 Synthetic orders (SpreadViewer): {len(synthetic_orders):,}")
    
    # Create comprehensive plots
    print(f"\n📈 Creating comprehensive plots...")
    
    fig = plt.figure(figsize=(24, 16))
    
    # Plot 1: All Trades Over Time (Top Left)
    ax1 = plt.subplot(3, 3, 1)
    if len(real_trades) > 0:
        ax1.scatter(real_trades.index, real_trades['price'], alpha=0.6, s=20, 
                   color='red', label=f'Real Trades ({len(real_trades):,})', marker='o')
    if len(synthetic_trades) > 0:
        ax1.scatter(synthetic_trades.index, synthetic_trades['price'], alpha=0.6, s=20,
                   color='blue', label=f'Synthetic Trades ({len(synthetic_trades):,})', marker='s')
    ax1.set_title('All Trades Over Time')
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Price (EUR/MWh)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Real Trades Detail (Top Center)
    ax2 = plt.subplot(3, 3, 2)
    if len(real_trades) > 0:
        scatter = ax2.scatter(real_trades.index, real_trades['price'], 
                             c=real_trades['volume'], cmap='Reds', alpha=0.7, s=30)
        plt.colorbar(scatter, ax=ax2, label='Volume')
        ax2.set_title(f'Real Trades Detail\n{len(real_trades):,} trades')
        
        # Stats
        stats_text = f"Price: €{real_trades['price'].min():.1f} - €{real_trades['price'].max():.1f}\n"
        stats_text += f"Mean: €{real_trades['price'].mean():.1f}"
        ax2.text(0.02, 0.98, stats_text, transform=ax2.transAxes, 
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax2.text(0.5, 0.5, 'No Real Trades', transform=ax2.transAxes, ha='center', va='center')
        ax2.set_title('Real Trades Detail\nNo Data')
    ax2.set_xlabel('Time')
    ax2.set_ylabel('Price (EUR/MWh)')
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Synthetic Trades Detail (Top Right)
    ax3 = plt.subplot(3, 3, 3)
    if len(synthetic_trades) > 0:
        scatter = ax3.scatter(synthetic_trades.index, synthetic_trades['price'], 
                             c=synthetic_trades['volume'], cmap='Blues', alpha=0.7, s=30)
        plt.colorbar(scatter, ax=ax3, label='Volume')
        ax3.set_title(f'Synthetic Trades Detail\n{len(synthetic_trades):,} trades')
        
        # Stats
        stats_text = f"Price: €{synthetic_trades['price'].min():.1f} - €{synthetic_trades['price'].max():.1f}\n"
        stats_text += f"Mean: €{synthetic_trades['price'].mean():.1f}"
        ax3.text(0.02, 0.98, stats_text, transform=ax3.transAxes, 
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax3.text(0.5, 0.5, 'No Synthetic Trades', transform=ax3.transAxes, ha='center', va='center')
        ax3.set_title('Synthetic Trades Detail\nNo Data')
    ax3.set_xlabel('Time')
    ax3.set_ylabel('Price (EUR/MWh)')
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Real Orders (Bid/Ask) (Middle Left)
    ax4 = plt.subplot(3, 3, 4)
    if len(real_orders) > 0:
        real_bids = real_orders[real_orders['b_price'].notna()]
        real_asks = real_orders[real_orders['a_price'].notna()]
        
        if len(real_bids) > 0:
            sample_size = min(2000, len(real_bids))
            bid_sample = real_bids.sample(sample_size).sort_index()
            ax4.scatter(bid_sample.index, bid_sample['b_price'], 
                       alpha=0.4, s=10, color='darkred', label=f'Bids ({len(real_bids):,})')
        
        if len(real_asks) > 0:
            sample_size = min(2000, len(real_asks))
            ask_sample = real_asks.sample(sample_size).sort_index()
            ax4.scatter(ask_sample.index, ask_sample['a_price'], 
                       alpha=0.4, s=10, color='darkblue', label=f'Asks ({len(real_asks):,})')
        
        ax4.set_title(f'Real Orders (DataFetcher)\n{len(real_orders):,} total orders')
        ax4.legend()
    else:
        ax4.text(0.5, 0.5, 'No Real Orders', transform=ax4.transAxes, ha='center', va='center')
        ax4.set_title('Real Orders\nNo Data')
    ax4.set_xlabel('Time')
    ax4.set_ylabel('Price (EUR/MWh)')
    ax4.grid(True, alpha=0.3)
    
    # Plot 5: Synthetic Orders (Bid/Ask) (Middle Center)
    ax5 = plt.subplot(3, 3, 5)
    if len(synthetic_orders) > 0:
        synth_bids = synthetic_orders[synthetic_orders['b_price'].notna()]
        synth_asks = synthetic_orders[synthetic_orders['a_price'].notna()]
        
        if len(synth_bids) > 0:
            sample_size = min(2000, len(synth_bids))
            bid_sample = synth_bids.sample(sample_size).sort_index()
            ax5.scatter(bid_sample.index, bid_sample['b_price'], 
                       alpha=0.4, s=10, color='lightcoral', label=f'Bids ({len(synth_bids):,})')
        
        if len(synth_asks) > 0:
            sample_size = min(2000, len(synth_asks))
            ask_sample = synth_asks.sample(sample_size).sort_index()
            ax5.scatter(ask_sample.index, ask_sample['a_price'], 
                       alpha=0.4, s=10, color='lightblue', label=f'Asks ({len(synth_asks):,})')
        
        ax5.set_title(f'Synthetic Orders (SpreadViewer)\n{len(synthetic_orders):,} total orders')
        ax5.legend()
    else:
        ax5.text(0.5, 0.5, 'No Synthetic Orders', transform=ax5.transAxes, ha='center', va='center')
        ax5.set_title('Synthetic Orders\nNo Data')
    ax5.set_xlabel('Time')
    ax5.set_ylabel('Price (EUR/MWh)')
    ax5.grid(True, alpha=0.3)
    
    # Plot 6: Price Distributions (Middle Right)
    ax6 = plt.subplot(3, 3, 6)
    if len(real_trades) > 0:
        ax6.hist(real_trades['price'], bins=30, alpha=0.6, color='red', 
                label=f'Real ({len(real_trades):,})', density=True)
    if len(synthetic_trades) > 0:
        ax6.hist(synthetic_trades['price'], bins=30, alpha=0.6, color='blue', 
                label=f'Synthetic ({len(synthetic_trades):,})', density=True)
    ax6.set_title('Trade Price Distributions')
    ax6.set_xlabel('Price (EUR/MWh)')
    ax6.set_ylabel('Density')
    ax6.legend()
    ax6.grid(True, alpha=0.3)
    
    # Plot 7: Daily Trade Counts (Bottom Left)
    ax7 = plt.subplot(3, 3, 7)
    if len(trades) > 0:
        trades['date'] = trades.index.date
        daily_real = real_trades.groupby(real_trades.index.date).size() if len(real_trades) > 0 else pd.Series(dtype=int)
        daily_synth = synthetic_trades.groupby(synthetic_trades.index.date).size() if len(synthetic_trades) > 0 else pd.Series(dtype=int)
        
        if len(daily_real) > 0:
            ax7.plot(daily_real.index, daily_real.values, 'r-o', alpha=0.7, label='Real Trades')
        if len(daily_synth) > 0:
            ax7.plot(daily_synth.index, daily_synth.values, 'b-s', alpha=0.7, label='Synthetic Trades')
        
        ax7.set_title('Daily Trade Counts')
        ax7.set_xlabel('Date')
        ax7.set_ylabel('Number of Trades')
        ax7.legend()
        ax7.tick_params(axis='x', rotation=45)
    else:
        ax7.text(0.5, 0.5, 'No Trade Data', transform=ax7.transAxes, ha='center', va='center')
        ax7.set_title('Daily Trade Counts\nNo Data')
    ax7.grid(True, alpha=0.3)
    
    # Plot 8: Hourly Trading Pattern (Bottom Center)
    ax8 = plt.subplot(3, 3, 8)
    if len(trades) > 0:
        hourly_real = real_trades.groupby(real_trades.index.hour).size() if len(real_trades) > 0 else pd.Series(dtype=int)
        hourly_synth = synthetic_trades.groupby(synthetic_trades.index.hour).size() if len(synthetic_trades) > 0 else pd.Series(dtype=int)
        
        hours = range(24)
        real_counts = [hourly_real.get(h, 0) for h in hours]
        synth_counts = [hourly_synth.get(h, 0) for h in hours]
        
        ax8.bar([h-0.2 for h in hours], real_counts, width=0.4, alpha=0.7, color='red', label='Real')
        ax8.bar([h+0.2 for h in hours], synth_counts, width=0.4, alpha=0.7, color='blue', label='Synthetic')
        
        ax8.set_title('Hourly Trading Pattern')
        ax8.set_xlabel('Hour of Day')
        ax8.set_ylabel('Number of Trades')
        ax8.legend()
        ax8.set_xticks(range(0, 24, 2))
    else:
        ax8.text(0.5, 0.5, 'No Trade Data', transform=ax8.transAxes, ha='center', va='center')
        ax8.set_title('Hourly Trading Pattern\nNo Data')
    ax8.grid(True, alpha=0.3)
    
    # Plot 9: Volume Analysis (Bottom Right)
    ax9 = plt.subplot(3, 3, 9)
    if len(trades) > 0:
        if len(real_trades) > 0:
            ax9.scatter(real_trades['price'], real_trades['volume'], alpha=0.6, s=20, 
                       color='red', label=f'Real ({len(real_trades):,})')
        if len(synthetic_trades) > 0:
            ax9.scatter(synthetic_trades['price'], synthetic_trades['volume'], alpha=0.6, s=20,
                       color='blue', label=f'Synthetic ({len(synthetic_trades):,})')
        
        ax9.set_title('Price vs Volume')
        ax9.set_xlabel('Price (EUR/MWh)')
        ax9.set_ylabel('Volume')
        ax9.legend()
    else:
        ax9.text(0.5, 0.5, 'No Trade Data', transform=ax9.transAxes, ha='center', va='center')
        ax9.set_title('Price vs Volume\nNo Data')
    ax9.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/latest_refetched_comprehensive_analysis.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"   💾 Comprehensive plot saved: {plot_path}")
    
    # Print summary statistics
    print(f"\n📊 COMPREHENSIVE ANALYSIS SUMMARY:")
    print(f"   📅 Data Period: {metadata['period']['start_date']} to {metadata['period']['end_date']}")
    print(f"   📈 Total Trades: {len(trades):,}")
    print(f"      • Real (DataFetcher): {len(real_trades):,}")
    print(f"      • Synthetic (SpreadViewer): {len(synthetic_trades):,}")
    print(f"   📋 Total Orders: {len(orders):,}")
    print(f"      • Real (DataFetcher): {len(real_orders):,}")
    print(f"      • Synthetic (SpreadViewer): {len(synthetic_orders):,}")
    
    if len(trades) > 0:
        print(f"   💰 Price Statistics:")
        print(f"      • Overall range: €{trades['price'].min():.2f} - €{trades['price'].max():.2f}")
        print(f"      • Overall mean: €{trades['price'].mean():.2f}")
        if len(real_trades) > 0:
            print(f"      • Real trades mean: €{real_trades['price'].mean():.2f}")
        if len(synthetic_trades) > 0:
            print(f"      • Synthetic trades mean: €{synthetic_trades['price'].mean():.2f}")
    
    print(f"\n🎉 Latest refetched data analysis completed!")
    print(f"   ✅ This shows your merged dataset with corrected relative tenor logic")
    print(f"   ✅ Both DataFetcher (real) and SpreadViewer (synthetic) data included")
    print(f"   ✅ Comprehensive 3.5-month period analysis")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()