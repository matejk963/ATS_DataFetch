#!/usr/bin/env python3
"""
Plot DataFetcher vs SpreadViewer TRADES separately using broker_id field
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# Set up paths
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"

print("📊 Separated TRADES Analysis - DataFetcher vs SpreadViewer")
print("=" * 60)

try:
    # Load the data
    print(f"📁 Loading data from: {os.path.basename(data_path)}")
    df = pd.read_parquet(data_path)
    
    print(f"📈 Total data loaded: {len(df)} records")
    print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
    
    # Focus on trade records only (have price values)
    all_trades = df[df['price'].notna()].copy()
    print(f"📊 Total trades found: {len(all_trades)} records")
    
    if len(all_trades) == 0:
        print("❌ No trades found in dataset!")
        exit(1)
    
    print(f"📋 Trade columns: {list(all_trades.columns)}")
    
    # Examine broker_id values
    broker_ids = all_trades['broker_id'].value_counts()
    print(f"🏢 Broker ID distribution:")
    for broker_id, count in broker_ids.items():
        print(f"   {broker_id}: {count} trades ({count/len(all_trades)*100:.1f}%)")
    
    # Separate trades by broker_id (identified from previous analysis)
    print("\\n🔍 Separating trade sources by broker_id...")
    
    # DataFetcher trades: broker_id == 9999.0 (synthetic trades)
    datafetcher_trades = all_trades[all_trades['broker_id'] == 9999.0].copy()
    
    # SpreadViewer trades: broker_id == 1441.0 (real exchange trades)
    spreadviewer_trades = all_trades[all_trades['broker_id'] == 1441.0].copy()
    
    # Handle any other broker_ids
    other_trades = all_trades[~all_trades['broker_id'].isin([9999.0, 1441.0])].copy()
    
    print(f"🔴 DataFetcher trades (broker_id=9999): {len(datafetcher_trades):,} records")
    print(f"🟣 SpreadViewer trades (broker_id=1441): {len(spreadviewer_trades):,} records")
    if len(other_trades) > 0:
        print(f"⚪ Other broker trades: {len(other_trades):,} records")
    
    # Display trade characteristics
    if len(datafetcher_trades) > 0:
        print(f"\\n🔴 DataFetcher Trade Characteristics:")
        print(f"   Price range: €{datafetcher_trades['price'].min():.2f} - €{datafetcher_trades['price'].max():.2f}")
        print(f"   Mean price: €{datafetcher_trades['price'].mean():.2f}")
        print(f"   Time range: {datafetcher_trades.index.min()} to {datafetcher_trades.index.max()}")
        if 'action' in datafetcher_trades.columns:
            action_counts = datafetcher_trades['action'].value_counts()
            print(f"   Actions: {dict(action_counts)}")
        
        # Sample trade IDs
        sample_ids = datafetcher_trades['tradeid'].dropna().head(3)
        if len(sample_ids) > 0:
            print(f"   Sample trade IDs: {list(sample_ids)}")
    
    if len(spreadviewer_trades) > 0:
        print(f"\\n🟣 SpreadViewer Trade Characteristics:")
        print(f"   Price range: €{spreadviewer_trades['price'].min():.2f} - €{spreadviewer_trades['price'].max():.2f}")
        print(f"   Mean price: €{spreadviewer_trades['price'].mean():.2f}")
        print(f"   Time range: {spreadviewer_trades.index.min()} to {spreadviewer_trades.index.max()}")
        if 'action' in spreadviewer_trades.columns:
            action_counts = spreadviewer_trades['action'].value_counts()
            print(f"   Actions: {dict(action_counts)}")
            
        # Sample trade IDs
        sample_ids = spreadviewer_trades['tradeid'].dropna().head(3)
        if len(sample_ids) > 0:
            print(f"   Sample trade IDs: {list(sample_ids)}")
    
    # Create three-panel comparison plots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 12))
    
    # Plot 1: DataFetcher trades only
    if len(datafetcher_trades) > 0:
        ax1.scatter(datafetcher_trades.index, datafetcher_trades['price'], 
                   c='red', s=40, alpha=0.7, label=f'DataFetcher Trades ({len(datafetcher_trades)})', marker='o')
        ax1.set_title('DataFetcher Trades (Synthetic - broker_id=9999)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price (EUR/MWh)', fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Add price statistics on plot
        ax1.text(0.02, 0.98, f'Mean: €{datafetcher_trades["price"].mean():.2f}\\n'
                             f'Range: €{datafetcher_trades["price"].min():.2f}-€{datafetcher_trades["price"].max():.2f}',
                transform=ax1.transAxes, verticalalignment='top', 
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax1.text(0.5, 0.5, 'No DataFetcher trades found', ha='center', va='center', 
                transform=ax1.transAxes, fontsize=14)
        ax1.set_title('DataFetcher Trades (No Data)', fontsize=14)
    
    # Plot 2: SpreadViewer trades only
    if len(spreadviewer_trades) > 0:
        ax2.scatter(spreadviewer_trades.index, spreadviewer_trades['price'], 
                   c='purple', s=40, alpha=0.7, label=f'SpreadViewer Trades ({len(spreadviewer_trades)})', marker='s')
        ax2.set_title('SpreadViewer Trades (Real Exchange - broker_id=1441)', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Price (EUR/MWh)', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Add price statistics on plot
        ax2.text(0.02, 0.98, f'Mean: €{spreadviewer_trades["price"].mean():.2f}\\n'
                             f'Range: €{spreadviewer_trades["price"].min():.2f}-€{spreadviewer_trades["price"].max():.2f}',
                transform=ax2.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    else:
        ax2.text(0.5, 0.5, 'No SpreadViewer trades found', ha='center', va='center', 
                transform=ax2.transAxes, fontsize=14)
        ax2.set_title('SpreadViewer Trades (No Data)', fontsize=14)
    
    # Plot 3: Combined overlay
    if len(datafetcher_trades) > 0:
        ax3.scatter(datafetcher_trades.index, datafetcher_trades['price'], 
                   c='red', s=50, alpha=0.8, label=f'DataFetcher ({len(datafetcher_trades)})', 
                   marker='o', edgecolors='darkred', linewidth=1, zorder=5)
    
    if len(spreadviewer_trades) > 0:
        ax3.scatter(spreadviewer_trades.index, spreadviewer_trades['price'], 
                   c='purple', s=50, alpha=0.8, label=f'SpreadViewer ({len(spreadviewer_trades)})', 
                   marker='s', edgecolors='darkmagenta', linewidth=1, zorder=4)
    
    if len(other_trades) > 0:
        ax3.scatter(other_trades.index, other_trades['price'], 
                   c='gray', s=30, alpha=0.6, label=f'Other ({len(other_trades)})', 
                   marker='^', zorder=3)
    
    ax3.set_title('Combined View: DataFetcher vs SpreadViewer Trades', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Time', fontsize=12)
    ax3.set_ylabel('Price (EUR/MWh)', fontsize=12)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Format time axes
    for ax in [ax1, ax2, ax3]:
        # Adjust time formatting based on data range
        if len(all_trades) > 0:
            time_span = all_trades.index.max() - all_trades.index.min()
            if time_span.days > 60:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/separated_trades_plot.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"\\n💾 Plot saved: {plot_path}")
    
    # Summary comparison
    print("\\n📊 Trade Source Comparison Summary:")
    print(f"   🔴 DataFetcher: {len(datafetcher_trades):,} trades ({len(datafetcher_trades)/len(all_trades)*100:.1f}%)")
    print(f"   🟣 SpreadViewer: {len(spreadviewer_trades):,} trades ({len(spreadviewer_trades)/len(all_trades)*100:.1f}%)")
    if len(other_trades) > 0:
        print(f"   ⚪ Other: {len(other_trades):,} trades ({len(other_trades)/len(all_trades)*100:.1f}%)")
    
    # Time overlap analysis
    if len(datafetcher_trades) > 0 and len(spreadviewer_trades) > 0:
        df_start, df_end = datafetcher_trades.index.min(), datafetcher_trades.index.max()
        sv_start, sv_end = spreadviewer_trades.index.min(), spreadviewer_trades.index.max()
        
        print(f"\\n⏰ Time Coverage:")
        print(f"   🔴 DataFetcher: {df_start} to {df_end}")
        print(f"   🟣 SpreadViewer: {sv_start} to {sv_end}")
        
        overlap_start = max(df_start, sv_start)
        overlap_end = min(df_end, sv_end)
        if overlap_start < overlap_end:
            print(f"   ⏲️  Overlap: {overlap_start} to {overlap_end} ({overlap_end - overlap_start})")
        else:
            print(f"   ❌ No time overlap between trade sources")
    
    print("\\n✅ Separated trades analysis completed successfully!")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()