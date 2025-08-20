#!/usr/bin/env python3
"""
Plot DataFetcher vs SpreadViewer trades separately from merged dataset
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import os

# Set up paths
data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"

print("📊 Separated Source Analysis - DataFetcher vs SpreadViewer")
print("=" * 60)

try:
    # Load the data
    print(f"📁 Loading data from: {os.path.basename(data_path)}")
    df = pd.read_parquet(data_path)
    
    print(f"📈 Total data loaded: {len(df)} records")
    print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
    print(f"📋 Columns: {list(df.columns)}")
    
    # Separate DataFetcher and SpreadViewer data
    print("\\n🔍 Separating data sources...")
    
    # DataFetcher trades: Have price but no bid/ask
    datafetcher_trades = df[df['price'].notna() & df['b_price'].isna()]
    
    # SpreadViewer data: Have bid/ask but no price
    spreadviewer_data = df[df['b_price'].notna() | df['a_price'].notna()]
    
    # For plotting, we need SpreadViewer trades - let's use mid price as proxy
    spreadviewer_orders = spreadviewer_data[spreadviewer_data['b_price'].notna() & spreadviewer_data['a_price'].notna()]
    spreadviewer_orders = spreadviewer_orders.copy()
    spreadviewer_orders['mid_price'] = (spreadviewer_orders['b_price'] + spreadviewer_orders['a_price']) / 2
    
    print(f"📊 DataFetcher trades: {len(datafetcher_trades):,} records")
    print(f"📊 SpreadViewer orders: {len(spreadviewer_orders):,} records")
    
    if len(datafetcher_trades) > 0:
        print(f"   💰 DataFetcher price range: ${datafetcher_trades['price'].min():.2f} - ${datafetcher_trades['price'].max():.2f}")
        print(f"   📈 DataFetcher mean: ${datafetcher_trades['price'].mean():.2f}")
    
    if len(spreadviewer_orders) > 0:
        print(f"   💚 SpreadViewer bid range: ${spreadviewer_orders['b_price'].min():.2f} - ${spreadviewer_orders['b_price'].max():.2f}")
        print(f"   💙 SpreadViewer ask range: ${spreadviewer_orders['a_price'].min():.2f} - ${spreadviewer_orders['a_price'].max():.2f}")
        print(f"   ⚪ SpreadViewer mid range: ${spreadviewer_orders['mid_price'].min():.2f} - ${spreadviewer_orders['mid_price'].max():.2f}")
    
    # Create comparison plots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 12))
    
    # Plot 1: DataFetcher trades only
    if len(datafetcher_trades) > 0:
        ax1.scatter(datafetcher_trades.index, datafetcher_trades['price'], 
                   c='red', s=50, alpha=0.7, label=f'DataFetcher Trades ({len(datafetcher_trades)})')
        ax1.set_title('DataFetcher Trades Only (Real Market Data)', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price ($)', fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    else:
        ax1.text(0.5, 0.5, 'No DataFetcher trades found', ha='center', va='center', 
                transform=ax1.transAxes, fontsize=14)
        ax1.set_title('DataFetcher Trades Only (No Data)', fontsize=14)
    
    # Plot 2: SpreadViewer data (bid/ask)
    if len(spreadviewer_orders) > 0:
        # Sample data for better performance if too many points
        if len(spreadviewer_orders) > 5000:
            sample_sv = spreadviewer_orders.sample(n=5000).sort_index()
            print(f"   📊 Sampling {len(sample_sv):,} SpreadViewer records for visualization")
        else:
            sample_sv = spreadviewer_orders
        
        ax2.plot(sample_sv.index, sample_sv['b_price'], 
                color='green', alpha=0.6, linewidth=1, label=f'SpreadViewer Bid ({len(spreadviewer_orders):,})')
        ax2.plot(sample_sv.index, sample_sv['a_price'], 
                color='blue', alpha=0.6, linewidth=1, label=f'SpreadViewer Ask ({len(spreadviewer_orders):,})')
        ax2.plot(sample_sv.index, sample_sv['mid_price'], 
                color='purple', alpha=0.8, linewidth=2, label='SpreadViewer Mid')
        ax2.set_title('SpreadViewer Data (Synthetic Spread Order Book)', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Price ($)', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    else:
        ax2.text(0.5, 0.5, 'No SpreadViewer data found', ha='center', va='center', 
                transform=ax2.transAxes, fontsize=14)
        ax2.set_title('SpreadViewer Data (No Data)', fontsize=14)
    
    # Plot 3: Combined overlay
    if len(datafetcher_trades) > 0:
        ax3.scatter(datafetcher_trades.index, datafetcher_trades['price'], 
                   c='red', s=60, alpha=0.8, label=f'DataFetcher Trades ({len(datafetcher_trades)})', 
                   marker='o', edgecolors='darkred', linewidth=1, zorder=5)
    
    if len(spreadviewer_orders) > 0:
        # Sample for overlay
        sample_sv = spreadviewer_orders.sample(n=min(2000, len(spreadviewer_orders))).sort_index()
        ax3.plot(sample_sv.index, sample_sv['mid_price'], 
                color='purple', alpha=0.4, linewidth=1, label=f'SpreadViewer Mid ({len(spreadviewer_orders):,})', zorder=1)
        ax3.fill_between(sample_sv.index, sample_sv['b_price'], sample_sv['a_price'], 
                        color='lightblue', alpha=0.2, label='Bid-Ask Spread', zorder=0)
    
    ax3.set_title('Combined View: DataFetcher Trades vs SpreadViewer Order Book', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Time', fontsize=12)
    ax3.set_ylabel('Price ($)', fontsize=12)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Format time axes
    for ax in [ax1, ax2, ax3]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/separated_sources_plot.png"
    plt.savefig(plot_path, dpi=150, bbox_inches='tight')
    print(f"\\n💾 Plot saved: {plot_path}")
    
    # Display detailed statistics
    print("\\n📊 Detailed Source Comparison:")
    
    if len(datafetcher_trades) > 0:
        print(f"\\n🔴 DataFetcher Trades ({len(datafetcher_trades)} records):")
        print(f"   Price range: ${datafetcher_trades['price'].min():.2f} - ${datafetcher_trades['price'].max():.2f}")
        print(f"   Mean price: ${datafetcher_trades['price'].mean():.2f}")
        print(f"   Median price: ${datafetcher_trades['price'].median():.2f}")
        print(f"   Std dev: ${datafetcher_trades['price'].std():.2f}")
        if 'volume' in datafetcher_trades.columns:
            print(f"   Volume range: {datafetcher_trades['volume'].min()} - {datafetcher_trades['volume'].max()}")
        if 'action' in datafetcher_trades.columns:
            action_counts = datafetcher_trades['action'].value_counts()
            print(f"   Actions: {dict(action_counts)}")
    
    if len(spreadviewer_orders) > 0:
        print(f"\\n🟣 SpreadViewer Orders ({len(spreadviewer_orders)} records):")
        print(f"   Bid range: ${spreadviewer_orders['b_price'].min():.2f} - ${spreadviewer_orders['b_price'].max():.2f}")
        print(f"   Ask range: ${spreadviewer_orders['a_price'].min():.2f} - ${spreadviewer_orders['a_price'].max():.2f}")
        print(f"   Mid range: ${spreadviewer_orders['mid_price'].min():.2f} - ${spreadviewer_orders['mid_price'].max():.2f}")
        print(f"   Mean bid: ${spreadviewer_orders['b_price'].mean():.2f}")
        print(f"   Mean ask: ${spreadviewer_orders['a_price'].mean():.2f}")
        print(f"   Mean spread: ${(spreadviewer_orders['a_price'] - spreadviewer_orders['b_price']).mean():.3f}")
    
    # Time coverage comparison
    if len(datafetcher_trades) > 0 and len(spreadviewer_orders) > 0:
        df_start, df_end = datafetcher_trades.index.min(), datafetcher_trades.index.max()
        sv_start, sv_end = spreadviewer_orders.index.min(), spreadviewer_orders.index.max()
        
        print(f"\\n⏰ Time Coverage Comparison:")
        print(f"   DataFetcher: {df_start} to {df_end} ({df_end - df_start})")
        print(f"   SpreadViewer: {sv_start} to {sv_end} ({sv_end - sv_start})")
        
        # Overlap analysis
        overlap_start = max(df_start, sv_start)
        overlap_end = min(df_end, sv_end)
        if overlap_start < overlap_end:
            print(f"   Overlap: {overlap_start} to {overlap_end} ({overlap_end - overlap_start})")
        else:
            print(f"   No time overlap between data sources")
    
    print("\\n✅ Separated source analysis completed successfully!")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()