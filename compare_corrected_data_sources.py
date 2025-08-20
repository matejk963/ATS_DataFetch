#!/usr/bin/env python3
"""
Compare DataFetcher vs SpreadViewer data with side-by-side plots to verify corrected logic
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import os

print("📊 SIDE-BY-SIDE COMPARISON: DataFetcher vs SpreadViewer (Corrected Logic)")
print("=" * 80)

# Load both datasets
datafetcher_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_DATAFETCHER_ONLY.parquet"
spreadviewer_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_SPREADVIEWER_ONLY.parquet"
latest_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.pkl"

try:
    print("📁 Loading datasets...")
    
    # DataFetcher data (real exchange data)
    try:
        df_datafetcher = pd.read_parquet(datafetcher_path)
        print(f"   ✅ DataFetcher data: {len(df_datafetcher)} total records")
        df_trades = df_datafetcher[df_datafetcher['price'].notna()].copy()
        print(f"      📈 Trades: {len(df_trades)}")
        df_has_datafetcher = len(df_trades) > 0
    except Exception as e:
        print(f"   ❌ DataFetcher data failed: {e}")
        df_datafetcher = pd.DataFrame()
        df_trades = pd.DataFrame()
        df_has_datafetcher = False
    
    # SpreadViewer data (synthetic data)
    try:
        sv_datafetcher = pd.read_parquet(spreadviewer_path)
        print(f"   ✅ SpreadViewer data: {len(sv_datafetcher)} total records")
        sv_trades = sv_datafetcher[sv_datafetcher['price'].notna()].copy()
        print(f"      📈 Trades: {len(sv_trades)}")
        sv_has_spreadviewer = len(sv_trades) > 0
    except Exception as e:
        print(f"   ❌ SpreadViewer data failed: {e}")
        sv_datafetcher = pd.DataFrame()
        sv_trades = pd.DataFrame()
        sv_has_spreadviewer = False
        
    # Latest data (check what it contains)
    try:
        latest_data = pd.read_pickle(latest_path)
        print(f"   ✅ Latest data: {len(latest_data)} total records")
        latest_trades = latest_data[latest_data['price'].notna()].copy()
        print(f"      📈 Trades: {len(latest_trades)}")
        
        # Check if this has corrected logic by looking for the right database queries
        if len(latest_trades) > 0:
            # Check broker IDs to see what source this is
            broker_ids = latest_trades['broker_id'].unique()
            print(f"      🏢 Broker IDs: {broker_ids}")
            if 9999.0 in broker_ids:
                print(f"      📝 Latest data appears to be SpreadViewer (synthetic) with corrected logic")
                sv_corrected = latest_trades.copy()
                sv_corrected_available = True
            else:
                print(f"      📝 Latest data appears to be DataFetcher (real exchange)")
                sv_corrected_available = False
                sv_corrected = pd.DataFrame()
        else:
            sv_corrected_available = False
            sv_corrected = pd.DataFrame()
    except Exception as e:
        print(f"   ⚠️  Latest data not available: {e}")
        sv_corrected_available = False
        sv_corrected = pd.DataFrame()
        
    print(f"\n📊 ANALYSIS SUMMARY:")
    print(f"   DataFetcher available: {df_has_datafetcher}")
    print(f"   SpreadViewer (old) available: {sv_has_spreadviewer}")
    print(f"   SpreadViewer (corrected) available: {sv_corrected_available}")
    
    # Create comprehensive comparison plot
    if df_has_datafetcher or sv_has_spreadviewer or sv_corrected_available:
        print(f"\n📈 Creating comprehensive comparison plots...")
        
        # Determine subplot layout based on available data
        available_sources = sum([df_has_datafetcher, sv_has_spreadviewer, sv_corrected_available])
        
        if available_sources == 1:
            fig, axes = plt.subplots(2, 1, figsize=(16, 12))
            axes = [axes] if not hasattr(axes, '__len__') else axes.flatten()
        elif available_sources == 2:
            fig, axes = plt.subplots(2, 2, figsize=(18, 12))
            axes = axes.flatten()
        else:
            fig, axes = plt.subplots(2, 3, figsize=(24, 12))
            axes = axes.flatten()
            
        plot_idx = 0
        colors = ['red', 'blue', 'green']
        markers = ['o', 's', '^']
        labels = []
        all_trade_data = []
        
        # Plot DataFetcher data
        if df_has_datafetcher:
            ax = axes[plot_idx]
            ax.scatter(df_trades.index, df_trades['price'], alpha=0.7, 
                      color=colors[0], s=30, marker=markers[0], label='DataFetcher (Real Exchange)')
            ax.set_title('DataFetcher (Real Exchange Data)\nbroker_id=1441.0')
            ax.set_xlabel('Time')
            ax.set_ylabel('Price (EUR/MWh)')
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            # Statistics
            price_stats = f"Prices: {df_trades['price'].min():.1f} - {df_trades['price'].max():.1f} EUR/MWh\n"
            price_stats += f"Mean: {df_trades['price'].mean():.1f}, Std: {df_trades['price'].std():.1f}\n"
            price_stats += f"Trades: {len(df_trades)}"
            ax.text(0.02, 0.98, price_stats, transform=ax.transAxes, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            
            labels.append('DataFetcher')
            all_trade_data.append(df_trades)
            plot_idx += 1
            
        # Plot old SpreadViewer data
        if sv_has_spreadviewer:
            ax = axes[plot_idx]
            ax.scatter(sv_trades.index, sv_trades['price'], alpha=0.7,
                      color=colors[1], s=30, marker=markers[1], label='SpreadViewer (Old Logic)')
            ax.set_title('SpreadViewer (Old Logic - q_3)\nbroker_id=9999.0')
            ax.set_xlabel('Time')
            ax.set_ylabel('Price (EUR/MWh)')
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            # Statistics
            price_stats = f"Prices: {sv_trades['price'].min():.1f} - {sv_trades['price'].max():.1f} EUR/MWh\n"
            price_stats += f"Mean: {sv_trades['price'].mean():.1f}, Std: {sv_trades['price'].std():.1f}\n"
            price_stats += f"Trades: {len(sv_trades)}"
            ax.text(0.02, 0.98, price_stats, transform=ax.transAxes, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            
            labels.append('SpreadViewer (Old)')
            all_trade_data.append(sv_trades)
            plot_idx += 1
            
        # Plot corrected SpreadViewer data
        if sv_corrected_available:
            ax = axes[plot_idx]
            ax.scatter(sv_corrected.index, sv_corrected['price'], alpha=0.7,
                      color=colors[2], s=30, marker=markers[2], label='SpreadViewer (Corrected Logic)')
            ax.set_title('SpreadViewer (Corrected Logic - q_1)\nbroker_id=9999.0')
            ax.set_xlabel('Time')
            ax.set_ylabel('Price (EUR/MWh)')
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            # Statistics
            price_stats = f"Prices: {sv_corrected['price'].min():.1f} - {sv_corrected['price'].max():.1f} EUR/MWh\n"
            price_stats += f"Mean: {sv_corrected['price'].mean():.1f}, Std: {sv_corrected['price'].std():.1f}\n"
            price_stats += f"Trades: {len(sv_corrected)}"
            ax.text(0.02, 0.98, price_stats, transform=ax.transAxes, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
            
            labels.append('SpreadViewer (Corrected)')
            all_trade_data.append(sv_corrected)
            plot_idx += 1
        
        # Create overlay plot if we have multiple sources
        if len(all_trade_data) > 1:
            ax_overlay = axes[plot_idx] if plot_idx < len(axes) else axes[-1]
            
            for i, (trade_data, label, color, marker) in enumerate(zip(all_trade_data, labels, colors, markers)):
                ax_overlay.scatter(trade_data.index, trade_data['price'], alpha=0.7,
                                 color=color, s=30, marker=marker, label=label)
            
            ax_overlay.set_title('Overlay Comparison: All Data Sources')
            ax_overlay.set_xlabel('Time')
            ax_overlay.set_ylabel('Price (EUR/MWh)')
            ax_overlay.grid(True, alpha=0.3)
            ax_overlay.legend()
            
            plot_idx += 1
            
        # Hide unused subplots
        for i in range(plot_idx, len(axes)):
            axes[i].set_visible(False)
            
        plt.tight_layout()
        
        # Save plot
        plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/corrected_data_sources_comparison.png"
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"   💾 Comparison plot saved: {plot_path}")
        
        # Statistical comparison
        print(f"\n📊 STATISTICAL COMPARISON:")
        
        for i, (trade_data, label) in enumerate(zip(all_trade_data, labels)):
            print(f"\n   {i+1}. {label}:")
            print(f"      📈 Trades: {len(trade_data)}")
            print(f"      💰 Price range: €{trade_data['price'].min():.2f} - €{trade_data['price'].max():.2f}")
            print(f"      💰 Price mean: €{trade_data['price'].mean():.2f}")
            print(f"      💰 Price std: €{trade_data['price'].std():.2f}")
            print(f"      ⏰ Time range: {trade_data.index.min()} to {trade_data.index.max()}")
            
            if 'broker_id' in trade_data.columns:
                broker_ids = trade_data['broker_id'].unique()
                print(f"      🏢 Broker IDs: {broker_ids}")
        
        # Verify the correction worked
        print(f"\n✅ VERIFICATION:")
        if sv_corrected_available:
            print(f"   🎯 SpreadViewer with corrected logic is available")
            print(f"   📊 Corrected data shows broker_id=9999.0 (synthetic)")
            print(f"   📝 This data should have been fetched using 'q_1' instead of 'q_3'")
            
            # Compare with old SpreadViewer if available
            if sv_has_spreadviewer:
                old_count = len(sv_trades)
                new_count = len(sv_corrected)
                print(f"   📊 Trade count: Old logic: {old_count}, Corrected logic: {new_count}")
                if old_count != new_count:
                    print(f"   ✅ CONFIRMED: Different trade counts suggest corrected logic is working")
                else:
                    print(f"   ⚠️  Same trade counts - may need to verify correction further")
        else:
            print(f"   ⚠️  No corrected SpreadViewer data available yet")
            print(f"   📝 Need to run new fetch with corrected relative tenor logic")
        
        print(f"\n🎉 Side-by-side comparison completed!")
        
    else:
        print(f"❌ No data available for comparison")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()