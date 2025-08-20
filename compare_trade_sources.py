#!/usr/bin/env python3
"""
Compare trade characteristics from DataFetcher vs SpreadViewer to identify distinguishing features
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import os

print("🔍 TRADE SOURCE COMPARISON ANALYSIS")
print("=" * 60)

# Load both datasets
datafetcher_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_DATAFETCHER_ONLY.parquet"
spreadviewer_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_SPREADVIEWER_ONLY.parquet"

try:
    print("📁 Loading DataFetcher-only data...")
    df_datafetcher = pd.read_parquet(datafetcher_path)
    print(f"   📊 DataFetcher dataset: {len(df_datafetcher)} total records")
    
    print("📁 Loading SpreadViewer-only data...")
    df_spreadviewer = pd.read_parquet(spreadviewer_path)
    print(f"   📊 SpreadViewer dataset: {len(df_spreadviewer)} total records")
    
    # Focus on trade records only
    df_trades = df_datafetcher[df_datafetcher['price'].notna()].copy()
    sv_trades = df_spreadviewer[df_spreadviewer['price'].notna()].copy()
    
    print(f"\\n🔍 TRADE RECORDS ONLY:")
    print(f"   📈 DataFetcher trades: {len(df_trades)}")
    print(f"   📈 SpreadViewer trades: {len(sv_trades)}")
    
    if len(df_trades) == 0:
        print("   ⚠️  No DataFetcher trades found!")
    if len(sv_trades) == 0:
        print("   ⚠️  No SpreadViewer trades found!")
        
    print("\\n" + "="*60)
    print("DETAILED COMPARISON ANALYSIS")
    print("="*60)
    
    # Function to analyze dataset characteristics
    def analyze_trades(df, source_name):
        print(f"\\n🔍 {source_name.upper()} TRADE ANALYSIS:")
        print(f"   📊 Total trades: {len(df)}")
        
        if len(df) == 0:
            print(f"   ❌ No {source_name} trades to analyze")
            return
            
        # Basic statistics
        print(f"   💰 Price range: €{df['price'].min():.2f} - €{df['price'].max():.2f}")
        print(f"   💰 Price mean: €{df['price'].mean():.2f}")
        print(f"   💰 Price median: €{df['price'].median():.2f}")
        print(f"   💰 Price std: €{df['price'].std():.2f}")
        
        # Volume analysis
        if 'volume' in df.columns:
            vol_data = df['volume'].dropna()
            if len(vol_data) > 0:
                print(f"   📦 Volume range: {vol_data.min():.1f} - {vol_data.max():.1f}")
                print(f"   📦 Volume mean: {vol_data.mean():.2f}")
                print(f"   📦 Volume unique values: {sorted(vol_data.unique())}")
        
        # Action analysis  
        if 'action' in df.columns:
            action_data = df['action'].dropna()
            if len(action_data) > 0:
                action_counts = action_data.value_counts().sort_index()
                print(f"   ⚡ Actions: {dict(action_counts)}")
        
        # Broker ID analysis
        if 'broker_id' in df.columns:
            broker_data = df['broker_id'].dropna()
            if len(broker_data) > 0:
                broker_counts = broker_data.value_counts().sort_index()
                print(f"   🏢 Broker IDs: {dict(broker_counts)}")
        
        # Trade ID pattern analysis
        if 'tradeid' in df.columns:
            tradeid_data = df['tradeid'].dropna()
            if len(tradeid_data) > 0:
                print(f"   🏷️  Trade ID samples (first 5):")
                for i, trade_id in enumerate(tradeid_data.head(5)):
                    print(f"      {i+1}. {trade_id}")
                
                # Pattern analysis
                patterns = {}
                for trade_id in tradeid_data:
                    if 'synth_' in str(trade_id):
                        patterns['synthetic'] = patterns.get('synthetic', 0) + 1
                    elif 'Eurex' in str(trade_id):
                        patterns['eurex'] = patterns.get('eurex', 0) + 1
                    elif 'T7' in str(trade_id):
                        patterns['t7'] = patterns.get('t7', 0) + 1
                    else:
                        patterns['other'] = patterns.get('other', 0) + 1
                
                if patterns:
                    print(f"   🔍 Trade ID patterns: {patterns}")
        
        # Time coverage
        print(f"   ⏰ Time range: {df.index.min()} to {df.index.max()}")
        print(f"   ⏰ Time span: {df.index.max() - df.index.min()}")
        
        # Column analysis
        print(f"   📋 Available columns: {list(df.columns)}")
        
        # NaN analysis for key fields
        key_fields = ['price', 'volume', 'action', 'broker_id', 'tradeid', 'b_price', 'a_price']
        nan_analysis = {}
        for field in key_fields:
            if field in df.columns:
                nan_count = df[field].isna().sum()
                nan_pct = (nan_count / len(df)) * 100
                nan_analysis[field] = {'nan_count': nan_count, 'nan_pct': nan_pct}
        
        print(f"   🔍 NaN analysis:")
        for field, stats in nan_analysis.items():
            print(f"      {field}: {stats['nan_count']} NaNs ({stats['nan_pct']:.1f}%)")
    
    # Analyze both sources
    analyze_trades(df_trades, "DataFetcher")
    analyze_trades(sv_trades, "SpreadViewer")
    
    print("\\n" + "="*60)
    print("KEY DISTINGUISHING CHARACTERISTICS")
    print("="*60)
    
    distinguishing_features = []
    
    # Compare broker IDs
    if len(df_trades) > 0 and len(sv_trades) > 0:
        df_brokers = set(df_trades['broker_id'].dropna().unique())
        sv_brokers = set(sv_trades['broker_id'].dropna().unique())
        
        print(f"\\n🏢 BROKER ID COMPARISON:")
        print(f"   DataFetcher broker IDs: {sorted(df_brokers)}")
        print(f"   SpreadViewer broker IDs: {sorted(sv_brokers)}")
        
        if df_brokers != sv_brokers:
            distinguishing_features.append("Different broker_id values")
            print(f"   ✅ DISTINGUISHER: Different broker IDs can separate sources!")
    
    # Compare trade ID patterns
    if len(df_trades) > 0 and len(sv_trades) > 0:
        df_sample_ids = df_trades['tradeid'].dropna().head(3).tolist()
        sv_sample_ids = sv_trades['tradeid'].dropna().head(3).tolist()
        
        print(f"\\n🏷️  TRADE ID PATTERN COMPARISON:")
        print(f"   DataFetcher sample IDs: {df_sample_ids}")
        print(f"   SpreadViewer sample IDs: {sv_sample_ids}")
        
        # Check for pattern differences
        df_has_synth = any('synth_' in str(tid) for tid in df_sample_ids)
        sv_has_synth = any('synth_' in str(tid) for tid in sv_sample_ids)
        df_has_eurex = any('Eurex' in str(tid) for tid in df_sample_ids)
        sv_has_eurex = any('Eurex' in str(tid) for tid in sv_sample_ids)
        
        if df_has_synth != sv_has_synth or df_has_eurex != sv_has_eurex:
            distinguishing_features.append("Different tradeid patterns")
            print(f"   ✅ DISTINGUISHER: Trade ID patterns can separate sources!")
    
    # Compare volume patterns
    if len(df_trades) > 0 and len(sv_trades) > 0:
        df_vols = df_trades['volume'].dropna().unique()
        sv_vols = sv_trades['volume'].dropna().unique()
        
        print(f"\\n📦 VOLUME PATTERN COMPARISON:")
        print(f"   DataFetcher volumes: {sorted(df_vols) if len(df_vols) < 20 else f'{len(df_vols)} unique values'}")
        print(f"   SpreadViewer volumes: {sorted(sv_vols) if len(sv_vols) < 20 else f'{len(sv_vols)} unique values'}")
        
        # Check if volume patterns are distinctly different
        df_vol_set = set(df_vols)
        sv_vol_set = set(sv_vols)
        if len(df_vol_set.intersection(sv_vol_set)) / max(len(df_vol_set), len(sv_vol_set)) < 0.5:
            distinguishing_features.append("Different volume patterns")
            print(f"   ✅ DISTINGUISHER: Volume patterns differ significantly!")
    
    print(f"\\n🎯 SUMMARY OF DISTINGUISHING FEATURES:")
    if distinguishing_features:
        for i, feature in enumerate(distinguishing_features, 1):
            print(f"   {i}. {feature}")
        print(f"\\n✅ Found {len(distinguishing_features)} reliable methods to distinguish sources!")
    else:
        print(f"   ⚠️  No clear distinguishing features found between sources")
    
    # Create visualization if both sources have trades
    if len(df_trades) > 0 and len(sv_trades) > 0:
        print(f"\\n📊 Creating comparison visualization...")
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # Plot 1: Price distributions
        if len(df_trades) > 0:
            ax1.hist(df_trades['price'], bins=20, alpha=0.7, label='DataFetcher', color='red')
        if len(sv_trades) > 0:
            ax1.hist(sv_trades['price'], bins=20, alpha=0.7, label='SpreadViewer', color='blue')
        ax1.set_title('Price Distribution Comparison')
        ax1.set_xlabel('Price (EUR/MWh)')
        ax1.set_ylabel('Frequency')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Time series comparison  
        if len(df_trades) > 0:
            ax2.scatter(df_trades.index, df_trades['price'], 
                       alpha=0.7, label='DataFetcher', color='red', s=30)
        if len(sv_trades) > 0:
            ax2.scatter(sv_trades.index, sv_trades['price'], 
                       alpha=0.7, label='SpreadViewer', color='blue', s=30, marker='s')
        ax2.set_title('Trade Prices Over Time')
        ax2.set_xlabel('Time')
        ax2.set_ylabel('Price (EUR/MWh)')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Broker ID comparison
        if len(df_trades) > 0 and 'broker_id' in df_trades.columns:
            df_broker_counts = df_trades['broker_id'].value_counts()
            ax3.bar([f"DF_{int(b)}" for b in df_broker_counts.index], df_broker_counts.values, 
                   alpha=0.7, color='red', label='DataFetcher')
        
        if len(sv_trades) > 0 and 'broker_id' in sv_trades.columns:
            sv_broker_counts = sv_trades['broker_id'].value_counts()
            x_pos = len(df_broker_counts) if len(df_trades) > 0 else 0
            ax3.bar([f"SV_{int(b)}" for b in sv_broker_counts.index], sv_broker_counts.values, 
                   alpha=0.7, color='blue', label='SpreadViewer')
        
        ax3.set_title('Broker ID Distribution')
        ax3.set_xlabel('Broker ID')
        ax3.set_ylabel('Trade Count')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: Volume comparison
        if len(df_trades) > 0 and 'volume' in df_trades.columns:
            df_vol_data = df_trades['volume'].dropna()
            if len(df_vol_data) > 0:
                ax4.hist(df_vol_data, bins=10, alpha=0.7, label='DataFetcher', color='red')
        
        if len(sv_trades) > 0 and 'volume' in sv_trades.columns:
            sv_vol_data = sv_trades['volume'].dropna() 
            if len(sv_vol_data) > 0:
                ax4.hist(sv_vol_data, bins=10, alpha=0.7, label='SpreadViewer', color='blue')
        
        ax4.set_title('Volume Distribution Comparison')
        ax4.set_xlabel('Volume')
        ax4.set_ylabel('Frequency')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save plot
        plot_path = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/trade_source_comparison.png"
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"   💾 Comparison plot saved: {plot_path}")
    
    print("\\n✅ Trade source comparison analysis completed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()