#!/usr/bin/env python3
"""
Analyze the refetched data with n_s synchronization fix
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

print("📊 ANALYZING REFETCHED DATA WITH N_S SYNCHRONIZATION FIX")
print("=" * 70)

# Load the new data
data_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet'

try:
    df = pd.read_parquet(data_file)
    
    print(f"📁 Loaded refetched data:")
    print(f"   📊 Total records: {len(df):,}")
    print(f"   📅 Date range: {df.index.min()} to {df.index.max()}")
    print(f"   🔍 Columns: {list(df.columns)}")
    
    # Analyze trades vs orders
    trades_mask = df['price'].notna()
    orders_mask = (df['b_price'].notna()) | (df['a_price'].notna())
    
    trades = df[trades_mask].copy()
    orders = df[orders_mask].copy()
    
    print(f"\n📈 Data breakdown:")
    print(f"   🔄 Trades: {len(trades):,}")
    print(f"   📊 Orders: {len(orders):,}")
    
    # Check broker_id distribution in trades
    if 'broker_id' in trades.columns and not trades.empty:
        broker_counts = trades['broker_id'].value_counts().sort_index()
        print(f"\n🏢 Broker distribution in trades:")
        
        for broker_id, count in broker_counts.items():
            source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else "Unknown"
            print(f"   {broker_id}: {count:,} trades ({source})")
    
    # Focus on June 26-27 critical period
    critical_start = pd.Timestamp('2025-06-26')
    critical_end = pd.Timestamp('2025-06-27 23:59:59')
    
    critical_data = df[(df.index >= critical_start) & (df.index <= critical_end)]
    critical_trades = critical_data[critical_data['price'].notna()]
    
    print(f"\n🔍 CRITICAL PERIOD ANALYSIS (June 26-27):")
    print(f"   📅 Period: {critical_start.date()} to {critical_end.date()}")
    print(f"   📊 Total records: {len(critical_data):,}")
    print(f"   🔄 Trades: {len(critical_trades):,}")
    
    if not critical_trades.empty and 'broker_id' in critical_trades.columns:
        critical_real = critical_trades[critical_trades['broker_id'] == 1441.0]
        critical_synth = critical_trades[critical_trades['broker_id'] == 9999.0]
        
        print(f"   🏢 DataFetcher trades: {len(critical_real):,}")
        print(f"   🏢 SpreadViewer trades: {len(critical_synth):,}")
        
        if not critical_real.empty:
            real_min, real_max, real_mean = critical_real['price'].min(), critical_real['price'].max(), critical_real['price'].mean()
            print(f"   📊 DataFetcher prices: €{real_min:.2f} - €{real_max:.2f} (mean: €{real_mean:.2f})")
        
        if not critical_synth.empty:
            synth_min, synth_max, synth_mean = critical_synth['price'].min(), critical_synth['price'].max(), critical_synth['price'].mean()
            print(f"   📊 SpreadViewer prices: €{synth_min:.2f} - €{synth_max:.2f} (mean: €{synth_mean:.2f})")
            
            # Calculate price discrepancy
            if not critical_real.empty:
                discrepancy = abs(real_mean - synth_mean)
                print(f"   💰 Price discrepancy: €{discrepancy:.2f}")
                
                if discrepancy < 1.0:
                    print(f"   ✅ EXCELLENT: Price discrepancy under €1.00 - fix successful!")
                elif discrepancy < 5.0:
                    print(f"   ✅ GOOD: Price discrepancy under €5.00 - significant improvement!")
                else:
                    print(f"   ⚠️  WARNING: Still large price discrepancy - fix may not be fully applied")
        else:
            print(f"   ❌ NO SPREADVIEWER TRADES: Synthetic data generation may have failed")
    
    # Check if we have synthetic data at all
    if 'broker_id' in trades.columns and not trades.empty:
        synth_total = trades[trades['broker_id'] == 9999.0]
        real_total = trades[trades['broker_id'] == 1441.0]
        
        print(f"\n📊 OVERALL DATA COMPOSITION:")
        print(f"   📈 Total DataFetcher trades: {len(real_total):,}")
        print(f"   📈 Total SpreadViewer trades: {len(synth_total):,}")
        
        if len(synth_total) == 0:
            print(f"\n🚨 CRITICAL ISSUE:")
            print(f"   ❌ NO SPREADVIEWER DATA GENERATED!")
            print(f"   📋 Possible causes:")
            print(f"      1. Our fix broke the SpreadViewer data generation")
            print(f"      2. SpreadViewer encountered an error during fetch")
            print(f"      3. Period too short for SpreadViewer to generate synthetic data")
            print(f"      4. Configuration issue in synchronized product_dates function")
            
        elif len(synth_total) < 50:
            print(f"   ⚠️  Very few SpreadViewer trades - may indicate partial failure")
        else:
            print(f"   ✅ Both data sources have reasonable amounts of data")
            
            # Compare price ranges
            if not real_total.empty and not synth_total.empty:
                real_range = f"€{real_total['price'].min():.2f}-€{real_total['price'].max():.2f}"
                synth_range = f"€{synth_total['price'].min():.2f}-€{synth_total['price'].max():.2f}"
                
                print(f"   📊 DataFetcher price range: {real_range}")
                print(f"   📊 SpreadViewer price range: {synth_range}")
                
                overall_discrepancy = abs(real_total['price'].mean() - synth_total['price'].mean())
                print(f"   💰 Overall price discrepancy: €{overall_discrepancy:.2f}")
    
    print(f"\n🎯 FIX ASSESSMENT:")
    print("=" * 70)
    
    if 'broker_id' in trades.columns and not trades.empty:
        synth_count = len(trades[trades['broker_id'] == 9999.0])
        
        if synth_count == 0:
            print(f"🚨 FIX STATUS: INCONCLUSIVE")
            print(f"   Cannot assess fix effectiveness - no SpreadViewer data generated")
            print(f"   Need to investigate why SpreadViewer returned no data")
        elif synth_count < 10:
            print(f"🚨 FIX STATUS: INCONCLUSIVE") 
            print(f"   Insufficient SpreadViewer data to assess fix ({synth_count} trades)")
        else:
            # We have enough data to assess
            if len(critical_trades) > 0:
                print(f"✅ FIX STATUS: CAN BE ASSESSED")
                print(f"   SpreadViewer generated {synth_count} trades")
                print(f"   Critical period has {len(critical_trades)} trades for comparison")
            else:
                print(f"⚠️  FIX STATUS: LIMITED ASSESSMENT")
                print(f"   SpreadViewer data exists but no trades in critical period")

except Exception as e:
    print(f"❌ Error analyzing data: {e}")
    import traceback
    traceback.print_exc()