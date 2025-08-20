#!/usr/bin/env python3
"""
Diagnose why price discrepancies persist after n_s synchronization fix
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, date, timedelta
from data_fetch_engine import calculate_synchronized_product_dates

print("🔍 DIAGNOSING PERSISTENT PRICE DISCREPANCIES")
print("=" * 70)

# Load the data that still shows discrepancies
data_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet'
df = pd.read_parquet(data_file)

print(f"📁 Loaded data: {len(df):,} records")
print(f"📅 Date range: {df.index.min()} to {df.index.max()}")

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
    real_trades = critical_trades[critical_trades['broker_id'] == 1441.0]
    synth_trades = critical_trades[critical_trades['broker_id'] == 9999.0]
    
    print(f"   🏢 DataFetcher trades: {len(real_trades):,}")
    print(f"   🏢 SpreadViewer trades: {len(synth_trades):,}")
    
    if not real_trades.empty:
        print(f"   📊 DataFetcher prices: €{real_trades['price'].min():.2f} - €{real_trades['price'].max():.2f} (mean: €{real_trades['price'].mean():.2f})")
    
    if not synth_trades.empty:
        print(f"   📊 SpreadViewer prices: €{synth_trades['price'].min():.2f} - €{synth_trades['price'].max():.2f} (mean: €{synth_trades['price'].mean():.2f})")
    
    if not real_trades.empty and not synth_trades.empty:
        discrepancy = abs(real_trades['price'].mean() - synth_trades['price'].mean())
        print(f"   💥 Average price discrepancy: €{discrepancy:.2f}")
        
        if discrepancy > 5.0:
            print(f"   ⚠️  STILL LARGE DISCREPANCY AFTER N_S FIX!")
        else:
            print(f"   ✅ Price discrepancy within acceptable range")

print(f"\n🔧 TESTING OUR SYNCHRONIZED FUNCTION:")
print("=" * 70)

# Test our synchronized function with the exact same parameters
test_date = date(2025, 6, 26)
dates = pd.date_range(test_date, test_date, freq='B')
tenors_list = ['q', 'q']  # Both are quarterly
tn1_list = [1, 2]  # q_1 and q_2 relative periods
n_s = 3

print(f"📋 Test parameters:")
print(f"   📅 Date: {test_date}")
print(f"   📊 Tenors: {tenors_list}")
print(f"   📊 Periods: {tn1_list}")
print(f"   🔧 n_s: {n_s}")

try:
    result = calculate_synchronized_product_dates(dates, tenors_list, tn1_list, n_s)
    
    print(f"\n✅ Synchronized function results:")
    for i, (tenor, tn) in enumerate(zip(tenors_list, tn1_list)):
        product_dates = result[i]
        if len(product_dates) > 0:
            delivery_date = product_dates[0]
            quarter = ((delivery_date.month - 1) // 3) + 1
            print(f"   📊 {tenor}_{tn}: {delivery_date.date()} (Q{quarter} {delivery_date.year})")
            
except Exception as e:
    print(f"❌ Error testing synchronized function: {e}")

print(f"\n🤔 POSSIBLE ROOT CAUSES:")
print("=" * 70)
print(f"1. 🔧 Fix not applied: The synchronized function might not be called during data generation")
print(f"2. 📊 Different data sources: DataFetcher vs SpreadViewer might be querying fundamentally different datasets")
print(f"3. 🔄 Cache issues: Old data or calculations might be cached")
print(f"4. 📅 Timing mismatch: The actual relative period calculation might be different")
print(f"5. 🏢 Broker mapping: Different broker IDs might represent different calculation methods")

print(f"\n🔍 DETAILED CONTRACT ANALYSIS:")
print("=" * 70)

# Check what contracts these should actually be querying
print(f"For June 26, 2025 (Q2 2025, in last 3 business days):")
print(f"Both systems SHOULD use Q3 2025 perspective:")
print(f"   - debq4_25: German base Q4 2025 → delivery Oct 1, 2025")
print(f"   - frbq4_25: French base Q4 2025 → delivery Oct 1, 2025")
print(f"   - q_1 relative from Q3 → Q4 2025 contracts")
print(f"   - q_2 relative from Q3 → Q1 2026 contracts")

print(f"\nBUT the price discrepancy suggests:")
print(f"   - DataFetcher (€19-20): Likely querying Q4 2025 contracts ✅") 
print(f"   - SpreadViewer (€31-33): Likely querying Q1 2026 contracts ❌")
print(f"   - This means SpreadViewer is STILL using q_2 instead of q_1!")

print(f"\n🚨 HYPOTHESIS:")
print("=" * 70)
print(f"The synchronized function works correctly, but:")
print(f"1. SpreadViewer might not be calling our synchronized function")
print(f"2. There might be a different code path that bypasses our fix")
print(f"3. The fix might only apply to some data sources, not both")
print(f"4. The contracts configuration might be incorrect")

print(f"\n📋 NEXT STEPS:")
print("=" * 70)
print(f"1. 🔍 Verify the synchronized function is actually being called")
print(f"2. 📊 Add debug logging to trace which contracts are being queried")
print(f"3. 🔄 Check if there are multiple code paths for SpreadViewer calls")
print(f"4. ⚠️  Consider if the fix was applied to the right place in the code")

print(f"\n🎯 RECOMMENDATION:")
print("=" * 70)
print(f"Add debug logging to the data generation process to see:")
print(f"- Which product_dates function is being called")
print(f"- What dates are being returned by each system") 
print(f"- Whether both systems are using the same relative periods")