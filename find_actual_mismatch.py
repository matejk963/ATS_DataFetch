#!/usr/bin/env python3
"""
Find the actual mismatch in the refetched data - where is the q_1/q_2 inconsistency?
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

import pandas as pd
from datetime import datetime, timedelta
import json

print("🔍 ANALYZING ACTUAL REFETCHED DATA FOR MISMATCH")
print("=" * 60)

# Load the latest refetched data
data_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet"
metadata_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data_metadata.json"
integration_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_results_v2.json"

try:
    # Load the actual data
    print(f"📂 Loading data from: {os.path.basename(data_file)}")
    df = pd.read_parquet(data_file)
    
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    with open(integration_file, 'r') as f:
        integration_results = json.load(f)
    
    print(f"✅ Data loaded: {len(df)} total records")
    print(f"📅 Date range: {metadata['unified_data_info']['date_range']['start']} to {metadata['unified_data_info']['date_range']['end']}")
    print(f"🔧 n_s parameter: {metadata['n_s']}")
    
except Exception as e:
    print(f"❌ Error loading data: {e}")
    exit(1)

print(f"\n🔍 ANALYZING BROKER ID SEPARATION")
print("=" * 40)

# Check broker IDs to understand data sources
broker_counts = df['broker_id'].value_counts().sort_index()
print(f"📊 Broker ID distribution:")
for broker_id, count in broker_counts.items():
    source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else "Unknown"
    print(f"   {broker_id}: {count:,} records ({source})")

print(f"\n🔍 ANALYZING PRICE RANGES BY SOURCE")
print("=" * 40)

# Analyze price ranges for each source around June 26-27
df['timestamp'] = pd.to_datetime(df.index)
df['date'] = df['timestamp'].dt.date

# Focus on June 26-27 where the spike was observed
june_26 = datetime(2025, 6, 26).date()
june_27 = datetime(2025, 6, 27).date()

critical_dates = [june_26, june_27]

for date in critical_dates:
    print(f"\n📅 Analysis for {date}")
    print("-" * 30)
    
    day_data = df[df['date'] == date]
    if len(day_data) == 0:
        print(f"   ⚠️  No data for {date}")
        continue
    
    for broker_id in sorted(day_data['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        broker_data = day_data[day_data['broker_id'] == broker_id]
        
        if len(broker_data) > 0:
            price_stats = broker_data['price'].describe()
            print(f"   🟢 {source}:")
            print(f"      📊 Records: {len(broker_data)}")
            print(f"      💰 Price range: €{price_stats['min']:.2f} - €{price_stats['max']:.2f}")
            print(f"      📈 Mean: €{price_stats['mean']:.2f}")
            
            # Check for spikes around €32.5 (the value user mentioned)
            spike_data = broker_data[broker_data['price'] > 30.0]
            if len(spike_data) > 0:
                print(f"      ⚡ Spikes >€30: {len(spike_data)} records, max €{spike_data['price'].max():.2f}")

print(f"\n🔍 EXAMINING INTEGRATION RESULTS FOR CLUES")
print("=" * 40)

# Examine the integration results for any clues about relative period usage
print(f"📊 Real spread data (DataFetcher):")
print(f"   🔄 Trades: {integration_results['real_spread_data']['spread_trades']}")
print(f"   📋 Orders: {integration_results['real_spread_data']['spread_orders']}")

print(f"\n📊 Synthetic spread data (SpreadViewer):")
print(f"   🔄 Trades: {integration_results['synthetic_spread_data']['spread_trades']}")
print(f"   📋 Orders: {integration_results['synthetic_spread_data']['spread_orders']}")
print(f"   📊 Method: {integration_results['synthetic_spread_data']['method']}")
print(f"   🔧 Periods processed: {integration_results['synthetic_spread_data']['periods_processed']}")

print(f"\n🔍 LOOKING FOR SIGNS OF RELATIVE PERIOD MISMATCH")
print("=" * 50)

# If there are price discrepancies despite n_s synchronization,
# it suggests different contracts are being fetched (q_1 vs q_2)

# Check price correlation between sources
datafetcher_data = df[df['broker_id'] == 1441.0]['price']
spreadviewer_data = df[df['broker_id'] == 9999.0]['price']

if len(datafetcher_data) > 0 and len(spreadviewer_data) > 0:
    df_mean = datafetcher_data.mean()
    sv_mean = spreadviewer_data.mean()
    price_diff = abs(df_mean - sv_mean)
    
    print(f"📊 Overall price comparison:")
    print(f"   🟢 DataFetcher mean: €{df_mean:.2f}")
    print(f"   🔵 SpreadViewer mean: €{sv_mean:.2f}")
    print(f"   ⚖️  Difference: €{price_diff:.2f}")
    
    if price_diff > 10.0:
        print(f"   🚨 SIGNIFICANT DIFFERENCE: €{price_diff:.2f}")
        print(f"       This suggests different underlying contracts!")
        print(f"       Likely cause: q_1 vs q_2 mapping mismatch")
    else:
        print(f"   ✅ Prices are synchronized")

print(f"\n🎯 HYPOTHESIS TESTING")
print("=" * 30)

# Based on analysis, provide hypothesis
if 'price_diff' in locals() and price_diff > 10.0:
    print(f"💡 HYPOTHESIS: Despite n_s synchronization logic, SpreadViewer is still")
    print(f"   fetching different relative periods than DataFetcher.")
    print(f"   ")
    print(f"🔍 POSSIBLE CAUSES:")
    print(f"   1. SpreadViewer caching old relative period mappings")
    print(f"   2. Multiple code paths in SpreadViewer not using synchronized function")
    print(f"   3. Contract specification parsing differences")
    print(f"   4. Database/data source inconsistencies")
    print(f"   ")
    print(f"🔧 NEXT INVESTIGATION:")
    print(f"   - Trace SpreadViewer execution to verify which relative periods are used")
    print(f"   - Check if synchronized function is called in ALL SpreadViewer code paths")
    print(f"   - Verify contract parsing produces same relative mappings")
    
    # Suggest specific investigation
    print(f"\n🎯 CRITICAL QUESTION:")
    print(f"   Is SpreadViewer actually calling our synchronized function?")
    print(f"   Or is there another code path that bypasses it?")
else:
    print(f"✅ Prices appear synchronized. Issue may be elsewhere.")

print(f"\n📋 SUMMARY:")
print(f"   📊 Total records: {len(df):,}")
print(f"   🟢 DataFetcher records: {len(df[df['broker_id'] == 1441.0]):,}")
print(f"   🔵 SpreadViewer records: {len(df[df['broker_id'] == 9999.0]):,}")
if 'price_diff' in locals():
    print(f"   ⚖️  Price difference: €{price_diff:.2f}")