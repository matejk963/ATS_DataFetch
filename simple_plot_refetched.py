#!/usr/bin/env python3
"""
Simple plot of refetched data
"""

import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np

print("📊 SIMPLE PLOT OF CORRECTED DATA")
print("=" * 35)

# Load data
data_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet"

try:
    df = pd.read_parquet(data_file)
    df.index = pd.to_datetime(df.index)
    
    print(f"✅ Loaded: {len(df)} records")
    
    # Check sources
    broker_counts = df['broker_id'].value_counts()
    print(f"📊 Data sources:")
    for broker_id, count in broker_counts.items():
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
        print(f"   {source}: {count:,} records")
    
    # Simple plots
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    fig.suptitle('Corrected debq4_25/frbq4_25 Spread Data', fontsize=14)
    
    # Plot 1: Time series
    ax1 = axes[0]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
        color = 'blue' if broker_id == 1441.0 else 'red'
        
        data = df[df['broker_id'] == broker_id]
        ax1.plot(data.index, data['price'], 'o', alpha=0.7, markersize=3,
                label=f'{source} ({len(data)} records)', color=color)
    
    ax1.set_title('Price Time Series')
    ax1.set_ylabel('Price (€)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Distribution
    ax2 = axes[1]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" 
        color = 'blue' if broker_id == 1441.0 else 'red'
        
        data = df[df['broker_id'] == broker_id]
        ax2.hist(data['price'], bins=20, alpha=0.7, color=color,
                label=f'{source}: μ={data["price"].mean():.2f}€')
    
    ax2.set_title('Price Distribution')
    ax2.set_xlabel('Price (€)')
    ax2.set_ylabel('Count')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Statistics
    print(f"\n📊 STATISTICS:")
    print(f"   💰 Price range: €{df['price'].min():.2f} - €{df['price'].max():.2f}")
    print(f"   📈 Mean price: €{df['price'].mean():.2f}")
    
    # Analysis
    if len(broker_counts) == 1:
        if 1441.0 in broker_counts:
            print(f"\n✅ GOOD: Only DataFetcher data (SpreadViewer fix worked)")
            print(f"   🔧 SpreadViewer correctly returned 0 records due to tenor issue")
        else:
            print(f"\n⚠️  Only SpreadViewer data - DataFetcher missing")
    else:
        df_mean = df[df['broker_id'] == 1441.0]['price'].mean()
        sv_mean = df[df['broker_id'] == 9999.0]['price'].mean()
        diff = abs(df_mean - sv_mean)
        print(f"\n📊 Price comparison:")
        print(f"   DataFetcher: €{df_mean:.2f}")
        print(f"   SpreadViewer: €{sv_mean:.2f}")
        print(f"   Difference: €{diff:.2f}")
        
        if diff < 1.0:
            print(f"✅ SUCCESS: Prices synchronized!")
        else:
            print(f"⚠️  Still {diff:.2f}€ difference")
    
    output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/corrected_data_plot.png"
    plt.savefig(output_file, dpi=200, bbox_inches='tight')
    print(f"\n✅ Plot saved: {output_file}")
    
    plt.show()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()