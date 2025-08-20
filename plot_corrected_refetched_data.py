#!/usr/bin/env python3
"""
Plot the corrected refetched data - should show only DataFetcher data now
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import json

print("📊 PLOTTING CORRECTED REFETCHED DATA")
print("=" * 40)

# Load the refetched data  
data_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet"
metadata_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data_metadata.json"

try:
    df = pd.read_parquet(data_file)
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    print(f"✅ Data loaded: {len(df)} records")
    print(f"📅 Period: {metadata['period']['start_date']} to {metadata['period']['end_date']}")
    print(f"🔧 n_s: {metadata['n_s']}")
    
    # Check broker distribution
    broker_counts = df['broker_id'].value_counts().sort_index()
    print(f"\n📊 Data sources:")
    for broker_id, count in broker_counts.items():
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        print(f"   {source}: {count:,} records")
    
    # Set up the plot
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Corrected debq4_25/frbq4_25 Spread Data (Post n_s Fix)\nJune 24 - July 1, 2025', fontsize=14, fontweight='bold')
    
    # Convert index to datetime
    df.index = pd.to_datetime(df.index)
    df['date'] = df.index.date
    df['hour'] = df.index.hour
    
    # Plot 1: Price time series by source
    ax1 = axes[0, 0]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        
        broker_data = df[df['broker_id'] == broker_id]
        ax1.scatter(broker_data.index, broker_data['price'], 
                   alpha=0.6, s=20, label=f'{source} ({len(broker_data)})', color=color)
    
    ax1.set_title('Price Time Series by Source')
    ax1.set_xlabel('Date/Time')
    ax1.set_ylabel('Price (€)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Price distribution by source
    ax2 = axes[0, 1] 
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        
        broker_data = df[df['broker_id'] == broker_id]
        ax2.hist(broker_data['price'], bins=30, alpha=0.7, 
                label=f'{source}\nμ={broker_data["price"].mean():.2f}€', 
                color=color, edgecolor='black', linewidth=0.5)
    
    ax2.set_title('Price Distribution by Source')
    ax2.set_xlabel('Price (€)')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Daily price summary
    ax3 = axes[1, 0]
    daily_stats = df.groupby(['date', 'broker_id'])['price'].agg(['mean', 'min', 'max', 'count']).reset_index()
    
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        
        broker_daily = daily_stats[daily_stats['broker_id'] == broker_id]
        ax3.plot(broker_daily['date'], broker_daily['mean'], 
                marker='o', label=f'{source} Mean', color=color, linewidth=2)
        ax3.fill_between(broker_daily['date'], broker_daily['min'], broker_daily['max'], 
                        alpha=0.3, color=color)
    
    ax3.set_title('Daily Price Range by Source')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Price (€)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    # Plot 4: Action type distribution
    ax4 = axes[1, 1]
    action_counts = df.groupby(['action', 'broker_id']).size().unstack(fill_value=0)
    
    if len(action_counts.columns) > 0:
        colors = ['blue' if col == 1441.0 else 'red' if col == 9999.0 else 'gray' for col in action_counts.columns]
        labels = ['DataFetcher' if col == 1441.0 else 'SpreadViewer' if col == 9999.0 else f'Unknown({col})' for col in action_counts.columns]
        
        ax4.bar(range(len(action_counts.index)), action_counts.iloc[:, 0], 
               color=colors[0], alpha=0.7, label=labels[0])
        
        if len(action_counts.columns) > 1:
            ax4.bar(range(len(action_counts.index)), action_counts.iloc[:, 1], 
                   bottom=action_counts.iloc[:, 0], color=colors[1], alpha=0.7, label=labels[1])
    
    ax4.set_title('Action Type Distribution')
    ax4.set_xlabel('Action Type')
    ax4.set_ylabel('Count')
    ax4.set_xticks(range(len(action_counts.index)))
    ax4.set_xticklabels(action_counts.index, rotation=45)
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/corrected_refetched_data_analysis.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"✅ Plot saved to: {output_file}")
    
    # Show key statistics
    print(f"\n📊 KEY STATISTICS:")
    print(f"{'='*50}")
    
    if len(broker_counts) == 1 and 1441.0 in broker_counts:
        print(f"✅ SUCCESS: Only DataFetcher data present")
        print(f"   📊 Total records: {len(df):,}")
        print(f"   💰 Price range: €{df['price'].min():.2f} - €{df['price'].max():.2f}")
        print(f"   📈 Mean price: €{df['price'].mean():.2f}")
        print(f"   📅 Date range: {df.index.min().strftime('%Y-%m-%d %H:%M')} to {df.index.max().strftime('%Y-%m-%d %H:%M')}")
        
        print(f"\n🎯 INTERPRETATION:")
        print(f"   • SpreadViewer correctly returned 0 records (broken tenors fixed)")
        print(f"   • Only DataFetcher data shows spread prices around €{df['price'].mean():.2f}")
        print(f"   • No more €32+ price spikes from mismatched relative periods")
        
    elif 9999.0 in broker_counts:
        df_data = df[df['broker_id'] == 1441.0]
        sv_data = df[df['broker_id'] == 9999.0]
        
        if len(df_data) > 0 and len(sv_data) > 0:
            price_diff = abs(df_data['price'].mean() - sv_data['price'].mean())
            print(f"📊 DataFetcher: {len(df_data):,} records, mean €{df_data['price'].mean():.2f}")
            print(f"📊 SpreadViewer: {len(sv_data):,} records, mean €{sv_data['price'].mean():.2f}")
            print(f"⚖️  Price difference: €{price_diff:.2f}")
            
            if price_diff < 1.0:
                print(f"✅ SUCCESS: Price discrepancy resolved!")
            else:
                print(f"⚠️  Still {price_diff:.2f}€ difference - needs investigation")
    
    plt.show()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()