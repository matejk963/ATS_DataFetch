#!/usr/bin/env python3
"""
Plot the latest refetched data with n_s=2
"""

import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np
from datetime import datetime

print("📊 PLOTTING LATEST REFETCHED DATA (n_s=2)")
print("=" * 45)

# Load the latest data
data_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet"
metadata_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data_metadata.json"

try:
    df = pd.read_parquet(data_file)
    df.index = pd.to_datetime(df.index)
    
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    print(f"✅ Data loaded: {len(df):,} records")
    print(f"📅 Timestamp: {metadata['timestamp']}")
    print(f"🔧 n_s parameter: {metadata['n_s']}")
    print(f"📅 Period: {metadata['period']['start_date']} to {metadata['period']['end_date']}")
    
    # Check broker distribution
    broker_counts = df['broker_id'].value_counts().sort_index()
    print(f"\n📊 Data sources:")
    for broker_id, count in broker_counts.items():
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        print(f"   {source}: {count:,} records")
    
    # Create comprehensive plot
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'debq4_25/frbq4_25 Spread Data (n_s={metadata["n_s"]})\nTimestamp: {metadata["timestamp"]}\nJune 24 - July 1, 2025', 
                 fontsize=14, fontweight='bold')
    
    # Plot 1: Full time series
    ax1 = axes[0, 0]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        alpha = 0.8 if broker_id == 1441.0 else 0.4
        size = 4 if broker_id == 1441.0 else 2
        
        data = df[df['broker_id'] == broker_id]
        ax1.scatter(data.index, data['price'], alpha=alpha, s=size,
                   label=f'{source} ({len(data):,})', color=color)
    
    ax1.set_title('Price Time Series')
    ax1.set_ylabel('Price (€)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Price distributions
    ax2 = axes[0, 1]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        
        data = df[df['broker_id'] == broker_id]
        ax2.hist(data['price'], bins=30, alpha=0.7, color=color,
                label=f'{source}\nμ={data["price"].mean():.2f}€\nn={len(data):,}',
                density=True)
    
    ax2.set_title('Price Distributions')
    ax2.set_xlabel('Price (€)')
    ax2.set_ylabel('Density')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Daily price comparison
    ax3 = axes[1, 0]
    df['date'] = df.index.date
    daily_stats = df.groupby(['date', 'broker_id'])['price'].agg(['mean', 'std', 'count']).reset_index()
    
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        
        broker_daily = daily_stats[daily_stats['broker_id'] == broker_id]
        if len(broker_daily) > 0:
            ax3.errorbar(broker_daily['date'], broker_daily['mean'], yerr=broker_daily['std'],
                        marker='o', label=f'{source}', color=color, linewidth=2, capsize=3)
    
    ax3.set_title('Daily Average Prices ± Standard Deviation')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('Price (€)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    # Plot 4: Critical analysis
    ax4 = axes[1, 1]
    
    if len(broker_counts) > 1:
        df_data = df[df['broker_id'] == 1441.0]
        sv_data = df[df['broker_id'] == 9999.0]
        
        # Create price difference analysis
        ax4.axis('off')
        
        price_diff = abs(df_data['price'].mean() - sv_data['price'].mean())
        
        analysis_text = f"""PRICE SYNCHRONIZATION ANALYSIS
        
📊 Data Summary (n_s={metadata['n_s']}):
• Total Records: {len(df):,}
• DataFetcher: {len(df_data):,} records
• SpreadViewer: {len(sv_data):,} records

💰 Price Statistics:
• DataFetcher: €{df_data['price'].mean():.2f} ± {df_data['price'].std():.2f}
• SpreadViewer: €{sv_data['price'].mean():.2f} ± {sv_data['price'].std():.2f}
• Difference: €{price_diff:.3f}

📅 Date Range:
• {df.index.min().strftime('%Y-%m-%d %H:%M')}
• {df.index.max().strftime('%Y-%m-%d %H:%M')}

🎯 Price Range:
• Overall: €{df['price'].min():.2f} - €{df['price'].max():.2f}
• DataFetcher: €{df_data['price'].min():.2f} - €{df_data['price'].max():.2f}
• SpreadViewer: €{sv_data['price'].min():.2f} - €{sv_data['price'].max():.2f}"""

        if price_diff < 0.1:
            status = "✅ EXCELLENT"
        elif price_diff < 1.0:
            status = "✅ GOOD"
        elif price_diff < 5.0:
            status = "⚠️ MODERATE"
        else:
            status = "❌ POOR"
            
        analysis_text += f"\n\n🏆 Synchronization: {status}"
        
        ax4.text(0.05, 0.95, analysis_text, transform=ax4.transAxes, fontsize=9,
                verticalalignment='top', fontfamily='monospace')
    else:
        ax4.text(0.5, 0.5, 'Only one data source available', 
                ha='center', va='center', transform=ax4.transAxes)
        ax4.set_title('Price Analysis')
    
    plt.tight_layout()
    
    # Save plot
    output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/ns2_refetched_data_plot.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Plot saved: {output_file}")
    
    # Detailed statistics
    print(f"\n📊 DETAILED ANALYSIS:")
    print("=" * 30)
    print(f"   💰 Overall price range: €{df['price'].min():.2f} - €{df['price'].max():.2f}")
    print(f"   📈 Overall mean: €{df['price'].mean():.2f}")
    
    if len(broker_counts) > 1:
        df_data = df[df['broker_id'] == 1441.0]
        sv_data = df[df['broker_id'] == 9999.0]
        
        print(f"\n🔍 Source Comparison:")
        print(f"   🟢 DataFetcher: €{df_data['price'].mean():.2f} ± {df_data['price'].std():.2f}")
        print(f"   🔵 SpreadViewer: €{sv_data['price'].mean():.2f} ± {sv_data['price'].std():.2f}")
        
        price_diff = abs(df_data['price'].mean() - sv_data['price'].mean())
        print(f"   ⚖️ Price difference: €{price_diff:.3f}")
        
        if price_diff < 0.1:
            print(f"   ✅ EXCELLENT: Systems perfectly synchronized!")
        elif price_diff < 1.0:
            print(f"   ✅ GOOD: Systems well aligned")
        else:
            print(f"   ⚠️ Price difference needs attention")
        
        # Critical dates analysis
        print(f"\n🎯 Critical Dates with n_s={metadata['n_s']}:")
        critical_dates = [
            datetime(2025, 6, 26).date(),
            datetime(2025, 6, 27).date(),
            datetime(2025, 6, 30).date()
        ]
        
        for test_date in critical_dates:
            day_data = df[df['date'] == test_date]
            if len(day_data) > 0:
                print(f"   📅 {test_date}:")
                for broker_id in sorted(day_data['broker_id'].unique()):
                    source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
                    broker_day = day_data[day_data['broker_id'] == broker_id]
                    if len(broker_day) > 0:
                        print(f"      {source}: {len(broker_day)} records, €{broker_day['price'].mean():.2f}")
    
    print(f"\n🎉 Analysis complete!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()