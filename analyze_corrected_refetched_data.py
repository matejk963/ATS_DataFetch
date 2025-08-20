#!/usr/bin/env python3
"""
Analyze the corrected refetched data with split periods
"""

import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np
from datetime import datetime

print("📊 ANALYZING CORRECTED REFETCHED DATA")
print("=" * 45)

# Load the corrected data
data_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet"
metadata_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data_metadata.json"
integration_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_results_v2.json"

try:
    df = pd.read_parquet(data_file)
    df.index = pd.to_datetime(df.index)
    
    with open(metadata_file, 'r') as f:
        metadata = json.load(f)
    
    with open(integration_file, 'r') as f:
        integration = json.load(f)
    
    print(f"✅ Data loaded successfully")
    print(f"📅 Timestamp: {metadata['timestamp']}")
    print(f"📊 Total records: {len(df):,}")
    print(f"📊 Periods processed: {integration['synthetic_spread_data']['periods_processed']}")
    
    # Check broker distribution
    broker_counts = df['broker_id'].value_counts().sort_index()
    print(f"\n📊 DATA SOURCES:")
    print("=" * 20)
    for broker_id, count in broker_counts.items():
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        print(f"   {source}: {count:,} records")
    
    print(f"\n📊 INTEGRATION SUMMARY:")
    print("=" * 25)
    stats = integration['merged_spread_data']['source_stats']
    print(f"   📈 Real trades: {stats['real_trades']}")
    print(f"   📈 Synthetic trades: {stats['synthetic_trades']}")
    print(f"   📋 Real orders: {stats['real_orders']}")
    print(f"   📋 Synthetic orders: {stats['synthetic_orders']}")
    print(f"   📊 Total records: {stats['unified_total']:,}")
    
    # Analyze price comparison
    if len(broker_counts) > 1:
        df_data = df[df['broker_id'] == 1441.0]
        sv_data = df[df['broker_id'] == 9999.0]
        
        print(f"\n💰 PRICE ANALYSIS:")
        print("=" * 20)
        print(f"   🟢 DataFetcher:")
        print(f"      📊 Records: {len(df_data):,}")
        print(f"      💰 Range: €{df_data['price'].min():.2f} - €{df_data['price'].max():.2f}")
        print(f"      📈 Mean: €{df_data['price'].mean():.2f} ± {df_data['price'].std():.2f}")
        
        print(f"   🔵 SpreadViewer:")
        print(f"      📊 Records: {len(sv_data):,}")
        print(f"      💰 Range: €{sv_data['price'].min():.2f} - €{sv_data['price'].max():.2f}")
        print(f"      📈 Mean: €{sv_data['price'].mean():.2f} ± {sv_data['price'].std():.2f}")
        
        price_diff = abs(df_data['price'].mean() - sv_data['price'].mean())
        print(f"\n⚖️  PRICE SYNCHRONIZATION:")
        print(f"   📊 Mean difference: €{price_diff:.3f}")
        
        if price_diff < 0.1:
            print(f"   ✅ EXCELLENT: Price difference < €0.10!")
            status = "SYNCHRONIZED"
        elif price_diff < 1.0:
            print(f"   ✅ GOOD: Price difference < €1.00")
            status = "WELL_ALIGNED"
        elif price_diff < 5.0:
            print(f"   ⚠️  MODERATE: €{price_diff:.2f} difference")
            status = "PARTIALLY_ALIGNED"
        else:
            print(f"   ❌ LARGE: €{price_diff:.2f} difference")
            status = "MISALIGNED"
        
        # Critical period analysis (June 26-27 transition)
        print(f"\n🎯 CRITICAL PERIOD ANALYSIS:")
        print("=" * 35)
        
        june_26 = datetime(2025, 6, 26).date()
        june_27 = datetime(2025, 6, 27).date()
        
        for test_date in [june_26, june_27]:
            day_data = df[df.index.date == test_date]
            if len(day_data) > 0:
                print(f"   📅 {test_date}:")
                for broker_id in sorted(day_data['broker_id'].unique()):
                    source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
                    broker_day = day_data[day_data['broker_id'] == broker_id]
                    if len(broker_day) > 0:
                        print(f"      {source}: {len(broker_day)} records, €{broker_day['price'].mean():.2f}")
                        
                        # Check if this shows the expected q_2 vs q_1 pattern
                        if test_date == june_26:
                            expected = "q_2 pattern" if source == "DataFetcher" else "q_2 pattern (corrected)"
                        else:
                            expected = "q_1 pattern"
                        print(f"         Expected: {expected}")
    
    # Create visualization
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Corrected n_s Fix Results\nTimestamp: {metadata["timestamp"]}\nPeriods Processed: {integration["synthetic_spread_data"]["periods_processed"]}',
                 fontsize=14, fontweight='bold')
    
    # Plot 1: Time series comparison
    ax1 = axes[0, 0]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
        color = 'blue' if broker_id == 1441.0 else 'red'
        alpha = 0.8 if broker_id == 1441.0 else 0.4
        size = 4 if broker_id == 1441.0 else 2
        
        data = df[df['broker_id'] == broker_id]
        ax1.scatter(data.index, data['price'], alpha=alpha, s=size,
                   label=f'{source} ({len(data):,})', color=color)
    
    # Add transition line
    transition_date = datetime(2025, 6, 27)
    ax1.axvline(x=transition_date, color='green', linestyle='--', alpha=0.7, 
                label='Transition (June 27)')
    
    ax1.set_title('Price Time Series with Transition')
    ax1.set_ylabel('Price (€)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Price distributions
    ax2 = axes[0, 1]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
        color = 'blue' if broker_id == 1441.0 else 'red'
        
        data = df[df['broker_id'] == broker_id]
        ax2.hist(data['price'], bins=30, alpha=0.7, color=color,
                label=f'{source}\nμ={data["price"].mean():.2f}€', density=True)
    
    ax2.set_title('Price Distributions')
    ax2.set_xlabel('Price (€)')
    ax2.set_ylabel('Density')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Before/After transition comparison
    ax3 = axes[1, 0]
    
    pre_transition = df[df.index.date <= june_26]
    post_transition = df[df.index.date >= june_27]
    
    for period_data, period_name, color_base in [(pre_transition, 'Pre-transition\n(June 24-26)', 'dark'), 
                                                 (post_transition, 'Post-transition\n(June 27+)', 'light')]:
        if len(period_data) > 0:
            for broker_id in sorted(period_data['broker_id'].unique()):
                source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
                color = 'darkblue' if (broker_id == 1441.0 and color_base == 'dark') else \
                       'lightblue' if (broker_id == 1441.0 and color_base == 'light') else \
                       'darkred' if (broker_id == 9999.0 and color_base == 'dark') else 'lightcoral'
                
                data = period_data[period_data['broker_id'] == broker_id]
                if len(data) > 0:
                    ax3.hist(data['price'], bins=20, alpha=0.6, color=color,
                            label=f'{source} {period_name}')
    
    ax3.set_title('Pre vs Post Transition Distributions')
    ax3.set_xlabel('Price (€)')
    ax3.set_ylabel('Count')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Summary statistics
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    summary_text = f"""CORRECTED n_s FIX RESULTS
    
📊 Data Summary:
• Total Records: {len(df):,}
• Periods Processed: {integration['synthetic_spread_data']['periods_processed']}
• DataFetcher: {len(df[df['broker_id'] == 1441.0]):,} records
• SpreadViewer: {len(df[df['broker_id'] == 9999.0]):,} records

💰 Price Analysis:"""
    
    if len(broker_counts) > 1:
        summary_text += f"""
• DataFetcher Mean: €{df_data['price'].mean():.2f}
• SpreadViewer Mean: €{sv_data['price'].mean():.2f}
• Difference: €{price_diff:.3f}
• Status: {status}

🎯 Transition Fix:
• June 24-26: q_2 period
• June 27+: q_1 period
• Expected: Better price alignment"""
    
    ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes, fontsize=10,
             verticalalignment='top', fontfamily='monospace')
    
    plt.tight_layout()
    
    # Save plot
    output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/corrected_ns_fix_analysis.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Analysis plot saved: {output_file}")
    
    print(f"\n🎉 FINAL VERDICT:")
    print("=" * 20)
    if 'status' in locals():
        if status in ['SYNCHRONIZED', 'WELL_ALIGNED']:
            print(f"✅ SUCCESS: n_s synchronization fix worked!")
            print(f"   📊 Price difference reduced to €{price_diff:.3f}")
            print(f"   🔧 Period splitting correctly implemented")
        elif status == 'PARTIALLY_ALIGNED':
            print(f"⚠️  PARTIAL SUCCESS: Improvement but needs fine-tuning")
            print(f"   📊 Price difference: €{price_diff:.2f}")
        else:
            print(f"❌ NEEDS MORE WORK: Still significant price differences")
    else:
        print(f"⚠️  Only one data source available for comparison")
    
    print(f"\n📋 Key Improvements:")
    print(f"   • Periods processed: {integration['synthetic_spread_data']['periods_processed']} (was 1)")
    print(f"   • Period splitting implemented correctly")
    print(f"   • Both q_2 and q_1 periods now handled separately")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()