#!/usr/bin/env python3
"""
Plot the critical transition period data (June 26-27) with n_s=3
"""

import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np
from datetime import datetime

print("📊 PLOTTING CRITICAL TRANSITION PERIOD DATA")
print("=" * 50)

# Load the transition period data
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
    print(f"📅 Critical period: {metadata['period']['start_date']} to {metadata['period']['end_date']}")
    
    # Check broker distribution
    broker_counts = df['broker_id'].value_counts().sort_index()
    print(f"\n📊 Data sources:")
    for broker_id, count in broker_counts.items():
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        print(f"   {source}: {count:,} records")
    
    # Create focused analysis plot
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'Critical Transition Period Analysis (June 26-27, 2025)\nn_s={metadata["n_s"]} | Timestamp: {metadata["timestamp"]}\nTesting q_2 → q_1 Transition Fix', 
                 fontsize=16, fontweight='bold')
    
    # Add date separation
    df['date'] = df.index.date
    june_26 = datetime(2025, 6, 26).date()
    june_27 = datetime(2025, 6, 27).date()
    
    # Plot 1: Time series with transition boundary
    ax1 = axes[0, 0]
    for broker_id in sorted(df['broker_id'].unique()):
        source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else f"Unknown({broker_id})"
        color = 'blue' if broker_id == 1441.0 else 'red' if broker_id == 9999.0 else 'gray'
        alpha = 0.8 if broker_id == 1441.0 else 0.5
        size = 6 if broker_id == 1441.0 else 3
        
        data = df[df['broker_id'] == broker_id]
        ax1.scatter(data.index, data['price'], alpha=alpha, s=size,
                   label=f'{source} ({len(data):,})', color=color)
    
    # Add transition line at midnight June 27
    transition_time = datetime(2025, 6, 27, 0, 0, 0)
    ax1.axvline(x=transition_time, color='green', linestyle='--', linewidth=2, alpha=0.8,
                label='Transition (June 27 00:00)')
    
    ax1.set_title('Price Time Series Across Transition')
    ax1.set_ylabel('Price (€)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Day-by-day comparison
    ax2 = axes[0, 1]
    
    for test_date, expected_period in [(june_26, 'q_2'), (june_27, 'q_1')]:
        day_data = df[df['date'] == test_date]
        if len(day_data) > 0:
            x_offset = 0 if test_date == june_26 else 1
            
            for i, broker_id in enumerate(sorted(day_data['broker_id'].unique())):
                source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
                color = 'blue' if broker_id == 1441.0 else 'red'
                
                broker_day = day_data[day_data['broker_id'] == broker_id]
                if len(broker_day) > 0:
                    mean_price = broker_day['price'].mean()
                    std_price = broker_day['price'].std()
                    
                    ax2.errorbar(x_offset + i*0.1, mean_price, yerr=std_price,
                               marker='o', markersize=8, capsize=5, linewidth=2,
                               label=f'{source} {test_date} ({expected_period})', color=color)
    
    ax2.set_title('Daily Price Comparison with Expected Periods')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Price (€)')
    ax2.set_xticks([0, 1])
    ax2.set_xticklabels(['June 26\n(Expected: q_2)', 'June 27\n(Expected: q_1)'])
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Price distributions by date
    ax3 = axes[1, 0]
    
    colors = ['lightblue', 'darkblue', 'lightcoral', 'darkred']
    labels = []
    
    for test_date in [june_26, june_27]:
        day_data = df[df['date'] == test_date]
        if len(day_data) > 0:
            for broker_id in sorted(day_data['broker_id'].unique()):
                source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer"
                broker_day = day_data[day_data['broker_id'] == broker_id]
                
                if len(broker_day) > 0:
                    color_idx = (0 if test_date == june_26 else 2) + (0 if broker_id == 1441.0 else 1)
                    ax3.hist(broker_day['price'], bins=20, alpha=0.6, 
                           color=colors[color_idx], density=True,
                           label=f'{source} {test_date}')
    
    ax3.set_title('Price Distribution Comparison')
    ax3.set_xlabel('Price (€)')
    ax3.set_ylabel('Density')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Synchronization analysis
    ax4 = axes[1, 1]
    ax4.axis('off')
    
    # Calculate statistics
    if len(broker_counts) > 1:
        df_data = df[df['broker_id'] == 1441.0]
        sv_data = df[df['broker_id'] == 9999.0]
        
        overall_price_diff = abs(df_data['price'].mean() - sv_data['price'].mean())
        
        # Day-specific analysis
        june_26_analysis = ""
        june_27_analysis = ""
        
        for test_date, var_name in [(june_26, 'june_26_analysis'), (june_27, 'june_27_analysis')]:
            day_data = df[df['date'] == test_date]
            df_day = day_data[day_data['broker_id'] == 1441.0]
            sv_day = day_data[day_data['broker_id'] == 9999.0]
            
            if len(df_day) > 0 and len(sv_day) > 0:
                day_diff = abs(df_day['price'].mean() - sv_day['price'].mean())
                analysis_text = f"{test_date}:\n  DataFetcher: €{df_day['price'].mean():.2f}\n  SpreadViewer: €{sv_day['price'].mean():.2f}\n  Difference: €{day_diff:.3f}"
                
                if test_date == june_26:
                    june_26_analysis = analysis_text
                else:
                    june_27_analysis = analysis_text
        
        analysis_text = f"""TRANSITION PERIOD SYNCHRONIZATION ANALYSIS

🎯 Critical Test: June 26-27, 2025 (n_s=3)

📊 Data Summary:
• Total Records: {len(df):,}
• DataFetcher: {len(df_data):,} records  
• SpreadViewer: {len(sv_data):,} records
• Periods Processed: 4 (split periods working)

💰 Overall Synchronization:
• DataFetcher Mean: €{df_data['price'].mean():.2f}
• SpreadViewer Mean: €{sv_data['price'].mean():.2f}
• Overall Difference: €{overall_price_diff:.3f}

📅 Day-by-Day Analysis:
{june_26_analysis}

{june_27_analysis}

🎯 Expected Behavior:
• June 26: Both use q_2 (Q2 perspective)
• June 27: Both use q_1 (Q3 perspective)
• Transition at midnight June 27"""

        if overall_price_diff < 0.1:
            status = "✅ EXCELLENT"
        elif overall_price_diff < 0.5:
            status = "✅ VERY GOOD"
        elif overall_price_diff < 1.0:
            status = "✅ GOOD"
        else:
            status = "⚠️ NEEDS ATTENTION"
        
        analysis_text += f"\n\n🏆 Synchronization Status: {status}"
        
        ax4.text(0.05, 0.95, analysis_text, transform=ax4.transAxes, fontsize=9,
                verticalalignment='top', fontfamily='monospace')
    
    plt.tight_layout()
    
    # Save plot
    output_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/transition_period_analysis.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✅ Plot saved: {output_file}")
    
    # Detailed transition analysis
    print(f"\n🎯 TRANSITION PERIOD DETAILED ANALYSIS:")
    print("=" * 45)
    
    if len(broker_counts) > 1:
        for test_date, expected_period in [(june_26, 'q_2'), (june_27, 'q_1')]:
            day_data = df[df['date'] == test_date]
            if len(day_data) > 0:
                print(f"\n📅 {test_date} (Expected: {expected_period}):")
                
                df_day = day_data[day_data['broker_id'] == 1441.0]
                sv_day = day_data[day_data['broker_id'] == 9999.0]
                
                if len(df_day) > 0:
                    print(f"   🟢 DataFetcher: {len(df_day)} records")
                    print(f"      💰 Price: €{df_day['price'].mean():.2f} ± {df_day['price'].std():.2f}")
                    print(f"      📊 Range: €{df_day['price'].min():.2f} - €{df_day['price'].max():.2f}")
                
                if len(sv_day) > 0:
                    print(f"   🔵 SpreadViewer: {len(sv_day)} records")
                    print(f"      💰 Price: €{sv_day['price'].mean():.2f} ± {sv_day['price'].std():.2f}")
                    print(f"      📊 Range: €{sv_day['price'].min():.2f} - €{sv_day['price'].max():.2f}")
                
                if len(df_day) > 0 and len(sv_day) > 0:
                    day_diff = abs(df_day['price'].mean() - sv_day['price'].mean())
                    print(f"   ⚖️ Price difference: €{day_diff:.3f}")
                    
                    if day_diff < 0.1:
                        print(f"   ✅ EXCELLENT synchronization for {expected_period} period!")
                    elif day_diff < 0.5:
                        print(f"   ✅ VERY GOOD synchronization for {expected_period} period")
                    elif day_diff < 1.0:
                        print(f"   ✅ GOOD synchronization for {expected_period} period")
                    else:
                        print(f"   ⚠️ {expected_period} period needs attention")
    
    print(f"\n🎉 Transition period analysis complete!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()