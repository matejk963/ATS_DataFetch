#!/usr/bin/env python3
"""
Simple plot like before but both contracts on same timeline - no gaps, just continuous
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_same_timeline_simple():
    """Simple continuous plot with both contracts on same timeline"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Creating Simple Same-Timeline Plot")
        print("=" * 40)
        
        # Fetch leg 1
        config1 = {
            'contracts': ['debm01_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching debm01_25 data...")
        result1 = integrated_fetch(config1)
        leg1_data = result1['single_leg_data']
        
        # Fetch leg 2
        config2 = {
            'contracts': ['debm02_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching debm02_25 data...")
        result2 = integrated_fetch(config2)
        leg2_data = result2['single_leg_data']
        
        # Extract clean data
        leg1_orders = leg1_data['orders'].dropna()
        leg1_trades = leg1_data['trades'].dropna()
        leg2_orders = leg2_data['orders'].dropna()  
        leg2_trades = leg2_data['trades'].dropna()
        
        # Calculate mid prices
        leg1_mid = (leg1_orders['b_price'] + leg1_orders['a_price']) / 2
        leg2_mid = (leg2_orders['b_price'] + leg2_orders['a_price']) / 2
        
        print(f"📊 Data: Leg1 {len(leg1_mid):,} points, Leg2 {len(leg2_mid):,} points")
        
        # Create union timeline and align both to it
        all_times = leg1_orders.index.union(leg2_orders.index)
        
        # Forward fill to align on same timeline
        leg1_mid_aligned = leg1_mid.reindex(all_times, method='ffill')
        leg2_mid_aligned = leg2_mid.reindex(all_times, method='ffill')
        leg1_orders_aligned = leg1_orders.reindex(all_times, method='ffill')
        leg2_orders_aligned = leg2_orders.reindex(all_times, method='ffill')
        
        # Remove periods before either contract starts
        valid_mask = ~(leg1_mid_aligned.isna() | leg2_mid_aligned.isna())
        
        leg1_mid_sync = leg1_mid_aligned[valid_mask]
        leg2_mid_sync = leg2_mid_aligned[valid_mask]
        leg1_orders_sync = leg1_orders_aligned[valid_mask]
        leg2_orders_sync = leg2_orders_aligned[valid_mask]
        sync_times = all_times[valid_mask]
        
        print(f"📊 Synchronized: {len(sync_times):,} aligned points")
        
        # Sample for plotting (keep it smooth but not too dense)
        sample_every = max(1, len(sync_times) // 3000)
        sample_idx = range(0, len(sync_times), sample_every)
        
        sample_times = sync_times[sample_idx]
        sample_mid1 = leg1_mid_sync.iloc[sample_idx]
        sample_mid2 = leg2_mid_sync.iloc[sample_idx]
        sample_orders1 = leg1_orders_sync.iloc[sample_idx]
        sample_orders2 = leg2_orders_sync.iloc[sample_idx]
        
        print(f"📊 Plotting: {len(sample_times):,} points (every {sample_every})")
        
        # Create simple plot - same style as before but both on same timeline
        fig, ax = plt.subplots(1, 1, figsize=(16, 10))
        fig.suptitle('Price Evolution - Same Timeline\nGerman Power: debm01_25 vs debm02_25 (No Gaps)', 
                     fontsize=16, fontweight='bold')
        
        # Colors
        color1, color2 = '#1f77b4', '#ff7f0e'  # Blue, Orange
        
        # Plot bid-ask spreads as filled areas
        ax.fill_between(sample_times, 
                       sample_orders1['b_price'], sample_orders1['a_price'],
                       alpha=0.3, color=color1, label='Jan 2025 Bid-Ask')
        ax.fill_between(sample_times,
                       sample_orders2['b_price'], sample_orders2['a_price'], 
                       alpha=0.3, color=color2, label='Feb 2025 Bid-Ask')
        
        # Plot mid price lines
        ax.plot(sample_times, sample_mid1.values, 
               color=color1, linewidth=1.5, alpha=0.9, label='debm01_25 (Jan 2025)')
        ax.plot(sample_times, sample_mid2.values,
               color=color2, linewidth=1.5, alpha=0.9, label='debm02_25 (Feb 2025)')
        
        # Add trade points
        if len(leg1_trades) > 0:
            # Sample trades for visibility
            trade_sample1 = leg1_trades['price'][::max(1, len(leg1_trades)//500)]
            ax.scatter(trade_sample1.index, trade_sample1.values,
                      color='red', alpha=0.4, s=12, label='Jan Trades', zorder=5)
        
        if len(leg2_trades) > 0:
            # Sample trades for visibility  
            trade_sample2 = leg2_trades['price'][::max(1, len(leg2_trades)//500)]
            ax.scatter(trade_sample2.index, trade_sample2.values,
                      color='darkred', alpha=0.4, s=12, label='Feb Trades', zorder=5)
        
        ax.set_title(f'Continuous Price Evolution - Synchronized Timeline\n{len(sync_times):,} data points aligned', 
                    fontsize=14, fontweight='bold')
        ax.set_ylabel('Price (€/MWh)', fontsize=12)
        ax.set_xlabel('Date & Time', fontsize=12)
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)
        
        # Format time axis nicely
        import matplotlib.dates as mdates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        # Add statistics box
        spread_stats = leg1_mid_sync - leg2_mid_sync
        stats = (f'Jan Avg: {leg1_mid_sync.mean():.2f} €/MWh\n'
                f'Feb Avg: {leg2_mid_sync.mean():.2f} €/MWh\n'
                f'Avg Spread: {spread_stats.mean():.3f} €/MWh\n'
                f'Correlation: {leg1_mid_sync.corr(leg2_mid_sync):.3f}\n'
                f'Jan Trades: {len(leg1_trades):,}\n'
                f'Feb Trades: {len(leg2_trades):,}')
        ax.text(0.02, 0.98, stats, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.9))
        
        plt.tight_layout()
        
        # Save plot with descriptive filename
        start_date = config1['period']['start_date'].replace('-', '')
        end_date = config1['period']['end_date'].replace('-', '')
        filename = f"prices_timeline_{config1['contracts'][0]}_vs_{config2['contracts'][0]}_{start_date}_to_{end_date}.png"
        output_path = f'/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/{filename}'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"📈 Same timeline plot saved: {output_path}")
        
        # Print summary
        print(f"\n📊 SAME TIMELINE ANALYSIS:")
        print("=" * 40)
        print(f"🔵 debm01_25 (January 2025):")
        print(f"   Original points: {len(leg1_mid):,}")
        print(f"   Price: {leg1_mid_sync.mean():.2f} ± {leg1_mid_sync.std():.2f} €/MWh")
        print(f"   Trades: {len(leg1_trades):,}")
        
        print(f"🟠 debm02_25 (February 2025):")
        print(f"   Original points: {len(leg2_mid):,}")
        print(f"   Price: {leg2_mid_sync.mean():.2f} ± {leg2_mid_sync.std():.2f} €/MWh")
        print(f"   Trades: {len(leg2_trades):,}")
        
        print(f"\n📈 Timeline Synchronization:")
        print(f"   Synchronized points: {len(sync_times):,}")
        print(f"   Time span: {sync_times[-1] - sync_times[0]}")
        print(f"   Calendar spread: {spread_stats.mean():.3f} €/MWh")
        print(f"   Price correlation: {leg1_mid_sync.corr(leg2_mid_sync):.3f}")
        
        print(f"\n✅ PLOT FEATURES:")
        print(f"   📍 Both contracts on identical timeline")
        print(f"   📍 No time gaps - continuous evolution")
        print(f"   📍 Bid-ask spreads as filled areas")
        print(f"   📍 Mid prices as continuous lines")
        print(f"   📍 Trade points overlaid as dots")
        print(f"   📍 Same style as before but synchronized")
        
        plt.show()
        print(f"\n🎉 Same timeline plotting completed!")
        
    except Exception as e:
        print(f"❌ Same timeline plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_same_timeline_simple()