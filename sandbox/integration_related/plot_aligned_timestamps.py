#!/usr/bin/env python3
"""
Plot prices aligned on timestamps - show both contracts on same time axis
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_aligned_timestamps():
    """Plot both contracts aligned on the same timestamp axis"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Creating Timestamp-Aligned Price Plot")
        print("=" * 45)
        
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
        
        # Extract data with timestamps
        leg1_orders = leg1_data['orders']
        leg1_trades = leg1_data['trades']
        leg2_orders = leg2_data['orders']
        leg2_trades = leg2_data['trades']
        
        print(f"📊 Data inspection:")
        print(f"   Leg1 orders index type: {type(leg1_orders.index)}")
        print(f"   Leg1 orders index name: {leg1_orders.index.name}")
        print(f"   Leg1 orders columns: {list(leg1_orders.columns)}")
        print(f"   Leg1 trades index type: {type(leg1_trades.index)}")
        print(f"   Leg1 trades columns: {list(leg1_trades.columns)}")
        
        print(f"   Leg2 orders index type: {type(leg2_orders.index)}")
        print(f"   Leg2 orders columns: {list(leg2_orders.columns)}")
        
        # Check timestamp ranges
        print(f"\n📅 Timestamp ranges:")
        print(f"   Leg1 orders: {leg1_orders.index[0]} to {leg1_orders.index[-1]}")
        print(f"   Leg1 trades: {leg1_trades.index[0]} to {leg1_trades.index[-1]}")
        print(f"   Leg2 orders: {leg2_orders.index[0]} to {leg2_orders.index[-1]}")
        print(f"   Leg2 trades: {leg2_trades.index[0]} to {leg2_trades.index[-1]}")
        
        # Calculate mid prices
        leg1_mid = (leg1_orders['b_price'] + leg1_orders['a_price']) / 2
        leg2_mid = (leg2_orders['b_price'] + leg2_orders['a_price']) / 2
        
        print(f"\n📊 Mid price data:")
        print(f"   Leg1 mid: {len(leg1_mid):,} points")
        print(f"   Leg2 mid: {len(leg2_mid):,} points")
        
        # Create union of all timestamps for alignment
        all_timestamps = leg1_orders.index.union(leg2_orders.index).union(
                        leg1_trades.index).union(leg2_trades.index)
        
        print(f"   Combined timestamps: {len(all_timestamps):,} unique points")
        print(f"   Time range: {all_timestamps[0]} to {all_timestamps[-1]}")
        
        # Align data to common timeline using forward fill
        leg1_mid_aligned = leg1_mid.reindex(all_timestamps, method='ffill')
        leg2_mid_aligned = leg2_mid.reindex(all_timestamps, method='ffill')
        leg1_orders_aligned = leg1_orders.reindex(all_timestamps, method='ffill')
        leg2_orders_aligned = leg2_orders.reindex(all_timestamps, method='ffill')
        
        # Remove NaN values (periods before first data point)
        valid_mask = ~(leg1_mid_aligned.isna() | leg2_mid_aligned.isna())
        
        leg1_mid_clean = leg1_mid_aligned[valid_mask]
        leg2_mid_clean = leg2_mid_aligned[valid_mask]
        leg1_orders_clean = leg1_orders_aligned[valid_mask]
        leg2_orders_clean = leg2_orders_aligned[valid_mask]
        aligned_timestamps = all_timestamps[valid_mask]
        
        print(f"   Clean aligned data: {len(leg1_mid_clean):,} points")
        
        # Sample for plotting performance (keep every Nth point)
        sample_every = max(1, len(aligned_timestamps) // 5000)  # Max 5000 points
        sample_idx = range(0, len(aligned_timestamps), sample_every)
        
        sample_timestamps = aligned_timestamps[sample_idx]
        sample_mid1 = leg1_mid_clean.iloc[sample_idx]
        sample_mid2 = leg2_mid_clean.iloc[sample_idx]
        sample_orders1 = leg1_orders_clean.iloc[sample_idx]
        sample_orders2 = leg2_orders_clean.iloc[sample_idx]
        
        print(f"   Sampled for plotting: {len(sample_timestamps):,} points (every {sample_every})")
        
        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12))
        fig.suptitle('Timestamp-Aligned Price Evolution\nGerman Power Contracts: debm01_25 vs debm02_25\n(Synchronized Timeline)', 
                     fontsize=16, fontweight='bold')
        
        # Colors
        color1, color2 = '#1f77b4', '#ff7f0e'  # Blue, Orange
        
        # Plot 1: Both contracts on same timeline - Mid prices
        ax1.plot(sample_timestamps, sample_mid1.values, 
                color=color1, linewidth=1.5, alpha=0.8, label='debm01_25 (Jan 2025)')
        ax1.plot(sample_timestamps, sample_mid2.values, 
                color=color2, linewidth=1.5, alpha=0.8, label='debm02_25 (Feb 2025)')
        
        # Add bid-ask spreads as filled areas
        ax1.fill_between(sample_timestamps, 
                        sample_orders1['b_price'], sample_orders1['a_price'], 
                        alpha=0.2, color=color1)
        ax1.fill_between(sample_timestamps, 
                        sample_orders2['b_price'], sample_orders2['a_price'], 
                        alpha=0.2, color=color2)
        
        ax1.set_title('Mid Prices - Timestamp Synchronized\n(Bid-Ask Spreads Shown as Filled Areas)', 
                     fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price (€/MWh)', fontsize=12)
        ax1.legend(loc='upper right')
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
        ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        
        # Add price difference area
        price_diff = sample_mid1.values - sample_mid2.values
        ax1_twin = ax1.twinx()
        ax1_twin.fill_between(sample_timestamps, 0, price_diff, 
                             where=(price_diff >= 0), alpha=0.15, color='green', 
                             label='Jan > Feb')
        ax1_twin.fill_between(sample_timestamps, 0, price_diff,
                             where=(price_diff < 0), alpha=0.15, color='red', 
                             label='Feb > Jan')
        ax1_twin.set_ylabel('Price Difference (€/MWh)', fontsize=10)
        ax1_twin.legend(loc='lower right', fontsize=9)
        
        # Statistics box
        stats = (f'Jan Avg: {leg1_mid_clean.mean():.2f} €/MWh\n'
                f'Feb Avg: {leg2_mid_clean.mean():.2f} €/MWh\n'
                f'Spread: {(leg1_mid_clean - leg2_mid_clean).mean():.3f} €/MWh\n'
                f'Correlation: {leg1_mid_clean.corr(leg2_mid_clean):.3f}')
        ax1.text(0.02, 0.98, stats, transform=ax1.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.9))
        
        # Plot 2: Calendar spread evolution
        calendar_spread = leg1_mid_clean - leg2_mid_clean
        sample_spread = calendar_spread.iloc[sample_idx]
        
        ax2.plot(sample_timestamps, sample_spread.values, 
                color='#e31a1c', linewidth=1.5, alpha=0.8, label='Calendar Spread (Jan - Feb)')
        ax2.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax2.axhline(y=sample_spread.mean(), color='red', linestyle=':', alpha=0.7, 
                   label=f'Mean: {sample_spread.mean():.3f} €/MWh')
        
        # Fill positive/negative areas
        ax2.fill_between(sample_timestamps, 0, sample_spread.values,
                        where=(sample_spread.values >= 0), alpha=0.3, color='green', 
                        label='Contango (Jan > Feb)')
        ax2.fill_between(sample_timestamps, 0, sample_spread.values,
                        where=(sample_spread.values < 0), alpha=0.3, color='red', 
                        label='Backwardation (Feb > Jan)')
        
        ax2.set_title('Calendar Spread Evolution - Synchronized Timeline', 
                     fontsize=14, fontweight='bold')
        ax2.set_ylabel('Spread (€/MWh)', fontsize=12)
        ax2.set_xlabel('Date & Time', fontsize=12)
        ax2.legend(loc='upper right')
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        ax2.xaxis.set_major_locator(mdates.HourLocator(interval=6))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
        
        # Spread statistics
        spread_stats = (f'Avg Spread: {calendar_spread.mean():.3f} €/MWh\n'
                       f'Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f}\n'
                       f'Volatility: {calendar_spread.std():.3f} €/MWh\n'
                       f'% Time Jan > Feb: {(calendar_spread > 0).mean()*100:.1f}%')
        ax2.text(0.02, 0.98, spread_stats, transform=ax2.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))
        
        plt.tight_layout()
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/prices_timestamp_aligned.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"📈 Timestamp-aligned plot saved: {output_path}")
        
        # Print detailed analysis
        print(f"\n📊 TIMESTAMP-ALIGNED ANALYSIS:")
        print("=" * 50)
        
        print(f"📅 Temporal Coverage:")
        print(f"   Start: {aligned_timestamps[0]}")
        print(f"   End: {aligned_timestamps[-1]}")
        print(f"   Duration: {aligned_timestamps[-1] - aligned_timestamps[0]}")
        print(f"   Synchronized Points: {len(aligned_timestamps):,}")
        
        print(f"\n💰 Price Comparison:")
        print(f"   Jan 2025 (debm01_25): {leg1_mid_clean.mean():.2f} ± {leg1_mid_clean.std():.2f} €/MWh")
        print(f"   Feb 2025 (debm02_25): {leg2_mid_clean.mean():.2f} ± {leg2_mid_clean.std():.2f} €/MWh")
        
        print(f"\n📈 Calendar Spread (Jan - Feb):")
        print(f"   Average: {calendar_spread.mean():.3f} €/MWh")
        print(f"   Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f} €/MWh")
        print(f"   Volatility: {calendar_spread.std():.3f} €/MWh")
        print(f"   Correlation: {leg1_mid_clean.corr(leg2_mid_clean):.3f}")
        
        print(f"\n🔍 Market Dynamics:")
        contango_pct = (calendar_spread > 0).mean() * 100
        backwardation_pct = (calendar_spread < 0).mean() * 100
        print(f"   Time in Contango (Jan > Feb): {contango_pct:.1f}%")
        print(f"   Time in Backwardation (Feb > Jan): {backwardation_pct:.1f}%")
        
        if calendar_spread.mean() > 0:
            print(f"   💡 Overall Structure: CONTANGO - January premium reflects winter demand")
        else:
            print(f"   💡 Overall Structure: BACKWARDATION - February premium (unusual)")
        
        print(f"\n✅ SYNCHRONIZED PLOTTING:")
        print(f"   📍 Both contracts plotted on identical timestamp axis")
        print(f"   📍 Forward-fill interpolation maintains last known prices")
        print(f"   📍 No time gaps - continuous synchronized evolution")
        print(f"   📍 Real-time calendar spread calculation")
        print(f"   📍 Market structure analysis overlaid")
        
        plt.show()
        print(f"\n🎉 Timestamp-aligned analysis completed!")
        
    except Exception as e:
        print(f"❌ Timestamp-aligned plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_aligned_timestamps()