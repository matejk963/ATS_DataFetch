#!/usr/bin/env python3
"""
Plot integration results by re-running the integration and capturing the data
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_integration_final():
    """Plot by running integration and capturing data"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Running integration to capture data for plotting...")
        
        # Same config that worked
        config = {
            'contracts': ['debm01_25', 'debm02_25'],
            'period': {
                'start_date': '2024-12-02',
                'end_date': '2024-12-06'
            },
            'n_s': 3,
            'mode': 'spread'
        }
        
        # Run integration
        result = integrated_fetch(config)
        
        if not result:
            print("❌ No integration results")
            return
        
        print(f"📊 Integration result keys: {list(result.keys())}")
        
        # Extract data
        leg1_data = result.get('leg_1_data', {})
        leg2_data = result.get('leg_2_data', {})
        
        if not leg1_data and not leg2_data:
            print("❌ No leg data found")
            return
        
        print(f"📊 Leg 1 data keys: {list(leg1_data.keys()) if leg1_data else 'None'}")
        print(f"📊 Leg 2 data keys: {list(leg2_data.keys()) if leg2_data else 'None'}")
        
        # Get actual dataframes
        leg1_orders = leg1_data.get('orders', pd.DataFrame())
        leg1_trades = leg1_data.get('trades', pd.DataFrame()) 
        leg2_orders = leg2_data.get('orders', pd.DataFrame())
        leg2_trades = leg2_data.get('trades', pd.DataFrame())
        
        print(f"📊 Data sizes:")
        print(f"   Leg 1: {len(leg1_orders):,} orders, {len(leg1_trades):,} trades")
        print(f"   Leg 2: {len(leg2_orders):,} orders, {len(leg2_trades):,} trades")
        
        if leg1_orders.empty and leg2_orders.empty:
            print("❌ No order data to plot")
            return
        
        # Create comprehensive plots
        fig = plt.figure(figsize=(20, 16))
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.2)
        
        # Colors
        color1, color2 = '#2E86AB', '#A23B72'  # Blue and Purple
        
        # Plot 1: Price Evolution - Leg 1
        ax1 = fig.add_subplot(gs[0, 0])
        if not leg1_orders.empty and 'b_price' in leg1_orders.columns and 'a_price' in leg1_orders.columns:
            mid1 = (leg1_orders['b_price'] + leg1_orders['a_price']) / 2
            spread1 = leg1_orders['a_price'] - leg1_orders['b_price']
            
            # Sample for plotting (every 50th point)
            sample_idx = range(0, len(mid1), max(1, len(mid1)//2000))
            sample_times = mid1.index[sample_idx]
            sample_mids = mid1.iloc[sample_idx]
            sample_bids = leg1_orders['b_price'].iloc[sample_idx]
            sample_asks = leg1_orders['a_price'].iloc[sample_idx]
            
            # Plot bid-ask spread
            ax1.fill_between(sample_times, sample_bids, sample_asks, alpha=0.3, color=color1, label='Bid-Ask Spread')
            ax1.plot(sample_times, sample_mids, color=color1, linewidth=1.5, label='Mid Price')
            
            ax1.set_title(f'debm01_25 (Jan 2025) - Price Evolution\n{len(leg1_orders):,} order updates', 
                         fontsize=14, fontweight='bold')
            ax1.set_ylabel('Price (€/MWh)', fontsize=12)
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # Format x-axis
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax1.tick_params(axis='x', rotation=45)
            
            # Add statistics box
            stats_text = f'Avg: {mid1.mean():.2f} €/MWh\nRange: {mid1.min():.2f} - {mid1.max():.2f}\nStd: {mid1.std():.2f}\nAvg Spread: {spread1.mean():.3f}'
            ax1.text(0.02, 0.98, stats_text, transform=ax1.transAxes, fontsize=10,
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        # Plot 2: Price Evolution - Leg 2
        ax2 = fig.add_subplot(gs[0, 1])
        if not leg2_orders.empty and 'b_price' in leg2_orders.columns and 'a_price' in leg2_orders.columns:
            mid2 = (leg2_orders['b_price'] + leg2_orders['a_price']) / 2
            spread2 = leg2_orders['a_price'] - leg2_orders['b_price']
            
            # Sample for plotting
            sample_idx = range(0, len(mid2), max(1, len(mid2)//2000))
            sample_times = mid2.index[sample_idx]
            sample_mids = mid2.iloc[sample_idx]
            sample_bids = leg2_orders['b_price'].iloc[sample_idx]
            sample_asks = leg2_orders['a_price'].iloc[sample_idx]
            
            # Plot bid-ask spread
            ax2.fill_between(sample_times, sample_bids, sample_asks, alpha=0.3, color=color2, label='Bid-Ask Spread')
            ax2.plot(sample_times, sample_mids, color=color2, linewidth=1.5, label='Mid Price')
            
            ax2.set_title(f'debm02_25 (Feb 2025) - Price Evolution\n{len(leg2_orders):,} order updates', 
                         fontsize=14, fontweight='bold')
            ax2.set_ylabel('Price (€/MWh)', fontsize=12)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
            
            # Format x-axis
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax2.tick_params(axis='x', rotation=45)
            
            # Add statistics box
            stats_text = f'Avg: {mid2.mean():.2f} €/MWh\nRange: {mid2.min():.2f} - {mid2.max():.2f}\nStd: {mid2.std():.2f}\nAvg Spread: {spread2.mean():.3f}'
            ax2.text(0.02, 0.98, stats_text, transform=ax2.transAxes, fontsize=10,
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='plum', alpha=0.8))
        
        # Plot 3: Trade Volume Analysis - Leg 1
        ax3 = fig.add_subplot(gs[1, 0])
        if not leg1_trades.empty and 'volume' in leg1_trades.columns:
            # Hourly volume
            hourly_vol1 = leg1_trades['volume'].resample('H').sum()
            bars1 = ax3.bar(hourly_vol1.index, hourly_vol1.values, alpha=0.7, color=color1, 
                           width=0.03, edgecolor='white', linewidth=0.5)
            
            ax3.set_title(f'debm01_25 - Trade Volume by Hour\n{len(leg1_trades):,} trades, {leg1_trades["volume"].sum():,.0f} MWh total', 
                         fontsize=14, fontweight='bold')
            ax3.set_ylabel('Volume (MWh)', fontsize=12)
            ax3.grid(True, alpha=0.3)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax3.tick_params(axis='x', rotation=45)
            
            # Add volume statistics
            vol_stats = f'Max Hour: {hourly_vol1.max():,.0f} MWh\nAvg Hour: {hourly_vol1.mean():,.0f} MWh\nAvg Trade: {leg1_trades["volume"].mean():.1f} MWh'
            ax3.text(0.02, 0.98, vol_stats, transform=ax3.transAxes, fontsize=10,
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        # Plot 4: Trade Volume Analysis - Leg 2  
        ax4 = fig.add_subplot(gs[1, 1])
        if not leg2_trades.empty and 'volume' in leg2_trades.columns:
            # Hourly volume
            hourly_vol2 = leg2_trades['volume'].resample('H').sum()
            bars2 = ax4.bar(hourly_vol2.index, hourly_vol2.values, alpha=0.7, color=color2,
                           width=0.03, edgecolor='white', linewidth=0.5)
            
            ax4.set_title(f'debm02_25 - Trade Volume by Hour\n{len(leg2_trades):,} trades, {leg2_trades["volume"].sum():,.0f} MWh total', 
                         fontsize=14, fontweight='bold')
            ax4.set_ylabel('Volume (MWh)', fontsize=12)
            ax4.grid(True, alpha=0.3)
            ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax4.tick_params(axis='x', rotation=45)
            
            # Add volume statistics
            vol_stats = f'Max Hour: {hourly_vol2.max():,.0f} MWh\nAvg Hour: {hourly_vol2.mean():,.0f} MWh\nAvg Trade: {leg2_trades["volume"].mean():.1f} MWh'
            ax4.text(0.02, 0.98, vol_stats, transform=ax4.transAxes, fontsize=10,
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='plum', alpha=0.8))
        
        # Plot 5: Calendar Spread Analysis (spans both columns)
        ax5 = fig.add_subplot(gs[2, :])
        if (not leg1_orders.empty and not leg2_orders.empty and 
            'b_price' in leg1_orders.columns and 'b_price' in leg2_orders.columns):
            
            # Calculate spread
            mid1_aligned = mid1.reindex(mid1.index.union(mid2.index)).ffill()
            mid2_aligned = mid2.reindex(mid1.index.union(mid2.index)).ffill()
            calendar_spread = mid1_aligned - mid2_aligned
            calendar_spread = calendar_spread.dropna()
            
            # Sample for plotting
            sample_spread = calendar_spread[::max(1, len(calendar_spread)//3000)]
            
            # Plot spread evolution
            ax5.plot(sample_spread.index, sample_spread.values, color='#E85A4F', linewidth=1.5, alpha=0.8)
            ax5.axhline(y=0, color='black', linestyle='--', alpha=0.5)
            ax5.axhline(y=calendar_spread.mean(), color='red', linestyle=':', alpha=0.7, label=f'Mean: {calendar_spread.mean():.3f}')
            
            # Fill positive/negative areas
            ax5.fill_between(sample_spread.index, 0, sample_spread.values, 
                            where=(sample_spread.values >= 0), alpha=0.3, color='green', label='Jan Premium')
            ax5.fill_between(sample_spread.index, 0, sample_spread.values,
                            where=(sample_spread.values < 0), alpha=0.3, color='red', label='Feb Premium')
            
            ax5.set_title(f'Calendar Spread (Jan - Feb 2025)\nSpread Evolution Over Time', 
                         fontsize=14, fontweight='bold')
            ax5.set_ylabel('Spread (€/MWh)', fontsize=12)
            ax5.set_xlabel('Date & Time', fontsize=12)
            ax5.legend()
            ax5.grid(True, alpha=0.3)
            ax5.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
            ax5.tick_params(axis='x', rotation=45)
            
            # Add spread statistics
            spread_stats = (f'Avg Spread: {calendar_spread.mean():.3f} €/MWh\n'
                           f'Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f}\n'
                           f'Volatility: {calendar_spread.std():.3f} €/MWh\n'
                           f'Structure: {"Contango" if calendar_spread.mean() > 0 else "Backwardation"}')
            ax5.text(0.02, 0.98, spread_stats, transform=ax5.transAxes, fontsize=11,
                    verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9))
        
        plt.suptitle('German Power Market Integration Analysis\ndebm01_25 vs debm02_25 (December 2-6, 2024)', 
                     fontsize=18, fontweight='bold', y=0.98)
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_comprehensive_plot.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"📈 Comprehensive plot saved: {output_path}")
        
        # Print detailed statistics
        print(f"\n📊 COMPREHENSIVE STATISTICS:")
        print("=" * 70)
        
        if not leg1_orders.empty and 'b_price' in leg1_orders.columns:
            print(f"🔵 debm01_25 (January 2025 Delivery):")
            print(f"   📊 Order Book: {len(leg1_orders):,} updates")
            print(f"   💰 Avg Price: {mid1.mean():.2f} ± {mid1.std():.2f} €/MWh")
            print(f"   📈 Price Range: {mid1.min():.2f} - {mid1.max():.2f} €/MWh")
            print(f"   📏 Avg Bid-Ask: {spread1.mean():.3f} €/MWh")
            if not leg1_trades.empty:
                print(f"   💹 Trades: {len(leg1_trades):,} ({leg1_trades['volume'].sum():,.0f} MWh)")
                print(f"   📦 Avg Trade Size: {leg1_trades['volume'].mean():.1f} MWh")
        
        if not leg2_orders.empty and 'b_price' in leg2_orders.columns:
            print(f"\n🟣 debm02_25 (February 2025 Delivery):")
            print(f"   📊 Order Book: {len(leg2_orders):,} updates")
            print(f"   💰 Avg Price: {mid2.mean():.2f} ± {mid2.std():.2f} €/MWh")
            print(f"   📈 Price Range: {mid2.min():.2f} - {mid2.max():.2f} €/MWh")
            print(f"   📏 Avg Bid-Ask: {spread2.mean():.3f} €/MWh")
            if not leg2_trades.empty:
                print(f"   💹 Trades: {len(leg2_trades):,} ({leg2_trades['volume'].sum():,.0f} MWh)")
                print(f"   📦 Avg Trade Size: {leg2_trades['volume'].mean():.1f} MWh")
        
        if 'calendar_spread' in locals():
            print(f"\n📈 CALENDAR SPREAD ANALYSIS:")
            print(f"   📊 Average Spread: {calendar_spread.mean():.3f} €/MWh")
            print(f"   📏 Spread Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f} €/MWh")
            print(f"   📊 Spread Volatility: {calendar_spread.std():.3f} €/MWh")
            print(f"   📈 Market Structure: {'Contango (Jan > Feb)' if calendar_spread.mean() > 0 else 'Backwardation (Feb > Jan)'}")
            print(f"   💡 Interpretation: {'Winter premium for Jan delivery' if calendar_spread.mean() > 0 else 'Feb delivery trades at premium'}")
        
        plt.show()
        print(f"\n🎉 COMPREHENSIVE INTEGRATION ANALYSIS COMPLETED!")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Integration plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_integration_final()