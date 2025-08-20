#!/usr/bin/env python3
"""
Plot individual leg data by calling integration with individual leg mode
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

def plot_individual_legs():
    """Plot individual leg data"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Fetching individual leg data for plotting...")
        
        # Config for individual legs (not spread)
        config = {
            'contracts': ['debm01_25'],  # Single contract for individual mode
            'period': {
                'start_date': '2024-12-02',
                'end_date': '2024-12-06'
            },
            'n_s': 3,
            'mode': 'individual'  # Changed to individual
        }
        
        print("📡 Fetching Leg 1 (debm01_25)...")
        result1 = integrated_fetch(config)
        
        # Config for second leg
        config['contracts'] = ['debm02_25']
        print("📡 Fetching Leg 2 (debm02_25)...")
        result2 = integrated_fetch(config)
        
        if not result1 or not result2:
            print("❌ Failed to get individual leg data")
            return
        
        print(f"📊 Result 1 keys: {list(result1.keys())}")
        print(f"📊 Result 2 keys: {list(result2.keys())}")
        
        # Extract individual leg data
        leg1_data = result1.get('individual_data', {}).get('debm01_25', {}) if 'individual_data' in result1 else {}
        leg2_data = result2.get('individual_data', {}).get('debm02_25', {}) if 'individual_data' in result2 else {}
        
        print(f"📊 Leg 1 data keys: {list(leg1_data.keys()) if leg1_data else 'None'}")
        print(f"📊 Leg 2 data keys: {list(leg2_data.keys()) if leg2_data else 'None'}")
        
        if not leg1_data and not leg2_data:
            print("❌ No individual leg data found")
            return
        
        # Get DataFrames
        leg1_orders = leg1_data.get('orders', pd.DataFrame())
        leg1_trades = leg1_data.get('trades', pd.DataFrame())
        leg2_orders = leg2_data.get('orders', pd.DataFrame())
        leg2_trades = leg2_data.get('trades', pd.DataFrame())
        
        print(f"📊 Data sizes:")
        print(f"   Leg 1: {len(leg1_orders):,} orders, {len(leg1_trades):,} trades")  
        print(f"   Leg 2: {len(leg2_orders):,} orders, {len(leg2_trades):,} trades")
        
        # Create plots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
        fig.suptitle('German Power Individual Contracts: debm01_25 vs debm02_25\n(December 2-6, 2024)', 
                     fontsize=16, fontweight='bold')
        
        colors = ['#1f77b4', '#ff7f0e']
        
        # Plot Leg 1 Orders
        if not leg1_orders.empty and 'b_price' in leg1_orders.columns and 'a_price' in leg1_orders.columns:
            mid1 = (leg1_orders['b_price'] + leg1_orders['a_price']) / 2
            spread1 = leg1_orders['a_price'] - leg1_orders['b_price']
            
            # Sample for cleaner plot
            sample1 = mid1[::max(1, len(mid1)//1500)]
            ax1.plot(sample1.index, sample1.values, color=colors[0], linewidth=1, alpha=0.8)
            ax1.set_title(f'debm01_25 (Jan 2025) Mid Prices\n{len(leg1_orders):,} order updates', fontweight='bold')
            ax1.set_ylabel('Price (€/MWh)')
            ax1.grid(True, alpha=0.3)
            ax1.tick_params(axis='x', rotation=45)
            
            # Stats box
            stats1 = f'Avg: {mid1.mean():.2f}€\nRange: {mid1.min():.2f}-{mid1.max():.2f}€\nStd: {mid1.std():.2f}€\nSpread: {spread1.mean():.3f}€'
            ax1.text(0.02, 0.98, stats1, transform=ax1.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        # Plot Leg 2 Orders
        if not leg2_orders.empty and 'b_price' in leg2_orders.columns and 'a_price' in leg2_orders.columns:
            mid2 = (leg2_orders['b_price'] + leg2_orders['a_price']) / 2
            spread2 = leg2_orders['a_price'] - leg2_orders['b_price']
            
            # Sample for cleaner plot
            sample2 = mid2[::max(1, len(mid2)//1500)]
            ax2.plot(sample2.index, sample2.values, color=colors[1], linewidth=1, alpha=0.8)
            ax2.set_title(f'debm02_25 (Feb 2025) Mid Prices\n{len(leg2_orders):,} order updates', fontweight='bold')
            ax2.set_ylabel('Price (€/MWh)')
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis='x', rotation=45)
            
            # Stats box
            stats2 = f'Avg: {mid2.mean():.2f}€\nRange: {mid2.min():.2f}-{mid2.max():.2f}€\nStd: {mid2.std():.2f}€\nSpread: {spread2.mean():.3f}€'
            ax2.text(0.02, 0.98, stats2, transform=ax2.transAxes, fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='lightorange', alpha=0.8))
        
        # Plot Leg 1 Trade Volume
        if not leg1_trades.empty and 'volume' in leg1_trades.columns:
            hourly_vol1 = leg1_trades['volume'].resample('H').sum()
            ax3.bar(hourly_vol1.index, hourly_vol1.values, alpha=0.7, color=colors[0], width=0.03)
            ax3.set_title(f'debm01_25 Hourly Trade Volume\n{len(leg1_trades):,} trades, {leg1_trades["volume"].sum():,.0f} MWh', fontweight='bold')
            ax3.set_ylabel('Volume (MWh)')
            ax3.grid(True, alpha=0.3)
            ax3.tick_params(axis='x', rotation=45)
        else:
            ax3.text(0.5, 0.5, 'No Trade Data\nAvailable', ha='center', va='center', transform=ax3.transAxes, fontsize=12)
            ax3.set_title('debm01_25 Trade Volume', fontweight='bold')
        
        # Plot Leg 2 Trade Volume
        if not leg2_trades.empty and 'volume' in leg2_trades.columns:
            hourly_vol2 = leg2_trades['volume'].resample('H').sum()
            ax4.bar(hourly_vol2.index, hourly_vol2.values, alpha=0.7, color=colors[1], width=0.03)
            ax4.set_title(f'debm02_25 Hourly Trade Volume\n{len(leg2_trades):,} trades, {leg2_trades["volume"].sum():,.0f} MWh', fontweight='bold')
            ax4.set_ylabel('Volume (MWh)')
            ax4.grid(True, alpha=0.3)
            ax4.tick_params(axis='x', rotation=45)
        else:
            ax4.text(0.5, 0.5, 'No Trade Data\nAvailable', ha='center', va='center', transform=ax4.transAxes, fontsize=12)
            ax4.set_title('debm02_25 Trade Volume', fontweight='bold')
        
        plt.tight_layout()
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/individual_legs_plot.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"📈 Individual legs plot saved: {output_path}")
        
        # Print statistics
        print(f"\n📊 INDIVIDUAL LEG STATISTICS:")
        print("=" * 50)
        
        if not leg1_orders.empty:
            print(f"🔵 debm01_25 (January 2025):")
            print(f"   Orders: {len(leg1_orders):,}")
            print(f"   Avg Price: {mid1.mean():.2f} ± {mid1.std():.2f} €/MWh")
            if not leg1_trades.empty:
                print(f"   Trades: {len(leg1_trades):,}, Volume: {leg1_trades['volume'].sum():,.0f} MWh")
        
        if not leg2_orders.empty:
            print(f"🟠 debm02_25 (February 2025):")
            print(f"   Orders: {len(leg2_orders):,}")
            print(f"   Avg Price: {mid2.mean():.2f} ± {mid2.std():.2f} €/MWh")
            if not leg2_trades.empty:
                print(f"   Trades: {len(leg2_trades):,}, Volume: {leg2_trades['volume'].sum():,.0f} MWh")
        
        # Calculate spread if both available
        if (not leg1_orders.empty and not leg2_orders.empty and 
            'b_price' in leg1_orders.columns and 'b_price' in leg2_orders.columns):
            
            mid1_aligned = mid1.reindex(mid1.index.union(mid2.index)).ffill()
            mid2_aligned = mid2.reindex(mid1.index.union(mid2.index)).ffill()
            calendar_spread = mid1_aligned - mid2_aligned
            calendar_spread = calendar_spread.dropna()
            
            print(f"\n📈 CALENDAR SPREAD (Jan - Feb):")
            print(f"   Average: {calendar_spread.mean():.3f} €/MWh")
            print(f"   Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f} €/MWh")
            print(f"   Structure: {'Contango' if calendar_spread.mean() > 0 else 'Backwardation'}")
        
        plt.show()
        print(f"\n🎉 Individual legs analysis completed!")
        
    except Exception as e:
        print(f"❌ Individual legs plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_individual_legs()