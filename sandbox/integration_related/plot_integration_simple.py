#!/usr/bin/env python3
"""
Simple plot of integration results using the actual saved data files
"""

import sys
import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_integration_simple():
    """Simple plot of integration results"""
    try:
        from src.core.data_fetcher import DataFetcher
        
        print("📊 Loading integration data for plotting...")
        
        # Initialize DataFetcher
        fetcher = DataFetcher()
        
        # Fetch individual leg data directly
        print("📡 Fetching Leg 1 (debm01_25)...")
        leg1_data = fetcher.fetch_individual_contract(
            'debm01_25', 
            '2024-12-02', 
            '2024-12-06'
        )
        
        print("📡 Fetching Leg 2 (debm02_25)...")
        leg2_data = fetcher.fetch_individual_contract(
            'debm02_25', 
            '2024-12-02', 
            '2024-12-06'
        )
        
        # Create figure with subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Integration Results: debm01_25 vs debm02_25 (Dec 2-6, 2024)', fontsize=16, fontweight='bold')
        
        # Colors
        color1, color2 = '#1f77b4', '#ff7f0e'  # Blue and Orange
        
        # Plot 1: Leg 1 Mid Prices
        if 'orders' in leg1_data and not leg1_data['orders'].empty:
            orders1 = leg1_data['orders']
            print(f"📊 Leg 1 orders columns: {list(orders1.columns)}")
            
            if 'b_price' in orders1.columns and 'a_price' in orders1.columns:
                mid_prices1 = (orders1['b_price'] + orders1['a_price']) / 2
                # Sample every 100th point for cleaner plotting
                sample1 = mid_prices1[::100] if len(mid_prices1) > 1000 else mid_prices1
                ax1.plot(sample1.index, sample1.values, color=color1, linewidth=1, alpha=0.8)
                ax1.set_title(f'Leg 1: debm01_25 Mid Prices\n{len(orders1):,} orders', fontweight='bold')
                ax1.set_ylabel('Price (€/MWh)')
                ax1.grid(True, alpha=0.3)
                ax1.tick_params(axis='x', rotation=45)
                
                # Add daily average line
                daily_avg1 = mid_prices1.resample('D').mean()
                ax1.plot(daily_avg1.index, daily_avg1.values, 'ro-', linewidth=2, markersize=4, alpha=0.7, label='Daily Avg')
                ax1.legend()
        
        # Plot 2: Leg 2 Mid Prices  
        if 'orders' in leg2_data and not leg2_data['orders'].empty:
            orders2 = leg2_data['orders']
            print(f"📊 Leg 2 orders columns: {list(orders2.columns)}")
            
            if 'b_price' in orders2.columns and 'a_price' in orders2.columns:
                mid_prices2 = (orders2['b_price'] + orders2['a_price']) / 2
                # Sample every 100th point for cleaner plotting
                sample2 = mid_prices2[::100] if len(mid_prices2) > 1000 else mid_prices2
                ax2.plot(sample2.index, sample2.values, color=color2, linewidth=1, alpha=0.8)
                ax2.set_title(f'Leg 2: debm02_25 Mid Prices\n{len(orders2):,} orders', fontweight='bold')
                ax2.set_ylabel('Price (€/MWh)')
                ax2.grid(True, alpha=0.3)
                ax2.tick_params(axis='x', rotation=45)
                
                # Add daily average line
                daily_avg2 = mid_prices2.resample('D').mean()
                ax2.plot(daily_avg2.index, daily_avg2.values, 'ro-', linewidth=2, markersize=4, alpha=0.7, label='Daily Avg')
                ax2.legend()
        
        # Plot 3: Trade Activity Leg 1
        if 'trades' in leg1_data and not leg1_data['trades'].empty:
            trades1 = leg1_data['trades']
            print(f"📊 Leg 1 trades columns: {list(trades1.columns)}")
            
            if 'volume' in trades1.columns:
                # Resample to hourly volume
                hourly_volume1 = trades1['volume'].resample('H').sum()
                bars1 = ax3.bar(hourly_volume1.index, hourly_volume1.values, 
                               alpha=0.7, color=color1, width=0.03, edgecolor='white', linewidth=0.5)
                ax3.set_title(f'Leg 1: Hourly Trade Volume\n{len(trades1):,} trades, {trades1["volume"].sum():,.0f} MWh total', 
                             fontweight='bold')
                ax3.set_ylabel('Volume (MWh)')
                ax3.grid(True, alpha=0.3)
                ax3.tick_params(axis='x', rotation=45)
        
        # Plot 4: Trade Activity Leg 2
        if 'trades' in leg2_data and not leg2_data['trades'].empty:
            trades2 = leg2_data['trades']
            print(f"📊 Leg 2 trades columns: {list(trades2.columns)}")
            
            if 'volume' in trades2.columns:
                # Resample to hourly volume
                hourly_volume2 = trades2['volume'].resample('H').sum()
                bars2 = ax4.bar(hourly_volume2.index, hourly_volume2.values, 
                               alpha=0.7, color=color2, width=0.03, edgecolor='white', linewidth=0.5)
                ax4.set_title(f'Leg 2: Hourly Trade Volume\n{len(trades2):,} trades, {trades2["volume"].sum():,.0f} MWh total', 
                             fontweight='bold')
                ax4.set_ylabel('Volume (MWh)')
                ax4.grid(True, alpha=0.3)
                ax4.tick_params(axis='x', rotation=45)
        
        # Adjust layout
        plt.tight_layout()
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_results_plot.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        
        print(f"📈 Plot saved: {output_path}")
        
        # Show detailed statistics
        print("\n📊 DETAILED STATISTICS:")
        print("=" * 60)
        
        if 'orders' in leg1_data and not leg1_data['orders'].empty and 'b_price' in leg1_data['orders'].columns:
            orders1 = leg1_data['orders']
            mid1 = (orders1['b_price'] + orders1['a_price']) / 2
            spread1 = orders1['a_price'] - orders1['b_price']
            
            print(f"🔵 Leg 1 (debm01_25 - Jan 2025):")
            print(f"   Orders: {len(orders1):,}")
            print(f"   Price range: {mid1.min():.2f} - {mid1.max():.2f} €/MWh")
            print(f"   Average price: {mid1.mean():.2f} €/MWh")
            print(f"   Price volatility: {mid1.std():.2f} €/MWh")
            print(f"   Average spread: {spread1.mean():.3f} €/MWh")
            
            if 'trades' in leg1_data and not leg1_data['trades'].empty:
                trades1 = leg1_data['trades']
                print(f"   Trades: {len(trades1):,}")
                print(f"   Total volume: {trades1['volume'].sum():,.0f} MWh")
                print(f"   Avg trade size: {trades1['volume'].mean():.1f} MWh")
        
        print()
        
        if 'orders' in leg2_data and not leg2_data['orders'].empty and 'b_price' in leg2_data['orders'].columns:
            orders2 = leg2_data['orders']
            mid2 = (orders2['b_price'] + orders2['a_price']) / 2
            spread2 = orders2['a_price'] - orders2['b_price']
            
            print(f"🟠 Leg 2 (debm02_25 - Feb 2025):")
            print(f"   Orders: {len(orders2):,}")
            print(f"   Price range: {mid2.min():.2f} - {mid2.max():.2f} €/MWh")
            print(f"   Average price: {mid2.mean():.2f} €/MWh")
            print(f"   Price volatility: {mid2.std():.2f} €/MWh")
            print(f"   Average spread: {spread2.mean():.3f} €/MWh")
            
            if 'trades' in leg2_data and not leg2_data['trades'].empty:
                trades2 = leg2_data['trades']
                print(f"   Trades: {len(trades2):,}")
                print(f"   Total volume: {trades2['volume'].sum():,.0f} MWh")
                print(f"   Avg trade size: {trades2['volume'].mean():.1f} MWh")
        
        # Calculate spread between contracts if both available
        if (('orders' in leg1_data and not leg1_data['orders'].empty and 'b_price' in leg1_data['orders'].columns) and 
            ('orders' in leg2_data and not leg2_data['orders'].empty and 'b_price' in leg2_data['orders'].columns)):
            
            print(f"\n📈 SPREAD ANALYSIS (Jan - Feb 2025):")
            print("-" * 40)
            
            # Align timestamps and calculate spread
            mid1_aligned = mid1.reindex(mid1.index.union(mid2.index)).ffill()
            mid2_aligned = mid2.reindex(mid1.index.union(mid2.index)).ffill()
            contract_spread = mid1_aligned - mid2_aligned
            contract_spread = contract_spread.dropna()
            
            print(f"   Average spread: {contract_spread.mean():.3f} €/MWh")
            print(f"   Spread range: {contract_spread.min():.3f} to {contract_spread.max():.3f} €/MWh")
            print(f"   Spread volatility: {contract_spread.std():.3f} €/MWh")
            
            if contract_spread.mean() > 0:
                print(f"   💡 Jan trades at premium to Feb (contango)")
            else:
                print(f"   💡 Feb trades at premium to Jan (backwardation)")
        
        plt.show()
        
        print(f"\n🎉 Integration results successfully plotted and analyzed!")
        
    except Exception as e:
        print(f"❌ Plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_integration_simple()