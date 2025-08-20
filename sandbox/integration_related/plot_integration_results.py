#!/usr/bin/env python3
"""
Plot integration results - trades and orders for the spread contracts
"""

import sys
import os
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import json

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_integration_results():
    """Plot the integration results"""
    try:
        from src.core.data_fetcher import DataFetcher
        
        # Load the saved results to get actual data
        config = {
            'contracts': ['debm01_25', 'debm02_25'],
            'period': {
                'start_date': '2024-12-02',
                'end_date': '2024-12-06'
            },
            'n_s': 3,
            'mode': 'spread'
        }
        
        print("📊 Loading integration data...")
        
        # Initialize DataFetcher
        fetcher = DataFetcher()
        
        # Parse contracts
        contract1_spec = fetcher.parse_contract_string(config['contracts'][0])
        contract2_spec = fetcher.parse_contract_string(config['contracts'][1])
        
        # Fetch individual leg data
        print("📡 Fetching Leg 1 (debm01_25)...")
        leg1_data = fetcher.fetch_individual_contract(
            contract1_spec['contract'], 
            config['period']['start_date'], 
            config['period']['end_date']
        )
        
        print("📡 Fetching Leg 2 (debm02_25)...")
        leg2_data = fetcher.fetch_individual_contract(
            contract2_spec['contract'], 
            config['period']['start_date'], 
            config['period']['end_date']
        )
        
        # Create figure with subplots
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Integration Results: debm01_25 vs debm02_25 (Dec 2-6, 2024)', fontsize=16, fontweight='bold')
        
        # Plot 1: Leg 1 Mid Prices
        if 'orders' in leg1_data and not leg1_data['orders'].empty:
            orders1 = leg1_data['orders']
            if 'b_price' in orders1.columns and 'a_price' in orders1.columns:
                mid_prices1 = (orders1['b_price'] + orders1['a_price']) / 2
                ax1.plot(mid_prices1.index, mid_prices1.values, 'b-', linewidth=0.8, alpha=0.7)
                ax1.set_title(f'Leg 1: debm01_25 Mid Prices\n{len(orders1):,} orders', fontweight='bold')
                ax1.set_ylabel('Price (€/MWh)')
                ax1.grid(True, alpha=0.3)
                ax1.tick_params(axis='x', rotation=45)
        
        # Plot 2: Leg 2 Mid Prices  
        if 'orders' in leg2_data and not leg2_data['orders'].empty:
            orders2 = leg2_data['orders']
            if 'b_price' in orders2.columns and 'a_price' in orders2.columns:
                mid_prices2 = (orders2['b_price'] + orders2['a_price']) / 2
                ax2.plot(mid_prices2.index, mid_prices2.values, 'r-', linewidth=0.8, alpha=0.7)
                ax2.set_title(f'Leg 2: debm02_25 Mid Prices\n{len(orders2):,} orders', fontweight='bold')
                ax2.set_ylabel('Price (€/MWh)')
                ax2.grid(True, alpha=0.3)
                ax2.tick_params(axis='x', rotation=45)
        
        # Plot 3: Trade Volume Leg 1
        if 'trades' in leg1_data and not leg1_data['trades'].empty:
            trades1 = leg1_data['trades']
            if 'volume' in trades1.columns:
                # Resample to hourly volume
                hourly_volume1 = trades1['volume'].resample('H').sum()
                ax3.bar(hourly_volume1.index, hourly_volume1.values, alpha=0.7, color='blue', width=0.03)
                ax3.set_title(f'Leg 1: Trade Volume by Hour\n{len(trades1):,} trades', fontweight='bold')
                ax3.set_ylabel('Volume (MWh)')
                ax3.grid(True, alpha=0.3)
                ax3.tick_params(axis='x', rotation=45)
        
        # Plot 4: Trade Volume Leg 2
        if 'trades' in leg2_data and not leg2_data['trades'].empty:
            trades2 = leg2_data['trades']
            if 'volume' in trades2.columns:
                # Resample to hourly volume
                hourly_volume2 = trades2['volume'].resample('H').sum()
                ax4.bar(hourly_volume2.index, hourly_volume2.values, alpha=0.7, color='red', width=0.03)
                ax4.set_title(f'Leg 2: Trade Volume by Hour\n{len(trades2):,} trades', fontweight='bold')
                ax4.set_ylabel('Volume (MWh)')
                ax4.grid(True, alpha=0.3)
                ax4.tick_params(axis='x', rotation=45)
        
        # Adjust layout and save
        plt.tight_layout()
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_results_plot.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        print(f"📈 Plot saved: {output_path}")
        
        # Show summary statistics
        print("\n📊 SUMMARY STATISTICS:")
        print("=" * 50)
        
        if 'orders' in leg1_data and not leg1_data['orders'].empty:
            orders1 = leg1_data['orders']
            mid1 = (orders1['b_price'] + orders1['a_price']) / 2
            print(f"Leg 1 (debm01_25):")
            print(f"  Orders: {len(orders1):,}")
            print(f"  Price range: {mid1.min():.2f} - {mid1.max():.2f} €/MWh")
            print(f"  Average price: {mid1.mean():.2f} €/MWh")
        
        if 'orders' in leg2_data and not leg2_data['orders'].empty:
            orders2 = leg2_data['orders']
            mid2 = (orders2['b_price'] + orders2['a_price']) / 2
            print(f"Leg 2 (debm02_25):")
            print(f"  Orders: {len(orders2):,}")
            print(f"  Price range: {mid2.min():.2f} - {mid2.max():.2f} €/MWh")
            print(f"  Average price: {mid2.mean():.2f} €/MWh")
        
        if 'trades' in leg1_data and not leg1_data['trades'].empty:
            trades1 = leg1_data['trades']
            print(f"  Trades: {len(trades1):,}")
            print(f"  Total volume: {trades1['volume'].sum():,.0f} MWh")
        
        if 'trades' in leg2_data and not leg2_data['trades'].empty:
            trades2 = leg2_data['trades']
            print(f"  Trades: {len(trades2):,}")
            print(f"  Total volume: {trades2['volume'].sum():,.0f} MWh")
        
        # Calculate spread
        if (('orders' in leg1_data and not leg1_data['orders'].empty) and 
            ('orders' in leg2_data and not leg2_data['orders'].empty)):
            
            # Align timestamps and calculate spread
            mid1_aligned = mid1.reindex(mid1.index.union(mid2.index)).ffill()
            mid2_aligned = mid2.reindex(mid1.index.union(mid2.index)).ffill()
            spread = mid1_aligned - mid2_aligned
            spread = spread.dropna()
            
            print(f"\nSpread Statistics (Leg1 - Leg2):")
            print(f"  Average spread: {spread.mean():.3f} €/MWh")
            print(f"  Spread range: {spread.min():.3f} to {spread.max():.3f} €/MWh")
            print(f"  Spread volatility: {spread.std():.3f} €/MWh")
        
        plt.show()
        
    except Exception as e:
        print(f"❌ Plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_integration_results()