#!/usr/bin/env python3
"""
Plot integration results by calling DataFetcher directly
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_direct():
    """Plot by calling DataFetcher methods directly"""
    try:
        from src.core.data_fetcher import DataFetcher
        
        print("📊 Creating direct DataFetcher plots...")
        
        # Initialize DataFetcher
        fetcher = DataFetcher()
        
        # Create contract configs
        contract1_config = {
            'contract': 'debm01_25',
            'start_date': '2024-12-02',
            'end_date': '2024-12-06'
        }
        
        contract2_config = {
            'contract': 'debm02_25', 
            'start_date': '2024-12-02',
            'end_date': '2024-12-06'
        }
        
        print("📡 Fetching contract data...")
        
        # Fetch data using correct method
        leg1_result = fetcher.fetch_contract_data(contract1_config, datetime.strptime('2024-12-02', '%Y-%m-%d'), datetime.strptime('2024-12-06', '%Y-%m-%d'))
        leg2_result = fetcher.fetch_contract_data(contract2_config, datetime.strptime('2024-12-02', '%Y-%m-%d'), datetime.strptime('2024-12-06', '%Y-%m-%d'))
        
        print(f"📊 Leg 1 result keys: {list(leg1_result.keys()) if leg1_result else 'None'}")
        print(f"📊 Leg 2 result keys: {list(leg2_result.keys()) if leg2_result else 'None'}")
        
        # Extract data
        leg1_data = leg1_result.get('data', pd.DataFrame()) if leg1_result else pd.DataFrame()
        leg2_data = leg2_result.get('data', pd.DataFrame()) if leg2_result else pd.DataFrame()
        
        if leg1_data.empty and leg2_data.empty:
            print("❌ No data retrieved")
            return
        
        print(f"📊 Leg 1 data: {len(leg1_data)} rows")
        print(f"📊 Leg 2 data: {len(leg2_data)} rows")
        
        if not leg1_data.empty:
            print(f"📊 Leg 1 columns: {list(leg1_data.columns)}")
        if not leg2_data.empty:
            print(f"📊 Leg 2 columns: {list(leg2_data.columns)}")
        
        # Create plots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        fig.suptitle('Integration Results: debm01_25 vs debm02_25 (Dec 2-6, 2024)', fontsize=16, fontweight='bold')
        
        colors = ['#1f77b4', '#ff7f0e']
        
        # Plot Leg 1
        if not leg1_data.empty:
            if 'b_price' in leg1_data.columns and 'a_price' in leg1_data.columns:
                mid_prices1 = (leg1_data['b_price'] + leg1_data['a_price']) / 2
                # Sample for cleaner plotting
                sample1 = mid_prices1[::max(1, len(mid_prices1)//2000)]
                ax1.plot(sample1.index, sample1.values, color=colors[0], linewidth=1, alpha=0.8)
                ax1.set_title(f'debm01_25 (Jan 2025)\n{len(leg1_data):,} records', fontweight='bold')
                ax1.set_ylabel('Price (€/MWh)')
                ax1.grid(True, alpha=0.3)
                ax1.tick_params(axis='x', rotation=45)
                
                # Add stats box
                ax1.text(0.02, 0.98, 
                        f'Avg: {mid_prices1.mean():.2f} €/MWh\n'
                        f'Range: {mid_prices1.min():.2f}-{mid_prices1.max():.2f}\n'
                        f'Vol: {mid_prices1.std():.2f}',
                        transform=ax1.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
        
        # Plot Leg 2
        if not leg2_data.empty:
            if 'b_price' in leg2_data.columns and 'a_price' in leg2_data.columns:
                mid_prices2 = (leg2_data['b_price'] + leg2_data['a_price']) / 2
                # Sample for cleaner plotting
                sample2 = mid_prices2[::max(1, len(mid_prices2)//2000)]
                ax2.plot(sample2.index, sample2.values, color=colors[1], linewidth=1, alpha=0.8)
                ax2.set_title(f'debm02_25 (Feb 2025)\n{len(leg2_data):,} records', fontweight='bold')
                ax2.set_ylabel('Price (€/MWh)')
                ax2.grid(True, alpha=0.3)
                ax2.tick_params(axis='x', rotation=45)
                
                # Add stats box
                ax2.text(0.02, 0.98,
                        f'Avg: {mid_prices2.mean():.2f} €/MWh\n'
                        f'Range: {mid_prices2.min():.2f}-{mid_prices2.max():.2f}\n'
                        f'Vol: {mid_prices2.std():.2f}',
                        transform=ax2.transAxes, verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='lightorange', alpha=0.8))
        
        plt.tight_layout()
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_direct_plot.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"📈 Plot saved: {output_path}")
        
        # Show statistics
        print(f"\n📊 STATISTICS:")
        print("=" * 50)
        
        if not leg1_data.empty and 'b_price' in leg1_data.columns:
            mid1 = (leg1_data['b_price'] + leg1_data['a_price']) / 2
            spread1 = leg1_data['a_price'] - leg1_data['b_price']
            print(f"🔵 debm01_25 (Jan 2025):")
            print(f"   Records: {len(leg1_data):,}")
            print(f"   Avg Price: {mid1.mean():.2f} ± {mid1.std():.2f} €/MWh")
            print(f"   Price Range: {mid1.min():.2f} - {mid1.max():.2f} €/MWh")
            print(f"   Avg Spread: {spread1.mean():.3f} €/MWh")
        
        if not leg2_data.empty and 'b_price' in leg2_data.columns:
            mid2 = (leg2_data['b_price'] + leg2_data['a_price']) / 2  
            spread2 = leg2_data['a_price'] - leg2_data['b_price']
            print(f"🟠 debm02_25 (Feb 2025):")
            print(f"   Records: {len(leg2_data):,}")
            print(f"   Avg Price: {mid2.mean():.2f} ± {mid2.std():.2f} €/MWh")
            print(f"   Price Range: {mid2.min():.2f} - {mid2.max():.2f} €/MWh")
            print(f"   Avg Spread: {spread2.mean():.3f} €/MWh")
            
            # Calculate contract spread if both available
            if not leg1_data.empty and 'b_price' in leg1_data.columns:
                # Align and calculate spread
                mid1_aligned = mid1.reindex(mid1.index.union(mid2.index)).ffill()
                mid2_aligned = mid2.reindex(mid1.index.union(mid2.index)).ffill()
                contract_spread = mid1_aligned - mid2_aligned
                contract_spread = contract_spread.dropna()
                
                print(f"\n📈 CALENDAR SPREAD (Jan - Feb 2025):")
                print(f"   Average: {contract_spread.mean():.3f} €/MWh")
                print(f"   Range: {contract_spread.min():.3f} to {contract_spread.max():.3f} €/MWh")
                print(f"   Volatility: {contract_spread.std():.3f} €/MWh")
                
                if contract_spread.mean() > 0:
                    print(f"   💡 Jan premium to Feb (contango structure)")
                else:
                    print(f"   💡 Feb premium to Jan (backwardation structure)")
        
        plt.show()
        print(f"\n🎉 Direct plotting completed successfully!")
        
    except Exception as e:
        print(f"❌ Direct plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_direct()