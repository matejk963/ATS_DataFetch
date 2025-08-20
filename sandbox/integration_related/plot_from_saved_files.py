#!/usr/bin/env python3
"""
Plot integration results from saved pickle/csv files
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
from glob import glob

def plot_from_saved_files():
    """Plot from saved result files"""
    print("📊 Looking for saved integration files...")
    
    # Look for saved data files
    data_dir = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test'
    
    # Find pickle files or CSV files
    pickle_files = glob(f"{data_dir}/*debm*_25*.pkl")
    csv_files = glob(f"{data_dir}/*debm*_25*.csv") 
    
    print(f"📁 Found files:")
    print(f"   Pickle files: {len(pickle_files)}")
    for f in pickle_files[:5]:  # Show first 5
        print(f"     {os.path.basename(f)}")
    
    print(f"   CSV files: {len(csv_files)}")  
    for f in csv_files[:5]:  # Show first 5
        print(f"     {os.path.basename(f)}")
    
    # Try to load and plot data
    if pickle_files:
        plot_pickle_data(pickle_files)
    elif csv_files:
        plot_csv_data(csv_files)
    else:
        print("❌ No saved data files found")
        print("💡 Run the integration script first to generate data")

def plot_pickle_data(pickle_files):
    """Plot data from pickle files"""
    print(f"\n📊 Loading pickle data...")
    
    # Create figure
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Integration Results from Saved Data', fontsize=16, fontweight='bold')
    
    # Colors for contracts
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    contract_data = {}
    
    for i, file in enumerate(pickle_files[:4]):  # Max 4 files
        try:
            print(f"📁 Loading: {os.path.basename(file)}")
            data = pd.read_pickle(file)
            contract_name = os.path.basename(file).split('.')[0]
            
            if isinstance(data, dict):
                # If it's a dict, look for common keys
                if 'orders' in data and not data['orders'].empty:
                    contract_data[contract_name] = data
                    print(f"   ✅ Found orders: {len(data['orders']):,} rows")
                    
                    orders = data['orders']
                    
                    # Plot mid prices if available
                    if 'b_price' in orders.columns and 'a_price' in orders.columns:
                        mid_prices = (orders['b_price'] + orders['a_price']) / 2
                        # Sample for cleaner plotting
                        sample = mid_prices[::max(1, len(mid_prices)//1000)]
                        
                        ax = axes[i//2, i%2]
                        ax.plot(sample.index, sample.values, color=colors[i], linewidth=1, alpha=0.8)
                        ax.set_title(f'{contract_name} Mid Prices\n{len(orders):,} orders', fontweight='bold')
                        ax.set_ylabel('Price (€/MWh)')
                        ax.grid(True, alpha=0.3)
                        ax.tick_params(axis='x', rotation=45)
                        
                        # Add statistics
                        ax.text(0.02, 0.98, f'Avg: {mid_prices.mean():.2f} €/MWh\nRange: {mid_prices.min():.2f}-{mid_prices.max():.2f}', 
                               transform=ax.transAxes, verticalalignment='top',
                               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
                
                if 'trades' in data and not data['trades'].empty:
                    print(f"   ✅ Found trades: {len(data['trades']):,} rows")
            
            elif isinstance(data, pd.DataFrame):
                # If it's a DataFrame directly
                contract_data[contract_name] = {'orders': data}
                print(f"   ✅ DataFrame with {len(data):,} rows")
                
                if 'b_price' in data.columns and 'a_price' in data.columns:
                    mid_prices = (data['b_price'] + data['a_price']) / 2
                    sample = mid_prices[::max(1, len(mid_prices)//1000)]
                    
                    ax = axes[i//2, i%2]
                    ax.plot(sample.index, sample.values, color=colors[i], linewidth=1, alpha=0.8)
                    ax.set_title(f'{contract_name} Mid Prices\n{len(data):,} records', fontweight='bold')
                    ax.set_ylabel('Price (€/MWh)')
                    ax.grid(True, alpha=0.3)
                    ax.tick_params(axis='x', rotation=45)
                    
        except Exception as e:
            print(f"   ❌ Failed to load {file}: {e}")
    
    # Remove empty subplots
    for i in range(len(pickle_files), 4):
        axes[i//2, i%2].remove()
    
    plt.tight_layout()
    
    # Save plot
    output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/saved_data_plot.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"📈 Plot saved: {output_path}")
    
    # Print summary
    print(f"\n📊 SUMMARY:")
    print("=" * 40)
    for contract, data in contract_data.items():
        if 'orders' in data:
            orders = data['orders']
            if 'b_price' in orders.columns and 'a_price' in orders.columns:
                mid = (orders['b_price'] + orders['a_price']) / 2
                print(f"{contract}:")
                print(f"  Records: {len(orders):,}")
                print(f"  Price: {mid.mean():.2f} ± {mid.std():.2f} €/MWh")
    
    plt.show()

def plot_csv_data(csv_files):
    """Plot data from CSV files"""
    print(f"\n📊 Loading CSV data...")
    # Similar logic for CSV files
    pass

if __name__ == "__main__":
    plot_from_saved_files()