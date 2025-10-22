#!/usr/bin/env python3
"""
Simple Spread Combinations Plotter
==================================

Creates individual PNG plots for all spread combinations showing only trade prices.
Features:
- One PNG file per spread combination
- Clean price plots without gaps
- Trade prices only (no orders/bid-ask)
- Sequential x-axis (no time gaps)
- All data types on one plot per spread

Usage:
    python sandbox/combinations_simple_plotter.py
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
data_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test'

sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [14, 8]
plt.rcParams['font.size'] = 11

def find_spread_files():
    """Find all spread data files in the test directory"""
    print(f"🔍 Searching for spread files in: {data_path}")
    
    # Look for parquet files with spread patterns
    pattern = os.path.join(data_path, "*_tr_ba_data_test_*.parquet")
    files = glob.glob(pattern)
    
    # Parse spread combinations
    spreads = {}
    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Extract spread name and data type
        # Expected format: contract1_contract2_tr_ba_data_test_datatype.parquet
        parts = filename.replace('.parquet', '').split('_')
        
        if len(parts) >= 6:  # Minimum parts for valid spread file
            # Find where 'tr' appears to split contract names from metadata
            try:
                tr_index = parts.index('tr')
                contract_parts = parts[:tr_index]
                
                # Try different contract split points
                if len(contract_parts) >= 2:
                    # For debm11_25_debm12_25 -> ['debm11', '25', 'debm12', '25']
                    if len(contract_parts) == 4:
                        contract1 = f"{contract_parts[0]}_{contract_parts[1]}"
                        contract2 = f"{contract_parts[2]}_{contract_parts[3]}"
                    # For simpler cases
                    elif len(contract_parts) == 2:
                        contract1 = contract_parts[0]
                        contract2 = contract_parts[1]
                    else:
                        # Try to split in half
                        mid = len(contract_parts) // 2
                        contract1 = '_'.join(contract_parts[:mid])
                        contract2 = '_'.join(contract_parts[mid:])
                    
                    spread_name = f"{contract1}_{contract2}"
                    
                    # Get data type (synthetic, real, merged)
                    data_type = parts[-1]  # Last part after 'test'
                    
                    if spread_name not in spreads:
                        spreads[spread_name] = {}
                    
                    spreads[spread_name][data_type] = file_path
                    print(f"   📊 Found: {spread_name} ({data_type})")
                    
            except ValueError:
                print(f"   ⚠️  Skipping invalid file format: {filename}")
                continue
    
    print(f"✅ Found {len(spreads)} spread combinations")
    return spreads

def load_spread_data(file_path, data_type):
    """Load and process spread data from parquet file"""
    try:
        df = pd.read_parquet(file_path)
        
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # Extract only trade prices (where price is not NaN)
        trades_df = df[df['price'].notna()].copy()
        
        if trades_df.empty:
            print(f"   ⚠️  No trade data found in {data_type}")
            return None
        
        # Sort by time and remove duplicates
        trades_df = trades_df.sort_index().drop_duplicates()
        
        print(f"   ✅ Loaded {len(trades_df):,} trades from {data_type}")
        return trades_df
        
    except Exception as e:
        print(f"   ❌ Failed to load {file_path}: {e}")
        return None

def create_price_plot(spread_name, spread_data):
    """Create price plot for a single spread"""
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Color scheme
    colors = {
        'synthetic': '#3498DB',  # Blue
        'real': '#E74C3C',       # Red  
        'merged': '#2ECC71'      # Green
    }
    
    line_styles = {
        'synthetic': '-',
        'real': '--', 
        'merged': '-.'
    }
    
    markers = {
        'synthetic': 'o',
        'real': 's',
        'merged': '^'
    }
    
    all_prices = []
    max_points = 0
    
    # Plot each data type
    for data_type in ['synthetic', 'real', 'merged']:  # Specific order
        if data_type not in spread_data:
            continue
            
        df = spread_data[data_type]
        if df.empty:
            continue
        
        # Create sequential x-axis (no gaps)
        x_vals = np.arange(len(df))
        prices = df['price'].values
        
        # Determine marker frequency based on data size
        marker_every = max(1, len(df) // 100) if len(df) > 100 else 1
        
        # Plot line
        line = ax.plot(x_vals, prices, 
                      color=colors[data_type],
                      linestyle=line_styles[data_type],
                      linewidth=2,
                      alpha=0.8,
                      label=f'{data_type.capitalize()} ({len(df):,} trades)')
        
        # Add markers sparsely for visibility
        if len(df) <= 1000:  # Only add markers for smaller datasets
            ax.plot(x_vals[::marker_every], prices[::marker_every],
                   marker=markers[data_type],
                   color=colors[data_type],
                   markersize=4,
                   alpha=0.6,
                   linestyle='None')
        
        all_prices.extend(prices)
        max_points = max(max_points, len(df))
    
    if all_prices:
        # Set axis properties
        ax.set_title(f'{spread_name} - Trade Prices Over Time', 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Sequential Time Points', fontsize=13)
        ax.set_ylabel('Price', fontsize=13)
        ax.legend(loc='best', frameon=True, shadow=True, fontsize=11)
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Set y-axis limits with padding
        price_min, price_max = min(all_prices), max(all_prices)
        price_range = price_max - price_min
        if price_range > 0:
            ax.set_ylim(price_min - 0.05 * price_range, 
                       price_max + 0.05 * price_range)
        
        # Set x-axis limits
        ax.set_xlim(0, max_points)
        
        # Add statistics text box
        stats_text = f"Price Range: {price_min:.3f} - {price_max:.3f}\n"
        stats_text += f"Total Trade Points: {len(all_prices):,}\n"
        
        # Calculate basic statistics
        prices_array = np.array(all_prices)
        stats_text += f"Mean Price: {np.mean(prices_array):.3f}\n"
        stats_text += f"Price Std: {np.std(prices_array):.3f}\n"
        stats_text += f"Data Sources: {len(spread_data)}"
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
               bbox=dict(boxstyle="round,pad=0.6", facecolor="white", alpha=0.95, edgecolor='gray'),
               verticalalignment='top', fontsize=10, fontfamily='monospace')
        
        # Add time period info if available
        date_ranges = []
        for data_type, df in spread_data.items():
            if not df.empty:
                start_date = df.index.min().strftime('%Y-%m-%d')
                end_date = df.index.max().strftime('%Y-%m-%d')
                date_ranges.append(f"{data_type}: {start_date} to {end_date}")
        
        if date_ranges:
            date_text = "Data Periods:\n" + "\n".join(date_ranges)
            ax.text(0.98, 0.02, date_text, transform=ax.transAxes,
                   bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.9),
                   verticalalignment='bottom', horizontalalignment='right', 
                   fontsize=9, fontfamily='monospace')
    else:
        ax.text(0.5, 0.5, f'No trade data available for {spread_name}', 
               ha='center', va='center', transform=ax.transAxes, 
               fontsize=16, style='italic', color='red')
        ax.set_title(f'{spread_name} - No Trade Data', fontsize=16, fontweight='bold')
    
    plt.tight_layout()
    return fig

def plot_all_combinations():
    """Plot all spread combinations"""
    print("🎨 Simple Spread Combinations Plotter")
    print("=" * 50)
    
    # Find all spread files
    spread_files = find_spread_files()
    
    if not spread_files:
        print("❌ No spread files found!")
        return
    
    # Create output directory
    output_dir = os.path.join(project_root, 'sandbox', 'plots', 'combinations')
    os.makedirs(output_dir, exist_ok=True)
    print(f"\n📁 Output directory: {output_dir}")
    
    # Process each spread
    total_spreads = len(spread_files)
    successful_plots = 0
    
    for i, (spread_name, files) in enumerate(spread_files.items(), 1):
        print(f"\n📈 Processing spread {i}/{total_spreads}: {spread_name}")
        print("=" * 60)
        
        # Load data for this spread
        spread_data = {}
        for data_type, file_path in files.items():
            data = load_spread_data(file_path, data_type)
            if data is not None:
                spread_data[data_type] = data
        
        if not spread_data:
            print(f"   ❌ No valid data for {spread_name}")
            continue
        
        # Create plot
        try:
            fig = create_price_plot(spread_name, spread_data)
            
            # Save plot
            plot_path = os.path.join(output_dir, f'{spread_name}_trades_only.png')
            fig.savefig(plot_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close(fig)  # Free memory
            
            successful_plots += 1
            print(f"   ✅ Saved: {spread_name}_trades_only.png")
            
        except Exception as e:
            print(f"   ❌ Failed to create plot for {spread_name}: {e}")
            continue
    
    # Summary
    print(f"\n🎉 Plot generation completed!")
    print(f"   📊 Total spreads found: {total_spreads}")
    print(f"   ✅ Successful plots: {successful_plots}")
    print(f"   📁 Plots saved in: {output_dir}")
    
    if successful_plots > 0:
        print(f"\n📋 Generated files:")
        for i, spread_name in enumerate(spread_files.keys(), 1):
            plot_file = f"{spread_name}_trades_only.png"
            print(f"   {i}. {plot_file}")

def main():
    """Main function"""
    try:
        plot_all_combinations()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()