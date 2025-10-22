#!/usr/bin/env python3
"""
Merged Plots Generator
=====================

Generates high-quality plots showing only merged data for each spread combination.
Each plot is saved as a separate PNG file with comprehensive statistics.

Usage:
    python sandbox/merged_plots_generator.py
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

# Configure matplotlib for high quality plots
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [14, 8]
plt.rcParams['font.size'] = 11
plt.rcParams['axes.grid'] = True
plt.rcParams['grid.alpha'] = 0.3

def find_merged_spread_files():
    """Find merged spread data files only"""
    print(f"🔍 Searching for merged spread files in: {data_path}")
    
    # Look for merged parquet files only
    pattern = os.path.join(data_path, "*_tr_ba_data_test_merged.parquet")
    files = glob.glob(pattern)
    
    spreads = {}
    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Extract spread name
        # Expected format: contract1_contract2_tr_ba_data_test_merged.parquet
        parts = filename.replace('_tr_ba_data_test_merged.parquet', '').split('_')
        
        if len(parts) >= 2:
            try:
                # For debm11_25_debm12_25 -> ['debm11', '25', 'debm12', '25']
                if len(parts) == 4:
                    contract1 = f"{parts[0]}_{parts[1]}"
                    contract2 = f"{parts[2]}_{parts[3]}"
                # For simpler cases
                elif len(parts) == 2:
                    contract1 = parts[0]
                    contract2 = parts[1]
                else:
                    # Try to split in half
                    mid = len(parts) // 2
                    contract1 = '_'.join(parts[:mid])
                    contract2 = '_'.join(parts[mid:])
                
                spread_name = f"{contract1}_{contract2}"
                spreads[spread_name] = file_path
                print(f"   📊 Found merged data: {spread_name}")
                
            except Exception as e:
                print(f"   ⚠️  Error parsing {filename}: {e}")
                continue
    
    print(f"✅ Found {len(spreads)} merged spread files")
    return spreads

def load_spread_data(file_path, spread_name):
    """Load merged spread data"""
    try:
        df = pd.read_parquet(file_path)
        
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        # Extract only trade prices (where price is not NaN)
        trades_df = df[df['price'].notna()].copy()
        
        if trades_df.empty:
            print(f"   ⚠️  No trade data found in {spread_name}")
            return None
        
        # Sort by time and remove duplicates
        trades_df = trades_df.sort_index().drop_duplicates()
        
        print(f"   ✅ Loaded {len(trades_df):,} trades for {spread_name}")
        return trades_df
        
    except Exception as e:
        print(f"   ❌ Failed to load {file_path}: {e}")
        return None

def create_merged_plot(spread_name, trades_df):
    """Create high-quality plot for merged data only"""
    fig, ax = plt.subplots(figsize=(16, 10))
    
    if trades_df.empty:
        ax.text(0.5, 0.5, f'No trade data for {spread_name}', 
               ha='center', va='center', transform=ax.transAxes, 
               fontsize=18, style='italic', color='red')
        ax.set_title(f'{spread_name} - No Data', fontsize=16, fontweight='bold')
        return fig
    
    # Create sequential x-axis (no gaps)
    x_vals = np.arange(len(trades_df))
    prices = trades_df['price'].values
    
    # Create main price line
    line = ax.plot(x_vals, prices, 
                   color='#2E86C1',  # Professional blue
                   linewidth=2,
                   alpha=0.9,
                   label=f'Merged Data ({len(trades_df):,} trades)',
                   zorder=3)
    
    # Add scatter points for better visibility
    if len(trades_df) <= 5000:
        # For smaller datasets, show more points
        sample_every = max(1, len(trades_df) // 2000)
        ax.scatter(x_vals[::sample_every], prices[::sample_every], 
                  color='#2E86C1', 
                  s=12, 
                  alpha=0.7,
                  zorder=4,
                  edgecolors='white',
                  linewidth=0.5)
    else:
        # For larger datasets, sample strategically
        sample_indices = np.linspace(0, len(trades_df)-1, 1500, dtype=int)
        ax.scatter(x_vals[sample_indices], prices[sample_indices], 
                  color='#2E86C1', 
                  s=8, 
                  alpha=0.6,
                  zorder=4)
    
    # Calculate statistics
    price_min, price_max = prices.min(), prices.max()
    price_mean = np.mean(prices)
    price_std = np.std(prices)
    price_median = np.median(prices)
    
    # Set axis properties with professional styling
    ax.set_title(f'{spread_name} - Merged Trade Prices Analysis', 
                fontsize=18, fontweight='bold', pad=25, color='#2C3E50')
    ax.set_xlabel('Sequential Time Points', fontsize=14, color='#34495E')
    ax.set_ylabel('Price', fontsize=14, color='#34495E')
    ax.legend(loc='best', frameon=True, shadow=True, fontsize=12)
    
    # Enhanced grid
    ax.grid(True, alpha=0.4, linestyle='-', linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Set axis limits with smart padding
    price_range = price_max - price_min
    if price_range > 0:
        padding = 0.08 * price_range
        ax.set_ylim(price_min - padding, price_max + padding)
    
    ax.set_xlim(-len(trades_df)*0.02, len(trades_df)*1.02)
    
    # Comprehensive statistics box
    stats_text = "📊 COMPREHENSIVE STATISTICS\n"
    stats_text += "=" * 35 + "\n"
    stats_text += f"Total Trade Points: {len(trades_df):,}\n"
    stats_text += f"Price Range: {price_min:.4f} - {price_max:.4f}\n"
    stats_text += f"Price Spread: {price_range:.4f}\n"
    stats_text += f"Mean Price: {price_mean:.4f}\n"
    stats_text += f"Median Price: {price_median:.4f}\n"
    stats_text += f"Standard Deviation: {price_std:.4f}\n"
    stats_text += f"Coefficient of Variation: {(price_std/price_mean)*100:.2f}%\n"
    
    # Add percentiles
    percentiles = [5, 25, 75, 95]
    pct_values = np.percentile(prices, percentiles)
    stats_text += "\nPercentiles:\n"
    for pct, val in zip(percentiles, pct_values):
        stats_text += f"  {pct:2d}th: {val:.4f}\n"
    
    # Add volatility measure
    if len(prices) > 1:
        returns = np.diff(prices) / prices[:-1]
        volatility = np.std(returns) * 100
        stats_text += f"\nPrice Volatility: {volatility:.2f}%"
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
           bbox=dict(boxstyle="round,pad=0.8", 
                    facecolor="white", 
                    alpha=0.95, 
                    edgecolor='#2E86C1',
                    linewidth=2),
           verticalalignment='top', 
           fontsize=10, 
           fontfamily='monospace',
           color='#2C3E50')
    
    # Time period and data info box
    start_date = trades_df.index.min()
    end_date = trades_df.index.max()
    duration = end_date - start_date
    
    time_text = "📅 DATA PERIOD & INFO\n"
    time_text += "=" * 25 + "\n"
    time_text += f"Start: {start_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
    time_text += f"End: {end_date.strftime('%Y-%m-%d %H:%M:%S')}\n"
    time_text += f"Duration: {duration.days} days, {duration.seconds//3600} hours\n"
    time_text += f"Frequency: {len(trades_df)/max(1, duration.total_seconds()/3600):.1f} trades/hour\n"
    
    # Add data quality metrics
    time_text += f"\nData Quality:\n"
    time_text += f"  No missing values ✓\n"
    time_text += f"  Sequential ordering ✓\n"
    time_text += f"  Duplicate removal ✓"
    
    ax.text(0.98, 0.02, time_text, transform=ax.transAxes,
           bbox=dict(boxstyle="round,pad=0.6", 
                    facecolor="lightyellow", 
                    alpha=0.95,
                    edgecolor='orange',
                    linewidth=2),
           verticalalignment='bottom', 
           horizontalalignment='right', 
           fontsize=10, 
           fontfamily='monospace',
           color='#2C3E50')
    
    # Add trend line if there's sufficient data
    if len(trades_df) > 10:
        z = np.polyfit(x_vals, prices, 1)
        p = np.poly1d(z)
        ax.plot(x_vals, p(x_vals), 
               color='red', 
               linestyle='--', 
               alpha=0.8, 
               linewidth=2,
               label=f'Trend (slope: {z[0]:.6f})')
        
        # Update legend
        ax.legend(loc='best', frameon=True, shadow=True, fontsize=12)
    
    # Style improvements
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#34495E')
    ax.spines['bottom'].set_color('#34495E')
    
    # Color the y-axis based on price direction
    if len(prices) > 1:
        if prices[-1] > prices[0]:
            ax.tick_params(axis='y', colors='green')
        else:
            ax.tick_params(axis='y', colors='red')
    
    plt.tight_layout()
    return fig

def generate_all_merged_plots():
    """Generate plots for all merged spread data"""
    print("🎨 Merged Plots Generator")
    print("=" * 50)
    
    # Find merged files
    spread_files = find_merged_spread_files()
    
    if not spread_files:
        print("❌ No merged spread files found!")
        return
    
    # Create output directory
    output_dir = os.path.join(project_root, 'sandbox', 'plots', 'merged_only')
    os.makedirs(output_dir, exist_ok=True)
    print(f"\n📁 Output directory: {output_dir}")
    
    # Process each spread
    successful_plots = 0
    
    for i, (spread_name, file_path) in enumerate(spread_files.items(), 1):
        print(f"\n📈 Processing {i}/{len(spread_files)}: {spread_name}")
        print("=" * 60)
        
        # Load data
        trades_df = load_spread_data(file_path, spread_name)
        
        if trades_df is None:
            print(f"   ❌ No valid data for {spread_name}")
            continue
        
        # Create plot
        try:
            fig = create_merged_plot(spread_name, trades_df)
            
            # Save plot with high quality
            plot_path = os.path.join(output_dir, f'{spread_name}_merged_analysis.png')
            fig.savefig(plot_path, dpi=300, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            plt.close(fig)  # Free memory
            
            successful_plots += 1
            print(f"   ✅ Saved: {spread_name}_merged_analysis.png")
            
        except Exception as e:
            print(f"   ❌ Failed to create plot for {spread_name}: {e}")
            continue
    
    # Summary
    print(f"\n🎉 Plot generation completed!")
    print(f"   📊 Total spreads: {len(spread_files)}")
    print(f"   ✅ Successful plots: {successful_plots}")
    print(f"   📁 Plots saved in: {output_dir}")
    
    if successful_plots > 0:
        print(f"\n📋 Generated high-quality merged plots:")
        for spread_name in spread_files.keys():
            plot_file = f"{spread_name}_merged_analysis.png"
            print(f"   • {plot_file}")

def main():
    """Main function"""
    try:
        generate_all_merged_plots()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()