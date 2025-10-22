#!/usr/bin/env python3
"""
Spread Combinations Plotter
===========================

Creates tabbed plots for all spread combinations showing only trade prices.
Features:
- One tab per spread combination
- Clean price plots without gaps
- Trade prices only (no orders/bid-ask)
- Tabbed interface for easy navigation
- Sequential x-axis (no time gaps)

Usage:
    python sandbox/combinations_plotter.py
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import tkinter as tk
from tkinter import ttk
import glob
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
data_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test'

sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('seaborn-v0_8')

class SpreadCombinationsPlotter:
    """Interactive plotter for spread combinations with tabbed interface"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Spread Combinations Plotter")
        self.root.geometry("1400x800")
        
        # Data storage
        self.spread_data = {}
        self.figures = {}
        
        # Create main frame
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Status label
        self.status_label = ttk.Label(self.main_frame, text="Loading spread data...")
        self.status_label.pack(pady=5)
        
        # Load data and create plots
        self.load_all_spreads()
        self.create_all_plots()
        
    def find_spread_files(self):
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
    
    def load_spread_data(self, file_path, data_type):
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
    
    def load_all_spreads(self):
        """Load all spread data files"""
        self.status_label.config(text="Finding spread files...")
        self.root.update()
        
        spread_files = self.find_spread_files()
        
        if not spread_files:
            self.status_label.config(text="No spread files found!")
            return
        
        total_spreads = len(spread_files)
        for i, (spread_name, files) in enumerate(spread_files.items(), 1):
            self.status_label.config(text=f"Loading {spread_name} ({i}/{total_spreads})...")
            self.root.update()
            
            print(f"\n📈 Loading spread: {spread_name}")
            
            spread_data = {}
            for data_type, file_path in files.items():
                data = self.load_spread_data(file_path, data_type)
                if data is not None:
                    spread_data[data_type] = data
            
            if spread_data:
                self.spread_data[spread_name] = spread_data
                print(f"   ✅ Successfully loaded {spread_name} with {len(spread_data)} data types")
            else:
                print(f"   ❌ No valid data for {spread_name}")
        
        self.status_label.config(text=f"Loaded {len(self.spread_data)} spreads successfully!")
    
    def create_price_plot(self, spread_name, spread_data):
        """Create price plot for a single spread"""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        colors = {'synthetic': '#2E86C1', 'real': '#E74C3C', 'merged': '#28B463'}
        markers = {'synthetic': 'o', 'real': 's', 'merged': '^'}
        
        all_prices = []
        
        for data_type, df in spread_data.items():
            if df.empty:
                continue
            
            # Create sequential x-axis (no gaps)
            x_vals = np.arange(len(df))
            prices = df['price'].values
            
            # Plot with different styles for each data type
            ax.plot(x_vals, prices, 
                   color=colors.get(data_type, '#34495E'),
                   marker=markers.get(data_type, 'o'),
                   markersize=3,
                   linewidth=1.5,
                   alpha=0.8,
                   label=f'{data_type.capitalize()} ({len(df):,} trades)',
                   markevery=max(1, len(df)//500))  # Show markers sparsely for large datasets
            
            all_prices.extend(prices)
        
        if all_prices:
            # Set axis properties
            ax.set_title(f'{spread_name} - Trade Prices Over Time', 
                        fontsize=14, fontweight='bold', pad=20)
            ax.set_xlabel('Sequential Time Points', fontsize=12)
            ax.set_ylabel('Price', fontsize=12)
            ax.legend(loc='best', frameon=True, shadow=True)
            ax.grid(True, alpha=0.3, linestyle='--')
            
            # Set y-axis limits with some padding
            price_min, price_max = min(all_prices), max(all_prices)
            price_range = price_max - price_min
            ax.set_ylim(price_min - 0.05 * price_range, 
                       price_max + 0.05 * price_range)
            
            # Add statistics text box
            stats_text = f"Price Range: {price_min:.3f} - {price_max:.3f}\n"
            stats_text += f"Total Points: {len(all_prices):,}\n"
            
            # Calculate basic statistics
            prices_array = np.array(all_prices)
            stats_text += f"Mean: {np.mean(prices_array):.3f}\n"
            stats_text += f"Std: {np.std(prices_array):.3f}"
            
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                   bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.9),
                   verticalalignment='top', fontsize=10, fontfamily='monospace')
        else:
            ax.text(0.5, 0.5, 'No trade data available', 
                   ha='center', va='center', transform=ax.transAxes, 
                   fontsize=14, style='italic')
            ax.set_title(f'{spread_name} - No Data', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        return fig
    
    def create_all_plots(self):
        """Create tabs and plots for all spreads"""
        if not self.spread_data:
            self.status_label.config(text="No data to plot!")
            return
        
        total_spreads = len(self.spread_data)
        
        for i, (spread_name, spread_data) in enumerate(self.spread_data.items(), 1):
            self.status_label.config(text=f"Creating plot {i}/{total_spreads}: {spread_name}")
            self.root.update()
            
            # Create tab frame
            tab_frame = ttk.Frame(self.notebook)
            self.notebook.add(tab_frame, text=spread_name)
            
            # Create plot
            fig = self.create_price_plot(spread_name, spread_data)
            self.figures[spread_name] = fig
            
            # Create canvas for matplotlib
            canvas = FigureCanvasTkAgg(fig, tab_frame)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
            
            # Add navigation toolbar
            toolbar = NavigationToolbar2Tk(canvas, tab_frame)
            toolbar.update()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
            
            print(f"   ✅ Created plot for {spread_name}")
        
        self.status_label.config(text=f"✅ All {total_spreads} plots created successfully!")
    
    def save_all_plots(self):
        """Save all plots as PNG files"""
        output_dir = os.path.join(project_root, 'sandbox', 'plots', 'combinations')
        os.makedirs(output_dir, exist_ok=True)
        
        print(f"\n💾 Saving plots to: {output_dir}")
        
        for spread_name, fig in self.figures.items():
            plot_path = os.path.join(output_dir, f'{spread_name}_trades_only.png')
            fig.savefig(plot_path, dpi=300, bbox_inches='tight')
            print(f"   📁 Saved: {spread_name}_trades_only.png")
        
        print(f"✅ Saved {len(self.figures)} plots")
    
    def run(self):
        """Run the interactive plotter"""
        # Add save button
        button_frame = ttk.Frame(self.main_frame)
        button_frame.pack(pady=5)
        
        save_button = ttk.Button(button_frame, text="Save All Plots", 
                                command=self.save_all_plots)
        save_button.pack(side=tk.LEFT, padx=5)
        
        quit_button = ttk.Button(button_frame, text="Quit", 
                                command=self.root.quit)
        quit_button.pack(side=tk.LEFT, padx=5)
        
        print(f"\n🎨 Starting interactive plotter...")
        print(f"   📊 {len(self.spread_data)} spreads loaded")
        print(f"   🖥️  GUI window opened")
        
        # Start the GUI
        self.root.mainloop()


def main():
    """Main function"""
    print("🎨 Spread Combinations Plotter")
    print("=" * 50)
    
    try:
        plotter = SpreadCombinationsPlotter()
        plotter.run()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🏁 Plotter finished")


if __name__ == "__main__":
    main()