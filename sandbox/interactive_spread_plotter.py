#!/usr/bin/env python3
"""
Interactive Spread Plotter
==========================

Interactive tabbed GUI for viewing spread combinations.
Features:
- One tab per spread combination
- Shows only merged data (trade prices)
- Clean plots without gaps
- Interactive navigation and zoom
- Real-time statistics

Usage:
    python sandbox/interactive_spread_plotter.py
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import tkinter as tk
from tkinter import ttk, messagebox
import glob
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
data_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test'

sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('seaborn-v0_8')

class InteractiveSpreadPlotter:
    """Interactive tabbed plotter for spread combinations"""
    
    def __init__(self):
        # Initialize main window
        self.root = tk.Tk()
        self.root.title("Interactive Spread Combinations Plotter")
        self.root.geometry("1600x900")
        self.root.configure(bg='#f0f0f0')
        
        # Data storage
        self.spread_data = {}
        self.figures = {}
        self.canvases = {}
        
        # Setup GUI
        self.setup_gui()
        
        # Load data
        self.load_data()
        
    def setup_gui(self):
        """Setup the GUI layout"""
        # Main container
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(self.main_frame, 
                               text="Interactive Spread Combinations Plotter", 
                               font=('Arial', 16, 'bold'))
        title_label.pack(pady=(0, 10))
        
        # Status frame
        self.status_frame = ttk.Frame(self.main_frame)
        self.status_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.status_label = ttk.Label(self.status_frame, 
                                     text="Initializing...", 
                                     font=('Arial', 10))
        self.status_label.pack(side=tk.LEFT)
        
        # Progress bar
        self.progress = ttk.Progressbar(self.status_frame, mode='indeterminate')
        self.progress.pack(side=tk.RIGHT, padx=(10, 0))
        
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # Control buttons frame
        self.button_frame = ttk.Frame(self.main_frame)
        self.button_frame.pack(fill=tk.X)
        
        # Refresh button
        self.refresh_btn = ttk.Button(self.button_frame, 
                                     text="🔄 Refresh Data", 
                                     command=self.refresh_data)
        self.refresh_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Save current plot button
        self.save_current_btn = ttk.Button(self.button_frame, 
                                          text="💾 Save Current Plot", 
                                          command=self.save_current_plot)
        self.save_current_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Save all plots button
        self.save_all_btn = ttk.Button(self.button_frame, 
                                      text="💾 Save All Plots", 
                                      command=self.save_all_plots)
        self.save_all_btn.pack(side=tk.LEFT, padx=(0, 10))
        
        # Exit button
        self.exit_btn = ttk.Button(self.button_frame, 
                                  text="❌ Exit", 
                                  command=self.on_closing)
        self.exit_btn.pack(side=tk.RIGHT)
        
        # Info label
        self.info_label = ttk.Label(self.button_frame, 
                                   text="Use mouse wheel to zoom, drag to pan", 
                                   font=('Arial', 9),
                                   foreground='gray')
        self.info_label.pack(side=tk.RIGHT, padx=(0, 20))
        
    def find_merged_spread_files(self):
        """Find merged spread data files only"""
        self.update_status("Searching for merged spread files...")
        
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
    
    def load_spread_data(self, file_path, spread_name):
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
    
    def load_data(self):
        """Load all spread data"""
        self.progress.start(10)
        self.update_status("Loading spread data...")
        
        # Find files
        spread_files = self.find_merged_spread_files()
        
        if not spread_files:
            self.update_status("No merged spread files found!")
            self.progress.stop()
            messagebox.showwarning("No Data", "No merged spread data files found!")
            return
        
        # Load each spread
        total = len(spread_files)
        for i, (spread_name, file_path) in enumerate(spread_files.items(), 1):
            self.update_status(f"Loading {spread_name} ({i}/{total})...")
            self.root.update()
            
            data = self.load_spread_data(file_path, spread_name)
            if data is not None:
                self.spread_data[spread_name] = data
        
        self.progress.stop()
        
        if self.spread_data:
            self.create_plots()
            self.update_status(f"✅ Loaded {len(self.spread_data)} spreads successfully!")
        else:
            self.update_status("❌ No valid spread data loaded!")
            messagebox.showerror("Error", "No valid spread data could be loaded!")
    
    def create_price_plot(self, spread_name, trades_df):
        """Create interactive price plot for a spread"""
        # Create figure with higher DPI for better quality
        fig = Figure(figsize=(12, 7), dpi=100, facecolor='white')
        ax = fig.add_subplot(111)
        
        if trades_df.empty:
            ax.text(0.5, 0.5, f'No trade data for {spread_name}', 
                   ha='center', va='center', transform=ax.transAxes, 
                   fontsize=16, style='italic', color='red')
            ax.set_title(f'{spread_name} - No Data', fontsize=14, fontweight='bold')
            return fig
        
        # Create sequential x-axis (no gaps)
        x_vals = np.arange(len(trades_df))
        prices = trades_df['price'].values
        
        # Plot line with good visual properties
        ax.plot(x_vals, prices, 
               color='#2E86C1',  # Nice blue
               linewidth=1.5,
               alpha=0.8,
               label=f'Merged Data ({len(trades_df):,} trades)')
        
        # Add scatter points for better visibility (sample points for large datasets)
        if len(trades_df) <= 2000:
            # For smaller datasets, show all points
            ax.scatter(x_vals, prices, 
                      color='#2E86C1', 
                      s=8, 
                      alpha=0.6,
                      zorder=5)
        else:
            # For larger datasets, sample points
            sample_indices = np.linspace(0, len(trades_df)-1, 1000, dtype=int)
            ax.scatter(x_vals[sample_indices], prices[sample_indices], 
                      color='#2E86C1', 
                      s=6, 
                      alpha=0.6,
                      zorder=5)
        
        # Set axis properties
        ax.set_title(f'{spread_name} - Merged Trade Prices', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xlabel('Sequential Time Points', fontsize=12)
        ax.set_ylabel('Price', fontsize=12)
        ax.legend(loc='best', frameon=True, shadow=True)
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Set axis limits with padding
        price_min, price_max = prices.min(), prices.max()
        price_range = price_max - price_min
        if price_range > 0:
            padding = 0.05 * price_range
            ax.set_ylim(price_min - padding, price_max + padding)
        
        ax.set_xlim(0, len(trades_df))
        
        # Add comprehensive statistics
        stats_text = f"📊 STATISTICS\n"
        stats_text += f"{'='*25}\n"
        stats_text += f"Total Trades: {len(trades_df):,}\n"
        stats_text += f"Price Range: {price_min:.3f} - {price_max:.3f}\n"
        stats_text += f"Mean Price: {np.mean(prices):.3f}\n"
        stats_text += f"Std Dev: {np.std(prices):.3f}\n"
        stats_text += f"Median: {np.median(prices):.3f}\n"
        
        # Add percentiles
        p25, p75 = np.percentile(prices, [25, 75])
        stats_text += f"25th %ile: {p25:.3f}\n"
        stats_text += f"75th %ile: {p75:.3f}"
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
               bbox=dict(boxstyle="round,pad=0.6", 
                        facecolor="white", 
                        alpha=0.95, 
                        edgecolor='#2E86C1',
                        linewidth=1),
               verticalalignment='top', 
               fontsize=9, 
               fontfamily='monospace')
        
        # Add date range info
        start_date = trades_df.index.min().strftime('%Y-%m-%d %H:%M')
        end_date = trades_df.index.max().strftime('%Y-%m-%d %H:%M')
        date_text = f"📅 DATA PERIOD\n"
        date_text += f"{'='*20}\n"
        date_text += f"Start: {start_date}\n"
        date_text += f"End: {end_date}\n"
        
        # Calculate duration
        duration = trades_df.index.max() - trades_df.index.min()
        date_text += f"Duration: {duration.days} days"
        
        ax.text(0.98, 0.02, date_text, transform=ax.transAxes,
               bbox=dict(boxstyle="round,pad=0.4", 
                        facecolor="lightyellow", 
                        alpha=0.9,
                        edgecolor='orange',
                        linewidth=1),
               verticalalignment='bottom', 
               horizontalalignment='right', 
               fontsize=9, 
               fontfamily='monospace')
        
        fig.tight_layout()
        return fig
    
    def create_plots(self):
        """Create all plots in tabs"""
        if not self.spread_data:
            return
        
        total = len(self.spread_data)
        for i, (spread_name, trades_df) in enumerate(self.spread_data.items(), 1):
            self.update_status(f"Creating plot {i}/{total}: {spread_name}")
            self.root.update()
            
            # Create tab frame
            tab_frame = ttk.Frame(self.notebook)
            self.notebook.add(tab_frame, text=spread_name)
            
            # Create plot
            fig = self.create_price_plot(spread_name, trades_df)
            self.figures[spread_name] = fig
            
            # Create canvas for matplotlib
            canvas = FigureCanvasTkAgg(fig, tab_frame)
            canvas.draw()
            self.canvases[spread_name] = canvas
            
            # Pack canvas
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
            
            # Add navigation toolbar
            toolbar_frame = ttk.Frame(tab_frame)
            toolbar_frame.pack(fill=tk.X, pady=(0, 5))
            
            toolbar = NavigationToolbar2Tk(canvas, toolbar_frame)
            toolbar.update()
            
            print(f"   ✅ Created interactive plot for {spread_name}")
    
    def get_current_spread(self):
        """Get currently selected spread name"""
        try:
            current_tab = self.notebook.index(self.notebook.select())
            spread_name = list(self.spread_data.keys())[current_tab]
            return spread_name
        except:
            return None
    
    def save_current_plot(self):
        """Save the currently visible plot"""
        spread_name = self.get_current_spread()
        if not spread_name or spread_name not in self.figures:
            messagebox.showwarning("No Plot", "No plot selected to save!")
            return
        
        # Create output directory
        output_dir = os.path.join(project_root, 'sandbox', 'plots', 'interactive')
        os.makedirs(output_dir, exist_ok=True)
        
        # Save plot
        plot_path = os.path.join(output_dir, f'{spread_name}_interactive.png')
        self.figures[spread_name].savefig(plot_path, dpi=300, bbox_inches='tight')
        
        self.update_status(f"✅ Saved: {spread_name}_interactive.png")
        messagebox.showinfo("Saved", f"Plot saved as:\n{plot_path}")
    
    def save_all_plots(self):
        """Save all plots"""
        if not self.figures:
            messagebox.showwarning("No Plots", "No plots to save!")
            return
        
        # Create output directory
        output_dir = os.path.join(project_root, 'sandbox', 'plots', 'interactive')
        os.makedirs(output_dir, exist_ok=True)
        
        saved_count = 0
        for spread_name, fig in self.figures.items():
            try:
                plot_path = os.path.join(output_dir, f'{spread_name}_interactive.png')
                fig.savefig(plot_path, dpi=300, bbox_inches='tight')
                saved_count += 1
            except Exception as e:
                print(f"Failed to save {spread_name}: {e}")
        
        self.update_status(f"✅ Saved {saved_count}/{len(self.figures)} plots")
        messagebox.showinfo("Saved", f"Saved {saved_count} plots to:\n{output_dir}")
    
    def refresh_data(self):
        """Refresh all data and plots"""
        # Clear existing data
        self.spread_data.clear()
        self.figures.clear()
        self.canvases.clear()
        
        # Clear notebook tabs
        for tab in self.notebook.tabs():
            self.notebook.forget(tab)
        
        # Reload data
        self.load_data()
    
    def update_status(self, message):
        """Update status label"""
        self.status_label.config(text=message)
        self.root.update()
        print(message)
    
    def on_closing(self):
        """Handle window closing"""
        self.root.quit()
        self.root.destroy()
    
    def run(self):
        """Run the interactive application"""
        print("🎨 Starting Interactive Spread Plotter...")
        print(f"   📁 Data path: {data_path}")
        print(f"   🖥️  GUI application starting...")
        
        # Handle window closing
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
        # Center window on screen
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')
        
        # Start the GUI event loop
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted by user")
        finally:
            print("🏁 Interactive plotter closed")


def main():
    """Main function"""
    print("🎨 Interactive Spread Combinations Plotter")
    print("=" * 50)
    
    try:
        # Check if we're in a graphical environment
        if os.environ.get('DISPLAY') is None and os.name != 'nt':
            print("⚠️  Warning: No display detected. GUI may not work in headless environment.")
            print("   Try running with X11 forwarding or use the simple plotter instead.")
        
        plotter = InteractiveSpreadPlotter()
        plotter.run()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Make sure tkinter is installed: sudo apt-get install python3-tk")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()