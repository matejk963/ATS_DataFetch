#!/usr/bin/env python3
"""
Simple Spread Plot - Clean Visualization
=======================================

Create a clean, focused plot showing the persistent level shifts.
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

# Configure matplotlib for clean plots
plt.style.use('default')
plt.rcParams['figure.figsize'] = [14, 8]
plt.rcParams['font.size'] = 11

def plot_spread_clean():
    """Create clean spread plot showing persistent levels"""
    
    print("🎨 Creating clean spread visualization...")
    
    # Load spread data
    spread_data = pd.read_parquet('/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test/debm11_25_debq1_26_tr_ba_dataanalysis_real.parquet')
    spread_data.index = pd.to_datetime(spread_data.index)
    spread_data = spread_data.sort_index()
    
    # Create the plot
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    
    # Plot spread data points
    ax.scatter(spread_data.index, spread_data['price'], alpha=0.8, s=50, c='darkblue', 
              edgecolors='white', linewidth=0.5, label=f'Spread Trades (n={len(spread_data)})')
    
    # Add regime level lines to show persistent levels
    regimes = [
        {'start': '2025-09-10', 'end': '2025-09-17', 'level': -5.1, 'color': '#d62728', 'trades': 5},
        {'start': '2025-10-08', 'end': '2025-10-10', 'level': -3.5, 'color': '#ff7f0e', 'trades': 45},
        {'start': '2025-10-10', 'end': '2025-10-14', 'level': -2.7, 'color': '#2ca02c', 'trades': 53},
        {'start': '2025-10-14', 'end': '2025-10-16', 'level': -1.7, 'color': '#1f77b4', 'trades': 25}
    ]
    
    for i, regime in enumerate(regimes):
        start_time = pd.Timestamp(regime['start'])
        end_time = pd.Timestamp(regime['end'])
        
        # Draw horizontal line showing persistent level
        ax.axhline(y=regime['level'], xmin=(start_time - spread_data.index.min()).total_seconds() / (spread_data.index.max() - spread_data.index.min()).total_seconds(),
                  xmax=(end_time - spread_data.index.min()).total_seconds() / (spread_data.index.max() - spread_data.index.min()).total_seconds(),
                  color=regime['color'], linewidth=4, alpha=0.8, 
                  label=f'Level {i+1}: {regime["level"]:.1f} EUR/MWh ({regime["trades"]} trades)')
    
    # Add trend arrow showing overall movement
    arrow_props = dict(arrowstyle='->', lw=3, color='red', alpha=0.7)
    ax.annotate('', xy=(spread_data.index[-20], -2.0), xytext=(spread_data.index[5], -4.8),
               arrowprops=arrow_props)
    ax.text(spread_data.index[30], -4.0, 'Market Evolution\n+3.4 EUR/MWh', 
           fontsize=12, fontweight='bold', color='red', ha='center',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.8))
    
    # Formatting
    ax.set_title('DEBM11_25 vs DEBQ1_26 Spread: Persistent Level Shifts', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_ylabel('Spread Price (EUR/MWh)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Trading Date', fontsize=14, fontweight='bold')
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    ax.tick_params(axis='x', rotation=45)
    
    # Grid and styling
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(loc='upper left', fontsize=10)
    
    # Add statistics box
    stats_text = f'''Market Statistics:
• Total Period: 36 days
• Total Trades: {len(spread_data)}
• Price Range: {spread_data['price'].min():.2f} to {spread_data['price'].max():.2f} EUR/MWh
• Regime Evolution: -5.1 → -1.7 EUR/MWh (+3.4)
• Pattern: Persistent levels, not random spikes'''
    
    ax.text(0.98, 0.02, stats_text, transform=ax.transAxes, fontsize=9,
           verticalalignment='bottom', horizontalalignment='right',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'debm11_25_debq1_26_spread_clean.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Clean spread plot saved: {plot_path}")
    
    # Also create a zoomed version focusing on the regime transitions
    create_regime_zoom_plot(spread_data, regimes)

def create_regime_zoom_plot(spread_data, regimes):
    """Create zoomed plot focusing on regime transitions"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Regime Transitions: Zoomed Views', fontsize=16, fontweight='bold')
    
    # Define zoom periods for each major transition
    zoom_periods = [
        {'start': '2025-09-10', 'end': '2025-09-20', 'title': 'Regime 1: Initial Low Liquidity'},
        {'start': '2025-10-06', 'end': '2025-10-12', 'title': 'Regime 2→3: Liquidity Surge'},
        {'start': '2025-10-09', 'end': '2025-10-15', 'title': 'Regime 3→4: Peak Activity'},
        {'start': '2025-10-13', 'end': '2025-10-17', 'title': 'Regime 4: Stabilization'}
    ]
    
    for i, (ax, period) in enumerate(zip(axes.flat, zoom_periods)):
        start_time = pd.Timestamp(period['start'])
        end_time = pd.Timestamp(period['end'])
        
        # Filter data for this period
        period_data = spread_data[(spread_data.index >= start_time) & (spread_data.index <= end_time)]
        
        if len(period_data) > 0:
            ax.scatter(period_data.index, period_data['price'], alpha=0.8, s=60, 
                      c='darkblue', edgecolors='white', linewidth=1)
            
            # Add relevant regime lines
            for regime in regimes:
                regime_start = pd.Timestamp(regime['start'])
                regime_end = pd.Timestamp(regime['end'])
                
                if (regime_start <= end_time and regime_end >= start_time):
                    line_start = max(regime_start, start_time)
                    line_end = min(regime_end, end_time)
                    ax.axhline(y=regime['level'], color=regime['color'], 
                             linewidth=3, alpha=0.8, linestyle='--')
                    ax.text(line_start + (line_end - line_start)/2, regime['level'] + 0.1, 
                           f'{regime["level"]:.1f}', ha='center', fontweight='bold', 
                           color=regime['color'])
        
        ax.set_title(period['title'], fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
        
        if len(period_data) > 0:
            ax.set_ylim(period_data['price'].min() - 0.5, period_data['price'].max() + 0.5)
    
    plt.tight_layout()
    
    # Save zoom plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    zoom_path = output_dir / 'debm11_25_debq1_26_spread_regime_zoom.png'
    plt.savefig(zoom_path, dpi=300, bbox_inches='tight')
    print(f"✅ Regime zoom plot saved: {zoom_path}")

def main():
    """Main function"""
    plot_spread_clean()

if __name__ == "__main__":
    main()