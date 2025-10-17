#!/usr/bin/env python3
"""
Create a simple, clean plot showing spread prices over time
- Real data from parquet file
- Simple synthetic data simulation for comparison
- Basic trade count statistics
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def create_simple_spread_plot():
    """Create a simple spread plot with real and synthetic data"""
    
    # Load real data
    parquet_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet'
    df = pd.read_parquet(parquet_path)
    
    # Extract real trade data (non-NaN prices)
    real_trades = df.dropna(subset=['price']).copy()
    real_trades = real_trades.sort_index()
    
    # Calculate mid prices from bid/ask for additional context
    order_data = df.dropna(subset=['b_price', 'a_price'], how='all').copy()
    order_data['mid_price'] = (order_data['b_price'] + order_data['a_price']) / 2
    mid_prices = order_data.dropna(subset=['mid_price'])
    
    # Create simple synthetic data for comparison
    # Use real trade times as reference and add some synthetic points
    if len(real_trades) > 0:
        # Create synthetic trades at regular intervals
        start_time = real_trades.index.min()
        end_time = real_trades.index.max()
        
        # Generate synthetic timestamps (every 30 minutes)
        synthetic_times = pd.date_range(start=start_time, end=end_time, freq='30T')
        
        # Create synthetic prices with some realistic variation
        base_price = real_trades['price'].median()
        synthetic_prices = []
        
        for i, time in enumerate(synthetic_times):
            # Add some trend and noise
            trend = 0.1 * np.sin(i * 0.5)  # Slow oscillation
            noise = np.random.normal(0, 0.5)  # Random variation
            price = base_price + trend + noise
            synthetic_prices.append(price)
        
        synthetic_data = pd.DataFrame({
            'price': synthetic_prices,
            'volume': [1.0] * len(synthetic_times),
            'type': 'synthetic'
        }, index=synthetic_times)
    else:
        synthetic_data = pd.DataFrame()
    
    # Set up the plot
    plt.style.use('default')
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Plot real trade data
    if len(real_trades) > 0:
        ax.scatter(real_trades.index, real_trades['price'], 
                  c='#2E86AB', s=60, alpha=0.8, label=f'Real Trades (n={len(real_trades)})',
                  edgecolors='white', linewidth=0.5)
    
    # Plot synthetic data
    if len(synthetic_data) > 0:
        ax.scatter(synthetic_data.index, synthetic_data['price'],
                  c='#A23B72', s=40, alpha=0.7, label=f'Synthetic Trades (n={len(synthetic_data)})',
                  edgecolors='white', linewidth=0.5, marker='s')
    
    # Plot mid prices as background context (if available)
    if len(mid_prices) > 10:  # Only if we have enough data points
        # Sample mid prices to avoid overcrowding
        mid_sample = mid_prices.iloc[::10]  # Every 10th point
        ax.plot(mid_sample.index, mid_sample['mid_price'],
                color='#F18F01', alpha=0.3, linewidth=1, 
                label=f'Mid Prices (sample)', linestyle='--')
    
    # Formatting
    ax.set_xlabel('Time', fontsize=12, fontweight='bold')
    ax.set_ylabel('Spread Price', fontsize=12, fontweight='bold')
    ax.set_title('Spread Prices Over Time\nDE Base Q4 2025 - FR Base Q4 2025', 
                fontsize=14, fontweight='bold', pad=20)
    
    # Grid and legend
    ax.grid(True, alpha=0.3)
    ax.legend(frameon=True, fancybox=True, shadow=True, fontsize=10)
    
    # Format x-axis
    fig.autofmt_xdate()
    
    # Add statistics text box
    if len(real_trades) > 0 or len(synthetic_data) > 0:
        stats_text = []
        if len(real_trades) > 0:
            stats_text.append(f"Real Trades: {len(real_trades)}")
            stats_text.append(f"Price Range: {real_trades['price'].min():.2f} - {real_trades['price'].max():.2f}")
        if len(synthetic_data) > 0:
            stats_text.append(f"Synthetic Trades: {len(synthetic_data)}")
        
        if len(mid_prices) > 0:
            stats_text.append(f"Mid Price Points: {len(mid_prices)}")
        
        # Add time range
        if len(real_trades) > 0:
            time_span = real_trades.index.max() - real_trades.index.min()
            stats_text.append(f"Time Span: {time_span}")
        
        textstr = '\n'.join(stats_text)
        props = dict(boxstyle='round', facecolor='white', alpha=0.8)
        ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=9,
                verticalalignment='top', bbox=props)
    
    plt.tight_layout()
    
    # Save the plot
    output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/simple_spread_plot.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")
    
    # Show basic statistics
    print("\n=== SPREAD PLOT STATISTICS ===")
    if len(real_trades) > 0:
        print(f"Real Trade Count: {len(real_trades)}")
        print(f"Real Price Range: {real_trades['price'].min():.3f} to {real_trades['price'].max():.3f}")
        print(f"Real Price Mean: {real_trades['price'].mean():.3f}")
    
    if len(synthetic_data) > 0:
        print(f"Synthetic Trade Count: {len(synthetic_data)}")
        print(f"Synthetic Price Range: {synthetic_data['price'].min():.3f} to {synthetic_data['price'].max():.3f}")
    
    if len(mid_prices) > 0:
        print(f"Mid Price Points: {len(mid_prices)}")
        print(f"Mid Price Range: {mid_prices['mid_price'].min():.3f} to {mid_prices['mid_price'].max():.3f}")
    
    plt.show()
    return output_path

if __name__ == "__main__":
    create_simple_spread_plot()