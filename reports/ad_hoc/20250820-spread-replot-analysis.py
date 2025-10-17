#!/usr/bin/env python3
"""
Replot Analysis for debq4_25_frbq4_25 Spread Contracts
Analyzing corrected n_s logic implementation results
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import seaborn as sns
from pathlib import Path

# Set style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_integration_results():
    """Load the latest integration results"""
    results_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_results_v2.json"
    
    try:
        with open(results_path, 'r') as f:
            data = json.load(f)
        print(f"Loaded integration results from {results_path}")
        return data
    except Exception as e:
        print(f"Error loading results: {e}")
        return None

def process_trade_data(trades_data):
    """Process trade data into DataFrame"""
    if not trades_data:
        return pd.DataFrame()
    
    df_list = []
    for trade in trades_data:
        df_list.append({
            'timestamp': pd.to_datetime(trade.get('timestamp')),
            'price': float(trade.get('price', 0)),
            'quantity': float(trade.get('quantity', 0)),
            'source': trade.get('source', 'unknown'),
            'trade_type': trade.get('trade_type', 'unknown')
        })
    
    if df_list:
        df = pd.DataFrame(df_list)
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df
    return pd.DataFrame()

def calculate_daily_stats(df, source_col='source'):
    """Calculate daily statistics"""
    if df.empty:
        return pd.DataFrame()
    
    daily_stats = df.groupby([df['timestamp'].dt.date, source_col]).agg({
        'price': ['mean', 'min', 'max', 'count'],
        'quantity': 'sum'
    }).round(4)
    
    daily_stats.columns = ['_'.join(col).strip() for col in daily_stats.columns]
    return daily_stats.reset_index()

def create_comprehensive_plot(data):
    """Create comprehensive analysis plot"""
    # Extract data components
    stats = data.get('summary_statistics', {})
    merged_trades = data.get('merged_data', {}).get('trades', [])
    
    # Process trade data
    trades_df = process_trade_data(merged_trades)
    
    if trades_df.empty:
        print("No trade data found to plot")
        return
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 14))
    gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3, height_ratios=[2, 1, 1])
    
    # Main title
    fig.suptitle('DEBQ4_25 - FRBQ4_25 Spread Analysis\nCorrected n_s Logic Implementation', 
                 fontsize=16, fontweight='bold', y=0.95)
    
    # 1. Main time series plot
    ax1 = fig.add_subplot(gs[0, :])
    
    # Separate real and synthetic trades
    real_trades = trades_df[trades_df['trade_type'] == 'real']
    synthetic_trades = trades_df[trades_df['trade_type'] == 'synthetic']
    
    if not real_trades.empty:
        ax1.scatter(real_trades['timestamp'], real_trades['price'], 
                   alpha=0.8, s=60, c='red', label=f'Real Trades (n={len(real_trades)})', 
                   edgecolors='darkred', linewidth=0.5)
    
    if not synthetic_trades.empty:
        ax1.scatter(synthetic_trades['timestamp'], synthetic_trades['price'], 
                   alpha=0.6, s=30, c='blue', label=f'Synthetic Trades (n={len(synthetic_trades)})', 
                   edgecolors='darkblue', linewidth=0.5)
    
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Spread Price')
    ax1.set_title('Trade Prices Over Time', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Format x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # 2. Daily averages comparison
    ax2 = fig.add_subplot(gs[1, 0])
    daily_stats = calculate_daily_stats(trades_df, 'trade_type')
    
    if not daily_stats.empty:
        real_daily = daily_stats[daily_stats['trade_type'] == 'real']
        synthetic_daily = daily_stats[daily_stats['trade_type'] == 'synthetic']
        
        if not real_daily.empty:
            ax2.plot(pd.to_datetime(real_daily['timestamp']), real_daily['price_mean'], 
                    'ro-', label='Real (Daily Avg)', alpha=0.8, linewidth=2)
        
        if not synthetic_daily.empty:
            ax2.plot(pd.to_datetime(synthetic_daily['timestamp']), synthetic_daily['price_mean'], 
                    'bo-', label='Synthetic (Daily Avg)', alpha=0.8, linewidth=2)
    
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Daily Avg Price')
    ax2.set_title('Daily Average Prices', fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
    
    # 3. Trade count distribution
    ax3 = fig.add_subplot(gs[1, 1])
    trade_counts = trades_df['trade_type'].value_counts()
    
    colors = ['red' if x == 'real' else 'blue' for x in trade_counts.index]
    bars = ax3.bar(trade_counts.index, trade_counts.values, color=colors, alpha=0.7)
    
    # Add count labels on bars
    for bar, count in zip(bars, trade_counts.values):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                str(count), ha='center', va='bottom', fontweight='bold')
    
    ax3.set_title('Trade Count by Type', fontweight='bold')
    ax3.set_ylabel('Number of Trades')
    
    # 4. Price distribution
    ax4 = fig.add_subplot(gs[1, 2])
    
    if not real_trades.empty and not synthetic_trades.empty:
        ax4.hist(real_trades['price'], bins=20, alpha=0.7, color='red', 
                label='Real', density=True)
        ax4.hist(synthetic_trades['price'], bins=20, alpha=0.7, color='blue', 
                label='Synthetic', density=True)
        ax4.legend()
    elif not trades_df.empty:
        ax4.hist(trades_df['price'], bins=20, alpha=0.7, color='gray')
    
    ax4.set_xlabel('Price')
    ax4.set_ylabel('Density')
    ax4.set_title('Price Distribution', fontweight='bold')
    ax4.grid(True, alpha=0.3)
    
    # 5. Statistics summary table
    ax5 = fig.add_subplot(gs[2, :])
    ax5.axis('off')
    
    # Create summary statistics text
    summary_text = []
    summary_text.append("CORRECTED N_S LOGIC RESULTS SUMMARY")
    summary_text.append("=" * 50)
    
    # Extract key statistics
    total_trades = stats.get('total_trades', 0)
    total_orders = stats.get('total_orders', 0)
    real_count = len(real_trades) if not real_trades.empty else 0
    synthetic_count = len(synthetic_trades) if not synthetic_trades.empty else 0
    
    summary_text.append(f"Total Trades: {total_trades}")
    summary_text.append(f"Real Trades: {real_count}")
    summary_text.append(f"Synthetic Trades: {synthetic_count}")
    summary_text.append(f"Total Orders: {total_orders}")
    
    if not trades_df.empty:
        price_stats = trades_df['price'].describe()
        summary_text.append("")
        summary_text.append("PRICE STATISTICS:")
        summary_text.append(f"Mean: {price_stats['mean']:.4f}")
        summary_text.append(f"Std: {price_stats['std']:.4f}")
        summary_text.append(f"Min: {price_stats['min']:.4f}")
        summary_text.append(f"Max: {price_stats['max']:.4f}")
        
        # Time range
        time_range = trades_df['timestamp'].max() - trades_df['timestamp'].min()
        summary_text.append(f"Time Range: {time_range.days} days")
    
    # Data quality metrics
    if 'data_quality' in stats:
        quality = stats['data_quality']
        summary_text.append("")
        summary_text.append("DATA QUALITY:")
        for key, value in quality.items():
            summary_text.append(f"{key}: {value}")
    
    # Method comparison
    if 'method_comparison' in data:
        comparison = data['method_comparison']
        summary_text.append("")
        summary_text.append("METHOD COMPARISON:")
        for method, results in comparison.items():
            if isinstance(results, dict):
                summary_text.append(f"{method}: {results.get('total_trades', 'N/A')} trades")
    
    # Display summary text
    summary_str = "\n".join(summary_text)
    ax5.text(0.05, 0.95, summary_str, transform=ax5.transAxes, 
             fontfamily='monospace', fontsize=10, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    return fig

def main():
    """Main execution function"""
    print("Loading and analyzing corrected n_s logic results...")
    
    # Load data
    data = load_integration_results()
    if not data:
        print("Failed to load integration results")
        return
    
    # Create analysis plot
    fig = create_comprehensive_plot(data)
    
    if fig:
        # Save plot
        output_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/corrected_data_analysis.png"
        fig.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"Analysis plot saved to: {output_path}")
        
        plt.show()
        plt.close()
    
    print("Analysis complete!")

if __name__ == "__main__":
    main()