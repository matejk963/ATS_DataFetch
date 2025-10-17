#!/usr/bin/env python3
"""
Comprehensive Replot for debq4_25_frbq4_25 Spread Contracts
Using corrected n_s logic implementation results
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import seaborn as sns
from pathlib import Path
import pickle

# Set style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_integration_results():
    """Load integration results and actual DataFrame data"""
    results_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_results_v2.json"
    data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.pkl"
    
    try:
        # Load summary statistics
        with open(results_path, 'r') as f:
            summary = json.load(f)
        print(f"Loaded integration summary from {results_path}")
        
        # Load actual DataFrame data
        with open(data_path, 'rb') as f:
            df_data = pickle.load(f)
        print(f"Loaded DataFrame data from {data_path}")
        
        return summary, df_data
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None

def analyze_dataframe_structure(df):
    """Analyze the structure of the loaded DataFrame"""
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Index type: {type(df.index)}")
    if hasattr(df.index, 'names'):
        print(f"Index names: {df.index.names}")
    print(f"Data types:\n{df.dtypes}")
    print(f"\nFirst few rows:")
    print(df.head())
    return df

def separate_trade_types(df):
    """Separate real and synthetic trades from the unified DataFrame"""
    real_trades = pd.DataFrame()
    synthetic_trades = pd.DataFrame()
    
    # Check if there's a source or type column
    if 'source' in df.columns:
        real_trades = df[df['source'].str.contains('real', case=False, na=False)].copy()
        synthetic_trades = df[df['source'].str.contains('synthetic', case=False, na=False)].copy()
    elif 'type' in df.columns:
        real_trades = df[df['type'].str.contains('real', case=False, na=False)].copy()
        synthetic_trades = df[df['type'].str.contains('synthetic', case=False, na=False)].copy()
    elif 'data_source' in df.columns:
        real_trades = df[df['data_source'].str.contains('real', case=False, na=False)].copy()
        synthetic_trades = df[df['data_source'].str.contains('synthetic', case=False, na=False)].copy()
    else:
        # Try to infer from data patterns or use all data
        print("No explicit source column found, using all data as mixed")
        # We'll treat all data as mixed and create subsets based on other criteria
        real_trades = df.copy()
        synthetic_trades = pd.DataFrame()
    
    print(f"Real trades: {len(real_trades)}")
    print(f"Synthetic trades: {len(synthetic_trades)}")
    
    return real_trades, synthetic_trades

def create_comprehensive_replot(summary, df_data):
    """Create comprehensive replot of the corrected n_s logic results"""
    
    # Analyze DataFrame structure
    print("Analyzing DataFrame structure...")
    df = analyze_dataframe_structure(df_data)
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    gs = fig.add_gridspec(4, 3, hspace=0.4, wspace=0.3, height_ratios=[2, 1.2, 1.2, 1])
    
    # Main title
    fig.suptitle('DEBQ4_25 - FRBQ4_25 Spread Analysis\nCorrected n_s Logic Implementation Results', 
                 fontsize=16, fontweight='bold', y=0.96)
    
    # Extract key info from summary
    source_stats = summary.get('merged_spread_data', {}).get('source_stats', {})
    metadata = summary.get('metadata', {})
    
    # 1. Main time series plot - Trades and Orders
    ax1 = fig.add_subplot(gs[0, :])
    
    # Ensure datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'datetime' in df.columns:
            df = df.set_index('datetime')
        elif 'timestamp' in df.columns:
            df = df.set_index('timestamp')
    
    # Plot different data types if they exist
    if 'trade_price' in df.columns:
        trade_data = df[df['trade_price'].notna()]
        if not trade_data.empty:
            ax1.scatter(trade_data.index, trade_data['trade_price'], 
                       alpha=0.8, s=60, c='red', label=f'Trades (n={len(trade_data)})', 
                       edgecolors='darkred', linewidth=0.5, zorder=5)
    
    if 'mid_price' in df.columns:
        mid_data = df[df['mid_price'].notna()]
        if not mid_data.empty:
            # Sample mid prices to avoid overcrowding
            if len(mid_data) > 1000:
                mid_sample = mid_data.sample(n=1000).sort_index()
            else:
                mid_sample = mid_data
            ax1.scatter(mid_sample.index, mid_sample['mid_price'], 
                       alpha=0.4, s=15, c='blue', label=f'Mid Prices (n={len(mid_data)})', 
                       zorder=3)
    
    # If we have bid/ask data
    if 'bid_price' in df.columns and 'ask_price' in df.columns:
        bid_data = df[df['bid_price'].notna()]
        ask_data = df[df['ask_price'].notna()]
        
        if not bid_data.empty and not ask_data.empty:
            # Sample for visualization
            if len(bid_data) > 500:
                bid_sample = bid_data.sample(n=500).sort_index()
                ask_sample = ask_data.sample(n=500).sort_index()
            else:
                bid_sample = bid_data
                ask_sample = ask_data
            
            ax1.scatter(bid_sample.index, bid_sample['bid_price'], 
                       alpha=0.3, s=10, c='green', label=f'Bids (n={len(bid_data)})', 
                       zorder=2)
            ax1.scatter(ask_sample.index, ask_sample['ask_price'], 
                       alpha=0.3, s=10, c='orange', label=f'Asks (n={len(ask_data)})', 
                       zorder=2)
    
    ax1.set_xlabel('Date & Time')
    ax1.set_ylabel('Price')
    ax1.set_title('Spread Prices Over Time - Corrected n_s Logic Results', fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Format x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # 2. Hourly activity pattern
    ax2 = fig.add_subplot(gs[1, 0])
    
    if 'trade_price' in df.columns:
        trade_data = df[df['trade_price'].notna()]
        if not trade_data.empty:
            hourly_trades = trade_data.groupby(trade_data.index.hour).size()
            ax2.bar(hourly_trades.index, hourly_trades.values, alpha=0.7, color='red')
            ax2.set_xlabel('Hour of Day')
            ax2.set_ylabel('Trade Count')
            ax2.set_title('Hourly Trade Distribution', fontweight='bold')
            ax2.grid(True, alpha=0.3)
    
    # 3. Price distribution
    ax3 = fig.add_subplot(gs[1, 1])
    
    prices = []
    labels = []
    colors = []
    
    if 'trade_price' in df.columns:
        trade_prices = df[df['trade_price'].notna()]['trade_price']
        if not trade_prices.empty:
            prices.append(trade_prices)
            labels.append('Trades')
            colors.append('red')
    
    if 'mid_price' in df.columns:
        mid_prices = df[df['mid_price'].notna()]['mid_price']
        if not mid_prices.empty:
            prices.append(mid_prices)
            labels.append('Mid Prices')
            colors.append('blue')
    
    if prices:
        for price_data, label, color in zip(prices, labels, colors):
            ax3.hist(price_data, bins=30, alpha=0.6, color=color, label=label, density=True)
        ax3.legend()
        ax3.set_xlabel('Price')
        ax3.set_ylabel('Density')
        ax3.set_title('Price Distribution', fontweight='bold')
        ax3.grid(True, alpha=0.3)
    
    # 4. Data quality metrics
    ax4 = fig.add_subplot(gs[1, 2])
    
    # Calculate data quality metrics
    total_records = len(df)
    trade_records = len(df[df['trade_price'].notna()]) if 'trade_price' in df.columns else 0
    order_records = total_records - trade_records
    
    categories = ['Total\nRecords', 'Trade\nRecords', 'Order\nRecords']
    values = [total_records, trade_records, order_records]
    colors_bar = ['gray', 'red', 'blue']
    
    bars = ax4.bar(categories, values, color=colors_bar, alpha=0.7)
    
    # Add count labels on bars
    for bar, count in zip(bars, values):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01, 
                str(count), ha='center', va='bottom', fontweight='bold')
    
    ax4.set_title('Data Summary', fontweight='bold')
    ax4.set_ylabel('Count')
    
    # 5. Time series with moving average
    ax5 = fig.add_subplot(gs[2, :2])
    
    if 'trade_price' in df.columns or 'mid_price' in df.columns:
        # Create a combined price series for moving average
        price_series = pd.Series(dtype=float, index=df.index)
        
        if 'trade_price' in df.columns:
            trade_mask = df['trade_price'].notna()
            price_series[trade_mask] = df.loc[trade_mask, 'trade_price']
        
        if 'mid_price' in df.columns:
            mid_mask = df['mid_price'].notna() & price_series.isna()
            price_series[mid_mask] = df.loc[mid_mask, 'mid_price']
        
        # Calculate moving averages
        if not price_series.empty:
            ma_30min = price_series.rolling('30T').mean()
            ma_1hour = price_series.rolling('1H').mean()
            
            # Plot raw prices
            ax5.scatter(df.index[df['trade_price'].notna()], 
                       df.loc[df['trade_price'].notna(), 'trade_price'], 
                       alpha=0.6, s=20, c='red', label='Trades', zorder=3)
            
            # Plot moving averages
            ax5.plot(ma_30min.index, ma_30min.values, 'b-', alpha=0.8, 
                    linewidth=2, label='30-min MA')
            ax5.plot(ma_1hour.index, ma_1hour.values, 'g-', alpha=0.8, 
                    linewidth=2, label='1-hour MA')
    
    ax5.set_xlabel('Time')
    ax5.set_ylabel('Price')
    ax5.set_title('Price Trends with Moving Averages', fontweight='bold')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
    
    # 6. Volume analysis (if available)
    ax6 = fig.add_subplot(gs[2, 2])
    
    if 'trade_quantity' in df.columns or 'quantity' in df.columns:
        qty_col = 'trade_quantity' if 'trade_quantity' in df.columns else 'quantity'
        qty_data = df[df[qty_col].notna()][qty_col]
        
        if not qty_data.empty:
            ax6.hist(qty_data, bins=20, alpha=0.7, color='purple')
            ax6.set_xlabel('Quantity')
            ax6.set_ylabel('Frequency')
            ax6.set_title('Trade Volume Distribution', fontweight='bold')
            ax6.grid(True, alpha=0.3)
    
    # 7. Statistics summary
    ax7 = fig.add_subplot(gs[3, :])
    ax7.axis('off')
    
    # Create comprehensive summary
    summary_lines = []
    summary_lines.append("CORRECTED N_S LOGIC IMPLEMENTATION RESULTS")
    summary_lines.append("=" * 60)
    summary_lines.append("")
    
    # Contract information
    contracts = metadata.get('contracts', [])
    summary_lines.append(f"Contracts: {' - '.join(contracts)}")
    
    period = metadata.get('period', {})
    summary_lines.append(f"Analysis Period: {period.get('start_date', 'N/A')} to {period.get('end_date', 'N/A')}")
    summary_lines.append(f"n_s Parameter: {metadata.get('n_s', 'N/A')} business days")
    summary_lines.append("")
    
    # Data statistics from source_stats
    summary_lines.append("DATA SUMMARY:")
    summary_lines.append(f"  Real Trades: {source_stats.get('real_trades', 'N/A')}")
    summary_lines.append(f"  Real Orders: {source_stats.get('real_orders', 'N/A')}")
    summary_lines.append(f"  Synthetic Trades: {source_stats.get('synthetic_trades', 'N/A')}")
    summary_lines.append(f"  Synthetic Orders: {source_stats.get('synthetic_orders', 'N/A')}")
    summary_lines.append(f"  Merged Trades: {source_stats.get('merged_trades', 'N/A')}")
    summary_lines.append(f"  Merged Orders: {source_stats.get('merged_orders', 'N/A')}")
    summary_lines.append(f"  Unified Total: {source_stats.get('unified_total', 'N/A')}")
    summary_lines.append("")
    
    # DataFrame statistics
    summary_lines.append("LOADED DATA ANALYSIS:")
    summary_lines.append(f"  DataFrame Shape: {df.shape}")
    summary_lines.append(f"  Time Range: {df.index.min()} to {df.index.max()}")
    
    if 'trade_price' in df.columns:
        trade_prices = df[df['trade_price'].notna()]['trade_price']
        if not trade_prices.empty:
            summary_lines.append(f"  Trade Price Range: {trade_prices.min():.4f} to {trade_prices.max():.4f}")
            summary_lines.append(f"  Trade Price Mean: {trade_prices.mean():.4f} ± {trade_prices.std():.4f}")
    
    summary_lines.append("")
    summary_lines.append("KEY IMPROVEMENTS:")
    summary_lines.append("  ✓ Corrected n_s logic shifts delivery date forward by n_s business days")
    summary_lines.append("  ✓ Proper alignment of contract delivery periods")
    summary_lines.append("  ✓ Enhanced data integration from real and synthetic sources")
    summary_lines.append("  ✓ Unified timestamp alignment for spread calculations")
    
    # Display summary
    summary_text = "\n".join(summary_lines)
    ax7.text(0.02, 0.98, summary_text, transform=ax7.transAxes, 
             fontfamily='monospace', fontsize=9, verticalalignment='top',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    plt.tight_layout()
    return fig

def main():
    """Main execution function"""
    print("Creating comprehensive replot for corrected n_s logic results...")
    
    # Load data
    summary, df_data = load_integration_results()
    if summary is None or df_data is None:
        print("Failed to load integration data")
        return
    
    # Create comprehensive replot
    fig = create_comprehensive_replot(summary, df_data)
    
    if fig:
        # Save plot
        output_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/corrected_data_analysis.png"
        fig.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"Comprehensive replot saved to: {output_path}")
        
        plt.show()
        plt.close()
    
    print("Replot analysis complete!")

if __name__ == "__main__":
    main()