#!/usr/bin/env python3
"""
Demonstration: How to Separate DataFetcher vs SpreadViewer Data
Based on analysis of debq1_26_frbq1_26_tr_ba_data.parquet
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def separate_data_sources(df):
    """
    Separate DataFetcher trades from SpreadViewer quotes based on null patterns
    
    Args:
        df: DataFrame with merged data
        
    Returns:
        tuple: (datafetcher_trades, spreadviewer_quotes)
    """
    
    # DataFetcher: Has trade data (price, volume, action) but missing b_price/a_price
    datafetcher_mask = (
        df['price'].notna() & 
        df['volume'].notna() & 
        df['action'].notna()
    )
    datafetcher_trades = df[datafetcher_mask].copy()
    
    # SpreadViewer: Has bid/ask data but missing trade fields
    spreadviewer_mask = (
        df['b_price'].notna() | 
        df['a_price'].notna()
    ) 
    spreadviewer_quotes = df[spreadviewer_mask].copy()
    
    return datafetcher_trades, spreadviewer_quotes

def plot_separated_data(datafetcher_trades, spreadviewer_quotes, output_path):
    """
    Plot the separated data sources
    """
    plt.figure(figsize=(14, 8))
    
    # Plot DataFetcher trades
    if not datafetcher_trades.empty:
        buy_trades = datafetcher_trades[datafetcher_trades['action'] == 1.0]
        sell_trades = datafetcher_trades[datafetcher_trades['action'] == -1.0]
        
        if not buy_trades.empty:
            plt.scatter(buy_trades.index, buy_trades['price'], 
                       color='green', alpha=0.8, s=50, 
                       label=f'DataFetcher Buys ({len(buy_trades)})', marker='^')
        
        if not sell_trades.empty:
            plt.scatter(sell_trades.index, sell_trades['price'], 
                       color='red', alpha=0.8, s=50, 
                       label=f'DataFetcher Sells ({len(sell_trades)})', marker='v')
    
    # Plot SpreadViewer quotes (sample to avoid overcrowding)
    if not spreadviewer_quotes.empty:
        # Sample every 100th quote for visibility
        sample_quotes = spreadviewer_quotes.iloc[::100]
        
        bid_data = sample_quotes[sample_quotes['b_price'].notna()]
        ask_data = sample_quotes[sample_quotes['a_price'].notna()]
        
        if not bid_data.empty:
            plt.scatter(bid_data.index, bid_data['b_price'], 
                       color='blue', alpha=0.4, s=10, 
                       label=f'SpreadViewer Bids (sampled from {len(spreadviewer_quotes)})')
        
        if not ask_data.empty:
            plt.scatter(ask_data.index, ask_data['a_price'], 
                       color='orange', alpha=0.4, s=10, 
                       label=f'SpreadViewer Asks (sampled from {len(spreadviewer_quotes)})')
    
    plt.title('Separated DataFetcher Trades vs SpreadViewer Quotes')
    plt.xlabel('Time (Index)')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

def main():
    """Demonstrate the separation technique"""
    
    # Load data
    data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq1_26_frbq1_26_tr_ba_data.parquet"
    output_dir = Path("/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc")
    
    print("Loading data...")
    df = pd.read_parquet(data_path)
    print(f"Original data shape: {df.shape}")
    
    # Separate the data sources
    print("\nSeparating data sources...")
    datafetcher_trades, spreadviewer_quotes = separate_data_sources(df)
    
    print(f"DataFetcher trades: {len(datafetcher_trades)} rows")
    print(f"SpreadViewer quotes: {len(spreadviewer_quotes)} rows")
    
    # Show sample of each
    print(f"\nDataFetcher trades sample:")
    print(datafetcher_trades[['price', 'volume', 'action', 'broker_id']].head())
    
    print(f"\nSpreadViewer quotes sample:")
    print(spreadviewer_quotes[['b_price', 'a_price', '0']].head())
    
    # Create the plot
    output_path = output_dir / "20250819-separated-data-sources.png"
    print(f"\nCreating visualization: {output_path}")
    plot_separated_data(datafetcher_trades, spreadviewer_quotes, output_path)
    
    print("Demo complete!")
    
    return datafetcher_trades, spreadviewer_quotes

if __name__ == "__main__":
    trades, quotes = main()