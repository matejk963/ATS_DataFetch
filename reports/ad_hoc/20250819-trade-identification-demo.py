"""
Trade Source Identification Demo
Demonstrates how to identify DataFetcher vs SpreadViewer trades using broker_id
"""
import pandas as pd
import numpy as np

def identify_trade_sources(file_path):
    """
    Load trade data and identify sources using broker_id method
    
    Returns:
        tuple: (datafetcher_trades, spreadviewer_trades, summary)
    """
    print(f"Loading trade data from: {file_path}")
    df = pd.read_parquet(file_path)
    
    # Filter to actual trade data (remove NaN rows)
    trades = df.dropna(subset=['price', 'volume', 'action', 'broker_id'], how='all')
    print(f"Total trade records: {len(trades):,}")
    
    # Check broker_id distribution
    broker_dist = trades['broker_id'].value_counts().sort_index()
    print("\nBroker ID distribution:")
    for broker_id, count in broker_dist.items():
        if not pd.isna(broker_id):
            print(f"  {broker_id}: {count:,} trades ({count/len(trades)*100:.1f}%)")
    
    # Separate by broker_id (PRIMARY METHOD)
    datafetcher_trades = trades[trades['broker_id'] == 9999.0].copy()
    spreadviewer_trades = trades[trades['broker_id'] == 1441.0].copy()
    
    # Validate identification with tradeid patterns
    df_synthetic = 0
    sv_eurex = 0
    
    if len(datafetcher_trades) > 0:
        df_synthetic = datafetcher_trades['tradeid'].str.contains('synth_', na=False).sum()
    
    if len(spreadviewer_trades) > 0:
        sv_eurex = spreadviewer_trades['tradeid'].str.contains('Eurex T7/', na=False).sum()
    
    # Summary statistics
    summary = {
        'total_trades': len(trades),
        'datafetcher_count': len(datafetcher_trades),
        'spreadviewer_count': len(spreadviewer_trades),
        'datafetcher_percent': len(datafetcher_trades) / len(trades) * 100 if len(trades) > 0 else 0,
        'spreadviewer_percent': len(spreadviewer_trades) / len(trades) * 100 if len(trades) > 0 else 0,
        'validation_df_synthetic': df_synthetic,
        'validation_sv_eurex': sv_eurex,
        'coverage': (len(datafetcher_trades) + len(spreadviewer_trades)) / len(trades) * 100 if len(trades) > 0 else 0
    }
    
    return datafetcher_trades, spreadviewer_trades, summary

def print_identification_results(datafetcher_trades, spreadviewer_trades, summary):
    """Print detailed identification results"""
    print("\n" + "="*60)
    print("TRADE SOURCE IDENTIFICATION RESULTS")
    print("="*60)
    
    print(f"Method: broker_id field separation")
    print(f"Total trades analyzed: {summary['total_trades']:,}")
    print(f"Coverage: {summary['coverage']:.1f}%")
    print()
    
    print(f"📊 DataFetcher trades (broker_id=9999): {summary['datafetcher_count']:,} ({summary['datafetcher_percent']:.1f}%)")
    if summary['datafetcher_count'] > 0:
        print(f"   Validation: {summary['validation_df_synthetic']:,} synthetic tradeids")
        print("   Sample tradeids:")
        sample_ids = datafetcher_trades['tradeid'].dropna().head(3)
        for tid in sample_ids:
            print(f"     {tid}")
    
    print()
    print(f"📊 SpreadViewer trades (broker_id=1441): {summary['spreadviewer_count']:,} ({summary['spreadviewer_percent']:.1f}%)")
    if summary['spreadviewer_count'] > 0:
        print(f"   Validation: {summary['validation_sv_eurex']:,} Eurex tradeids")
        print("   Sample tradeids:")
        sample_ids = spreadviewer_trades['tradeid'].dropna().head(3)
        for tid in sample_ids:
            print(f"     {tid}")
    
    print("\nAction distribution comparison:")
    if summary['datafetcher_count'] > 0:
        df_actions = datafetcher_trades['action'].value_counts().sort_index()
        print("  DataFetcher:")
        for action, count in df_actions.items():
            action_name = "Buy" if action > 0 else "Sell" if action < 0 else "Neutral"
            print(f"    {action_name} ({action}): {count:,}")
    
    if summary['spreadviewer_count'] > 0:
        sv_actions = spreadviewer_trades['action'].value_counts().sort_index()
        print("  SpreadViewer:")
        for action, count in sv_actions.items():
            action_name = "Buy" if action > 0 else "Sell" if action < 0 else "Neutral"
            print(f"    {action_name} ({action}): {count:,}")

def demonstrate_identification_method():
    """Demonstrate the identification method on sample datasets"""
    
    print("TRADE SOURCE IDENTIFICATION DEMONSTRATION")
    print("="*60)
    print("Method: broker_id field (9999=DataFetcher, 1441=SpreadViewer)")
    print()
    
    # Test files
    test_files = [
        "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm08_25_frbm08_25_tr_ba_data.parquet",
        "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"
    ]
    
    for i, file_path in enumerate(test_files, 1):
        print(f"\n{'='*20} TEST {i} {'='*20}")
        try:
            df_trades, sv_trades, summary = identify_trade_sources(file_path)
            print_identification_results(df_trades, sv_trades, summary)
            
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

if __name__ == "__main__":
    demonstrate_identification_method()