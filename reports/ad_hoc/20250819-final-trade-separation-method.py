#!/usr/bin/env python3
"""
Final implementation: DataFetcher vs SpreadViewer trade separation method
"""

import pandas as pd

def separate_datafetcher_spreadviewer_trades(file_path):
    """
    Definitive method to separate DataFetcher from SpreadViewer trades
    in merged debq4_25_frbq4_25 dataset.
    
    Returns:
        datafetcher_trades: DataFrame with DataFetcher trades
        spreadviewer_trades: DataFrame with SpreadViewer trades
        summary: Dict with separation statistics
    """
    
    # Load data
    df = pd.read_parquet(file_path)
    
    # Filter for trade records only (records with price values)
    trades = df[df['price'].notna() & (df['price'] > 0)].copy()
    
    # SEPARATION METHOD: Use broker_id
    # broker_id 9999.0 = DataFetcher (synthetic trades)
    # broker_id 1441.0 = SpreadViewer (Eurex exchange trades)
    
    datafetcher_trades = trades[trades['broker_id'] == 9999.0].copy()
    spreadviewer_trades = trades[trades['broker_id'] == 1441.0].copy()
    
    # Verification based on trade ID patterns discovered:
    # DataFetcher: 'synth_buy_...' or 'synth_sell_...' format
    # SpreadViewer: 'Eurex T7/...' format
    
    datafetcher_verification = datafetcher_trades['tradeid'].str.contains('synth_').all()
    spreadviewer_verification = spreadviewer_trades['tradeid'].str.contains('Eurex T7/').all()
    
    summary = {
        'total_records': len(df),
        'total_trades': len(trades),
        'datafetcher_trades': len(datafetcher_trades),
        'spreadviewer_trades': len(spreadviewer_trades),
        'coverage_percent': (len(datafetcher_trades) + len(spreadviewer_trades)) / len(trades) * 100,
        'datafetcher_percent': len(datafetcher_trades) / len(trades) * 100,
        'spreadviewer_percent': len(spreadviewer_trades) / len(trades) * 100,
        'verification_passed': datafetcher_verification and spreadviewer_verification,
        'separation_method': 'broker_id',
        'datafetcher_broker_id': 9999.0,
        'spreadviewer_broker_id': 1441.0
    }
    
    return datafetcher_trades, spreadviewer_trades, summary

def print_separation_summary(summary):
    """Print detailed summary of the separation"""
    print("TRADE SEPARATION SUMMARY")
    print("=" * 50)
    print(f"Total records in file: {summary['total_records']:,}")
    print(f"Total trade records: {summary['total_trades']:,}")
    print(f"")
    print(f"DataFetcher trades: {summary['datafetcher_trades']:,} ({summary['datafetcher_percent']:.1f}%)")
    print(f"SpreadViewer trades: {summary['spreadviewer_trades']:,} ({summary['spreadviewer_percent']:.1f}%)")
    print(f"")
    print(f"Coverage: {summary['coverage_percent']:.1f}%")
    print(f"Verification passed: {'✓' if summary['verification_passed'] else '✗'}")
    print(f"")
    print(f"METHOD: {summary['separation_method']}")
    print(f"- DataFetcher broker_id: {summary['datafetcher_broker_id']}")
    print(f"- SpreadViewer broker_id: {summary['spreadviewer_broker_id']}")

def demonstrate_separation():
    """Demonstrate the separation method with actual data"""
    
    file_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"
    
    print("DEMONSTRATING TRADE SEPARATION METHOD")
    print("=" * 60)
    print(f"File: {file_path}")
    print()
    
    # Perform separation
    df_trades, sv_trades, summary = separate_datafetcher_spreadviewer_trades(file_path)
    
    # Print summary
    print_separation_summary(summary)
    
    print("\n" + "=" * 50)
    print("CHARACTERISTIC DIFFERENCES IDENTIFIED:")
    print("=" * 50)
    
    print("\n1. TRADE ID PATTERNS:")
    print(f"   DataFetcher: 'synth_buy_...' or 'synth_sell_...'")
    print(f"   Sample: {df_trades['tradeid'].iloc[0]}")
    print(f"   SpreadViewer: 'Eurex T7/...'") 
    print(f"   Sample: {sv_trades['tradeid'].iloc[0]}")
    
    print("\n2. PRICE PRECISION:")
    df_high_precision = (df_trades['price'].astype(str).str.split('.').str[1].str.len() >= 10).sum()
    sv_high_precision = (sv_trades['price'].astype(str).str.split('.').str[1].str.len() >= 10).sum()
    print(f"   DataFetcher: {df_high_precision}/{len(df_trades)} high precision ({df_high_precision/len(df_trades)*100:.1f}%)")
    print(f"   SpreadViewer: {sv_high_precision}/{len(sv_trades)} high precision ({sv_high_precision/len(sv_trades)*100:.1f}%)")
    
    print("\n3. VOLUME DISTRIBUTION:")
    df_vol_1 = (df_trades['volume'] == 1).sum()
    sv_vol_1 = (sv_trades['volume'] == 1).sum()
    print(f"   DataFetcher: {df_vol_1}/{len(df_trades)} volume=1 ({df_vol_1/len(df_trades)*100:.1f}%)")
    print(f"   SpreadViewer: {sv_vol_1}/{len(sv_trades)} volume=1 ({sv_vol_1/len(sv_trades)*100:.1f}%)")
    
    print("\n4. PRICE RANGES:")
    print(f"   DataFetcher: {df_trades['price'].min():.2f} - {df_trades['price'].max():.2f}")
    print(f"   SpreadViewer: {sv_trades['price'].min():.2f} - {sv_trades['price'].max():.2f}")
    
    print("\n5. ACTION DISTRIBUTION:")
    df_action_dist = df_trades['action'].value_counts()
    sv_action_dist = sv_trades['action'].value_counts()
    print(f"   DataFetcher: Buy={df_action_dist.get(1.0, 0)}, Sell={df_action_dist.get(-1.0, 0)}")
    print(f"   SpreadViewer: Buy={sv_action_dist.get(1.0, 0)}, Sell={sv_action_dist.get(-1.0, 0)}")
    
    return df_trades, sv_trades, summary

if __name__ == "__main__":
    df_trades, sv_trades, summary = demonstrate_separation()
    
    print("\n" + "=" * 60)
    print("READY-TO-USE SEPARATION FUNCTION:")
    print("=" * 60)
    print("""
# Use this function in your analysis:
datafetcher_trades, spreadviewer_trades, summary = separate_datafetcher_spreadviewer_trades(file_path)

# Results:
# - datafetcher_trades: DataFrame with synthetic trades (broker_id=9999)
# - spreadviewer_trades: DataFrame with exchange trades (broker_id=1441) 
# - summary: Statistics dictionary
    """)