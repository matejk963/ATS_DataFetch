#!/usr/bin/env python3
"""
Analysis script to understand how to separate DataFetcher trades from SpreadViewer trades
in the merged debq4_25_frbq4_25 dataset.

Date: 2025-08-19
Objective: Identify distinguishing characteristics between trades from different sources
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

def load_and_analyze_trades():
    """Load the merged dataset and analyze trade records"""
    
    # Load the data
    data_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"
    print(f"Loading data from: {data_path}")
    
    df = pd.read_parquet(data_path)
    print(f"Total records loaded: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data types:\n{df.dtypes}")
    
    return df

def analyze_trade_records(df):
    """Focus on records with price values (trades)"""
    
    print("\n" + "="*60)
    print("TRADE RECORDS ANALYSIS")
    print("="*60)
    
    # Filter for trade records (records with price values)
    trades = df[df['price'].notna() & (df['price'] > 0)]
    print(f"Total trade records (with price): {len(trades)}")
    print(f"Percentage of total: {len(trades)/len(df)*100:.2f}%")
    
    if len(trades) == 0:
        print("No trade records found!")
        return trades
    
    # Basic trade statistics
    print(f"\nPrice range: {trades['price'].min():.6f} - {trades['price'].max():.6f}")
    print(f"Volume range: {trades['volume'].min()} - {trades['volume'].max()}")
    
    return trades

def examine_potential_source_indicators(trades):
    """Examine fields that might indicate data source"""
    
    print("\n" + "="*60)
    print("POTENTIAL SOURCE INDICATORS")
    print("="*60)
    
    # Check broker_id patterns
    if 'broker_id' in trades.columns:
        print("\nBROKER_ID Analysis:")
        broker_counts = trades['broker_id'].value_counts()
        print(f"Unique broker_ids: {len(broker_counts)}")
        print("Top broker_ids:")
        print(broker_counts.head(10))
        
        print(f"\nBroker_id data type: {trades['broker_id'].dtype}")
        print(f"Sample broker_ids: {trades['broker_id'].unique()[:10]}")
    
    # Check action patterns
    if 'action' in trades.columns:
        print("\nACTION Analysis:")
        action_counts = trades['action'].value_counts()
        print(f"Unique actions: {list(action_counts.index)}")
        print("Action distribution:")
        print(action_counts)
    
    # Check tradeid patterns
    if 'tradeid' in trades.columns:
        print("\nTRADEID Analysis:")
        print(f"Total unique tradeids: {trades['tradeid'].nunique()}")
        print(f"Tradeid data type: {trades['tradeid'].dtype}")
        
        # Look for patterns in tradeid format
        sample_tradeids = trades['tradeid'].dropna().unique()[:20]
        print(f"Sample tradeids: {sample_tradeids}")
        
        # Check if tradeid has consistent format patterns
        tradeid_lens = trades['tradeid'].dropna().astype(str).str.len().value_counts()
        print(f"\nTradeid length distribution:")
        print(tradeid_lens.head())
    
    # Check for any columns that might contain source information
    potential_source_cols = [col for col in trades.columns 
                           if any(word in col.lower() for word in ['source', 'origin', 'system', 'feed', 'provider'])]
    if potential_source_cols:
        print(f"\nPotential source columns found: {potential_source_cols}")
        for col in potential_source_cols:
            print(f"{col} values: {trades[col].unique()}")
    else:
        print("\nNo obvious source indicator columns found")

def analyze_timestamp_patterns(trades):
    """Analyze timestamp patterns that might indicate different sources"""
    
    print("\n" + "="*60)
    print("TIMESTAMP PATTERN ANALYSIS")
    print("="*60)
    
    if 'timestamp' in trades.columns:
        trades['timestamp'] = pd.to_datetime(trades['timestamp'])
        
        print(f"Timestamp range: {trades['timestamp'].min()} to {trades['timestamp'].max()}")
        
        # Check for microsecond patterns
        trades['microseconds'] = trades['timestamp'].dt.microsecond
        microsec_patterns = trades['microseconds'].value_counts().head(10)
        print(f"\nTop microsecond values (might indicate different systems):")
        print(microsec_patterns)
        
        # Check for timing gaps or clusters
        trades_sorted = trades.sort_values('timestamp')
        time_diffs = trades_sorted['timestamp'].diff()
        
        print(f"\nTime differences between consecutive trades:")
        print(f"Mean: {time_diffs.mean()}")
        print(f"Median: {time_diffs.median()}")
        print(f"Min: {time_diffs.min()}")
        print(f"Max: {time_diffs.max()}")
        
        # Look for common time intervals (might indicate different update frequencies)
        time_diffs_ms = time_diffs.dt.total_seconds() * 1000
        common_intervals = time_diffs_ms.value_counts().head(10)
        print(f"\nMost common time intervals (ms):")
        print(common_intervals)
    
def analyze_volume_and_price_patterns(trades):
    """Look for patterns in volume and prices that might distinguish sources"""
    
    print("\n" + "="*60)
    print("VOLUME AND PRICE PATTERN ANALYSIS")
    print("="*60)
    
    # Volume analysis
    print("VOLUME PATTERNS:")
    volume_stats = trades['volume'].describe()
    print(volume_stats)
    
    # Look for common volume sizes (might indicate different sources)
    common_volumes = trades['volume'].value_counts().head(15)
    print(f"\nMost common volume sizes:")
    print(common_volumes)
    
    # Price precision analysis
    print("\nPRICE PATTERNS:")
    price_stats = trades['price'].describe()
    print(price_stats)
    
    # Check price precision (number of decimal places)
    price_strings = trades['price'].astype(str)
    decimal_places = price_strings.str.split('.').str[1].str.len()
    decimal_places = decimal_places.fillna(0)  # No decimal point means 0 decimal places
    
    print(f"\nPrice decimal places distribution:")
    print(decimal_places.value_counts().head())
    
    # Look for price clustering or common price increments
    price_diffs = trades.sort_values('price')['price'].diff().dropna()
    print(f"\nPrice increment statistics:")
    print(f"Mean price increment: {price_diffs.mean():.8f}")
    print(f"Median price increment: {price_diffs.median():.8f}")
    print(f"Most common price increments:")
    common_increments = price_diffs.value_counts().head(10)
    print(common_increments)

def analyze_potential_duplicates_or_overlaps(trades):
    """Look for potential duplicates or overlapping records from different sources"""
    
    print("\n" + "="*60)
    print("DUPLICATE/OVERLAP ANALYSIS")
    print("="*60)
    
    # Check for exact duplicates
    exact_dupes = trades.duplicated().sum()
    print(f"Exact duplicate records: {exact_dupes}")
    
    # Check for duplicates based on key fields
    key_fields = ['timestamp', 'price', 'volume']
    if all(field in trades.columns for field in key_fields):
        key_dupes = trades.duplicated(subset=key_fields).sum()
        print(f"Duplicates based on {key_fields}: {key_dupes}")
        
        if key_dupes > 0:
            print("\nSample duplicate records:")
            dupe_mask = trades.duplicated(subset=key_fields, keep=False)
            sample_dupes = trades[dupe_mask].head(10)
            print(sample_dupes[key_fields + ['broker_id', 'action', 'tradeid']])
    
    # Look for near-simultaneous trades (might be same trade from different sources)
    trades_sorted = trades.sort_values('timestamp')
    time_diffs = trades_sorted['timestamp'].diff()
    
    # Find trades within 1 second of each other
    close_trades = time_diffs < pd.Timedelta(seconds=1)
    close_trade_count = close_trades.sum()
    print(f"\nTrades within 1 second of each other: {close_trade_count}")
    
    if close_trade_count > 0:
        print("Sample close trades:")
        close_indices = trades_sorted[close_trades].index[:5]
        for idx in close_indices:
            current_trade = trades_sorted.loc[idx]
            prev_idx = trades_sorted.index[trades_sorted.index.get_loc(idx) - 1]
            prev_trade = trades_sorted.loc[prev_idx]
            
            print(f"\nPrevious: {prev_trade['timestamp']} - Price: {prev_trade['price']} - Vol: {prev_trade['volume']}")
            print(f"Current:  {current_trade['timestamp']} - Price: {current_trade['price']} - Vol: {current_trade['volume']}")

def identify_separation_strategies(trades):
    """Propose strategies for separating DataFetcher vs SpreadViewer trades"""
    
    print("\n" + "="*60)
    print("SEPARATION STRATEGY RECOMMENDATIONS")
    print("="*60)
    
    strategies = []
    
    # Strategy 1: Broker ID patterns
    if 'broker_id' in trades.columns:
        unique_brokers = trades['broker_id'].nunique()
        broker_dist = trades['broker_id'].value_counts()
        
        if unique_brokers <= 10:  # Reasonable number to analyze
            print("STRATEGY 1 - Broker ID Separation:")
            print(f"Found {unique_brokers} unique broker IDs")
            print("Broker distribution:")
            print(broker_dist)
            
            # Check if broker IDs show clear separation
            if len(broker_dist) == 2:
                print("✓ Two distinct broker IDs - likely represents the two sources!")
                strategies.append("Use broker_id to separate sources")
            elif broker_dist.iloc[0] > broker_dist.iloc[1:].sum():
                print("✓ One dominant broker ID - might indicate primary vs secondary source")
                strategies.append("Primary broker_id for one source, others for second source")
    
    # Strategy 2: Trade ID patterns
    if 'tradeid' in trades.columns:
        print("\nSTRATEGY 2 - Trade ID Patterns:")
        sample_ids = trades['tradeid'].dropna().astype(str).head(20).tolist()
        
        # Look for different ID formats
        id_patterns = {}
        for tid in sample_ids:
            if tid.isdigit():
                id_patterns['numeric'] = id_patterns.get('numeric', 0) + 1
            elif '-' in tid:
                id_patterns['hyphenated'] = id_patterns.get('hyphenated', 0) + 1
            elif '_' in tid:
                id_patterns['underscore'] = id_patterns.get('underscore', 0) + 1
            else:
                id_patterns['other'] = id_patterns.get('other', 0) + 1
        
        print(f"Trade ID patterns in sample: {id_patterns}")
        
        if len(id_patterns) > 1:
            print("✓ Multiple trade ID formats detected - could distinguish sources")
            strategies.append("Use tradeid format patterns to separate sources")
    
    # Strategy 3: Timestamp microsecond patterns
    if 'timestamp' in trades.columns:
        print("\nSTRATEGY 3 - Timestamp Patterns:")
        trades['microseconds'] = pd.to_datetime(trades['timestamp']).dt.microsecond
        
        # Check if microseconds show clustering (different system timestamps)
        microsec_dist = trades['microseconds'].value_counts()
        zero_microsec_pct = (trades['microseconds'] == 0).mean() * 100
        
        print(f"Records with zero microseconds: {zero_microsec_pct:.1f}%")
        
        if zero_microsec_pct > 80 or zero_microsec_pct < 20:
            print("✓ Clear microsecond pattern - one source rounds to seconds")
            strategies.append("Use microsecond precision to separate sources")
    
    # Strategy 4: Volume patterns
    print("\nSTRATEGY 4 - Volume Patterns:")
    volume_dist = trades['volume'].value_counts().head(10)
    print("Top volume sizes:")
    print(volume_dist)
    
    # Check if there are two distinct volume ranges or patterns
    volume_q25, volume_q75 = trades['volume'].quantile([0.25, 0.75])
    small_vol_count = (trades['volume'] <= volume_q25).sum()
    large_vol_count = (trades['volume'] >= volume_q75).sum()
    
    print(f"Small volumes (<= {volume_q25}): {small_vol_count}")
    print(f"Large volumes (>= {volume_q75}): {large_vol_count}")
    
    if small_vol_count > 0 and large_vol_count > 0:
        ratio = small_vol_count / large_vol_count
        if ratio > 3 or ratio < 0.33:
            print("✓ Distinct volume patterns - could indicate different sources")
            strategies.append("Use volume size patterns to separate sources")
    
    # Strategy 5: Action field analysis
    if 'action' in trades.columns:
        print("\nSTRATEGY 5 - Action Field:")
        action_dist = trades['action'].value_counts()
        print("Action distribution:")
        print(action_dist)
        
        if len(action_dist) > 1:
            print("✓ Multiple action types - could be source-specific")
            strategies.append("Use action field values to distinguish sources")
    
    return strategies

def generate_separation_code(trades, strategies):
    """Generate code snippets for the recommended separation strategies"""
    
    print("\n" + "="*60)
    print("IMPLEMENTATION CODE EXAMPLES")
    print("="*60)
    
    print("# Example functions to separate DataFetcher vs SpreadViewer trades:\n")
    
    if "Use broker_id to separate sources" in strategies:
        print("def separate_by_broker_id(df):")
        print("    \"\"\"Separate trades based on broker_id\"\"\"")
        broker_counts = trades['broker_id'].value_counts()
        if len(broker_counts) >= 2:
            primary_broker = broker_counts.index[0]
            print(f"    datafetcher_trades = df[df['broker_id'] == '{primary_broker}']")
            print(f"    spreadviewer_trades = df[df['broker_id'] != '{primary_broker}']")
        print("    return datafetcher_trades, spreadviewer_trades\n")
    
    if "Use microsecond precision to separate sources" in strategies:
        print("def separate_by_timestamp_precision(df):")
        print("    \"\"\"Separate trades based on timestamp microsecond precision\"\"\"")
        print("    df['timestamp'] = pd.to_datetime(df['timestamp'])")
        print("    df['microseconds'] = df['timestamp'].dt.microsecond")
        print("    # Assuming one source rounds to seconds (microseconds = 0)")
        print("    datafetcher_trades = df[df['microseconds'] == 0]")
        print("    spreadviewer_trades = df[df['microseconds'] != 0]")
        print("    return datafetcher_trades, spreadviewer_trades\n")
    
    if "Use tradeid format patterns to separate sources" in strategies:
        print("def separate_by_tradeid_format(df):")
        print("    \"\"\"Separate trades based on tradeid format patterns\"\"\"")
        print("    df['tradeid_str'] = df['tradeid'].astype(str)")
        print("    # Example: numeric vs non-numeric trade IDs")
        print("    numeric_mask = df['tradeid_str'].str.isdigit()")
        print("    datafetcher_trades = df[numeric_mask]")
        print("    spreadviewer_trades = df[~numeric_mask]")
        print("    return datafetcher_trades, spreadviewer_trades\n")
    
    if "Use action field values to distinguish sources" in strategies:
        print("def separate_by_action(df):")
        print("    \"\"\"Separate trades based on action field values\"\"\"")
        action_counts = trades['action'].value_counts()
        if len(action_counts) >= 2:
            primary_action = action_counts.index[0]
            print(f"    datafetcher_trades = df[df['action'] == '{primary_action}']")
            print(f"    spreadviewer_trades = df[df['action'] != '{primary_action}']")
        print("    return datafetcher_trades, spreadviewer_trades\n")

def main():
    """Main analysis function"""
    print("TRADE SOURCE SEPARATION ANALYSIS")
    print("="*60)
    print("Analyzing merged debq4_25_frbq4_25 dataset to distinguish DataFetcher vs SpreadViewer trades")
    
    # Load data
    df = load_and_analyze_trades()
    
    # Focus on trades
    trades = analyze_trade_records(df)
    
    if len(trades) == 0:
        print("No trade records found. Exiting analysis.")
        return
    
    # Analyze different aspects
    examine_potential_source_indicators(trades)
    analyze_timestamp_patterns(trades)
    analyze_volume_and_price_patterns(trades)
    analyze_potential_duplicates_or_overlaps(trades)
    
    # Identify separation strategies
    strategies = identify_separation_strategies(trades)
    
    # Generate implementation code
    generate_separation_code(trades, strategies)
    
    print("\n" + "="*60)
    print("SUMMARY OF RECOMMENDED STRATEGIES:")
    print("="*60)
    if strategies:
        for i, strategy in enumerate(strategies, 1):
            print(f"{i}. {strategy}")
    else:
        print("No clear separation strategies identified.")
        print("Consider examining the original data sources or merging process.")
    
    print(f"\nAnalysis complete. Total trade records analyzed: {len(trades)}")

if __name__ == "__main__":
    main()