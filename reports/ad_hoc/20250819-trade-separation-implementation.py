"""
Production-Ready Trade Separation Implementation
Definitive method for separating DataFetcher vs SpreadViewer trades
"""
import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any

def separate_datafetcher_spreadviewer_trades(file_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Separate trades by data source using broker_id field.
    
    IDENTIFICATION METHOD:
    - broker_id = 9999.0 → DataFetcher synthetic trades
    - broker_id = 1441.0 → SpreadViewer exchange trades
    
    Args:
        file_path: Path to parquet file containing trade data
        
    Returns:
        Tuple of (datafetcher_trades, spreadviewer_trades, summary_stats)
    """
    # Load trade data
    df = pd.read_parquet(file_path)
    
    # Filter to actual trade records
    trades = df.dropna(subset=['price', 'volume', 'action', 'broker_id'], how='all')
    
    if len(trades) == 0:
        raise ValueError("No valid trade data found in file")
    
    # Validate broker_id field exists and has expected values
    broker_ids = trades['broker_id'].dropna().unique()
    expected_brokers = {9999.0, 1441.0}
    unknown_brokers = set(broker_ids) - expected_brokers
    
    if unknown_brokers:
        print(f"Warning: Unknown broker_ids detected: {unknown_brokers}")
    
    # Separate trades by broker_id
    datafetcher_trades = trades[trades['broker_id'] == 9999.0].copy()
    spreadviewer_trades = trades[trades['broker_id'] == 1441.0].copy()
    
    # Validation: Check tradeid patterns
    df_validation = True
    sv_validation = True
    
    if len(datafetcher_trades) > 0:
        synthetic_count = datafetcher_trades['tradeid'].str.contains('synth_', na=False).sum()
        df_validation = synthetic_count == len(datafetcher_trades)
        if not df_validation:
            print(f"Warning: DataFetcher validation failed - {synthetic_count}/{len(datafetcher_trades)} have synthetic tradeids")
    
    if len(spreadviewer_trades) > 0:
        eurex_count = spreadviewer_trades['tradeid'].str.contains('Eurex T7/', na=False).sum()
        sv_validation = eurex_count == len(spreadviewer_trades)
        if not sv_validation:
            print(f"Warning: SpreadViewer validation failed - {eurex_count}/{len(spreadviewer_trades)} have Eurex tradeids")
    
    # Summary statistics
    summary = {
        'total_trades': len(trades),
        'datafetcher_count': len(datafetcher_trades),
        'spreadviewer_count': len(spreadviewer_trades),
        'coverage_percent': (len(datafetcher_trades) + len(spreadviewer_trades)) / len(trades) * 100,
        'validation_passed': df_validation and sv_validation,
        'unknown_brokers': list(unknown_brokers),
        'separation_method': 'broker_id',
        'datafetcher_broker_id': 9999.0,
        'spreadviewer_broker_id': 1441.0
    }
    
    return datafetcher_trades, spreadviewer_trades, summary

def validate_trade_separation(datafetcher_trades: pd.DataFrame, 
                            spreadviewer_trades: pd.DataFrame, 
                            summary: Dict[str, Any]) -> bool:
    """
    Validate the trade separation results
    
    Returns:
        bool: True if validation passes
    """
    validation_results = []
    
    # Test 1: Coverage should be 100%
    coverage_ok = summary['coverage_percent'] >= 99.9
    validation_results.append(("Coverage >= 99.9%", coverage_ok))
    
    # Test 2: No overlapping tradeids
    if len(datafetcher_trades) > 0 and len(spreadviewer_trades) > 0:
        df_tradeids = set(datafetcher_trades['tradeid'].dropna())
        sv_tradeids = set(spreadviewer_trades['tradeid'].dropna())
        no_overlap = len(df_tradeids.intersection(sv_tradeids)) == 0
        validation_results.append(("No tradeid overlap", no_overlap))
    
    # Test 3: Pattern validation passed
    validation_results.append(("Pattern validation", summary['validation_passed']))
    
    # Test 4: No unknown brokers (or acceptable ones)
    no_unknown = len(summary['unknown_brokers']) == 0
    validation_results.append(("No unknown broker_ids", no_unknown))
    
    # Print validation results
    print("\nValidation Results:")
    all_passed = True
    for test_name, passed in validation_results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {test_name}: {status}")
        all_passed &= passed
    
    return all_passed

# Example usage and testing
if __name__ == "__main__":
    # Test the implementation
    test_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm08_25_frbm08_25_tr_ba_data.parquet"
    
    try:
        print("Testing trade separation implementation...")
        print(f"File: {test_file}")
        
        datafetcher_trades, spreadviewer_trades, summary = separate_datafetcher_spreadviewer_trades(test_file)
        
        print(f"\nResults:")
        print(f"- Total trades: {summary['total_trades']:,}")
        print(f"- DataFetcher trades: {summary['datafetcher_count']:,} ({summary['datafetcher_count']/summary['total_trades']*100:.1f}%)")
        print(f"- SpreadViewer trades: {summary['spreadviewer_count']:,} ({summary['spreadviewer_count']/summary['total_trades']*100:.1f}%)")
        print(f"- Coverage: {summary['coverage_percent']:.1f}%")
        print(f"- Method: {summary['separation_method']}")
        
        # Validate results
        validation_passed = validate_trade_separation(datafetcher_trades, spreadviewer_trades, summary)
        
        if validation_passed:
            print(f"\n🎉 Trade separation successful and validated!")
        else:
            print(f"\n⚠️  Trade separation completed with validation warnings")
            
    except Exception as e:
        print(f"Error: {e}")

# USAGE INSTRUCTIONS:
"""
To use this in your code:

from trade_separation import separate_datafetcher_spreadviewer_trades

# Load and separate trades
file_path = "your_trade_data.parquet"
datafetcher_trades, spreadviewer_trades, summary = separate_datafetcher_spreadviewer_trades(file_path)

# Use the separated DataFrames
print(f"DataFetcher trades: {len(datafetcher_trades)}")
print(f"SpreadViewer trades: {len(spreadviewer_trades)}")

# The separated DataFrames can be used for:
# - Separate analysis and visualization
# - Different processing pipelines  
# - Performance comparison
# - Source-specific metrics
"""