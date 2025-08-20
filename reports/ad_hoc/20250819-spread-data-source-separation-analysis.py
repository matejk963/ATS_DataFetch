#!/usr/bin/env python3
"""
Ad-hoc Analysis: Spread Data Source Separation
Analyze quarterly spread data to understand how to separate DataFetcher vs SpreadViewer trades
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configuration
DATA_PATH = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq1_26_frbq1_26_tr_ba_data.parquet"
OUTPUT_DIR = Path("/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc")
OUTPUT_DIR.mkdir(exist_ok=True)

def load_and_examine_data():
    """Load the parquet file and examine basic structure"""
    print("Loading data...")
    df = pd.read_parquet(DATA_PATH)
    
    print(f"Data shape: {df.shape}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nData types:\n{df.dtypes}")
    print(f"\nMemory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    return df

def analyze_basic_structure(df):
    """Analyze basic data structure and content"""
    print("\n" + "="*50)
    print("BASIC DATA STRUCTURE ANALYSIS")
    print("="*50)
    
    # Sample rows
    print("\nFirst 5 rows:")
    print(df.head())
    
    print("\nLast 5 rows:")
    print(df.tail())
    
    # Missing values
    print("\nMissing values per column:")
    missing = df.isnull().sum()
    print(missing[missing > 0])
    
    # Unique value counts for potential categorical columns
    print("\nUnique value counts for potential categorical columns:")
    for col in df.columns:
        unique_count = df[col].nunique()
        if unique_count < 50:  # Likely categorical
            print(f"{col}: {unique_count} unique values")
            if unique_count <= 10:
                print(f"  Values: {list(df[col].unique())}")

def analyze_potential_source_indicators(df):
    """Look for fields that might indicate data source"""
    print("\n" + "="*50)
    print("POTENTIAL SOURCE INDICATOR ANALYSIS")
    print("="*50)
    
    # Look for broker_id, action, source, origin type fields
    source_candidates = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['broker', 'source', 'origin', 'action', 'type', 'flag']):
            source_candidates.append(col)
    
    print(f"\nPotential source indicator columns: {source_candidates}")
    
    for col in source_candidates:
        print(f"\n{col} analysis:")
        print(f"  Unique values: {df[col].nunique()}")
        value_counts = df[col].value_counts()
        print(f"  Value distribution:\n{value_counts}")
        
        # If numeric, show some stats
        if df[col].dtype in ['int64', 'float64']:
            print(f"  Statistical summary:\n{df[col].describe()}")
    
    return source_candidates

def analyze_temporal_patterns(df):
    """Analyze time-based patterns that might distinguish sources"""
    print("\n" + "="*50)
    print("TEMPORAL PATTERN ANALYSIS")
    print("="*50)
    
    # Find datetime columns
    datetime_cols = []
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]' or 'time' in col.lower() or 'date' in col.lower():
            datetime_cols.append(col)
    
    print(f"\nDateTime columns found: {datetime_cols}")
    
    for col in datetime_cols[:2]:  # Analyze first 2 datetime columns
        if df[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(df[col]):
            print(f"\n{col} temporal analysis:")
            print(f"  Date range: {df[col].min()} to {df[col].max()}")
            print(f"  Total time span: {df[col].max() - df[col].min()}")
            
            # Time distribution by hour
            if hasattr(df[col].dt, 'hour'):
                hourly_dist = df[col].dt.hour.value_counts().sort_index()
                print(f"  Peak trading hours: {hourly_dist.head()}")
    
    return datetime_cols

def analyze_trading_patterns(df):
    """Analyze trading characteristics that might distinguish sources"""
    print("\n" + "="*50)
    print("TRADING PATTERN ANALYSIS")
    print("="*50)
    
    # Look for price, volume, quantity related columns
    trading_cols = []
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ['price', 'volume', 'qty', 'quantity', 'size', 'amount']):
            trading_cols.append(col)
    
    print(f"\nTrading-related columns: {trading_cols}")
    
    for col in trading_cols[:5]:  # Analyze first 5 trading columns
        if df[col].dtype in ['int64', 'float64']:
            print(f"\n{col} analysis:")
            print(f"  Statistical summary:\n{df[col].describe()}")
            
            # Look for patterns that might indicate synthetic vs real data
            zero_count = (df[col] == 0).sum()
            null_count = df[col].isnull().sum()
            print(f"  Zero values: {zero_count} ({zero_count/len(df)*100:.2f}%)")
            print(f"  Null values: {null_count} ({null_count/len(df)*100:.2f}%)")
            
            # Check for regular patterns or suspicious values
            if df[col].nunique() < 100:
                print(f"  Most common values:\n{df[col].value_counts().head()}")
    
    return trading_cols

def create_visualizations(df, datetime_cols, trading_cols, source_candidates):
    """Create visualizations to help identify patterns"""
    print("\n" + "="*50)
    print("CREATING VISUALIZATIONS")
    print("="*50)
    
    plt.style.use('default')
    fig_count = 0
    
    # 1. Time series plot of main trading activity
    if datetime_cols and trading_cols:
        main_time_col = datetime_cols[0]
        main_price_col = None
        
        # Find a price column
        for col in trading_cols:
            if 'price' in col.lower() and df[col].dtype in ['int64', 'float64']:
                main_price_col = col
                break
        
        if main_price_col and pd.api.types.is_datetime64_any_dtype(df[main_time_col]):
            plt.figure(figsize=(12, 6))
            
            # Sample data if too large
            sample_df = df.sample(min(10000, len(df))).sort_values(main_time_col)
            
            plt.scatter(sample_df[main_time_col], sample_df[main_price_col], alpha=0.6, s=1)
            plt.title(f'Trading Activity Over Time\n{main_price_col} vs {main_time_col}')
            plt.xlabel(main_time_col)
            plt.ylabel(main_price_col)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            fig_count += 1
            plt.savefig(OUTPUT_DIR / f"20250819-spread-data-timeseries-{fig_count}.png", dpi=150, bbox_inches='tight')
            plt.close()
    
    # 2. Distribution plots for potential source indicators
    if source_candidates:
        for i, col in enumerate(source_candidates[:3]):  # Max 3 source candidates
            if df[col].nunique() < 20:  # Only for categorical-like data
                plt.figure(figsize=(10, 6))
                df[col].value_counts().plot(kind='bar')
                plt.title(f'Distribution of {col}')
                plt.xlabel(col)
                plt.ylabel('Count')
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                fig_count += 1
                plt.savefig(OUTPUT_DIR / f"20250819-spread-data-source-dist-{fig_count}.png", dpi=150, bbox_inches='tight')
                plt.close()
    
    # 3. Trading volume/price distributions
    if trading_cols:
        for i, col in enumerate(trading_cols[:2]):  # Max 2 trading columns
            if df[col].dtype in ['int64', 'float64'] and df[col].nunique() > 10:
                plt.figure(figsize=(10, 6))
                
                # Remove extreme outliers for better visualization
                q1, q99 = df[col].quantile([0.01, 0.99])
                filtered_data = df[col][(df[col] >= q1) & (df[col] <= q99)]
                
                filtered_data.hist(bins=50, alpha=0.7)
                plt.title(f'Distribution of {col} (1st-99th percentile)')
                plt.xlabel(col)
                plt.ylabel('Frequency')
                plt.tight_layout()
                
                fig_count += 1
                plt.savefig(OUTPUT_DIR / f"20250819-spread-data-trading-dist-{fig_count}.png", dpi=150, bbox_inches='tight')
                plt.close()
    
    print(f"Created {fig_count} visualization files")
    return fig_count

def analyze_data_patterns(df):
    """Look for patterns that might indicate different data sources"""
    print("\n" + "="*50)
    print("DATA PATTERN ANALYSIS")
    print("="*50)
    
    patterns = {}
    
    # Look for duplicate patterns
    print(f"Total rows: {len(df)}")
    print(f"Duplicate rows: {df.duplicated().sum()}")
    
    # Analyze completeness patterns
    print(f"\nData completeness patterns:")
    completeness = df.isnull().mean() * 100
    print(completeness[completeness > 0].sort_values(ascending=False))
    
    # Look for regular time intervals (might indicate synthetic data)
    datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    if datetime_cols:
        main_time_col = datetime_cols[0]
        if len(df) > 1:
            df_sorted = df.sort_values(main_time_col)
            time_diffs = df_sorted[main_time_col].diff().dropna()
            
            print(f"\nTime interval patterns for {main_time_col}:")
            print(f"  Most common intervals:")
            interval_counts = time_diffs.value_counts().head(10)
            for interval, count in interval_counts.items():
                print(f"    {interval}: {count} occurrences")
            
            patterns['regular_intervals'] = interval_counts
    
    return patterns

def main():
    """Main analysis function"""
    print("Starting Spread Data Source Separation Analysis")
    print("="*60)
    
    # Load data
    df = load_and_examine_data()
    
    # Basic structure analysis
    analyze_basic_structure(df)
    
    # Look for source indicators
    source_candidates = analyze_potential_source_indicators(df)
    
    # Temporal pattern analysis
    datetime_cols = analyze_temporal_patterns(df)
    
    # Trading pattern analysis
    trading_cols = analyze_trading_patterns(df)
    
    # Data pattern analysis
    patterns = analyze_data_patterns(df)
    
    # Create visualizations
    fig_count = create_visualizations(df, datetime_cols, trading_cols, source_candidates)
    
    print("\n" + "="*60)
    print("ANALYSIS COMPLETE")
    print("="*60)
    print(f"Generated {fig_count} visualization files in {OUTPUT_DIR}")
    print("Detailed analysis saved to markdown report")
    
    return {
        'data_shape': df.shape,
        'source_candidates': source_candidates,
        'datetime_cols': datetime_cols,
        'trading_cols': trading_cols,
        'patterns': patterns,
        'fig_count': fig_count
    }

if __name__ == "__main__":
    results = main()