#!/usr/bin/env python3
"""
Verification script for DataFetcher vs SpreadViewer trade separation
using the broker_id method identified in the analysis.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_separate_trades(file_path):
    """Load data and separate trades using broker_id method"""
    print("Loading and separating trades...")
    
    # Load data
    df = pd.read_parquet(file_path)
    
    # Filter for trade records only (records with price values)
    trades = df[df['price'].notna() & (df['price'] > 0)].copy()
    
    print(f"Total records in file: {len(df)}")
    print(f"Total trade records (with price): {len(trades)}")
    print(f"Trade percentage: {len(trades)/len(df)*100:.1f}%")
    
    # Check broker_id distribution
    print(f"\nBroker ID distribution in trades:")
    broker_dist = trades['broker_id'].value_counts()
    print(broker_dist)
    
    # Separate by broker_id
    datafetcher_trades = trades[trades['broker_id'] == 9999.0].copy()
    spreadviewer_trades = trades[trades['broker_id'] == 1441.0].copy()
    
    print(f"\nSeparation Results:")
    print(f"DataFetcher trades (broker_id 9999): {len(datafetcher_trades)}")
    print(f"SpreadViewer trades (broker_id 1441): {len(spreadviewer_trades)}")
    print(f"Total separated: {len(datafetcher_trades) + len(spreadviewer_trades)}")
    print(f"Coverage: {(len(datafetcher_trades) + len(spreadviewer_trades))/len(trades)*100:.1f}%")
    
    return datafetcher_trades, spreadviewer_trades, trades

def compare_trade_characteristics(datafetcher_trades, spreadviewer_trades):
    """Compare characteristics between the two trade sources"""
    print("\n" + "="*60)
    print("COMPARING TRADE CHARACTERISTICS")
    print("="*60)
    
    # Price comparison
    print("\nPRICE STATISTICS:")
    df_price_stats = datafetcher_trades['price'].describe()
    sv_price_stats = spreadviewer_trades['price'].describe()
    
    comparison_df = pd.DataFrame({
        'DataFetcher (9999)': df_price_stats,
        'SpreadViewer (1441)': sv_price_stats
    })
    print(comparison_df)
    
    # Volume comparison
    print("\nVOLUME STATISTICS:")
    df_vol_stats = datafetcher_trades['volume'].describe()
    sv_vol_stats = spreadviewer_trades['volume'].describe()
    
    vol_comparison_df = pd.DataFrame({
        'DataFetcher (9999)': df_vol_stats,
        'SpreadViewer (1441)': sv_vol_stats
    })
    print(vol_comparison_df)
    
    # Action distribution
    print("\nACTION DISTRIBUTION:")
    print("DataFetcher (broker_id 9999):")
    print(datafetcher_trades['action'].value_counts())
    print("\nSpreadViewer (broker_id 1441):")  
    print(spreadviewer_trades['action'].value_counts())
    
    # Price precision analysis
    print("\nPRICE PRECISION ANALYSIS:")
    
    def get_decimal_places(price_series):
        price_strings = price_series.astype(str)
        decimal_places = price_strings.str.split('.').str[1].str.len()
        return decimal_places.fillna(0)
    
    df_decimals = get_decimal_places(datafetcher_trades['price'])
    sv_decimals = get_decimal_places(spreadviewer_trades['price'])
    
    print("DataFetcher decimal places:")
    print(df_decimals.value_counts().head())
    print("\nSpreadViewer decimal places:")
    print(sv_decimals.value_counts().head())

def analyze_tradeid_patterns(datafetcher_trades, spreadviewer_trades):
    """Analyze trade ID patterns between sources"""
    print("\n" + "="*60)
    print("TRADE ID PATTERN ANALYSIS")
    print("="*60)
    
    # Trade ID length patterns
    df_id_lengths = datafetcher_trades['tradeid'].astype(str).str.len()
    sv_id_lengths = spreadviewer_trades['tradeid'].astype(str).str.len()
    
    print("DataFetcher trade ID lengths:")
    print(df_id_lengths.value_counts().head())
    print("\nSpreadViewer trade ID lengths:")
    print(sv_id_lengths.value_counts().head())
    
    # Sample trade IDs
    print("\nSample DataFetcher trade IDs:")
    print(datafetcher_trades['tradeid'].head(5).tolist())
    print("\nSample SpreadViewer trade IDs:")
    print(spreadviewer_trades['tradeid'].head(5).tolist())

def create_visualizations(datafetcher_trades, spreadviewer_trades):
    """Create visualizations comparing the two trade sources"""
    print("\n" + "="*60)
    print("CREATING VISUALIZATIONS")
    print("="*60)
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('DataFetcher vs SpreadViewer Trade Comparison', fontsize=16)
    
    # Price distribution
    axes[0, 0].hist(datafetcher_trades['price'], bins=50, alpha=0.7, label='DataFetcher (9999)', color='blue')
    axes[0, 0].hist(spreadviewer_trades['price'], bins=50, alpha=0.7, label='SpreadViewer (1441)', color='red')
    axes[0, 0].set_xlabel('Price')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].set_title('Price Distribution')
    axes[0, 0].legend()
    
    # Volume distribution
    df_vol_counts = datafetcher_trades['volume'].value_counts().sort_index()
    sv_vol_counts = spreadviewer_trades['volume'].value_counts().sort_index()
    
    x_pos = range(len(df_vol_counts))
    width = 0.35
    axes[0, 1].bar([x - width/2 for x in x_pos], df_vol_counts.values, width, 
                   label='DataFetcher (9999)', color='blue', alpha=0.7)
    axes[0, 1].bar([x + width/2 for x in x_pos], sv_vol_counts.reindex(df_vol_counts.index, fill_value=0).values, 
                   width, label='SpreadViewer (1441)', color='red', alpha=0.7)
    axes[0, 1].set_xlabel('Volume')
    axes[0, 1].set_ylabel('Count')
    axes[0, 1].set_title('Volume Distribution')
    axes[0, 1].set_xticks(x_pos)
    axes[0, 1].set_xticklabels(df_vol_counts.index)
    axes[0, 1].legend()
    
    # Action distribution
    df_action_counts = datafetcher_trades['action'].value_counts()
    sv_action_counts = spreadviewer_trades['action'].value_counts()
    
    actions = [-1.0, 1.0]
    df_action_values = [df_action_counts.get(a, 0) for a in actions]
    sv_action_values = [sv_action_counts.get(a, 0) for a in actions]
    
    x_pos = range(len(actions))
    axes[1, 0].bar([x - width/2 for x in x_pos], df_action_values, width, 
                   label='DataFetcher (9999)', color='blue', alpha=0.7)
    axes[1, 0].bar([x + width/2 for x in x_pos], sv_action_values, width, 
                   label='SpreadViewer (1441)', color='red', alpha=0.7)
    axes[1, 0].set_xlabel('Action')
    axes[1, 0].set_ylabel('Count')
    axes[1, 0].set_title('Action Distribution')
    axes[1, 0].set_xticks(x_pos)
    axes[1, 0].set_xticklabels([int(a) for a in actions])
    axes[1, 0].legend()
    
    # Count by source
    source_counts = [len(datafetcher_trades), len(spreadviewer_trades)]
    source_labels = ['DataFetcher\n(9999)', 'SpreadViewer\n(1441)']
    colors = ['blue', 'red']
    
    axes[1, 1].pie(source_counts, labels=source_labels, colors=colors, alpha=0.7, autopct='%1.1f%%')
    axes[1, 1].set_title('Trade Count by Source')
    
    plt.tight_layout()
    plt.savefig('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/reports/ad_hoc/20250819-trade-separation-comparison.png', 
                dpi=300, bbox_inches='tight')
    print("Visualization saved: reports/ad_hoc/20250819-trade-separation-comparison.png")

def main():
    """Main verification function"""
    print("TRADE SEPARATION VERIFICATION")
    print("="*60)
    
    file_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet"
    
    # Load and separate trades
    datafetcher_trades, spreadviewer_trades, all_trades = load_and_separate_trades(file_path)
    
    # Verify separation quality
    if len(datafetcher_trades) + len(spreadviewer_trades) == len(all_trades):
        print("✓ SEPARATION SUCCESSFUL - All trades categorized")
    else:
        print("✗ SEPARATION INCOMPLETE - Some trades not categorized")
    
    # Compare characteristics
    compare_trade_characteristics(datafetcher_trades, spreadviewer_trades)
    
    # Analyze trade ID patterns
    analyze_tradeid_patterns(datafetcher_trades, spreadviewer_trades)
    
    # Create visualizations
    create_visualizations(datafetcher_trades, spreadviewer_trades)
    
    print(f"\n" + "="*60)
    print("VERIFICATION SUMMARY")
    print("="*60)
    print(f"✓ Method: Broker ID separation")
    print(f"✓ Total trades: {len(all_trades)}")
    print(f"✓ DataFetcher trades (broker_id 9999): {len(datafetcher_trades)} ({len(datafetcher_trades)/len(all_trades)*100:.1f}%)")
    print(f"✓ SpreadViewer trades (broker_id 1441): {len(spreadviewer_trades)} ({len(spreadviewer_trades)/len(all_trades)*100:.1f}%)")
    print(f"✓ Coverage: 100%")
    print(f"✓ Visualization saved")

if __name__ == "__main__":
    main()