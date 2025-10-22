#!/usr/bin/env python3
"""
Analyze Data Sources in Merged File
==================================

Check what data sources are present in the merged spread file
to confirm the data mixing hypothesis.
"""

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

def analyze_data_sources():
    """Analyze the data sources in the merged spread file"""
    
    print("🔍 Analyzing data sources in merged spread file...")
    
    # Load the merged dataset
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm11_25_debq1_26_tr_ba_data_test_merged.parquet'
    spread_data = pd.read_parquet(file_path)
    spread_data.index = pd.to_datetime(spread_data.index)
    spread_data = spread_data.sort_index()
    
    print(f"Total records: {len(spread_data):,}")
    print(f"Columns: {list(spread_data.columns)}")
    
    # Separate trades and orders
    trades = spread_data[spread_data['action'].isin([1.0, -1.0])].copy()
    orders = spread_data[~spread_data['action'].isin([1.0, -1.0])].copy()
    
    print(f"\nData breakdown:")
    print(f"  Trades: {len(trades):,}")
    print(f"  Orders: {len(orders):,}")
    
    # Analyze trade prices for clusters
    print(f"\n📊 TRADE PRICE CLUSTERING ANALYSIS:")
    print("=" * 50)
    
    if len(trades) > 0:
        # Use KMeans to identify clusters
        from sklearn.cluster import KMeans
        
        prices = trades['price'].values.reshape(-1, 1)
        
        # Try 2 clusters
        kmeans = KMeans(n_clusters=2, random_state=42)
        clusters = kmeans.fit_predict(prices)
        
        cluster_0 = trades[clusters == 0]['price']
        cluster_1 = trades[clusters == 1]['price']
        
        print(f"Cluster 0: {len(cluster_0):,} trades")
        print(f"  Mean: {cluster_0.mean():.2f} EUR/MWh")
        print(f"  Range: {cluster_0.min():.2f} to {cluster_0.max():.2f} EUR/MWh")
        print(f"  Std: {cluster_0.std():.2f} EUR/MWh")
        
        print(f"\nCluster 1: {len(cluster_1):,} trades")
        print(f"  Mean: {cluster_1.mean():.2f} EUR/MWh")
        print(f"  Range: {cluster_1.min():.2f} to {cluster_1.max():.2f} EUR/MWh")
        print(f"  Std: {cluster_1.std():.2f} EUR/MWh")
        
        print(f"\nGap between clusters: {abs(cluster_0.mean() - cluster_1.mean()):.2f} EUR/MWh")
        
        # Check if broker_id column contains source information
        print(f"\n🏢 BROKER/SOURCE ANALYSIS:")
        print("=" * 30)
        
        if 'broker_id' in trades.columns:
            broker_analysis = trades.groupby('broker_id').agg({
                'price': ['count', 'mean', 'std', 'min', 'max']
            }).round(2)
            broker_analysis.columns = ['count', 'mean_price', 'std_price', 'min_price', 'max_price']
            
            print("Broker ID breakdown:")
            for broker_id, stats in broker_analysis.iterrows():
                print(f"  Broker {broker_id}: {stats['count']:,} trades, "
                      f"avg {stats['mean_price']:.2f} EUR/MWh "
                      f"(range: {stats['min_price']:.2f}-{stats['max_price']:.2f})")
        
        # Temporal analysis - check if clusters correspond to different time periods
        print(f"\n📅 TEMPORAL CLUSTER ANALYSIS:")
        print("=" * 35)
        
        trades_with_clusters = trades.copy()
        trades_with_clusters['cluster'] = clusters
        
        for cluster_id in [0, 1]:
            cluster_data = trades_with_clusters[trades_with_clusters['cluster'] == cluster_id]
            if len(cluster_data) > 0:
                print(f"\nCluster {cluster_id} temporal distribution:")
                print(f"  First trade: {cluster_data.index.min()}")
                print(f"  Last trade:  {cluster_data.index.max()}")
                print(f"  Duration: {cluster_data.index.max() - cluster_data.index.min()}")
                
                # Daily distribution
                daily_counts = cluster_data.groupby(cluster_data.index.date).size()
                print(f"  Daily distribution: {daily_counts.min()} to {daily_counts.max()} trades/day")
                print(f"  Active days: {len(daily_counts)}")
        
        # Create visualization
        create_source_analysis_plot(trades_with_clusters)

def create_source_analysis_plot(trades_with_clusters):
    """Create plot showing the data source separation"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Data Source Analysis: Merged Spread File Investigation', fontsize=16, fontweight='bold')
    
    # Plot 1: Price time series colored by cluster
    ax1 = axes[0, 0]
    
    cluster_0 = trades_with_clusters[trades_with_clusters['cluster'] == 0]
    cluster_1 = trades_with_clusters[trades_with_clusters['cluster'] == 1]
    
    ax1.scatter(cluster_0.index, cluster_0['price'], alpha=0.7, s=20, c='red', 
               label=f'Cluster 0 (n={len(cluster_0):,})')
    ax1.scatter(cluster_1.index, cluster_1['price'], alpha=0.7, s=20, c='blue',
               label=f'Cluster 1 (n={len(cluster_1):,})')
    
    ax1.set_title('Price Time Series by Cluster')
    ax1.set_ylabel('Price (EUR/MWh)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Histogram showing clear separation
    ax2 = axes[0, 1]
    
    ax2.hist(cluster_0['price'], bins=30, alpha=0.7, color='red', 
             label=f'Cluster 0\nMean: {cluster_0["price"].mean():.1f}')
    ax2.hist(cluster_1['price'], bins=30, alpha=0.7, color='blue',
             label=f'Cluster 1\nMean: {cluster_1["price"].mean():.1f}')
    
    ax2.set_title('Price Distribution by Cluster')
    ax2.set_xlabel('Price (EUR/MWh)')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Daily cluster activity
    ax3 = axes[1, 0]
    
    daily_cluster_0 = cluster_0.groupby(cluster_0.index.date).size()
    daily_cluster_1 = cluster_1.groupby(cluster_1.index.date).size()
    
    # Align dates
    all_dates = pd.date_range(
        min(daily_cluster_0.index.min(), daily_cluster_1.index.min()),
        max(daily_cluster_0.index.max(), daily_cluster_1.index.max()),
        freq='D'
    ).date
    
    daily_0_aligned = daily_cluster_0.reindex(all_dates, fill_value=0)
    daily_1_aligned = daily_cluster_1.reindex(all_dates, fill_value=0)
    
    ax3.bar(all_dates, daily_0_aligned, alpha=0.7, color='red', label='Cluster 0')
    ax3.bar(all_dates, daily_1_aligned, alpha=0.7, color='blue', bottom=daily_0_aligned, label='Cluster 1')
    
    ax3.set_title('Daily Trading Activity by Cluster')
    ax3.set_ylabel('Trades per Day')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.tick_params(axis='x', rotation=45)
    
    # Plot 4: Cluster transition analysis
    ax4 = axes[1, 1]
    
    # Show transitions between clusters over time
    trades_sorted = trades_with_clusters.sort_index()
    cluster_changes = trades_sorted['cluster'].diff() != 0
    transition_points = trades_sorted[cluster_changes]
    
    if len(transition_points) > 0:
        ax4.scatter(trades_sorted.index, trades_sorted['cluster'], alpha=0.6, s=10,
                   c=['red' if c == 0 else 'blue' for c in trades_sorted['cluster']])
        
        # Mark transition points
        ax4.scatter(transition_points.index, transition_points['cluster'], 
                   s=100, c='orange', marker='x', linewidth=3, 
                   label=f'Transitions (n={len(transition_points)})')
    
    ax4.set_title('Cluster Transitions Over Time')
    ax4.set_ylabel('Cluster ID')
    ax4.set_ylim(-0.5, 1.5)
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'data_source_cluster_analysis.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Data source analysis plot saved: {plot_path}")

def main():
    """Main function"""
    analyze_data_sources()

if __name__ == "__main__":
    main()