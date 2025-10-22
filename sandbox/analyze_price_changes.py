#!/usr/bin/env python3
"""
Analyze Price Changes in DEBM11_25 vs DEBQ1_26 Spread
====================================================

Deep analysis of why there are large price changes in this spread pair.
"""

import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

# Configure matplotlib
plt.style.use('seaborn-v0_8')
plt.rcParams['figure.figsize'] = [18, 14]

def analyze_spread_price_changes():
    """Analyze the large price changes in debm11_25 vs debq1_26 spread"""
    
    print("🔍 ANALYZING LARGE PRICE CHANGES: DEBM11_25 vs DEBQ1_26")
    print("=" * 60)
    
    # Load data
    base_path = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/test"
    
    df_original = pd.read_parquet(f"{base_path}/debm11_25_debq1_26_tr_ba_dataanalysis_real.parquet")
    df_cleaned = pd.read_parquet(f"{base_path}/debm11_25_debq1_26_tr_ba_dataanalysis_merged.parquet")
    
    # Convert to datetime
    df_original.index = pd.to_datetime(df_original.index)
    df_cleaned.index = pd.to_datetime(df_cleaned.index)
    
    # Find outliers
    outlier_mask = ~df_original.index.isin(df_cleaned.index)
    outliers = df_original[outlier_mask]
    
    print(f"📊 Data Summary:")
    print(f"   Original: {len(df_original)} trades")
    print(f"   Cleaned: {len(df_cleaned)} trades") 
    print(f"   Outliers: {len(outliers)} trades ({len(outliers)/len(df_original)*100:.1f}%)")
    print()
    
    # Analyze price characteristics
    print(f"📈 Price Analysis:")
    print(f"   Original range: {df_original['price'].min():.3f} to {df_original['price'].max():.3f}")
    print(f"   Original span: {df_original['price'].max() - df_original['price'].min():.3f}")
    print(f"   Cleaned range: {df_cleaned['price'].min():.3f} to {df_cleaned['price'].max():.3f}")
    print(f"   Cleaned span: {df_cleaned['price'].max() - df_cleaned['price'].min():.3f}")
    print(f"   Reduction in price volatility: {((df_original['price'].max() - df_original['price'].min()) - (df_cleaned['price'].max() - df_cleaned['price'].min())):.3f}")
    print()
    
    # Contract analysis
    print(f"🔧 Contract Analysis:")
    print(f"   DEBM11_25: German base load, November 2025 delivery")
    print(f"   DEBQ1_26: German base load, Q1 2026 delivery (Jan-Mar 2026)")
    print(f"   Time difference: ~2-4 months between delivery periods")
    print(f"   Spread formula: DEBM11_25 - DEBQ1_26")
    print()
    
    # Create comprehensive analysis plot
    fig, axes = plt.subplots(3, 2, figsize=(18, 14))
    fig.suptitle('DEBM11_25 vs DEBQ1_26 Spread: Price Change Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Original vs Cleaned Timeline
    ax1 = axes[0, 0]
    ax1.scatter(df_original.index, df_original['price'], alpha=0.6, s=20, c='red', label='Original (with outliers)')
    ax1.scatter(df_cleaned.index, df_cleaned['price'], alpha=0.8, s=15, c='blue', label='Cleaned')
    if len(outliers) > 0:
        ax1.scatter(outliers.index, outliers['price'], alpha=0.9, s=30, c='orange', 
                   marker='x', label=f'Outliers ({len(outliers)})')
    
    ax1.set_title('Price Timeline: Before vs After Outlier Cleaning')
    ax1.set_ylabel('Spread Price')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # Plot 2: Price Distribution Comparison
    ax2 = axes[0, 1]
    ax2.hist(df_original['price'], bins=20, alpha=0.6, color='red', label='Original', edgecolor='black')
    ax2.hist(df_cleaned['price'], bins=15, alpha=0.7, color='blue', label='Cleaned', edgecolor='black')
    ax2.axvline(df_original['price'].mean(), color='red', linestyle='--', label='Original Mean')
    ax2.axvline(df_cleaned['price'].mean(), color='blue', linestyle='--', label='Cleaned Mean')
    ax2.set_title('Price Distribution Comparison')
    ax2.set_xlabel('Spread Price')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Outlier Analysis
    ax3 = axes[1, 0]
    if len(outliers) > 0:
        ax3.scatter(outliers.index, outliers['price'], alpha=0.8, s=40, c='orange', marker='x')
        ax3.set_title(f'Outliers Only ({len(outliers)} points)')
        ax3.set_ylabel('Outlier Prices')
        ax3.grid(True, alpha=0.3)
        ax3.tick_params(axis='x', rotation=45)
        
        # Add outlier statistics
        outlier_stats = f'Outlier Range: {outliers["price"].min():.3f} to {outliers["price"].max():.3f}\\n'
        outlier_stats += f'Outlier Mean: {outliers["price"].mean():.3f}\\n'
        outlier_stats += f'Outlier Std: {outliers["price"].std():.3f}'
        ax3.text(0.05, 0.95, outlier_stats, transform=ax3.transAxes, 
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.8))
    else:
        ax3.text(0.5, 0.5, 'No outliers found', ha='center', va='center', transform=ax3.transAxes)
        ax3.set_title('No Outliers')
    
    # Plot 4: Box plot comparison
    ax4 = axes[1, 1]
    box_data = [df_original['price'], df_cleaned['price']]
    box_labels = ['Original', 'Cleaned']
    if len(outliers) > 0:
        box_data.append(outliers['price'])
        box_labels.append('Outliers')
    
    bp = ax4.boxplot(box_data, labels=box_labels, patch_artist=True)
    colors = ['red', 'blue', 'orange']
    for patch, color in zip(bp['boxes'], colors[:len(box_data)]):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    ax4.set_title('Statistical Distribution Comparison')
    ax4.set_ylabel('Spread Price')
    ax4.grid(True, alpha=0.3)
    
    # Plot 5: Price volatility over time
    ax5 = axes[2, 0]
    
    # Calculate rolling statistics
    window = min(10, len(df_original) // 3)
    if window > 1:
        df_original_sorted = df_original.sort_index()
        rolling_std_orig = df_original_sorted['price'].rolling(window=window, center=True).std()
        rolling_mean_orig = df_original_sorted['price'].rolling(window=window, center=True).mean()
        
        ax5.plot(df_original_sorted.index, rolling_std_orig, 'r-', alpha=0.8, label=f'Original Rolling Std ({window} pts)')
        ax5.set_title('Price Volatility Over Time')
        ax5.set_ylabel('Rolling Standard Deviation')
        ax5.legend()
        ax5.grid(True, alpha=0.3)
        ax5.tick_params(axis='x', rotation=45)
    else:
        ax5.text(0.5, 0.5, 'Insufficient data for rolling analysis', ha='center', va='center', transform=ax5.transAxes)
        ax5.set_title('Volatility Analysis (N/A)')
    
    # Plot 6: Contract delivery timeline
    ax6 = axes[2, 1]
    
    # Show delivery periods
    nov_2025 = pd.Timestamp('2025-11-01')
    q1_2026_start = pd.Timestamp('2026-01-01')
    q1_2026_end = pd.Timestamp('2026-03-31')
    
    # Create timeline
    timeline_dates = pd.date_range('2025-11-01', '2026-03-31', freq='D')
    y_position = [1] * len(timeline_dates)
    
    ax6.plot([nov_2025, nov_2025 + pd.DateOffset(months=1)], [1, 1], 'g-', linewidth=10, alpha=0.7, label='DEBM11_25 (Nov 2025)')
    ax6.plot([q1_2026_start, q1_2026_end], [0.9, 0.9], 'b-', linewidth=10, alpha=0.7, label='DEBQ1_26 (Q1 2026)')
    
    ax6.set_ylim(0.8, 1.2)
    ax6.set_title('Contract Delivery Periods')
    ax6.set_xlabel('Delivery Period')
    ax6.legend()
    ax6.tick_params(axis='x', rotation=45)
    
    # Add gap annotation
    gap_start = nov_2025 + pd.DateOffset(months=1)
    gap_end = q1_2026_start
    ax6.annotate('', xy=(gap_end, 0.95), xytext=(gap_start, 0.95),
                arrowprops=dict(arrowstyle='<->', color='red', lw=2))
    ax6.text((gap_start + gap_end) / 2, 0.95, '~1 month gap', ha='center', va='bottom', color='red', fontweight='bold')
    
    plt.tight_layout()
    
    # Save plot
    output_dir = Path(project_root) / 'sandbox' / 'plots'
    output_dir.mkdir(exist_ok=True)
    
    plot_path = output_dir / 'debm11_25_debq1_26_price_change_analysis.png'
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    print(f"✅ Analysis plot saved: {plot_path}")
    
    # Analysis conclusions
    print(f"🧠 ANALYSIS CONCLUSIONS:")
    print(f"=" * 40)
    print(f"1. CONTRACT MISMATCH:")
    print(f"   - DEBM11_25: November 2025 delivery (1 month)")
    print(f"   - DEBQ1_26: Q1 2026 delivery (3 months: Jan-Mar 2026)")
    print(f"   - Time gap: ~1 month between delivery periods")
    print()
    print(f"2. LARGE PRICE VARIATIONS:")
    print(f"   - Original spread range: {df_original['price'].max() - df_original['price'].min():.3f}")
    print(f"   - This represents a {df_original['price'].max() - df_original['price'].min():.1f} EUR/MWh variation")
    print(f"   - After cleaning: {df_cleaned['price'].max() - df_cleaned['price'].min():.3f} (much more stable)")
    print()
    print(f"3. OUTLIER CHARACTERISTICS:")
    print(f"   - {len(outliers)} outliers ({len(outliers)/len(df_original)*100:.1f}% of data)")
    print(f"   - Outliers span both extreme highs and lows")
    print(f"   - This suggests market illiquidity or pricing errors")
    print()
    print(f"4. MARKET FACTORS:")
    print(f"   - Cross-period spreads are inherently more volatile")
    print(f"   - Different contract structures (monthly vs quarterly)")
    print(f"   - Low liquidity leading to erratic pricing")
    print(f"   - Seasonal demand differences (Nov vs Q1)")

def main():
    """Main function"""
    analyze_spread_price_changes()

if __name__ == "__main__":
    main()