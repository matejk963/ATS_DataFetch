"""
Comprehensive Analysis of DEBM09_25 vs TTFBM09_25 Spread Data
Analysis Date: 2025-09-03
Data: German Base vs TTF Base September 2025 Spread
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style for publication-ready plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Load data
data_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_ttfbm09_25_tr_ba_data.parquet'
df = pd.read_parquet(data_path)

print("=== DEBM09_25 vs TTFBM09_25 Spread Analysis ===")
print(f"Data loaded: {df.shape[0]:,} records")
print(f"Time range: {df.index.min()} to {df.index.max()}")
print(f"Duration: {df.index.max() - df.index.min()}")

# Separate trade data and order book data
trades_mask = ~df['price'].isna()
orderbook_mask = ~df['b_price'].isna()

trades_df = df[trades_mask].copy()
orderbook_df = df[orderbook_mask].copy()

print(f"\nTrade records: {len(trades_df):,}")
print(f"Order book records: {len(orderbook_df):,}")

# Create output directory
output_dir = Path('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc')
output_dir.mkdir(exist_ok=True)

# Figure 1: Time Series Overview
fig, axes = plt.subplots(3, 1, figsize=(15, 12))
fig.suptitle('DEBM09_25 vs TTFBM09_25 Spread - Market Overview', fontsize=16, fontweight='bold')

# Plot 1: Mid-price and trades
ax1 = axes[0]
# Plot mid-price (column '0')
ax1.plot(df.index, df['0'], label='Mid Price', color='blue', alpha=0.7, linewidth=0.5)
# Plot trades
if len(trades_df) > 0:
    buy_trades = trades_df[trades_df['action'] == 1]
    sell_trades = trades_df[trades_df['action'] == -1]
    ax1.scatter(buy_trades.index, buy_trades['price'], c='green', s=10, alpha=0.7, label=f'Buy Trades ({len(buy_trades)})')
    ax1.scatter(sell_trades.index, sell_trades['price'], c='red', s=10, alpha=0.7, label=f'Sell Trades ({len(sell_trades)})')

ax1.set_ylabel('Price (EUR/MWh)', fontweight='bold')
ax1.legend()
ax1.grid(True, alpha=0.3)
ax1.set_title('Price and Trade Activity')

# Plot 2: Bid-Ask Spread
ax2 = axes[1]
if len(orderbook_df) > 0:
    ax2.plot(orderbook_df.index, orderbook_df['b_price'], label='Bid Price', color='green', alpha=0.7, linewidth=0.5)
    ax2.plot(orderbook_df.index, orderbook_df['a_price'], label='Ask Price', color='red', alpha=0.7, linewidth=0.5)
    ax2.fill_between(orderbook_df.index, orderbook_df['b_price'], orderbook_df['a_price'], 
                     alpha=0.2, color='gray', label='Bid-Ask Spread')
    
    # Calculate spread statistics
    spread = orderbook_df['a_price'] - orderbook_df['b_price']
    mean_spread = spread.mean()
    ax2.axhline(y=orderbook_df['b_price'].mean() + mean_spread/2, color='orange', 
                linestyle='--', alpha=0.8, label=f'Mean Spread: {mean_spread:.3f}')

ax2.set_ylabel('Price (EUR/MWh)', fontweight='bold')
ax2.legend()
ax2.grid(True, alpha=0.3)
ax2.set_title('Bid-Ask Spread Evolution')

# Plot 3: Trading Volume (if available)
ax3 = axes[2]
if len(trades_df) > 0:
    # Resample trades to hourly for volume visualization
    hourly_volume = trades_df.groupby(pd.Grouper(freq='1H'))['volume'].sum()
    hourly_trades = trades_df.groupby(pd.Grouper(freq='1H')).size()
    
    ax3_twin = ax3.twinx()
    bars = ax3.bar(hourly_volume.index, hourly_volume.values, alpha=0.6, color='lightblue', 
                   label='Volume (MWh)', width=0.02)
    line = ax3_twin.plot(hourly_trades.index, hourly_trades.values, color='orange', 
                         marker='o', markersize=3, label='Trade Count', linewidth=2)
    
    ax3.set_ylabel('Volume (MWh)', fontweight='bold')
    ax3_twin.set_ylabel('Trade Count', fontweight='bold', color='orange')
    ax3.legend(loc='upper left')
    ax3_twin.legend(loc='upper right')
else:
    ax3.text(0.5, 0.5, 'No volume data available', ha='center', va='center', transform=ax3.transAxes)

ax3.set_xlabel('Time', fontweight='bold')
ax3.grid(True, alpha=0.3)
ax3.set_title('Trading Activity')

# Format x-axis
for ax in axes:
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.savefig(output_dir / '20250903-debm-ttf-spread-overview.png', dpi=300, bbox_inches='tight')
plt.close()

# Figure 2: Price Distribution Analysis
fig, axes = plt.subplots(2, 2, figsize=(15, 10))
fig.suptitle('DEBM09_25 vs TTFBM09_25 - Price Distribution Analysis', fontsize=16, fontweight='bold')

# Plot 1: Mid-price distribution
ax1 = axes[0, 0]
mid_prices = df['0'].dropna()
ax1.hist(mid_prices, bins=50, alpha=0.7, color='blue', edgecolor='black')
ax1.axvline(mid_prices.mean(), color='red', linestyle='--', label=f'Mean: {mid_prices.mean():.3f}')
ax1.axvline(mid_prices.median(), color='green', linestyle='--', label=f'Median: {mid_prices.median():.3f}')
ax1.set_xlabel('Mid Price (EUR/MWh)')
ax1.set_ylabel('Frequency')
ax1.set_title('Mid Price Distribution')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 2: Trade price distribution (if available)
ax2 = axes[0, 1]
if len(trades_df) > 0:
    ax2.hist(trades_df['price'], bins=30, alpha=0.7, color='green', edgecolor='black')
    ax2.axvline(trades_df['price'].mean(), color='red', linestyle='--', 
                label=f'Mean: {trades_df["price"].mean():.3f}')
    ax2.set_xlabel('Trade Price (EUR/MWh)')
    ax2.set_ylabel('Frequency')
    ax2.set_title('Trade Price Distribution')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
else:
    ax2.text(0.5, 0.5, 'No trade data available', ha='center', va='center', transform=ax2.transAxes)

# Plot 3: Bid-Ask Spread Distribution
ax3 = axes[1, 0]
if len(orderbook_df) > 0:
    spread = orderbook_df['a_price'] - orderbook_df['b_price']
    ax3.hist(spread, bins=30, alpha=0.7, color='orange', edgecolor='black')
    ax3.axvline(spread.mean(), color='red', linestyle='--', label=f'Mean: {spread.mean():.4f}')
    ax3.axvline(spread.median(), color='green', linestyle='--', label=f'Median: {spread.median():.4f}')
    ax3.set_xlabel('Bid-Ask Spread (EUR/MWh)')
    ax3.set_ylabel('Frequency')
    ax3.set_title('Bid-Ask Spread Distribution')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
else:
    ax3.text(0.5, 0.5, 'No order book data available', ha='center', va='center', transform=ax3.transAxes)

# Plot 4: Intraday Pattern
ax4 = axes[1, 1]
# Group by hour to show intraday patterns
hourly_stats = df.groupby(df.index.hour)['0'].agg(['mean', 'std', 'count']).reset_index()
ax4.errorbar(hourly_stats['index'], hourly_stats['mean'], yerr=hourly_stats['std'], 
             marker='o', capsize=5, capthick=2, label='Mean ± Std')
ax4.set_xlabel('Hour of Day')
ax4.set_ylabel('Mid Price (EUR/MWh)')
ax4.set_title('Intraday Price Pattern')
ax4.legend()
ax4.grid(True, alpha=0.3)
ax4.set_xticks(range(0, 24, 2))

plt.tight_layout()
plt.savefig(output_dir / '20250903-debm-ttf-distribution-analysis.png', dpi=300, bbox_inches='tight')
plt.close()

# Figure 3: Market Microstructure Analysis
fig, axes = plt.subplots(2, 2, figsize=(15, 10))
fig.suptitle('DEBM09_25 vs TTFBM09_25 - Market Microstructure', fontsize=16, fontweight='bold')

# Plot 1: Price volatility over time
ax1 = axes[0, 0]
# Calculate rolling volatility (1-hour window)
returns = df['0'].pct_change()
rolling_vol = returns.rolling(window='1H').std() * np.sqrt(24 * 365) * 100  # Annualized volatility in %
ax1.plot(rolling_vol.index, rolling_vol, color='purple', alpha=0.7)
ax1.set_ylabel('Annualized Volatility (%)')
ax1.set_title('Rolling 1H Volatility')
ax1.grid(True, alpha=0.3)

# Plot 2: Order book depth (if available)
ax2 = axes[0, 1]
if len(orderbook_df) > 0:
    # Sample recent data for depth visualization
    recent_data = orderbook_df.tail(1000)
    ax2.scatter(recent_data.index, recent_data['b_price'], c='green', s=1, alpha=0.5, label='Bid')
    ax2.scatter(recent_data.index, recent_data['a_price'], c='red', s=1, alpha=0.5, label='Ask')
    ax2.plot(recent_data.index, recent_data['0'], color='blue', alpha=0.8, label='Mid')
    ax2.set_ylabel('Price (EUR/MWh)')
    ax2.set_title('Recent Order Book (Last 1000 Records)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
else:
    ax2.text(0.5, 0.5, 'No order book data available', ha='center', va='center', transform=ax2.transAxes)

# Plot 3: Trade size analysis (if available)
ax3 = axes[1, 0]
if len(trades_df) > 0 and 'volume' in trades_df.columns:
    volume_data = trades_df['volume'].dropna()
    if len(volume_data) > 0:
        ax3.hist(volume_data, bins=20, alpha=0.7, color='teal', edgecolor='black')
        ax3.axvline(volume_data.mean(), color='red', linestyle='--', 
                    label=f'Mean: {volume_data.mean():.1f} MWh')
        ax3.set_xlabel('Trade Size (MWh)')
        ax3.set_ylabel('Frequency')
        ax3.set_title('Trade Size Distribution')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
    else:
        ax3.text(0.5, 0.5, 'No volume data in trades', ha='center', va='center', transform=ax3.transAxes)
else:
    ax3.text(0.5, 0.5, 'No trade volume data available', ha='center', va='center', transform=ax3.transAxes)

# Plot 4: Price impact analysis
ax4 = axes[1, 1]
if len(trades_df) > 0:
    # Analyze price movement around trades
    trades_with_mid = trades_df.join(df['0'], rsuffix='_mid')
    price_impact = trades_with_mid['price'] - trades_with_mid['0']
    
    buy_impact = price_impact[trades_with_mid['action'] == 1]
    sell_impact = price_impact[trades_with_mid['action'] == -1]
    
    ax4.hist(buy_impact, bins=20, alpha=0.5, color='green', label=f'Buy Impact (n={len(buy_impact)})', density=True)
    ax4.hist(sell_impact, bins=20, alpha=0.5, color='red', label=f'Sell Impact (n={len(sell_impact)})', density=True)
    ax4.axvline(0, color='black', linestyle='-', alpha=0.8)
    ax4.set_xlabel('Price Impact (Trade - Mid)')
    ax4.set_ylabel('Density')
    ax4.set_title('Price Impact by Trade Direction')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
else:
    ax4.text(0.5, 0.5, 'No trade data for price impact analysis', ha='center', va='center', transform=ax4.transAxes)

# Format x-axis for time series
for i, ax in enumerate([ax1, ax2]):
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.savefig(output_dir / '20250903-debm-ttf-microstructure.png', dpi=300, bbox_inches='tight')
plt.close()

# Generate summary statistics
print("\n=== SUMMARY STATISTICS ===")
print(f"Mid Price Statistics:")
print(f"  Mean: {df['0'].mean():.3f} EUR/MWh")
print(f"  Std:  {df['0'].std():.3f} EUR/MWh")
print(f"  Min:  {df['0'].min():.3f} EUR/MWh")
print(f"  Max:  {df['0'].max():.3f} EUR/MWh")

if len(orderbook_df) > 0:
    spread = orderbook_df['a_price'] - orderbook_df['b_price']
    print(f"\nBid-Ask Spread Statistics:")
    print(f"  Mean: {spread.mean():.4f} EUR/MWh")
    print(f"  Std:  {spread.std():.4f} EUR/MWh")
    print(f"  Min:  {spread.min():.4f} EUR/MWh")
    print(f"  Max:  {spread.max():.4f} EUR/MWh")

if len(trades_df) > 0:
    print(f"\nTrade Statistics:")
    print(f"  Total Trades: {len(trades_df)}")
    print(f"  Buy Trades: {len(trades_df[trades_df['action'] == 1])}")
    print(f"  Sell Trades: {len(trades_df[trades_df['action'] == -1])}")
    print(f"  Average Trade Price: {trades_df['price'].mean():.3f} EUR/MWh")
    
    if 'volume' in trades_df.columns and not trades_df['volume'].isna().all():
        print(f"  Total Volume: {trades_df['volume'].sum():.1f} MWh")

print(f"\nAnalysis completed successfully!")
print(f"Plots saved to: {output_dir}")