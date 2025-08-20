#!/usr/bin/env python3
"""
Plot refetched data after n_s synchronization fix
Compare the results to verify price spikes are eliminated
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import seaborn as sns

print("📊 PLOTTING REFETCHED DATA AFTER N_S SYNCHRONIZATION FIX")
print("=" * 70)

# File paths
main_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debq4_25_frbq4_25_tr_ba_data.parquet'
test_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/debq4_25_frbq4_25_tr_ba_data.parquet'

print(f"📁 Loading data files:")
print(f"   📊 Main file: {main_file}")
print(f"   🧪 Test file: {test_file}")

try:
    # Load the data
    df_main = pd.read_parquet(main_file)
    df_test = pd.read_parquet(test_file)
    
    print(f"✅ Data loaded successfully:")
    print(f"   📊 Main dataset: {len(df_main):,} records")
    print(f"   🧪 Test dataset: {len(df_test):,} records")
    print()
    
    # Use the test dataset for detailed analysis (it's the one with the fix)
    df = df_test.copy()
    
    print(f"📊 Dataset Overview:")
    print(f"   📅 Date range: {df.index.min()} to {df.index.max()}")
    print(f"   📊 Total records: {len(df):,}")
    print(f"   🔍 Columns: {list(df.columns)}")
    print()
    
    # Separate trades and orders
    trades_mask = df['price'].notna()
    orders_mask = (df['b_price'].notna()) | (df['a_price'].notna())
    
    trades = df[trades_mask].copy()
    orders = df[orders_mask].copy()
    
    print(f"📈 Data Breakdown:")
    print(f"   🔄 Trades: {len(trades):,} records")
    print(f"   📊 Orders: {len(orders):,} records")
    
    # Check for broker_id distribution
    if 'broker_id' in trades.columns:
        broker_dist = trades['broker_id'].value_counts()
        print(f"   🏢 Broker distribution in trades:")
        for broker_id, count in broker_dist.items():
            source = "DataFetcher" if broker_id == 1441.0 else "SpreadViewer" if broker_id == 9999.0 else "Unknown"
            print(f"      {broker_id}: {count:,} trades ({source})")
    print()
    
    # Create comprehensive plots
    fig, axes = plt.subplots(3, 2, figsize=(20, 18))
    fig.suptitle(f'DE-FR Q4_25 Spread Data After N_S Fix (June 24 - July 5, 2025)\n'
                f'{len(trades):,} trades, {len(orders):,} orders', fontsize=16, y=0.98)
    
    # Plot 1: All prices over time
    ax1 = axes[0, 0]
    if not trades.empty:
        # Color by broker_id if available
        if 'broker_id' in trades.columns:
            real_trades = trades[trades['broker_id'] == 1441.0]
            synth_trades = trades[trades['broker_id'] == 9999.0]
            
            if not real_trades.empty:
                ax1.scatter(real_trades.index, real_trades['price'], 
                           alpha=0.6, s=15, color='blue', label=f'DataFetcher ({len(real_trades):,})')
            
            if not synth_trades.empty:
                ax1.scatter(synth_trades.index, synth_trades['price'], 
                           alpha=0.6, s=15, color='red', label=f'SpreadViewer ({len(synth_trades):,})')
        else:
            ax1.scatter(trades.index, trades['price'], alpha=0.6, s=15, color='blue', label='All trades')
    
    if not orders.empty:
        if orders['b_price'].notna().sum() > 0:
            ax1.plot(orders.index, orders['b_price'], alpha=0.3, color='green', label='Bid')
        if orders['a_price'].notna().sum() > 0:
            ax1.plot(orders.index, orders['a_price'], alpha=0.3, color='orange', label='Ask')
    
    ax1.set_title('All Prices Over Time')
    ax1.set_ylabel('Price (EUR/MWh)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Price distribution
    ax2 = axes[0, 1]
    if not trades.empty:
        if 'broker_id' in trades.columns:
            real_trades = trades[trades['broker_id'] == 1441.0]
            synth_trades = trades[trades['broker_id'] == 9999.0]
            
            if not real_trades.empty and not synth_trades.empty:
                ax2.hist([real_trades['price'], synth_trades['price']], 
                        bins=50, alpha=0.7, label=['DataFetcher', 'SpreadViewer'], 
                        color=['blue', 'red'])
            elif not real_trades.empty:
                ax2.hist(real_trades['price'], bins=50, alpha=0.7, label='DataFetcher', color='blue')
            elif not synth_trades.empty:
                ax2.hist(synth_trades['price'], bins=50, alpha=0.7, label='SpreadViewer', color='red')
        else:
            ax2.hist(trades['price'], bins=50, alpha=0.7, color='blue')
    
    ax2.set_title('Price Distribution')
    ax2.set_xlabel('Price (EUR/MWh)')
    ax2.set_ylabel('Frequency')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Focus on June 26-27 (critical transition days)
    ax3 = axes[1, 0]
    critical_start = pd.Timestamp('2025-06-26')
    critical_end = pd.Timestamp('2025-06-27 23:59:59')
    
    critical_trades = trades[(trades.index >= critical_start) & (trades.index <= critical_end)]
    
    if not critical_trades.empty:
        print(f"🔍 Critical period analysis (June 26-27):")
        print(f"   📅 Period: {critical_start.date()} to {critical_end.date()}")
        print(f"   🔄 Total trades: {len(critical_trades)}")
        
        if 'broker_id' in critical_trades.columns:
            critical_real = critical_trades[critical_trades['broker_id'] == 1441.0]
            critical_synth = critical_trades[critical_trades['broker_id'] == 9999.0]
            
            print(f"   🏢 DataFetcher trades: {len(critical_real)}")
            print(f"   🏢 SpreadViewer trades: {len(critical_synth)}")
            
            if not critical_real.empty:
                ax3.scatter(critical_real.index, critical_real['price'], 
                           alpha=0.8, s=30, color='blue', label=f'DataFetcher ({len(critical_real)})')
                print(f"   📊 DataFetcher price range: €{critical_real['price'].min():.2f} - €{critical_real['price'].max():.2f}")
            
            if not critical_synth.empty:
                ax3.scatter(critical_synth.index, critical_synth['price'], 
                           alpha=0.8, s=30, color='red', label=f'SpreadViewer ({len(critical_synth)})')
                print(f"   📊 SpreadViewer price range: €{critical_synth['price'].min():.2f} - €{critical_synth['price'].max():.2f}")
            
            # Check for price discrepancies
            if not critical_real.empty and not critical_synth.empty:
                real_mean = critical_real['price'].mean()
                synth_mean = critical_synth['price'].mean()
                discrepancy = abs(real_mean - synth_mean)
                print(f"   💰 Average price discrepancy: €{discrepancy:.2f}")
                
                if discrepancy > 5.0:
                    print(f"   ⚠️  WARNING: Large price discrepancy detected!")
                else:
                    print(f"   ✅ Price discrepancy within acceptable range")
        else:
            ax3.scatter(critical_trades.index, critical_trades['price'], 
                       alpha=0.8, s=30, color='blue', label='All trades')
        print()
    else:
        print(f"   ⚠️  No trades found in critical period")
    
    ax3.set_title('June 26-27 Critical Period (N_S Transition)')
    ax3.set_ylabel('Price (EUR/MWh)')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Daily statistics
    ax4 = axes[1, 1]
    if not trades.empty:
        daily_stats = trades.groupby(trades.index.date)['price'].agg(['count', 'mean', 'std', 'min', 'max'])
        
        ax4.plot(daily_stats.index, daily_stats['mean'], 'o-', label='Daily Mean', linewidth=2)
        ax4.fill_between(daily_stats.index, 
                        daily_stats['mean'] - daily_stats['std'], 
                        daily_stats['mean'] + daily_stats['std'], 
                        alpha=0.3, label='±1 Std Dev')
        
        ax4.set_title('Daily Price Statistics')
        ax4.set_ylabel('Price (EUR/MWh)')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
    
    # Plot 5: Spread analysis
    ax5 = axes[2, 0]
    valid_orders = orders[(orders['b_price'].notna()) & (orders['a_price'].notna())]
    
    if not valid_orders.empty:
        valid_orders['spread'] = valid_orders['a_price'] - valid_orders['b_price']
        
        # Remove negative spreads for plotting
        positive_spreads = valid_orders[valid_orders['spread'] >= 0]['spread']
        
        if not positive_spreads.empty:
            ax5.hist(positive_spreads, bins=50, alpha=0.7, color='purple')
            ax5.axvline(positive_spreads.mean(), color='red', linestyle='--', 
                       label=f'Mean: €{positive_spreads.mean():.3f}')
            
            print(f"📊 Bid-Ask Spread Analysis:")
            print(f"   📈 Valid spread records: {len(positive_spreads):,}")
            print(f"   📊 Average spread: €{positive_spreads.mean():.3f}")
            print(f"   📊 Spread range: €{positive_spreads.min():.3f} - €{positive_spreads.max():.3f}")
        
    ax5.set_title('Bid-Ask Spread Distribution')
    ax5.set_xlabel('Spread (EUR/MWh)')
    ax5.set_ylabel('Frequency')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    
    # Plot 6: Volume analysis (if available)
    ax6 = axes[2, 1]
    if not trades.empty and 'volume' in trades.columns and trades['volume'].notna().sum() > 0:
        valid_volume_trades = trades[trades['volume'].notna()]
        ax6.scatter(valid_volume_trades.index, valid_volume_trades['volume'], 
                   alpha=0.6, s=15, color='green')
        ax6.set_title('Trading Volume Over Time')
        ax6.set_ylabel('Volume')
    else:
        # Time-based trade frequency instead
        hourly_counts = trades.groupby(trades.index.hour).size()
        ax6.bar(hourly_counts.index, hourly_counts.values, alpha=0.7, color='green')
        ax6.set_title('Hourly Trade Frequency')
        ax6.set_xlabel('Hour of Day')
        ax6.set_ylabel('Number of Trades')
    
    ax6.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    plot_filename = 'refetched_ns_fix_analysis.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"📁 Plot saved: {plot_filename}")
    
    plt.show()
    
    # Summary statistics
    print(f"\n📋 SUMMARY ANALYSIS:")
    print("=" * 70)
    
    if not trades.empty:
        print(f"✅ Total trades: {len(trades):,}")
        print(f"📊 Price range: €{trades['price'].min():.2f} - €{trades['price'].max():.2f}")
        print(f"📊 Average price: €{trades['price'].mean():.2f}")
        print(f"📊 Price std dev: €{trades['price'].std():.2f}")
        
        # Check for extreme outliers
        price_q99 = trades['price'].quantile(0.99)
        price_q01 = trades['price'].quantile(0.01)
        outliers = trades[(trades['price'] > price_q99) | (trades['price'] < price_q01)]
        
        if not outliers.empty:
            print(f"⚠️  Price outliers (beyond 1-99 percentile): {len(outliers)} trades")
            print(f"   📊 Outlier price range: €{outliers['price'].min():.2f} - €{outliers['price'].max():.2f}")
        else:
            print(f"✅ No extreme price outliers detected")
    
    print(f"\n🎯 N_S SYNCHRONIZATION FIX ASSESSMENT:")
    print("=" * 70)
    print(f"✅ Data successfully fetched with synchronized n_s logic")
    print(f"📊 Both DataFetcher and SpreadViewer should now use same relative periods")
    print(f"🎉 Check the critical period plots above for price consistency")

except Exception as e:
    print(f"❌ Error loading/plotting data: {e}")
    import traceback
    traceback.print_exc()