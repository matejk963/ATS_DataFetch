#!/usr/bin/env python3
"""
Final correct plot using the actual data structure from integration
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_final_correct():
    """Final correct plot using exact data structure"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Creating Final Correct Integration Plots")
        print("=" * 50)
        
        # Fetch leg 1
        config1 = {
            'contracts': ['debm01_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching debm01_25 data...")
        result1 = integrated_fetch(config1)
        leg1_data = result1['single_leg_data']
        
        # Fetch leg 2
        config2 = {
            'contracts': ['debm02_25'],
            'period': {'start_date': '2024-12-02', 'end_date': '2024-12-06'},
            'n_s': 3, 'mode': 'individual'
        }
        
        print("📡 Fetching debm02_25 data...")
        result2 = integrated_fetch(config2)
        leg2_data = result2['single_leg_data']
        
        # Extract DataFrames
        leg1_orders = leg1_data['orders']
        leg1_trades = leg1_data['trades']
        leg1_mid = leg1_data['mid_prices']
        
        leg2_orders = leg2_data['orders']
        leg2_trades = leg2_data['trades']
        leg2_mid = leg2_data['mid_prices']
        
        print(f"📊 Data Summary:")
        print(f"   Leg 1: {len(leg1_orders):,} orders, {len(leg1_trades):,} trades")
        print(f"   Leg 2: {len(leg2_orders):,} orders, {len(leg2_trades):,} trades")
        
        # Create comprehensive plot
        fig = plt.figure(figsize=(20, 16))
        gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.25)
        
        # Colors and styling
        color1, color2 = '#2E86AB', '#A23B72'  # Blue and Purple
        
        # Plot 1: Leg 1 Price Evolution
        ax1 = fig.add_subplot(gs[0, 0])
        
        # Sample mid prices for cleaner plotting
        sample1 = leg1_mid[::max(1, len(leg1_mid)//2000)]
        ax1.plot(sample1.index, sample1.values, color=color1, linewidth=1.2, alpha=0.8, label='Mid Price')
        
        # Add bid-ask spread visualization
        sample_orders1 = leg1_orders[::max(1, len(leg1_orders)//2000)]
        ax1.fill_between(sample_orders1.index, sample_orders1['b_price'], sample_orders1['a_price'], 
                        alpha=0.2, color=color1, label='Bid-Ask Spread')
        
        ax1.set_title(f'debm01_25 (January 2025 Delivery)\nPrice Evolution\n{len(leg1_orders):,} order updates', 
                     fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price (€/MWh)', fontsize=12)
        ax1.legend(loc='upper right')
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
        
        # Statistics box
        spread1 = leg1_orders['a_price'] - leg1_orders['b_price']
        stats1 = (f'Avg Price: {leg1_mid.mean():.2f} €/MWh\n'
                 f'Range: {leg1_mid.min():.2f} - {leg1_mid.max():.2f}\n'
                 f'Volatility: {leg1_mid.std():.2f}\n'
                 f'Avg Spread: {spread1.mean():.3f} €/MWh')
        ax1.text(0.02, 0.98, stats1, transform=ax1.transAxes, fontsize=10, 
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9))
        
        # Plot 2: Leg 2 Price Evolution
        ax2 = fig.add_subplot(gs[0, 1])
        
        # Sample mid prices for cleaner plotting
        sample2 = leg2_mid[::max(1, len(leg2_mid)//2000)]
        ax2.plot(sample2.index, sample2.values, color=color2, linewidth=1.2, alpha=0.8, label='Mid Price')
        
        # Add bid-ask spread visualization
        sample_orders2 = leg2_orders[::max(1, len(leg2_orders)//2000)]
        ax2.fill_between(sample_orders2.index, sample_orders2['b_price'], sample_orders2['a_price'], 
                        alpha=0.2, color=color2, label='Bid-Ask Spread')
        
        ax2.set_title(f'debm02_25 (February 2025 Delivery)\nPrice Evolution\n{len(leg2_orders):,} order updates', 
                     fontsize=14, fontweight='bold')
        ax2.set_ylabel('Price (€/MWh)', fontsize=12)
        ax2.legend(loc='upper right')
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
        
        # Statistics box
        spread2 = leg2_orders['a_price'] - leg2_orders['b_price']
        stats2 = (f'Avg Price: {leg2_mid.mean():.2f} €/MWh\n'
                 f'Range: {leg2_mid.min():.2f} - {leg2_mid.max():.2f}\n'
                 f'Volatility: {leg2_mid.std():.2f}\n'
                 f'Avg Spread: {spread2.mean():.3f} €/MWh')
        ax2.text(0.02, 0.98, stats2, transform=ax2.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='plum', alpha=0.9))
        
        # Plot 3: Leg 1 Trade Activity
        ax3 = fig.add_subplot(gs[1, 0])
        
        # Hourly trade volume
        hourly_vol1 = leg1_trades['volume'].resample('H').sum()
        bars1 = ax3.bar(hourly_vol1.index, hourly_vol1.values, alpha=0.8, color=color1, 
                       width=0.03, edgecolor='white', linewidth=0.5)
        
        ax3.set_title(f'debm01_25 - Hourly Trade Volume\n{len(leg1_trades):,} trades, {leg1_trades["volume"].sum():,.0f} MWh total', 
                     fontsize=14, fontweight='bold')
        ax3.set_ylabel('Volume (MWh)', fontsize=12)
        ax3.grid(True, alpha=0.3)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
        
        # Trade statistics
        vol_stats1 = (f'Peak Hour: {hourly_vol1.max():,.0f} MWh\n'
                     f'Avg Hour: {hourly_vol1.mean():,.0f} MWh\n'
                     f'Avg Trade Size: {leg1_trades["volume"].mean():.1f} MWh\n'
                     f'Total Volume: {leg1_trades["volume"].sum():,.0f} MWh')
        ax3.text(0.02, 0.98, vol_stats1, transform=ax3.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9))
        
        # Plot 4: Leg 2 Trade Activity
        ax4 = fig.add_subplot(gs[1, 1])
        
        # Hourly trade volume
        hourly_vol2 = leg2_trades['volume'].resample('H').sum()
        bars2 = ax4.bar(hourly_vol2.index, hourly_vol2.values, alpha=0.8, color=color2,
                       width=0.03, edgecolor='white', linewidth=0.5)
        
        ax4.set_title(f'debm02_25 - Hourly Trade Volume\n{len(leg2_trades):,} trades, {leg2_trades["volume"].sum():,.0f} MWh total', 
                     fontsize=14, fontweight='bold')
        ax4.set_ylabel('Volume (MWh)', fontsize=12)
        ax4.grid(True, alpha=0.3)
        ax4.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
        
        # Trade statistics
        vol_stats2 = (f'Peak Hour: {hourly_vol2.max():,.0f} MWh\n'
                     f'Avg Hour: {hourly_vol2.mean():,.0f} MWh\n'
                     f'Avg Trade Size: {leg2_trades["volume"].mean():.1f} MWh\n'
                     f'Total Volume: {leg2_trades["volume"].sum():,.0f} MWh')
        ax4.text(0.02, 0.98, vol_stats2, transform=ax4.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='plum', alpha=0.9))
        
        # Plot 5: Calendar Spread Analysis (full width)
        ax5 = fig.add_subplot(gs[2, :])
        
        # Calculate calendar spread (Jan - Feb)
        mid1_aligned = leg1_mid.reindex(leg1_mid.index.union(leg2_mid.index)).ffill()
        mid2_aligned = leg2_mid.reindex(leg1_mid.index.union(leg2_mid.index)).ffill()
        calendar_spread = mid1_aligned - mid2_aligned
        calendar_spread = calendar_spread.dropna()
        
        # Sample for plotting
        sample_spread = calendar_spread[::max(1, len(calendar_spread)//3000)]
        
        # Plot spread with color coding
        ax5.plot(sample_spread.index, sample_spread.values, color='#E85A4F', linewidth=1.5, alpha=0.8, label='Calendar Spread')
        ax5.axhline(y=0, color='black', linestyle='--', alpha=0.6, linewidth=1)
        ax5.axhline(y=calendar_spread.mean(), color='red', linestyle=':', alpha=0.8, linewidth=2, 
                   label=f'Mean: {calendar_spread.mean():.3f} €/MWh')
        
        # Fill areas
        positive_mask = sample_spread.values >= 0
        negative_mask = sample_spread.values < 0
        ax5.fill_between(sample_spread.index, 0, sample_spread.values, 
                        where=positive_mask, alpha=0.3, color='green', label='Jan Premium (Contango)')
        ax5.fill_between(sample_spread.index, 0, sample_spread.values,
                        where=negative_mask, alpha=0.3, color='red', label='Feb Premium (Backwardation)')
        
        ax5.set_title('Calendar Spread Evolution (debm01_25 - debm02_25)\nJanuary vs February 2025 Price Differential', 
                     fontsize=14, fontweight='bold')
        ax5.set_ylabel('Spread (€/MWh)', fontsize=12)
        ax5.set_xlabel('Date & Time', fontsize=12)
        ax5.legend(loc='upper right')
        ax5.grid(True, alpha=0.3)
        ax5.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45)
        
        # Spread statistics
        spread_stats = (f'Average Spread: {calendar_spread.mean():.3f} €/MWh\n'
                       f'Spread Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f}\n'
                       f'Spread Volatility: {calendar_spread.std():.3f} €/MWh\n'
                       f'Market Structure: {"Contango" if calendar_spread.mean() > 0 else "Backwardation"}\n'
                       f'Days Analyzed: {(calendar_spread.index[-1] - calendar_spread.index[0]).days + 1}')
        ax5.text(0.02, 0.98, spread_stats, transform=ax5.transAxes, fontsize=11,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.95))
        
        plt.suptitle('German Power Market Analysis: January vs February 2025 Contracts\n'
                     'Integration Results from Dec 2-6, 2024 Trading Data', 
                     fontsize=18, fontweight='bold', y=0.98)
        
        # Save plot with high quality
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/integration_final_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
        print(f"📈 Final analysis plot saved: {output_path}")
        
        # Print comprehensive statistics
        print(f"\n📊 COMPREHENSIVE MARKET ANALYSIS:")
        print("=" * 70)
        
        print(f"🔵 debm01_25 (January 2025 Monthly Baseload):")
        print(f"   📊 Market Data: {len(leg1_orders):,} order updates over 5 trading days")
        print(f"   💰 Price Statistics:")
        print(f"     • Average: {leg1_mid.mean():.2f} €/MWh")
        print(f"     • Range: {leg1_mid.min():.2f} - {leg1_mid.max():.2f} €/MWh")
        print(f"     • Volatility (σ): {leg1_mid.std():.2f} €/MWh")
        print(f"     • Intraday Range: {leg1_mid.max() - leg1_mid.min():.2f} €/MWh")
        print(f"   📏 Liquidity: Avg bid-ask spread {spread1.mean():.3f} €/MWh")
        print(f"   💹 Trading Activity:")
        print(f"     • Total Trades: {len(leg1_trades):,}")
        print(f"     • Total Volume: {leg1_trades['volume'].sum():,.0f} MWh")
        print(f"     • Avg Trade Size: {leg1_trades['volume'].mean():.1f} MWh")
        print(f"     • Peak Hourly Volume: {hourly_vol1.max():,.0f} MWh")
        
        print(f"\n🟣 debm02_25 (February 2025 Monthly Baseload):")
        print(f"   📊 Market Data: {len(leg2_orders):,} order updates over 5 trading days")
        print(f"   💰 Price Statistics:")
        print(f"     • Average: {leg2_mid.mean():.2f} €/MWh")
        print(f"     • Range: {leg2_mid.min():.2f} - {leg2_mid.max():.2f} €/MWh")
        print(f"     • Volatility (σ): {leg2_mid.std():.2f} €/MWh")
        print(f"     • Intraday Range: {leg2_mid.max() - leg2_mid.min():.2f} €/MWh")
        print(f"   📏 Liquidity: Avg bid-ask spread {spread2.mean():.3f} €/MWh")
        print(f"   💹 Trading Activity:")
        print(f"     • Total Trades: {len(leg2_trades):,}")
        print(f"     • Total Volume: {leg2_trades['volume'].sum():,.0f} MWh")
        print(f"     • Avg Trade Size: {leg2_trades['volume'].mean():.1f} MWh")
        print(f"     • Peak Hourly Volume: {hourly_vol2.max():,.0f} MWh")
        
        print(f"\n📈 CALENDAR SPREAD ANALYSIS (Jan - Feb 2025):")
        print("=" * 50)
        print(f"   📊 Spread Statistics:")
        print(f"     • Average Spread: {calendar_spread.mean():.3f} €/MWh")
        print(f"     • Spread Range: {calendar_spread.min():.3f} to {calendar_spread.max():.3f} €/MWh")
        print(f"     • Spread Volatility: {calendar_spread.std():.3f} €/MWh")
        print(f"     • Data Points: {len(calendar_spread):,} observations")
        
        print(f"   🏗️ Market Structure:")
        if calendar_spread.mean() > 0:
            print(f"     • CONTANGO: January trades at {calendar_spread.mean():.3f} €/MWh premium to February")
            print(f"     • Winter premium reflects heating demand and supply constraints")
            print(f"     • Positive spread indicates normal seasonal expectations")
        else:
            print(f"     • BACKWARDATION: February trades at {abs(calendar_spread.mean()):.3f} €/MWh premium to January")
            print(f"     • Unusual structure - may indicate supply/demand imbalances")
        
        print(f"   📊 Market Efficiency:")
        correlation = leg1_mid.reindex(leg1_mid.index.intersection(leg2_mid.index)).corrwith(
                     leg2_mid.reindex(leg1_mid.index.intersection(leg2_mid.index)))
        if not correlation.empty:
            print(f"     • Price Correlation: {correlation.iloc[0]:.3f}")
            print(f"     • Market Integration: {'High' if correlation.iloc[0] > 0.9 else 'Moderate' if correlation.iloc[0] > 0.7 else 'Low'}")
        
        print(f"\n💡 MARKET INSIGHTS:")
        print("=" * 30)
        if leg1_mid.mean() > leg2_mid.mean():
            print(f"   • January delivers at {leg1_mid.mean() - leg2_mid.mean():.2f} €/MWh premium (winter premium)")
        else:
            print(f"   • February delivers at {leg2_mid.mean() - leg1_mid.mean():.2f} €/MWh premium (unusual)")
            
        if leg1_trades['volume'].sum() > leg2_trades['volume'].sum():
            print(f"   • January shows higher liquidity ({leg1_trades['volume'].sum() - leg2_trades['volume'].sum():,.0f} MWh more volume)")
        else:
            print(f"   • February shows higher liquidity ({leg2_trades['volume'].sum() - leg1_trades['volume'].sum():,.0f} MWh more volume)")
            
        if spread1.mean() < spread2.mean():
            print(f"   • January shows tighter spreads (better liquidity)")
        else:
            print(f"   • February shows tighter spreads (better liquidity)")
        
        plt.show()
        print(f"\n🎉 COMPREHENSIVE INTEGRATION ANALYSIS COMPLETED!")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Final plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_final_correct()