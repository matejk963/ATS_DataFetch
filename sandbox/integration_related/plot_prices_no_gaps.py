#!/usr/bin/env python3
"""
Plot only prices with orders, no gaps - continuous price evolution
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def plot_prices_no_gaps():
    """Plot only prices with actual data points, no gaps"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("📊 Creating Price Plot with No Gaps")
        print("=" * 40)
        
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
        
        # Extract data
        leg1_orders = leg1_data['orders']
        leg1_trades = leg1_data['trades']
        leg2_orders = leg2_data['orders']
        leg2_trades = leg2_data['trades']
        
        print(f"📊 Raw data: Leg1 {len(leg1_orders):,} orders, Leg2 {len(leg2_orders):,} orders")
        
        # Remove any NaN values and ensure continuous data
        leg1_orders_clean = leg1_orders.dropna()
        leg2_orders_clean = leg2_orders.dropna()
        leg1_trades_clean = leg1_trades.dropna()
        leg2_trades_clean = leg2_trades.dropna()
        
        print(f"📊 Clean data: Leg1 {len(leg1_orders_clean):,} orders, Leg2 {len(leg2_orders_clean):,} orders")
        
        # Calculate mid prices
        leg1_mid = (leg1_orders_clean['b_price'] + leg1_orders_clean['a_price']) / 2
        leg2_mid = (leg2_orders_clean['b_price'] + leg2_orders_clean['a_price']) / 2
        
        # Create sequential index for continuous plotting (no time gaps)
        leg1_sequential = pd.Series(leg1_mid.values, index=range(len(leg1_mid)))
        leg2_sequential = pd.Series(leg2_mid.values, index=range(len(leg2_mid)))
        
        # Also create trade price series
        leg1_trade_prices = leg1_trades_clean['price']
        leg2_trade_prices = leg2_trades_clean['price']
        leg1_trades_seq = pd.Series(leg1_trade_prices.values, index=range(len(leg1_trade_prices)))
        leg2_trades_seq = pd.Series(leg2_trade_prices.values, index=range(len(leg2_trade_prices)))
        
        # Create the plot
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12))
        fig.suptitle('Continuous Price Evolution - No Time Gaps\nGerman Power Contracts: debm01_25 vs debm02_25', 
                     fontsize=16, fontweight='bold')
        
        # Colors
        color1, color2 = '#1f77b4', '#ff7f0e'  # Blue, Orange
        trade_color1, trade_color2 = '#e31a1c', '#ff7f00'  # Red, Dark Orange
        
        # Plot 1: debm01_25 (January 2025)
        # Sample for smoother plotting if too many points
        sample_size1 = min(5000, len(leg1_sequential))
        if len(leg1_sequential) > sample_size1:
            step1 = len(leg1_sequential) // sample_size1
            sample_idx1 = range(0, len(leg1_sequential), step1)
            sampled_seq1 = leg1_sequential.iloc[sample_idx1]
            sampled_orders1 = leg1_orders_clean.iloc[sample_idx1]
        else:
            sampled_seq1 = leg1_sequential
            sampled_orders1 = leg1_orders_clean
        
        # Plot bid-ask spread
        ax1.fill_between(sampled_seq1.index, 
                        sampled_orders1['b_price'], 
                        sampled_orders1['a_price'], 
                        alpha=0.3, color=color1, label='Bid-Ask Spread')
        
        # Plot mid price line
        ax1.plot(sampled_seq1.index, sampled_seq1.values, 
                color=color1, linewidth=1.2, alpha=0.8, label='Mid Price (Orders)')
        
        # Overlay trade points
        if len(leg1_trades_seq) > 0:
            # Sample trades for visibility
            trade_sample_size1 = min(2000, len(leg1_trades_seq))
            if len(leg1_trades_seq) > trade_sample_size1:
                trade_step1 = len(leg1_trades_seq) // trade_sample_size1
                trade_sample_idx1 = range(0, len(leg1_trades_seq), trade_step1)
                sampled_trades1 = leg1_trades_seq.iloc[trade_sample_idx1]
            else:
                sampled_trades1 = leg1_trades_seq
                
            ax1.scatter(sampled_trades1.index, sampled_trades1.values, 
                       color=trade_color1, alpha=0.6, s=8, label='Trade Prices', zorder=5)
        
        ax1.set_title(f'debm01_25 (January 2025) - Continuous Price Evolution\n{len(leg1_orders_clean):,} order updates, {len(leg1_trades_clean):,} trades', 
                     fontsize=14, fontweight='bold')
        ax1.set_ylabel('Price (€/MWh)', fontsize=12)
        ax1.set_xlabel('Sequential Data Point (No Time Gaps)', fontsize=12)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Add statistics
        stats1 = (f'Avg: {leg1_mid.mean():.2f} €/MWh\n'
                 f'Range: {leg1_mid.min():.2f} - {leg1_mid.max():.2f}\n'
                 f'Std: {leg1_mid.std():.2f} €/MWh\n'
                 f'Data Points: {len(leg1_mid):,}')
        ax1.text(0.02, 0.98, stats1, transform=ax1.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9))
        
        # Plot 2: debm02_25 (February 2025)
        # Sample for smoother plotting if too many points
        sample_size2 = min(5000, len(leg2_sequential))
        if len(leg2_sequential) > sample_size2:
            step2 = len(leg2_sequential) // sample_size2
            sample_idx2 = range(0, len(leg2_sequential), step2)
            sampled_seq2 = leg2_sequential.iloc[sample_idx2]
            sampled_orders2 = leg2_orders_clean.iloc[sample_idx2]
        else:
            sampled_seq2 = leg2_sequential
            sampled_orders2 = leg2_orders_clean
        
        # Plot bid-ask spread
        ax2.fill_between(sampled_seq2.index, 
                        sampled_orders2['b_price'], 
                        sampled_orders2['a_price'], 
                        alpha=0.3, color=color2, label='Bid-Ask Spread')
        
        # Plot mid price line
        ax2.plot(sampled_seq2.index, sampled_seq2.values, 
                color=color2, linewidth=1.2, alpha=0.8, label='Mid Price (Orders)')
        
        # Overlay trade points
        if len(leg2_trades_seq) > 0:
            # Sample trades for visibility
            trade_sample_size2 = min(2000, len(leg2_trades_seq))
            if len(leg2_trades_seq) > trade_sample_size2:
                trade_step2 = len(leg2_trades_seq) // trade_sample_size2
                trade_sample_idx2 = range(0, len(leg2_trades_seq), trade_step2)
                sampled_trades2 = leg2_trades_seq.iloc[trade_sample_idx2]
            else:
                sampled_trades2 = leg2_trades_seq
                
            ax2.scatter(sampled_trades2.index, sampled_trades2.values, 
                       color=trade_color2, alpha=0.6, s=8, label='Trade Prices', zorder=5)
        
        ax2.set_title(f'debm02_25 (February 2025) - Continuous Price Evolution\n{len(leg2_orders_clean):,} order updates, {len(leg2_trades_clean):,} trades', 
                     fontsize=14, fontweight='bold')
        ax2.set_ylabel('Price (€/MWh)', fontsize=12)
        ax2.set_xlabel('Sequential Data Point (No Time Gaps)', fontsize=12)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Add statistics
        stats2 = (f'Avg: {leg2_mid.mean():.2f} €/MWh\n'
                 f'Range: {leg2_mid.min():.2f} - {leg2_mid.max():.2f}\n'
                 f'Std: {leg2_mid.std():.2f} €/MWh\n'
                 f'Data Points: {len(leg2_mid):,}')
        ax2.text(0.02, 0.98, stats2, transform=ax2.transAxes, fontsize=10,
                verticalalignment='top', bbox=dict(boxstyle='round', facecolor='peachpuff', alpha=0.9))
        
        plt.tight_layout()
        
        # Save plot
        output_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test/prices_continuous_no_gaps.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"📈 Continuous price plot saved: {output_path}")
        
        # Print summary
        print(f"\n📊 CONTINUOUS PRICE ANALYSIS:")
        print("=" * 50)
        print(f"🔵 debm01_25 (January 2025):")
        print(f"   Data Points: {len(leg1_mid):,} order updates")
        print(f"   Price Range: {leg1_mid.min():.2f} - {leg1_mid.max():.2f} €/MWh")
        print(f"   Average: {leg1_mid.mean():.2f} €/MWh")
        print(f"   Price Movement: {leg1_mid.max() - leg1_mid.min():.2f} €/MWh total range")
        print(f"   Trade Points: {len(leg1_trade_prices):,}")
        
        print(f"\n🟠 debm02_25 (February 2025):")
        print(f"   Data Points: {len(leg2_mid):,} order updates")
        print(f"   Price Range: {leg2_mid.min():.2f} - {leg2_mid.max():.2f} €/MWh")
        print(f"   Average: {leg2_mid.mean():.2f} €/MWh")
        print(f"   Price Movement: {leg2_mid.max() - leg2_mid.min():.2f} €/MWh total range")
        print(f"   Trade Points: {len(leg2_trade_prices):,}")
        
        print(f"\n💡 PLOTTING METHOD:")
        print(f"   ✅ No time gaps - only actual data points plotted")
        print(f"   ✅ Sequential index - continuous price evolution")
        print(f"   ✅ Bid-ask spreads shown as filled areas")
        print(f"   ✅ Trade prices overlaid as scatter points")
        print(f"   ✅ Smooth continuous lines connecting all data points")
        
        plt.show()
        print(f"\n🎉 Continuous price plotting completed!")
        
    except Exception as e:
        print(f"❌ Continuous price plotting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    plot_prices_no_gaps()