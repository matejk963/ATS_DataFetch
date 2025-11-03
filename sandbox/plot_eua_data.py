#!/usr/bin/env python3
"""
Plot EUA Data
=============

Plot the EUA contract data (euay1_25)
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

def plot_eua_data():
    """Plot EUA contract data"""
    
    print("📊 PLOTTING EUA DATA")
    print("=" * 40)
    
    # Load the EUA data
    eua_file = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/euay1_25_tr_ba_data.parquet"
    
    try:
        df = pd.read_parquet(eua_file)
        print(f"📊 Loaded EUA data: {len(df)} records")
        print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
        print(f"📋 Columns: {list(df.columns)}")
        
        # Try to find price column
        price_cols = [col for col in df.columns if 'price' in col.lower()]
        if not price_cols:
            # Look for other potential price columns
            price_cols = [col for col in df.columns if any(x in col.lower() for x in ['mid', 'close', 'value'])]
        
        if price_cols:
            price_col = price_cols[0]
            print(f"💰 Using price column: {price_col}")
        else:
            print(f"❌ No price column found in: {list(df.columns)}")
            return
        
        # Get basic statistics
        valid_data = df[price_col].dropna()
        print(f"💰 Valid data points: {len(valid_data)}")
        print(f"💰 Price range: {valid_data.min():.2f} to {valid_data.max():.2f}")
        print(f"💰 Mean price: {valid_data.mean():.2f}")
        print(f"💰 Std price: {valid_data.std():.2f}")
        
        # Create the plot
        plt.figure(figsize=(16, 10))
        
        # Main price plot
        plt.subplot(2, 1, 1)
        plt.plot(df.index, df[price_col], linewidth=0.8, color='darkgreen', alpha=0.9)
        plt.title('EUA Contract euay1_25 Price Data', fontsize=16, fontweight='bold')
        plt.ylabel('Price (EUR/tonne)', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Add statistics text
        plt.text(0.02, 0.95, f'Records: {len(valid_data):,} | Range: {valid_data.min():.1f}-{valid_data.max():.1f} | Mean: {valid_data.mean():.1f}', 
                transform=plt.gca().transAxes, fontsize=11, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.8))
        
        # Price changes plot
        plt.subplot(2, 1, 2)
        if len(valid_data) > 1:
            price_changes = valid_data.diff().dropna()
            plt.plot(price_changes.index, price_changes, linewidth=0.5, color='blue', alpha=0.7)
            
            print(f"📈 Price changes:")
            print(f"   Max increase: {price_changes.max():.3f}")
            print(f"   Max decrease: {price_changes.min():.3f}")
            print(f"   Std change: {price_changes.std():.3f}")
            
            # Add reference lines
            plt.axhline(y=0, color='black', linestyle='-', alpha=0.3)
            if len(price_changes) > 0:
                plt.axhline(y=price_changes.std(), color='orange', linestyle=':', alpha=0.6, label=f'±1σ ({price_changes.std():.3f})')
                plt.axhline(y=-price_changes.std(), color='orange', linestyle=':', alpha=0.6)
        
        plt.title('EUA Price Changes', fontsize=14, fontweight='bold')
        plt.ylabel('Price Change', fontsize=12)
        plt.xlabel('Time', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        plt.tight_layout()
        
        # Save the plot
        save_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/eua_contract_plot.png'
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        print(f"📊 EUA plot saved: {save_path}")
        
        # Additional analysis
        print(f"\n📈 DATA ANALYSIS:")
        
        # Resample to daily
        if len(valid_data) > 0:
            daily_stats = df.resample('D')[price_col].agg(['count', 'mean', 'std', 'min', 'max']).dropna()
            trading_days = daily_stats[daily_stats['count'] > 0]
            
            print(f"   Trading days: {len(trading_days)}")
            print(f"   Avg records per day: {daily_stats['count'].mean():.0f}")
            print(f"   Daily price volatility: {daily_stats['std'].mean():.3f}")
            
            if len(trading_days) > 1:
                daily_returns = daily_stats['mean'].pct_change().dropna()
                print(f"   Daily return volatility: {daily_returns.std():.3f}")
                print(f"   Max daily return: {daily_returns.max():.3f}")
                print(f"   Min daily return: {daily_returns.min():.3f}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def main():
    plot_eua_data()

if __name__ == "__main__":
    main()