#!/usr/bin/env python3
"""
Fetch Individual Contract Prices
===============================

Fetch individual prices for debm11_25 and debq1_26 to analyze spikes.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)

from src.core.data_fetcher import DataFetcher
from engines.data_fetch_engine import parse_absolute_contract

def fetch_individual_contracts():
    """Fetch individual contract prices for analysis"""
    
    print("🔍 FETCHING INDIVIDUAL CONTRACT PRICES")
    print("=" * 50)
    
    # Initialize DataFetcher
    output_base = "/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/individual_analysis"
    os.makedirs(output_base, exist_ok=True)
    
    # Initialize fetcher
    fetcher = DataFetcher(output_base)
    
    # Period for analysis
    start_date = "2025-09-01"
    end_date = "2025-10-16"
    
    contracts = ["debm11_25", "debq1_26"]
    
    results = {}
    
    for contract in contracts:
        print(f"\\n📊 Fetching {contract}...")
        try:
            # Parse contract to get market and tenor
            parsed = parse_absolute_contract(contract)
            
            # Create contract config
            contract_config = {
                'contract': contract,
                'market': parsed.market,
                'tenor': parsed.tenor,
                'start_date': start_date,
                'end_date': end_date,
                'venue_list': ['eex'],
                'allowed_broker_ids': [1441]
            }
            
            # Fetch contract data
            result = fetcher.fetch_contract_data(
                contract_config=contract_config,
                include_trades=True,
                include_orders=True
            )
            
            if result['success']:
                data = result['data']
                results[contract] = data
                
                # Basic statistics
                if 'trades' in data and len(data['trades']) > 0:
                    trades_df = data['trades']
                    print(f"   ✅ {contract}: {len(trades_df)} trades")
                    print(f"      Price range: {trades_df['price'].min():.3f} - {trades_df['price'].max():.3f}")
                    print(f"      Price mean: {trades_df['price'].mean():.3f} ± {trades_df['price'].std():.3f}")
                    
                    # Check for price spikes
                    price_mean = trades_df['price'].mean()
                    price_std = trades_df['price'].std()
                    spikes = trades_df[abs(trades_df['price'] - price_mean) > 3 * price_std]
                    if len(spikes) > 0:
                        print(f"      🚨 Found {len(spikes)} potential price spikes (>3σ)")
                        print(f"         Spike range: {spikes['price'].min():.3f} - {spikes['price'].max():.3f}")
                    else:
                        print(f"      ✅ No extreme price spikes detected (>3σ)")
                
                if 'orders' in data and len(data['orders']) > 0:
                    orders_df = data['orders']
                    print(f"   📋 {contract}: {len(orders_df)} orders")
                    
            else:
                print(f"   ❌ Failed to fetch {contract}: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"   ❌ Error fetching {contract}: {e}")
    
    # Analyze individual contracts vs spread
    if len(results) == 2:
        print(f"\\n🔍 INDIVIDUAL CONTRACT ANALYSIS:")
        print("=" * 40)
        
        contract1_data = results['debm11_25']
        contract2_data = results['debq1_26']
        
        if 'trades' in contract1_data and 'trades' in contract2_data:
            df1 = contract1_data['trades']
            df2 = contract2_data['trades']
            
            print(f"DEBM11_25 (November 2025):")
            print(f"   Trades: {len(df1)}")
            print(f"   Price range: {df1['price'].min():.3f} - {df1['price'].max():.3f} EUR/MWh")
            print(f"   Price volatility: {df1['price'].std():.3f} EUR/MWh")
            print(f"   Date range: {df1.index.min()} to {df1.index.max()}")
            
            print(f"\\nDEBQ1_26 (Q1 2026):")
            print(f"   Trades: {len(df2)}")
            print(f"   Price range: {df2['price'].min():.3f} - {df2['price'].max():.3f} EUR/MWh")
            print(f"   Price volatility: {df2['price'].std():.3f} EUR/MWh")
            print(f"   Date range: {df2.index.min()} to {df2.index.max()}")
            
            # Compare price levels
            print(f"\\nPRICE LEVEL COMPARISON:")
            price_diff = df1['price'].mean() - df2['price'].mean()
            print(f"   Average spread (DEBM11_25 - DEBQ1_26): {price_diff:.3f} EUR/MWh")
            
            # Check for synchronized spikes
            print(f"\\nSPIKE SYNCHRONIZATION ANALYSIS:")
            
            # Find outliers in each contract
            df1_outliers = find_outliers(df1)
            df2_outliers = find_outliers(df2)
            
            print(f"   DEBM11_25 outliers: {len(df1_outliers)} ({len(df1_outliers)/len(df1)*100:.1f}%)")
            print(f"   DEBQ1_26 outliers: {len(df2_outliers)} ({len(df2_outliers)/len(df2)*100:.1f}%)")
            
            if len(df1_outliers) > 0:
                print(f"   DEBM11_25 outlier range: {df1_outliers['price'].min():.3f} - {df1_outliers['price'].max():.3f}")
            if len(df2_outliers) > 0:
                print(f"   DEBQ1_26 outlier range: {df2_outliers['price'].min():.3f} - {df2_outliers['price'].max():.3f}")
                
    return results

def find_outliers(df, z_threshold=2.5):
    """Find outliers in price data using Z-score method"""
    if len(df) < 3:
        return pd.DataFrame()
    
    mean_price = df['price'].mean()
    std_price = df['price'].std()
    
    if std_price == 0:
        return pd.DataFrame()
    
    z_scores = abs((df['price'] - mean_price) / std_price)
    outliers = df[z_scores > z_threshold]
    
    return outliers

def main():
    """Main function"""
    results = fetch_individual_contracts()
    
    # Save summary
    print(f"\\n💾 Individual contract data fetched and analyzed.")
    print(f"📁 Data saved in: /mnt/c/Users/krajcovic/Documents/Testing Data/RawData/individual_analysis/")

if __name__ == "__main__":
    main()