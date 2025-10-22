#!/usr/bin/env python3
"""
Simple Individual Contract Fetcher
=================================

Direct approach to fetch individual contract data using TPData.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, time
from pathlib import Path

# Set up paths
project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
sys.path.insert(0, project_root)
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

from Database.TPData import TPData

def fetch_individual_contracts_simple():
    """Fetch individual contract data directly using TPData"""
    
    print("🔍 FETCHING INDIVIDUAL CONTRACT PRICES (Direct TPData)")
    print("=" * 60)
    
    # Initialize database connection
    db = TPData()
    
    # Contracts to analyze
    contracts = {
        'debm11_25': {'market': 'de', 'tenor': 'm', 'product': 'base'},
        'debq1_26': {'market': 'de', 'tenor': 'q', 'product': 'base'}
    }
    
    start_date = datetime(2025, 9, 1)
    end_date = datetime(2025, 10, 16)
    start_time = time(9, 0, 0)
    end_time = time(17, 25, 0)
    
    results = {}
    
    for contract_name, contract_info in contracts.items():
        print(f"\\n📊 Fetching {contract_name}...")
        
        try:
            # Fetch trades
            trades_data = db.get_trades(
                market=contract_info['market'],
                tenor=contract_info['tenor'],
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time,
                venue_list=['eex'],
                allowed_broker_ids=[1441]
            )
            
            if trades_data is not None and len(trades_data) > 0:
                print(f"   ✅ {contract_name}: {len(trades_data)} trades fetched")
                print(f"      Date range: {trades_data.index.min()} to {trades_data.index.max()}")
                print(f"      Price range: {trades_data['price'].min():.3f} - {trades_data['price'].max():.3f}")
                print(f"      Price mean ± std: {trades_data['price'].mean():.3f} ± {trades_data['price'].std():.3f}")
                
                # Check for price spikes
                price_mean = trades_data['price'].mean()
                price_std = trades_data['price'].std()
                
                # Find outliers (>2.5 standard deviations)
                z_scores = abs((trades_data['price'] - price_mean) / price_std)
                outliers = trades_data[z_scores > 2.5]
                
                if len(outliers) > 0:
                    print(f"      🚨 Found {len(outliers)} price outliers (>{2.5}σ): {len(outliers)/len(trades_data)*100:.1f}%")
                    print(f"         Outlier range: {outliers['price'].min():.3f} - {outliers['price'].max():.3f}")
                    
                    # Show extreme outliers
                    extreme_outliers = trades_data[z_scores > 3.0]
                    if len(extreme_outliers) > 0:
                        print(f"         Extreme outliers (>3σ): {len(extreme_outliers)}")
                        for idx, row in extreme_outliers.head(3).iterrows():
                            print(f"            {idx}: {row['price']:.3f} EUR/MWh (z={abs((row['price'] - price_mean) / price_std):.2f})")
                else:
                    print(f"      ✅ No significant outliers detected")
                
                results[contract_name] = trades_data
                
            else:
                print(f"   ⚠️  {contract_name}: No trade data found")
                
            # Fetch orders for additional analysis
            try:
                orders_data = db.get_best_orders_data(
                    market=contract_info['market'],
                    tenor=contract_info['tenor'],
                    start_date=start_date,
                    end_date=end_date,
                    start_time=start_time,
                    end_time=end_time,
                    venue_list=['eex']
                )
                
                if orders_data is not None and len(orders_data) > 0:
                    print(f"   📋 {contract_name}: {len(orders_data)} order book records")
                    if 'b_price' in orders_data.columns and 'a_price' in orders_data.columns:
                        bid_prices = orders_data['b_price'].dropna()
                        ask_prices = orders_data['a_price'].dropna()
                        if len(bid_prices) > 0 and len(ask_prices) > 0:
                            print(f"      Bid range: {bid_prices.min():.3f} - {bid_prices.max():.3f}")
                            print(f"      Ask range: {ask_prices.min():.3f} - {ask_prices.max():.3f}")
                            
                            # Check for negative spreads
                            valid_pairs = orders_data.dropna(subset=['b_price', 'a_price'])
                            if len(valid_pairs) > 0:
                                spreads = valid_pairs['a_price'] - valid_pairs['b_price']
                                negative_spreads = spreads[spreads < 0]
                                if len(negative_spreads) > 0:
                                    print(f"      ⚠️  {len(negative_spreads)} negative bid-ask spreads detected")
                                else:
                                    print(f"      ✅ All bid-ask spreads positive")
                            
            except Exception as e:
                print(f"   ⚠️  Order data fetch failed: {e}")
                
        except Exception as e:
            print(f"   ❌ Error fetching {contract_name}: {e}")
    
    # Compare individual contracts
    if len(results) == 2:
        print(f"\\n🔍 INDIVIDUAL vs SPREAD ANALYSIS:")
        print("=" * 50)
        
        debm11_data = results.get('debm11_25')
        debq1_data = results.get('debq1_26')
        
        if debm11_data is not None and debq1_data is not None:
            print(f"Individual Contract Statistics:")
            print(f"   DEBM11_25 (Nov 2025):")
            print(f"      {len(debm11_data)} trades")
            print(f"      Price: {debm11_data['price'].mean():.3f} ± {debm11_data['price'].std():.3f} EUR/MWh")
            print(f"      Range: {debm11_data['price'].min():.3f} - {debm11_data['price'].max():.3f}")
            print(f"      CV: {debm11_data['price'].std() / debm11_data['price'].mean() * 100:.1f}%")
            
            print(f"\\n   DEBQ1_26 (Q1 2026):")
            print(f"      {len(debq1_data)} trades")
            print(f"      Price: {debq1_data['price'].mean():.3f} ± {debq1_data['price'].std():.3f} EUR/MWh")
            print(f"      Range: {debq1_data['price'].min():.3f} - {debq1_data['price'].max():.3f}")
            print(f"      CV: {debq1_data['price'].std() / debq1_data['price'].mean() * 100:.1f}%")
            
            # Calculate theoretical spread
            print(f"\\nSpread Analysis:")
            avg_spread = debm11_data['price'].mean() - debq1_data['price'].mean()
            print(f"   Average price difference: {avg_spread:.3f} EUR/MWh")
            
            # Individual volatilities vs spread volatility
            combined_volatility = np.sqrt(debm11_data['price'].var() + debq1_data['price'].var())
            print(f"   Combined individual volatility: {combined_volatility:.3f} EUR/MWh")
            
            print(f"\\nConclusion:")
            print(f"   If individual contracts are stable, spread spikes likely come from:")
            print(f"   • Market structure differences (monthly vs quarterly)")
            print(f"   • Spread trading illiquidity")
            print(f"   • Cross-period arbitrage constraints")
            print(f"   • Data quality issues in spread calculation")

def main():
    """Main function"""
    fetch_individual_contracts_simple()

if __name__ == "__main__":
    main()