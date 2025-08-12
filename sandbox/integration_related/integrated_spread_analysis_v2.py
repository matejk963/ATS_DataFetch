# -*- coding: utf-8 -*-
"""
Integrated Spread Analysis System v2
Fixed contract format handling and improved data integration

This script integrates:
1. DataFetcher - For direct market data retrieval
2. Spread Analysis - For synthetic spread calculation
3. Period data caching - For performance optimization
4. Unified output to ATS_4_data directory
"""

import sys
import os
from datetime import datetime, time
from pathlib import Path
import pandas as pd
import numpy as np
import pickle
import json

# Add paths for imports
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

# Import required modules
from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData, norm_coeff
from Database.TPData import TPData, TPDataDa
from Math.accumfeatures import EMA, MA, MSTD
from src.core.data_fetcher import DataFetcher


class IntegratedSpreadAnalyzerV2:
    """
    Improved integrated system focusing on DataFetcher with spread analysis
    """
    
    def __init__(self, output_dir="C:/Users/krajcovic/Documents/Testing Data/ATS_4_data"):
        """
        Initialize the integrated analyzer
        
        Args:
            output_dir (str): Output directory for all results
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.raw_data_dir = self.output_dir / "raw_data"
        self.spread_dir = self.output_dir / "spread_analysis"
        self.reports_dir = self.output_dir / "reports"
        self.visualizations_dir = self.output_dir / "visualizations"
        
        for dir_path in [self.raw_data_dir, self.spread_dir, self.reports_dir, self.visualizations_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Integrated Analysis Directory: {self.output_dir}")
        print(f"   📊 Raw Data: {self.raw_data_dir}")
        print(f"   📈 Spreads: {self.spread_dir}")
        print(f"   📋 Reports: {self.reports_dir}")
        print(f"   📉 Visualizations: {self.visualizations_dir}")
        
        # Initialize DataFetcher
        self.data_fetcher = None
        self._init_data_fetcher()
        
    def _init_data_fetcher(self):
        """Initialize DataFetcher"""
        try:
            self.data_fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
            print("✅ DataFetcher initialized successfully")
        except Exception as e:
            print(f"❌ DataFetcher initialization failed: {e}")
    
    def _create_contract_configs(self, market, tenor, tn1_list, start_date, end_date):
        """
        Create contract configurations for DataFetcher
        
        Args:
            market (list): Market list (e.g., ['de', 'de'])
            tenor (list): Tenor list (e.g., ['m', 'm'])
            tn1_list (list): Contract offsets (e.g., [1, 2])
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            list: Contract configurations for DataFetcher
        """
        contract_configs = []
        
        for i, (mkt, tnr, offset) in enumerate(zip(market, tenor, tn1_list)):
            # Calculate contract month based on start date and offset
            if tnr == 'm':  # Monthly contracts
                contract_date = start_date + pd.DateOffset(months=offset-1)
                # Format as MM_YY for DataFetcher
                contract_spec = f"{contract_date.month:02d}_{str(contract_date.year)[2:]}"
                
                config = {
                    'market': mkt,
                    'tenor': tnr,
                    'contract': contract_spec,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'label': f"{mkt.upper()}_M+{offset}"  # For identification
                }
                
                contract_configs.append(config)
                print(f"📋 Contract {i+1}: {config['label']} = {mkt} {contract_spec} ({config['start_date']} to {config['end_date']})")
        
        return contract_configs
    
    def _fetch_market_data(self, contract_configs):
        """
        Fetch market data using DataFetcher
        
        Args:
            contract_configs (list): Contract configurations
            
        Returns:
            dict: Fetched market data keyed by contract label
        """
        print(f"\n📡 Fetching market data for {len(contract_configs)} contracts...")
        
        if not self.data_fetcher:
            print(f"❌ DataFetcher not available")
            return {}
        
        market_data = {}
        
        for config in contract_configs:
            try:
                print(f"🔄 Fetching {config['label']}...")
                
                # Fetch data
                result = self.data_fetcher.fetch_contract_data(
                    config, 
                    include_trades=True, 
                    include_orders=True
                )
                
                if result:
                    market_data[config['label']] = {
                        'config': config,
                        'trades': result.get('trades', pd.DataFrame()),
                        'orders': result.get('orders', pd.DataFrame()),
                        'mid_prices': result.get('mid_prices', pd.Series(dtype=float))
                    }
                    
                    trades_count = len(result.get('trades', []))
                    orders_count = len(result.get('orders', []))
                    print(f"✅ {config['label']}: {trades_count:,} trades, {orders_count:,} orders")
                else:
                    print(f"⚠️  No data returned for {config['label']}")
                    
            except Exception as e:
                print(f"❌ Failed to fetch {config['label']}: {e}")
        
        return market_data
    
    def _save_raw_data(self, market_data):
        """
        Save raw market data for reference
        
        Args:
            market_data (dict): Market data dictionary
        """
        print(f"\n💾 Saving raw market data...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for label, data in market_data.items():
            # Save each contract's data
            contract_dir = self.raw_data_dir / label.lower()
            contract_dir.mkdir(exist_ok=True)
            
            # Save trades
            if not data['trades'].empty:
                trades_file = contract_dir / f"trades_{timestamp}.csv"
                data['trades'].to_csv(trades_file)
                print(f"📊 Saved trades: {trades_file}")
            
            # Save orders
            if not data['orders'].empty:
                orders_file = contract_dir / f"orders_{timestamp}.csv"
                data['orders'].to_csv(orders_file)
                print(f"📈 Saved orders: {orders_file}")
            
            # Save mid prices
            if not data['mid_prices'].empty:
                mid_file = contract_dir / f"mid_prices_{timestamp}.csv"
                data['mid_prices'].to_csv(mid_file)
                print(f"📉 Saved mid prices: {mid_file}")
    
    def _calculate_spread(self, market_data, coeff_list):
        """
        Calculate synthetic spread from market data
        
        Args:
            market_data (dict): Market data dictionary
            coeff_list (list): Normalization coefficients
            
        Returns:
            pd.DataFrame: Spread data with timestamps
        """
        print(f"\n📈 Calculating synthetic spread...")
        print(f"📊 Coefficients: {coeff_list}")
        
        # Get contract labels
        labels = list(market_data.keys())
        
        if len(labels) < 2:
            print(f"❌ Need at least 2 contracts for spread calculation, got {len(labels)}")
            return pd.DataFrame()
        
        print(f"📋 Using contracts: {labels[0]} and {labels[1]}")
        
        # Get mid prices for both contracts
        data1 = market_data[labels[0]]
        data2 = market_data[labels[1]]
        
        # Use mid prices if available, otherwise create from orders
        if not data1['mid_prices'].empty:
            mid1 = data1['mid_prices']
        elif not data1['orders'].empty:
            mid1 = 0.5 * (data1['orders']['b_price'] + data1['orders']['a_price'])
        else:
            print(f"❌ No price data available for {labels[0]}")
            return pd.DataFrame()
        
        if not data2['mid_prices'].empty:
            mid2 = data2['mid_prices']
        elif not data2['orders'].empty:
            mid2 = 0.5 * (data2['orders']['b_price'] + data2['orders']['a_price'])
        else:
            print(f"❌ No price data available for {labels[1]}")
            return pd.DataFrame()
        
        # Align timestamps
        common_index = mid1.index.intersection(mid2.index)
        
        if len(common_index) == 0:
            print(f"❌ No common timestamps between contracts")
            return pd.DataFrame()
        
        print(f"✅ Found {len(common_index):,} common timestamps")
        
        # Align data
        mid1_aligned = mid1.reindex(common_index).dropna()
        mid2_aligned = mid2.reindex(common_index).dropna()
        
        # Recalculate common index after dropna
        final_index = mid1_aligned.index.intersection(mid2_aligned.index)
        mid1_final = mid1_aligned.reindex(final_index)
        mid2_final = mid2_aligned.reindex(final_index)
        
        print(f"✅ Final aligned data: {len(final_index):,} points")
        
        # Calculate spread: coeff[0] * contract1 + coeff[1] * contract2
        spread_series = coeff_list[0] * mid1_final + coeff_list[1] * mid2_final
        
        # Create spread DataFrame with bid/ask
        spread_std = spread_series.std()
        tick_size = max(0.01, spread_std * 0.001)  # Dynamic tick size
        
        spread_data = pd.DataFrame({
            'mid': spread_series,
            'bid': spread_series - tick_size,
            'ask': spread_series + tick_size,
            'spread_value': spread_series
        }, index=final_index)
        
        print(f"✅ Spread calculated:")
        print(f"   📊 Data points: {len(spread_data):,}")
        print(f"   📈 Range: {spread_series.min():.3f} to {spread_series.max():.3f}")
        print(f"   📉 Mean: {spread_series.mean():.3f}")
        print(f"   📋 Std Dev: {spread_std:.3f}")
        
        return spread_data
    
    def calc_ema_with_bands(self, spread_data, tau=5, margin=0.43, w=0, eql_p=-6.25):
        """
        Calculate EMA with bands for spread data
        
        Args:
            spread_data (pd.DataFrame): Spread data
            tau (int): EMA parameter
            margin (float): Band margin
            w (float): Weight parameter  
            eql_p (float): Equilibrium price
            
        Returns:
            pd.DataFrame: EMA bands data
        """
        print(f"\n📊 Calculating EMA bands (tau={tau}, margin={margin})...")
        
        if spread_data.empty:
            return pd.DataFrame()
        
        # Use mid prices for EMA calculation
        mid_series = spread_data['mid']
        mid_list = mid_series.values
        
        # Calculate differences for EMA weighting
        dif_list = [0.001]  # Initial difference
        dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
        
        # Initialize EMA
        model = EMA(tau, mid_list[0])
        ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
        
        # Apply equilibrium adjustment
        ema_adjusted = [w * eql_p + (1 - w) * x for x in ema_list]
        
        # Create bands
        bands_data = pd.DataFrame({
            'lower': [x - margin for x in ema_adjusted],
            'ema': ema_adjusted,
            'upper': [x + margin for x in ema_adjusted]
        }, index=mid_series.index)
        
        print(f"✅ EMA bands calculated for {len(bands_data):,} points")
        
        return bands_data
    
    def run_integrated_analysis(self, market=['de', 'de'], tenor=['m', 'm'], tn1_list=[1, 2], 
                              start_date=datetime(2025, 7, 1), end_date=datetime(2025, 7, 31),
                              tau=5, margin=0.43, eql_p=-6.25, w=0, save_data=True):
        """
        Run complete integrated spread analysis
        
        Args:
            market (list): Market specifications
            tenor (list): Tenor specifications  
            tn1_list (list): Contract offsets
            start_date (datetime): Analysis start date
            end_date (datetime): Analysis end date
            tau (int): EMA parameter
            margin (float): EMA band margin
            eql_p (float): Equilibrium price
            w (float): Weight parameter
            save_data (bool): Whether to save raw data
            
        Returns:
            dict: Analysis results
        """
        print(f"\n{'='*100}")
        print(f"🚀 INTEGRATED SPREAD ANALYSIS V2")
        print(f"{'='*100}")
        print(f"📊 Markets: {market}")
        print(f"📋 Tenors: {tenor}")
        print(f"🔢 Contract Offsets: {tn1_list}")
        print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"⚙️  EMA Parameters: tau={tau}, margin={margin}, eql_p={eql_p}, w={w}")
        print(f"{'='*100}")
        
        try:
            # Step 1: Create contract configurations
            print(f"\n🔧 Step 1: Contract Configuration")
            contract_configs = self._create_contract_configs(market, tenor, tn1_list, start_date, end_date)
            
            if not contract_configs:
                print(f"❌ No valid contract configurations created")
                return {}
            
            # Step 2: Fetch market data
            print(f"\n📡 Step 2: Market Data Acquisition")
            market_data = self._fetch_market_data(contract_configs)
            
            if not market_data:
                print(f"❌ No market data acquired")
                return {}
            
            # Step 3: Save raw data
            if save_data:
                print(f"\n💾 Step 3: Raw Data Storage")
                self._save_raw_data(market_data)
            
            # Step 4: Calculate spread
            print(f"\n📈 Step 4: Spread Calculation")
            coeff_list = norm_coeff([1, -2], market)
            spread_data = self._calculate_spread(market_data, coeff_list)
            
            if spread_data.empty:
                print(f"❌ Spread calculation failed")
                return {}
            
            # Step 5: EMA Analysis
            print(f"\n📊 Step 5: EMA Band Analysis")
            ema_bands = self.calc_ema_with_bands(spread_data, tau, margin, w, eql_p)
            
            # Step 6: Combine and save results
            print(f"\n💾 Step 6: Results Compilation")
            
            # Combine all analysis data
            combined_data = pd.concat([spread_data, ema_bands], axis=1)
            
            # Create results dictionary
            results = {
                'spread_data': spread_data,
                'ema_bands': ema_bands,
                'combined_data': combined_data,
                'market_data_summary': {
                    label: {
                        'trades_count': len(data['trades']),
                        'orders_count': len(data['orders']),
                        'config': data['config']
                    }
                    for label, data in market_data.items()
                },
                'parameters': {
                    'market': market,
                    'tenor': tenor,
                    'contracts': tn1_list,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'tau': tau,
                    'margin': margin,
                    'eql_p': eql_p,
                    'w': w,
                    'coefficients': coeff_list
                },
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'spread_points': len(spread_data),
                    'contracts_processed': len(market_data),
                    'spread_range': {
                        'min': float(spread_data['spread_value'].min()),
                        'max': float(spread_data['spread_value'].max()),
                        'mean': float(spread_data['spread_value'].mean()),
                        'std': float(spread_data['spread_value'].std())
                    }
                }
            }
            
            # Save results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save comprehensive analysis
            analysis_file = self.spread_dir / f"integrated_analysis_{timestamp}.pkl"
            with open(analysis_file, 'wb') as f:
                pickle.dump(results, f)
            print(f"📊 Analysis saved: {analysis_file}")
            
            # Save CSV for easy viewing
            csv_file = self.spread_dir / f"spread_with_bands_{timestamp}.csv"
            combined_data.to_csv(csv_file)
            print(f"📈 CSV saved: {csv_file}")
            
            # Save detailed report
            report_file = self.reports_dir / f"analysis_report_{timestamp}.json"
            with open(report_file, 'w') as f:
                json.dump({
                    'parameters': results['parameters'],
                    'metadata': results['metadata'],
                    'market_data_summary': results['market_data_summary'],
                    'files': {
                        'analysis': str(analysis_file),
                        'csv_data': str(csv_file),
                        'report': str(report_file)
                    }
                }, f, indent=2)
            print(f"📋 Report saved: {report_file}")
            
            # Final summary
            print(f"\n{'='*100}")
            print(f"🎉 ANALYSIS COMPLETED SUCCESSFULLY")
            print(f"{'='*100}")
            print(f"📊 Spread Data Points: {len(spread_data):,}")
            print(f"📈 Spread Range: {results['metadata']['spread_range']['min']:.3f} to {results['metadata']['spread_range']['max']:.3f}")
            print(f"📉 Spread Mean: {results['metadata']['spread_range']['mean']:.3f} ± {results['metadata']['spread_range']['std']:.3f}")
            print(f"📁 Output Directory: {self.output_dir}")
            print(f"💾 Files Generated: Analysis, CSV, Report")
            print(f"✅ Integration Status: SUCCESS")
            print(f"{'='*100}")
            
            return results
            
        except Exception as e:
            print(f"\n❌ ANALYSIS FAILED: {e}")
            import traceback
            traceback.print_exc()
            return {}


def main():
    """
    Main function demonstrating integrated spread analysis v2
    """
    print("🔗 Integrated Spread Analysis System V2")
    print("=" * 60)
    
    # Initialize the analyzer
    analyzer = IntegratedSpreadAnalyzerV2()
    
    # Run analysis with the current test parameters
    results = analyzer.run_integrated_analysis(
        market=['de', 'de'],           # German power market
        tenor=['m', 'm'],             # Monthly contracts
        tn1_list=[1, 2],              # M+1 and M+2 (July and August 2025)
        start_date=datetime(2025, 7, 1),
        end_date=datetime(2025, 7, 31),
        tau=5,                        # EMA smoothing
        margin=0.43,                  # Band width
        eql_p=-6.25,                  # Equilibrium price
        w=0,                          # Weight parameter
        save_data=True                # Save raw data
    )
    
    if results:
        print(f"\n🎉 INTEGRATION SUCCESSFUL!")
        print(f"📊 Generated comprehensive spread analysis")
        print(f"📈 {results['metadata']['spread_points']:,} data points processed")
        print(f"💾 All data saved to ATS_4_data directory")
    else:
        print(f"\n❌ INTEGRATION FAILED - Check logs above")
    
    return results


if __name__ == "__main__":
    results = main()