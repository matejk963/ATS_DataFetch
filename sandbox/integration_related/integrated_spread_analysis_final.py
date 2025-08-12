# -*- coding: utf-8 -*-
"""
Integrated Spread Analysis System - Final Version
Complete integration with data availability diagnostics and robust error handling

This script provides:
1. Data availability diagnostics
2. Flexible contract selection  
3. Automatic fallback to available data
4. Comprehensive spread analysis
5. Professional output to ATS_4_data directory
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


class IntegratedSpreadAnalyzerFinal:
    """
    Final integrated system with comprehensive data diagnostics and robust processing
    """
    
    def __init__(self, output_dir="C:/Users/krajcovic/Documents/Testing Data/ATS_4_data"):
        """
        Initialize the integrated analyzer
        
        Args:
            output_dir (str): Output directory for all results
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create comprehensive directory structure
        self.raw_data_dir = self.output_dir / "raw_data"
        self.spread_dir = self.output_dir / "spread_analysis"
        self.reports_dir = self.output_dir / "reports"
        self.diagnostics_dir = self.output_dir / "diagnostics"
        
        for dir_path in [self.raw_data_dir, self.spread_dir, self.reports_dir, self.diagnostics_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 ATS_4_data Analysis System")
        print(f"   📊 Raw Data: {self.raw_data_dir}")
        print(f"   📈 Spreads: {self.spread_dir}")
        print(f"   📋 Reports: {self.reports_dir}")
        print(f"   🔍 Diagnostics: {self.diagnostics_dir}")
        
        # Initialize DataFetcher
        self.data_fetcher = None
        self._init_data_fetcher()
        
    def _init_data_fetcher(self):
        """Initialize DataFetcher"""
        try:
            self.data_fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
            print("✅ DataFetcher ready")
        except Exception as e:
            print(f"❌ DataFetcher failed: {e}")
    
    def diagnose_data_availability(self, market_list=['de'], tenor_list=['m'], 
                                 contract_months=['07_25', '08_25', '09_25'], 
                                 start_date=datetime(2025, 7, 1), end_date=datetime(2025, 7, 31)):
        """
        Diagnose data availability for multiple contracts
        
        Args:
            market_list (list): Markets to test
            tenor_list (list): Tenors to test
            contract_months (list): Contract specifications to test
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            dict: Data availability report
        """
        print(f"\n🔍 DATA AVAILABILITY DIAGNOSTICS")
        print(f"{'='*80}")
        print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"📊 Testing {len(market_list)} markets × {len(tenor_list)} tenors × {len(contract_months)} contracts")
        
        availability_report = {
            'timestamp': datetime.now().isoformat(),
            'test_parameters': {
                'markets': market_list,
                'tenors': tenor_list,
                'contracts': contract_months,
                'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            },
            'results': []
        }
        
        if not self.data_fetcher:
            print("❌ DataFetcher not available for diagnostics")
            return availability_report
        
        # Test each combination
        for market in market_list:
            for tenor in tenor_list:
                for contract in contract_months:
                    try:
                        print(f"\n🔄 Testing {market} {tenor} {contract}...")
                        
                        config = {
                            'market': market,
                            'tenor': tenor,
                            'contract': contract,
                            'start_date': start_date.strftime('%Y-%m-%d'),
                            'end_date': end_date.strftime('%Y-%m-%d')
                        }
                        
                        # Quick fetch
                        result = self.data_fetcher.fetch_contract_data(config)
                        
                        trades_count = len(result.get('trades', []))
                        orders_count = len(result.get('orders', []))
                        mid_count = len(result.get('mid_prices', []))
                        
                        status = "✅ GOOD" if (trades_count > 0 or orders_count > 0) else "❌ NO DATA"
                        
                        contract_result = {
                            'contract_id': f"{market}_{tenor}_{contract}",
                            'config': config,
                            'trades_count': trades_count,
                            'orders_count': orders_count,
                            'mid_prices_count': mid_count,
                            'has_data': trades_count > 0 or orders_count > 0,
                            'status': status
                        }
                        
                        availability_report['results'].append(contract_result)
                        print(f"   {status}: {trades_count:,} trades, {orders_count:,} orders")
                        
                    except Exception as e:
                        error_result = {
                            'contract_id': f"{market}_{tenor}_{contract}",
                            'config': config,
                            'error': str(e),
                            'has_data': False,
                            'status': f"❌ ERROR: {str(e)[:50]}"
                        }
                        availability_report['results'].append(error_result)
                        print(f"   ❌ ERROR: {e}")
        
        # Summary
        total_tests = len(availability_report['results'])
        successful_tests = len([r for r in availability_report['results'] if r.get('has_data', False)])
        
        print(f"\n{'='*80}")
        print(f"📊 DIAGNOSTICS SUMMARY")
        print(f"{'='*80}")
        print(f"✅ Successful: {successful_tests}/{total_tests}")
        print(f"❌ No Data: {total_tests - successful_tests}/{total_tests}")
        
        if successful_tests >= 2:
            print(f"🎉 SUFFICIENT DATA for spread analysis!")
            good_contracts = [r for r in availability_report['results'] if r.get('has_data', False)]
            print(f"📋 Available contracts:")
            for contract in good_contracts[:5]:  # Show first 5
                print(f"   ✅ {contract['contract_id']}: {contract['trades_count']:,} trades, {contract['orders_count']:,} orders")
        else:
            print(f"⚠️  INSUFFICIENT DATA - need at least 2 contracts with data")
        
        # Save diagnostics
        diag_file = self.diagnostics_dir / f"data_availability_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(diag_file, 'w') as f:
            json.dump(availability_report, f, indent=2)
        print(f"💾 Diagnostics saved: {diag_file}")
        
        return availability_report
    
    def find_best_spread_pair(self, availability_report):
        """
        Find the best pair of contracts for spread analysis
        
        Args:
            availability_report (dict): Output from diagnose_data_availability
            
        Returns:
            tuple: (contract1_config, contract2_config) or (None, None)
        """
        print(f"\n🔍 Finding best spread pair...")
        
        good_contracts = [r for r in availability_report['results'] if r.get('has_data', False)]
        
        if len(good_contracts) < 2:
            print(f"❌ Need at least 2 contracts with data, found {len(good_contracts)}")
            return None, None
        
        # Sort by total data volume (trades + orders)
        good_contracts.sort(key=lambda x: x['trades_count'] + x['orders_count'], reverse=True)
        
        # Take the two best contracts
        contract1 = good_contracts[0]
        contract2 = good_contracts[1]
        
        print(f"✅ Selected spread pair:")
        print(f"   📊 Contract 1: {contract1['contract_id']} ({contract1['trades_count']:,} trades, {contract1['orders_count']:,} orders)")
        print(f"   📊 Contract 2: {contract2['contract_id']} ({contract2['trades_count']:,} trades, {contract2['orders_count']:,} orders)")
        
        return contract1['config'], contract2['config']
    
    def fetch_spread_data(self, config1, config2):
        """
        Fetch data for spread analysis
        
        Args:
            config1 (dict): First contract configuration
            config2 (dict): Second contract configuration
            
        Returns:
            tuple: (data1, data2)
        """
        print(f"\n📡 Fetching spread data...")
        
        if not self.data_fetcher:
            return None, None
        
        try:
            # Fetch both contracts
            print(f"🔄 Fetching {config1['market']} {config1['contract']}...")
            data1 = self.data_fetcher.fetch_contract_data(config1)
            
            print(f"🔄 Fetching {config2['market']} {config2['contract']}...")
            data2 = self.data_fetcher.fetch_contract_data(config2)
            
            print(f"✅ Data fetched successfully")
            return data1, data2
            
        except Exception as e:
            print(f"❌ Failed to fetch spread data: {e}")
            return None, None
    
    def calculate_spread(self, data1, data2, coeff=[1, -1]):
        """
        Calculate spread from two datasets
        
        Args:
            data1 (dict): First contract data
            data2 (dict): Second contract data
            coeff (list): Spread coefficients
            
        Returns:
            pd.DataFrame: Spread data
        """
        print(f"\n📈 Calculating spread with coefficients {coeff}...")
        
        # Get price series from both datasets
        def get_price_series(data):
            if data.get('mid_prices') is not None and not data['mid_prices'].empty:
                return data['mid_prices']
            elif data.get('orders') is not None and not data['orders'].empty:
                orders = data['orders']
                if 'b_price' in orders.columns and 'a_price' in orders.columns:
                    return 0.5 * (orders['b_price'] + orders['a_price'])
            return pd.Series(dtype=float)
        
        price1 = get_price_series(data1)
        price2 = get_price_series(data2)
        
        if price1.empty or price2.empty:
            print(f"❌ No price data available")
            return pd.DataFrame()
        
        # Align timestamps
        common_index = price1.index.intersection(price2.index)
        
        if len(common_index) == 0:
            print(f"❌ No common timestamps")
            return pd.DataFrame()
        
        # Calculate spread
        p1_aligned = price1.reindex(common_index).dropna()
        p2_aligned = price2.reindex(common_index).dropna()
        
        final_index = p1_aligned.index.intersection(p2_aligned.index)
        
        if len(final_index) == 0:
            print(f"❌ No valid aligned data")
            return pd.DataFrame()
        
        p1_final = p1_aligned.reindex(final_index)
        p2_final = p2_aligned.reindex(final_index)
        
        # Spread calculation
        spread_series = coeff[0] * p1_final + coeff[1] * p2_final
        
        # Create spread DataFrame
        tick_size = max(0.01, spread_series.std() * 0.001)
        
        spread_data = pd.DataFrame({
            'spread': spread_series,
            'bid': spread_series - tick_size,
            'ask': spread_series + tick_size,
            'contract1_price': p1_final,
            'contract2_price': p2_final
        }, index=final_index)
        
        print(f"✅ Spread calculated:")
        print(f"   📊 Points: {len(spread_data):,}")
        print(f"   📈 Range: {spread_series.min():.3f} to {spread_series.max():.3f}")
        print(f"   📉 Mean: {spread_series.mean():.3f} ± {spread_series.std():.3f}")
        
        return spread_data
    
    def apply_ema_analysis(self, spread_data, tau=5, margin=0.43, eql_p=-6.25, w=0):
        """
        Apply EMA analysis to spread data
        
        Args:
            spread_data (pd.DataFrame): Spread data
            tau (int): EMA parameter
            margin (float): Band margin
            eql_p (float): Equilibrium price
            w (float): Weight parameter
            
        Returns:
            pd.DataFrame: EMA bands
        """
        print(f"\n📊 Applying EMA analysis (tau={tau}, margin={margin})...")
        
        if spread_data.empty:
            return pd.DataFrame()
        
        spread_values = spread_data['spread'].values
        
        # Calculate EMA with tick-based weighting
        dif_list = [0.001] + [abs(x - xl) for x, xl in zip(spread_values[1:], spread_values[:-1])]
        
        model = EMA(tau, spread_values[0])
        ema_values = [model.push(x, dx) for x, dx in zip(spread_values, dif_list)]
        
        # Apply equilibrium adjustment
        ema_adjusted = [w * eql_p + (1 - w) * x for x in ema_values]
        
        # Create bands
        ema_bands = pd.DataFrame({
            'ema_lower': [x - margin for x in ema_adjusted],
            'ema_center': ema_adjusted,
            'ema_upper': [x + margin for x in ema_adjusted]
        }, index=spread_data.index)
        
        print(f"✅ EMA bands calculated for {len(ema_bands):,} points")
        
        return ema_bands
    
    def run_complete_analysis(self, test_contracts=['07_25', '08_25', '09_25'], 
                             start_date=datetime(2025, 6, 1), end_date=datetime(2025, 7, 31)):
        """
        Run complete integrated analysis with diagnostics
        
        Args:
            test_contracts (list): Contract specifications to test
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            dict: Complete analysis results
        """
        print(f"\n{'='*100}")
        print(f"🚀 COMPLETE INTEGRATED SPREAD ANALYSIS")
        print(f"{'='*100}")
        print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"📊 Test Contracts: {test_contracts}")
        print(f"🎯 Output: {self.output_dir}")
        print(f"{'='*100}")
        
        try:
            # Step 1: Diagnose data availability
            availability_report = self.diagnose_data_availability(
                contract_months=test_contracts,
                start_date=start_date,
                end_date=end_date
            )
            
            # Step 2: Find best spread pair
            config1, config2 = self.find_best_spread_pair(availability_report)
            
            if not config1 or not config2:
                print(f"❌ Could not find suitable contract pair")
                return {'status': 'failed', 'reason': 'insufficient_data'}
            
            # Step 3: Fetch spread data
            data1, data2 = self.fetch_spread_data(config1, config2)
            
            if not data1 or not data2:
                print(f"❌ Failed to fetch spread data")
                return {'status': 'failed', 'reason': 'fetch_failed'}
            
            # Step 4: Calculate spread
            spread_data = self.calculate_spread(data1, data2, coeff=[1, -1])
            
            if spread_data.empty:
                print(f"❌ Spread calculation failed")
                return {'status': 'failed', 'reason': 'spread_calc_failed'}
            
            # Step 5: EMA analysis
            ema_bands = self.apply_ema_analysis(spread_data)
            
            # Step 6: Combine results
            combined_data = pd.concat([spread_data, ema_bands], axis=1)
            
            # Step 7: Save comprehensive results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            results = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'contracts': {
                    'contract1': config1,
                    'contract2': config2
                },
                'data_summary': {
                    'spread_points': len(spread_data),
                    'date_range': f"{spread_data.index[0]} to {spread_data.index[-1]}",
                    'spread_stats': {
                        'min': float(spread_data['spread'].min()),
                        'max': float(spread_data['spread'].max()),
                        'mean': float(spread_data['spread'].mean()),
                        'std': float(spread_data['spread'].std())
                    }
                },
                'availability_report': availability_report
            }
            
            # Save spread analysis
            analysis_file = self.spread_dir / f"complete_analysis_{timestamp}.pkl"
            with open(analysis_file, 'wb') as f:
                pickle.dump({
                    'spread_data': spread_data,
                    'ema_bands': ema_bands,
                    'combined_data': combined_data,
                    'raw_data': {'data1': data1, 'data2': data2},
                    'results': results
                }, f)
            
            # Save CSV
            csv_file = self.spread_dir / f"spread_analysis_{timestamp}.csv"
            combined_data.to_csv(csv_file)
            
            # Save report
            report_file = self.reports_dir / f"analysis_report_{timestamp}.json"
            with open(report_file, 'w') as f:
                json.dump({
                    **results,
                    'files': {
                        'analysis': str(analysis_file),
                        'csv': str(csv_file),
                        'report': str(report_file)
                    }
                }, f, indent=2)
            
            print(f"\n{'='*100}")
            print(f"🎉 ANALYSIS COMPLETED SUCCESSFULLY")
            print(f"{'='*100}")
            print(f"📊 Spread Points: {len(spread_data):,}")
            print(f"📈 Contracts: {config1['contract']} vs {config2['contract']}")
            print(f"📉 Spread Range: {results['data_summary']['spread_stats']['min']:.3f} to {results['data_summary']['spread_stats']['max']:.3f}")
            print(f"💾 Files: {analysis_file.name}, {csv_file.name}, {report_file.name}")
            print(f"📁 Location: {self.output_dir}")
            print(f"✅ Status: SUCCESS")
            print(f"{'='*100}")
            
            return results
            
        except Exception as e:
            print(f"\n❌ COMPLETE ANALYSIS FAILED: {e}")
            import traceback
            traceback.print_exc()
            return {'status': 'failed', 'reason': 'exception', 'error': str(e)}


def main():
    """
    Main function for complete integrated analysis
    """
    print("🔗 Integrated Spread Analysis - Final System")
    print("=" * 70)
    
    # Initialize analyzer
    analyzer = IntegratedSpreadAnalyzerFinal()
    
    # Run complete analysis with broader date range and more contracts
    results = analyzer.run_complete_analysis(
        test_contracts=['07_25', '08_25', '09_25', '10_25', '11_25'],  # Test more contracts
        start_date=datetime(2025, 6, 1),   # Earlier start date
        end_date=datetime(2025, 7, 31)     # Current end date
    )
    
    if results.get('status') == 'success':
        print(f"\n🎉 COMPLETE INTEGRATION SUCCESS!")
        print(f"📊 Processed {results['data_summary']['spread_points']:,} spread data points")
        print(f"📈 Successfully analyzed {results['contracts']['contract1']['contract']} vs {results['contracts']['contract2']['contract']}")
        print(f"💾 All results saved to ATS_4_data directory")
    else:
        print(f"\n❌ INTEGRATION INCOMPLETE")
        print(f"📋 Reason: {results.get('reason', 'unknown')}")
        if results.get('error'):
            print(f"❌ Error: {results['error']}")
    
    return results


if __name__ == "__main__":
    results = main()