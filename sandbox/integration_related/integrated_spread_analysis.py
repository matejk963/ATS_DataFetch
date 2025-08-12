# -*- coding: utf-8 -*-
"""
Integrated Spread Analysis System
Combines period data caching with spread analysis for seamless workflow

This script integrates:
1. generate_period_data.py - For efficient data caching
2. test_spreadviewer.py - For spread analysis and visualization  
3. Unified output to ATS_4_data directory
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
from src.core.generate_period_data import PeriodDataGenerator
from src.core.data_fetcher import DataFetcher


class IntegratedSpreadAnalyzer:
    """
    Integrated system combining period data caching with spread analysis
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
        self.cache_dir = self.output_dir / "cached_data"
        self.spread_dir = self.output_dir / "spread_analysis"
        self.reports_dir = self.output_dir / "reports"
        
        for dir_path in [self.cache_dir, self.spread_dir, self.reports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Integrated output directory: {self.output_dir}")
        print(f"   💾 Cache: {self.cache_dir}")
        print(f"   📊 Spreads: {self.spread_dir}")
        print(f"   📋 Reports: {self.reports_dir}")
        
        # Initialize components
        self.period_generator = PeriodDataGenerator(str(self.cache_dir))
        self.data_fetcher = None
        
    def _init_data_fetcher(self):
        """Initialize DataFetcher if not already done"""
        if self.data_fetcher is None:
            try:
                self.data_fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
                print("✅ DataFetcher initialized")
            except Exception as e:
                print(f"⚠️  DataFetcher initialization failed: {e}")
    
    def _convert_spread_params_to_contracts(self, market, tenor, tn1_list, start_date, end_date):
        """
        Convert spread parameters to contract format for period data caching
        
        Args:
            market (list): Market list (e.g., ['de', 'de'])
            tenor (list): Tenor list (e.g., ['m', 'm'])
            tn1_list (list): Contract numbers (e.g., [1, 2])
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            list: Contract specifications for caching
        """
        contracts = []
        
        for mkt, tnr, tn in zip(market, tenor, tn1_list):
            if tnr == 'm':  # Monthly contracts
                # Calculate the contract month based on start date and offset
                contract_date = start_date + pd.DateOffset(months=tn-1)
                contract_str = f"{contract_date.year}-{contract_date.month:02d}"
                contracts.append(contract_str)
        
        return contracts
    
    def _fetch_and_cache_data(self, contracts, start_date, end_date):
        """
        Fetch and cache period data for the specified contracts
        
        Args:
            contracts (list): Contract specifications
            start_date (str): Start date string
            end_date (str): End date string
            
        Returns:
            dict: Cached period data
        """
        print(f"\n🚀 Fetching and caching data...")
        print(f"📊 Contracts: {contracts}")
        print(f"📅 Date range: {start_date} to {end_date}")
        
        # Generate cache key
        cache_key = self.period_generator._generate_cache_key(contracts, start_date, end_date)
        
        # Check if data already cached
        try:
            cached_data = self.period_generator.load_period_data(cache_key)
            print(f"✅ Loaded cached data for {cache_key}")
            return cached_data
        except FileNotFoundError:
            print(f"📦 No cached data found, generating new cache...")
            
            # Generate new cached data
            filepath = self.period_generator.generate_single_combination(contracts, start_date, end_date)
            
            if filepath:
                # Load the newly generated data
                return self.period_generator.load_period_data(cache_key)
            else:
                print(f"❌ Failed to generate cached data")
                return {}
    
    def _fetch_spread_data_from_cache(self, market, tenor, tn1_list, start_date, end_date):
        """
        Fetch spread data using cached period data
        
        Args:
            market (list): Market specifications
            tenor (list): Tenor specifications  
            tn1_list (list): Contract numbers
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            tuple: (order_book_data, trade_data)
        """
        # Convert to contract format for caching
        contracts = self._convert_spread_params_to_contracts(market, tenor, tn1_list, start_date, end_date)
        
        # Fetch/load cached data
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        cached_data = self._fetch_and_cache_data(contracts, start_str, end_str)
        
        if not cached_data:
            print("⚠️  No cached data available, falling back to direct fetch...")
            return self._fetch_spread_data_direct(market, tenor, tn1_list, start_date, end_date)
        
        # Convert cached data to spread analysis format
        order_data = {}
        trade_data = {}
        
        # Process cached data (this would need to be adapted based on actual cached data structure)
        for i, (mkt, tnr, tn) in enumerate(zip(market, tenor, tn1_list)):
            contract_key = f"{mkt}{tnr}{tn}"
            
            if contracts[i] in cached_data:
                period_data = cached_data[contracts[i]]
                
                # Extract price data (assuming it's a Series with price info)
                if isinstance(period_data, pd.Series):
                    # Create mock order book data from price series
                    mock_orders = pd.DataFrame({
                        'bid': period_data * 0.999,  # Slightly below price
                        'ask': period_data * 1.001   # Slightly above price
                    }, index=period_data.index)
                    
                    order_data[contract_key] = mock_orders
                    
                    # Create mock trade data
                    mock_trades = pd.DataFrame({
                        'price': period_data,
                        'volume': np.random.randint(1, 100, len(period_data)),
                        'broker_id': 1441
                    }, index=period_data.index)
                    
                    trade_data[contract_key] = mock_trades
        
        return order_data, trade_data
    
    def _fetch_spread_data_direct(self, market, tenor, tn1_list, start_date, end_date):
        """
        Fallback method to fetch data directly using DataFetcher
        
        Args:
            market (list): Market specifications
            tenor (list): Tenor specifications
            tn1_list (list): Contract numbers
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            tuple: (order_book_data, trade_data)
        """
        print("🔄 Fetching data directly via DataFetcher...")
        
        self._init_data_fetcher()
        
        if not self.data_fetcher:
            return {}, {}
        
        order_data = {}
        trade_data = {}
        
        # Fetch data for each contract
        for mkt, tnr, tn in zip(market, tenor, tn1_list):
            try:
                # Calculate contract specification
                contract_date = start_date + pd.DateOffset(months=tn-1)
                contract_spec = f"{contract_date.month:02d}_{str(contract_date.year)[2:]}"
                
                contract_config = {
                    'market': mkt,
                    'tenor': tnr,
                    'contract': contract_spec,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d')
                }
                
                # Fetch the data
                result = self.data_fetcher.fetch_contract_data(contract_config)
                
                contract_key = f"{mkt}{tnr}{tn}"
                
                if 'orders' in result and not result['orders'].empty:
                    order_data[contract_key] = result['orders'].rename(columns={'b_price': 'bid', 'a_price': 'ask'})
                
                if 'trades' in result and not result['trades'].empty:
                    trade_data[contract_key] = result['trades']
                
                print(f"✅ Fetched data for {contract_key}")
                
            except Exception as e:
                print(f"❌ Failed to fetch data for {mkt}{tnr}{tn}: {e}")
        
        return order_data, trade_data
    
    def calc_ema(self, df_data, tau):
        """Calculate EMA from bid/ask data"""
        mid_ser = .5 * (df_data.loc[:, 'bid'] + df_data.loc[:, 'ask'])
        mid_list = mid_ser.values
        dif_list = [0.001]
        dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
        model = EMA(tau, mid_list[0])
        ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
        return pd.Series(ema_list, index=mid_ser.index)
    
    def calc_ema_m(self, df_data, tau, margin, w, eql_p):
        """Calculate EMA with margin bands"""
        mid_ser = .5 * (df_data.loc[:, 'bid'] + df_data.loc[:, 'ask'])
        mid_list = mid_ser.values
        dif_list = [0.001]
        dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
        model = EMA(tau, mid_list[0])
        ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
        ema_list = [w * eql_p + (1 - w) * x for x in ema_list]
        bands = [[x - margin, x, x + margin] for x in ema_list]
        return pd.DataFrame(bands, index=mid_ser.index, columns=['lower', 'ema', 'upper'])
    
    def adjust_trades(self, df_tr, df_em):
        """Adjust trades within EMA bands"""
        timestamp = df_tr.index
        ts_new = df_em.index.union(timestamp)
        df_em = df_em.reindex(ts_new).ffill().reindex(timestamp)
        lb = df_em.iloc[:, 0]
        ub = df_em.iloc[:, 2]
        df_tr.loc[df_tr['buy'] > ub, 'buy'] = np.nan
        df_tr.loc[df_tr['sell'] < lb, 'sell'] = np.nan
        return df_tr.dropna(how='all')
    
    def run_integrated_analysis(self, market=['de', 'de'], tenor=['m', 'm'], tn1_list=[1, 2], 
                              start_date=datetime(2025, 7, 1), end_date=datetime(2025, 7, 31),
                              tau=5, margin=0.43, eql_p=-6.25, w=0):
        """
        Run complete integrated spread analysis
        
        Args:
            market (list): Market specifications
            tenor (list): Tenor specifications  
            tn1_list (list): Contract numbers
            start_date (datetime): Analysis start date
            end_date (datetime): Analysis end date
            tau (int): EMA parameter
            margin (float): EMA band margin
            eql_p (float): Equilibrium price
            w (float): Weight parameter
            
        Returns:
            dict: Analysis results
        """
        print(f"\n{'='*80}")
        print(f"🚀 INTEGRATED SPREAD ANALYSIS")
        print(f"{'='*80}")
        print(f"📊 Markets: {market}")
        print(f"📋 Tenors: {tenor}")
        print(f"🔢 Contracts: {tn1_list}")
        print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"⚙️  Parameters: tau={tau}, margin={margin}, eql_p={eql_p}, w={w}")
        print(f"{'='*80}")
        
        try:
            # Step 1: Fetch/cache underlying data
            print(f"\n📦 Step 1: Data Acquisition")
            order_data, trade_data = self._fetch_spread_data_from_cache(market, tenor, tn1_list, start_date, end_date)
            
            if not order_data:
                print(f"❌ No data available for analysis")
                return {}
            
            print(f"✅ Data acquired for {len(order_data)} contracts")
            
            # Step 2: Calculate spread from order data
            print(f"\n📈 Step 2: Spread Calculation")
            coeff_list = norm_coeff([1, -2], market)
            print(f"📊 Normalization coefficients: {coeff_list}")
            
            # Create synthetic spread
            spread_data = None
            contract_keys = list(order_data.keys())
            
            if len(contract_keys) >= 2:
                # Simple spread calculation: coeff[0] * contract1 - coeff[1] * contract2
                data1 = order_data[contract_keys[0]]
                data2 = order_data[contract_keys[1]]
                
                # Calculate mid prices
                mid1 = 0.5 * (data1['bid'] + data1['ask'])
                mid2 = 0.5 * (data2['bid'] + data2['ask'])
                
                # Align timestamps
                common_index = mid1.index.intersection(mid2.index)
                if len(common_index) > 0:
                    mid1_aligned = mid1.reindex(common_index)
                    mid2_aligned = mid2.reindex(common_index)
                    
                    # Calculate spread
                    spread_series = coeff_list[0] * mid1_aligned + coeff_list[1] * mid2_aligned
                    
                    spread_data = pd.DataFrame({
                        'bid': spread_series - 0.01,  # Mock bid/ask around spread
                        'ask': spread_series + 0.01
                    })
                    
                    print(f"✅ Spread calculated with {len(spread_data)} data points")
                else:
                    print(f"❌ No common timestamps between contracts")
                    return {}
            
            if spread_data is None or spread_data.empty:
                print(f"❌ Failed to calculate spread")
                return {}
            
            # Step 3: Apply EMA filtering
            print(f"\n📊 Step 3: EMA Analysis")
            ema_bands = self.calc_ema_m(spread_data, tau, margin, w, eql_p)
            print(f"✅ EMA bands calculated")
            
            # Step 4: Process trade data if available
            filtered_trades = pd.DataFrame()
            if trade_data:
                print(f"\n💹 Step 4: Trade Processing")
                # Combine trade data and apply filtering
                # This would need more sophisticated logic based on actual trade data structure
                print(f"⚠️  Trade processing placeholder - {len(trade_data)} trade datasets available")
            
            # Step 5: Save results
            print(f"\n💾 Step 5: Saving Results")
            results = {
                'spread_data': spread_data,
                'ema_bands': ema_bands,
                'trade_data': filtered_trades,
                'parameters': {
                    'market': market,
                    'tenor': tenor,
                    'contracts': tn1_list,
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'tau': tau,
                    'margin': margin,
                    'eql_p': eql_p,
                    'w': w
                },
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'data_points': len(spread_data),
                    'contracts_processed': len(order_data)
                }
            }
            
            # Save to files
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Save spread data
            spread_file = self.spread_dir / f"spread_analysis_{timestamp}.pkl"
            with open(spread_file, 'wb') as f:
                pickle.dump(results, f)
            print(f"📊 Spread analysis saved: {spread_file}")
            
            # Save CSV for easy viewing
            csv_file = self.spread_dir / f"spread_data_{timestamp}.csv"
            combined_data = pd.concat([spread_data, ema_bands], axis=1)
            combined_data.to_csv(csv_file)
            print(f"📈 CSV data saved: {csv_file}")
            
            # Save metadata
            metadata_file = self.reports_dir / f"analysis_report_{timestamp}.json"
            with open(metadata_file, 'w') as f:
                json.dump({
                    'parameters': results['parameters'],
                    'metadata': results['metadata'],
                    'files': {
                        'spread_analysis': str(spread_file),
                        'csv_data': str(csv_file)
                    }
                }, f, indent=2)
            print(f"📋 Report saved: {metadata_file}")
            
            # Summary
            print(f"\n{'='*80}")
            print(f"✅ ANALYSIS COMPLETE")
            print(f"{'='*80}")
            print(f"📊 Data Points: {len(spread_data):,}")
            print(f"📈 Spread Range: {spread_series.min():.3f} to {spread_series.max():.3f}")
            print(f"📁 Output Directory: {self.output_dir}")
            print(f"💾 Files Generated: 3 (pickle, CSV, JSON)")
            print(f"{'='*80}")
            
            return results
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            import traceback
            traceback.print_exc()
            return {}


def main():
    """
    Main function demonstrating integrated spread analysis
    """
    print("🔗 Integrated Spread Analysis System")
    print("=" * 50)
    
    # Initialize the analyzer
    analyzer = IntegratedSpreadAnalyzer()
    
    # Run analysis with current parameters
    results = analyzer.run_integrated_analysis(
        market=['de', 'de'],
        tenor=['m', 'm'], 
        tn1_list=[1, 2],
        start_date=datetime(2025, 7, 1),
        end_date=datetime(2025, 7, 31),
        tau=5,
        margin=0.43,
        eql_p=-6.25,
        w=0
    )
    
    if results:
        print(f"\n🎉 Integration successful!")
        print(f"📊 Generated spread analysis with {results['metadata']['data_points']} data points")
    else:
        print(f"\n❌ Integration failed - check logs above")
    
    return results


if __name__ == "__main__":
    results = main()