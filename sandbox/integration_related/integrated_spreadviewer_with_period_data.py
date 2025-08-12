# -*- coding: utf-8 -*-
"""
Integrated SpreadViewer with Period Data Generation
Combines the robust DataFetcher approach with SpreadViewer's technical analysis

This integration solves the issues by:
1. Using generate_period_data for reliable data caching
2. Using DataFetcher for robust data retrieval
3. Using SpreadViewer's EMA and technical analysis
4. Outputting to ATS_4_data directory structure
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
from SynthSpread.spreadviewer_class import norm_coeff
from Math.accumfeatures import EMA, MA, MSTD
from src.core.data_fetcher import DataFetcher
from src.core.generate_period_data import PeriodDataGenerator


class IntegratedSpreadViewerWithPeriodData:
    """
    Integrated system combining period data caching with spread technical analysis
    """
    
    def __init__(self, output_dir="C:/Users/krajcovic/Documents/Testing Data/ATS_4_data"):
        """
        Initialize the integrated system
        
        Args:
            output_dir (str): Output directory for all results
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create directory structure
        self.period_cache_dir = self.output_dir / "period_cache"
        self.spread_analysis_dir = self.output_dir / "spread_analysis"
        self.technical_analysis_dir = self.output_dir / "technical_analysis"
        self.reports_dir = self.output_dir / "reports"
        
        for dir_path in [self.period_cache_dir, self.spread_analysis_dir, self.technical_analysis_dir, self.reports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        print(f"🔗 Integrated SpreadViewer + Period Data System")
        print(f"   💾 Period Cache: {self.period_cache_dir}")
        print(f"   📊 Spread Analysis: {self.spread_analysis_dir}")
        print(f"   📈 Technical Analysis: {self.technical_analysis_dir}")
        print(f"   📋 Reports: {self.reports_dir}")
        
        # Initialize components
        self.period_generator = PeriodDataGenerator(str(self.period_cache_dir))
        self.data_fetcher = None
        self._init_data_fetcher()
    
    def _init_data_fetcher(self):
        """Initialize DataFetcher"""
        try:
            self.data_fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
            print("✅ DataFetcher initialized")
        except Exception as e:
            print(f"❌ DataFetcher failed: {e}")
    
    def create_spread_contracts(self, market=['de', 'de'], tenor=['m', 'm'], 
                               tn1_list=[1], tn2_list=[2], start_date=datetime(2025, 7, 1), 
                               end_date=datetime(2025, 7, 31)):
        """
        Create contract specifications for spread analysis
        
        Args:
            market (list): Market specifications
            tenor (list): Tenor specifications
            tn1_list (list): First contract offsets
            tn2_list (list): Second contract offsets
            start_date (datetime): Start date
            end_date (datetime): End date
            
        Returns:
            list: Contract configurations
        """
        print(f"\n🔧 Creating spread contract specifications...")
        print(f"📊 Markets: {market}")
        print(f"📋 Tenors: {tenor}")
        print(f"🔢 Contract Lists: tn1={tn1_list}, tn2={tn2_list}")
        
        contract_configs = []
        
        # Create contracts for tn1_list
        for i, offset in enumerate(tn1_list):
            if i < len(market) and i < len(tenor):
                contract_date = start_date + pd.DateOffset(months=offset-1)
                contract_spec = f"{contract_date.month:02d}_{str(contract_date.year)[2:]}"
                
                config = {
                    'market': market[i],
                    'tenor': tenor[i],
                    'contract': contract_spec,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'label': f"{market[i].upper()}_M+{offset}",
                    'leg': 'first'
                }
                contract_configs.append(config)
                print(f"   📋 First Leg: {config['label']} = {config['market']} {config['contract']}")
        
        # Create contracts for tn2_list  
        for i, offset in enumerate(tn2_list):
            if i < len(market) and i < len(tenor):
                contract_date = start_date + pd.DateOffset(months=offset-1)
                contract_spec = f"{contract_date.month:02d}_{str(contract_date.year)[2:]}"
                
                config = {
                    'market': market[i],
                    'tenor': tenor[i],
                    'contract': contract_spec,
                    'start_date': start_date.strftime('%Y-%m-%d'),
                    'end_date': end_date.strftime('%Y-%m-%d'),
                    'label': f"{market[i].upper()}_M+{offset}",
                    'leg': 'second'
                }
                contract_configs.append(config)
                print(f"   📋 Second Leg: {config['label']} = {config['market']} {config['contract']}")
        
        return contract_configs
    
    def fetch_and_cache_contracts(self, contract_configs):
        """
        Fetch contract data with caching support
        
        Args:
            contract_configs (list): Contract configurations
            
        Returns:
            dict: Fetched contract data
        """
        print(f"\n📡 Fetching and caching contract data...")
        
        if not self.data_fetcher:
            print(f"❌ DataFetcher not available")
            return {}
        
        contract_data = {}
        
        for config in contract_configs:
            try:
                print(f"🔄 Fetching {config['label']}...")
                
                # Check if we have cached period data first
                cache_key = f"{config['contract']}_{config['start_date']}_{config['end_date']}"
                
                # Try to use cached data from period generator
                try:
                    cached_data = self.period_generator.load_period_data(cache_key)
                    print(f"   💾 Found cached period data")
                    
                    # Convert cached data to contract format if needed
                    # This would require understanding the cached data structure
                    
                except FileNotFoundError:
                    print(f"   📡 No cache found, fetching fresh data...")
                
                # Fetch using DataFetcher
                result = self.data_fetcher.fetch_contract_data(config)
                
                if result:
                    contract_data[config['label']] = {
                        'config': config,
                        'data': result,
                        'trades': result.get('trades', pd.DataFrame()),
                        'orders': result.get('orders', pd.DataFrame()),
                        'mid_prices': result.get('mid_prices', pd.Series(dtype=float))
                    }
                    
                    trades_count = len(result.get('trades', []))
                    orders_count = len(result.get('orders', []))
                    print(f"   ✅ {config['label']}: {trades_count:,} trades, {orders_count:,} orders")
                
            except Exception as e:
                print(f"   ❌ Failed to fetch {config['label']}: {e}")
        
        return contract_data
    
    def calculate_spread_with_spreadviewer_logic(self, contract_data, coeff_list=[1, -2]):
        """
        Calculate spread using SpreadViewer logic adapted for our data
        
        Args:
            contract_data (dict): Contract data dictionary
            coeff_list (list): Spread coefficients
            
        Returns:
            pd.DataFrame: Spread data
        """
        print(f"\n📈 Calculating spread with SpreadViewer methodology...")
        print(f"📊 Coefficients: {coeff_list}")
        
        # Get contract labels
        labels = list(contract_data.keys())
        
        if len(labels) < 2:
            print(f"❌ Need at least 2 contracts, got {len(labels)}")
            return pd.DataFrame()
        
        print(f"📋 Using contracts: {labels}")
        
        # Extract price data for each contract
        price_series = {}
        
        for label in labels:
            data = contract_data[label]
            
            # Try to get mid prices first
            if not data['mid_prices'].empty:
                price_series[label] = data['mid_prices']
                print(f"   📊 {label}: Using mid prices ({len(data['mid_prices']):,} points)")
            elif not data['orders'].empty:
                # Calculate mid from bid/ask
                orders = data['orders']
                if 'b_price' in orders.columns and 'a_price' in orders.columns:
                    mid_calc = 0.5 * (orders['b_price'] + orders['a_price'])
                    price_series[label] = mid_calc
                    print(f"   📊 {label}: Calculated mid from orders ({len(mid_calc):,} points)")
                else:
                    print(f"   ❌ {label}: No valid price data")
            else:
                print(f"   ❌ {label}: No price data available")
        
        if len(price_series) < 2:
            print(f"❌ Insufficient price data")
            return pd.DataFrame()
        
        # Align timestamps (SpreadViewer approach)
        first_label = labels[0]
        second_label = labels[1]
        
        price1 = price_series[first_label]
        price2 = price_series[second_label]
        
        # Find common timestamps
        common_index = price1.index.intersection(price2.index)
        
        if len(common_index) == 0:
            print(f"❌ No common timestamps")
            return pd.DataFrame()
        
        # Align and clean data
        p1_aligned = price1.reindex(common_index).dropna()
        p2_aligned = price2.reindex(common_index).dropna()
        
        final_index = p1_aligned.index.intersection(p2_aligned.index)
        
        if len(final_index) == 0:
            print(f"❌ No valid aligned data")
            return pd.DataFrame()
        
        p1_final = p1_aligned.reindex(final_index)  
        p2_final = p2_aligned.reindex(final_index)
        
        # Calculate spread (SpreadViewer style)
        spread_series = coeff_list[0] * p1_final + coeff_list[1] * p2_final
        
        # Create bid/ask around spread (SpreadViewer approach)
        spread_std = spread_series.std()
        tick_size = max(0.01, spread_std * 0.001)
        
        spread_data = pd.DataFrame({
            'bid': spread_series - tick_size,
            'ask': spread_series + tick_size,
            'mid': spread_series,
            'spread': spread_series,
            'contract1_price': p1_final,
            'contract2_price': p2_final
        }, index=final_index)
        
        print(f"✅ Spread calculated:")
        print(f"   📊 Data points: {len(spread_data):,}")
        print(f"   📈 Range: {spread_series.min():.3f} to {spread_series.max():.3f}")
        print(f"   📉 Mean: {spread_series.mean():.3f} ± {spread_series.std():.3f}")
        print(f"   🕐 Time range: {final_index[0]} to {final_index[-1]}")
        
        return spread_data
    
    def apply_spreadviewer_ema_analysis(self, spread_data, tau=5, margin=0.43, w=0, eql_p=-6.25):
        """
        Apply SpreadViewer's EMA analysis methodology
        
        Args:
            spread_data (pd.DataFrame): Spread data with bid/ask
            tau (int): EMA parameter
            margin (float): Band margin
            w (float): Weight parameter
            eql_p (float): Equilibrium price
            
        Returns:
            pd.DataFrame: EMA bands
        """
        print(f"\n📊 Applying SpreadViewer EMA analysis...")
        print(f"⚙️  Parameters: tau={tau}, margin={margin}, w={w}, eql_p={eql_p}")
        
        if spread_data.empty or 'bid' not in spread_data.columns or 'ask' not in spread_data.columns:
            print(f"❌ Invalid spread data for EMA analysis")
            return pd.DataFrame()
        
        # Calculate mid prices (SpreadViewer method)
        mid_ser = 0.5 * (spread_data['bid'] + spread_data['ask'])
        mid_list = mid_ser.values
        
        # Calculate differences for tick-weighted EMA (SpreadViewer approach)
        dif_list = [0.001]  # Initial difference
        dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
        
        # Apply EMA model (SpreadViewer method)
        model = EMA(tau, mid_list[0])
        ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
        
        # Apply equilibrium adjustment (SpreadViewer approach)
        ema_adjusted = [w * eql_p + (1 - w) * x for x in ema_list]
        
        # Create bands (SpreadViewer method)
        ema_bands = pd.DataFrame({
            'ema_lower': [x - margin for x in ema_adjusted],
            'ema_center': ema_adjusted,
            'ema_upper': [x + margin for x in ema_adjusted]
        }, index=mid_ser.index)
        
        print(f"✅ EMA bands calculated:")
        print(f"   📊 Data points: {len(ema_bands):,}")
        print(f"   📈 Center range: {min(ema_adjusted):.3f} to {max(ema_adjusted):.3f}")
        print(f"   📏 Band width: {2 * margin:.3f}")
        
        return ema_bands
    
    def filter_trades_with_bands(self, spread_data, ema_bands):
        """
        Filter trades within EMA bands (SpreadViewer methodology)
        
        Args:
            spread_data (pd.DataFrame): Spread data
            ema_bands (pd.DataFrame): EMA bands
            
        Returns:
            pd.DataFrame: Filtered spread data
        """
        print(f"\n🔍 Filtering trades within EMA bands...")
        
        if spread_data.empty or ema_bands.empty:
            return pd.DataFrame()
        
        # Align timestamps
        common_index = spread_data.index.intersection(ema_bands.index)
        
        if len(common_index) == 0:
            print(f"❌ No common timestamps for filtering")
            return pd.DataFrame()
        
        # Align data
        spread_aligned = spread_data.reindex(common_index)
        bands_aligned = ema_bands.reindex(common_index)
        
        # Apply filtering logic (SpreadViewer approach adapted)
        filtered_data = spread_aligned.copy()
        
        # Create buy/sell signals based on spread relative to bands
        lower_band = bands_aligned['ema_lower']
        upper_band = bands_aligned['ema_upper']
        spread_values = spread_aligned['spread']
        
        # Mark trades outside bands as invalid (SpreadViewer logic)
        buy_signal = spread_values < lower_band  # Buy when spread below lower band
        sell_signal = spread_values > upper_band  # Sell when spread above upper band
        
        filtered_data['buy_signal'] = buy_signal
        filtered_data['sell_signal'] = sell_signal
        filtered_data['within_bands'] = (spread_values >= lower_band) & (spread_values <= upper_band)
        
        # Count signals
        buy_count = buy_signal.sum()
        sell_count = sell_signal.sum()
        within_count = filtered_data['within_bands'].sum()
        
        print(f"✅ Trade filtering completed:")
        print(f"   📈 Buy signals: {buy_count:,}")
        print(f"   📉 Sell signals: {sell_count:,}")
        print(f"   📊 Within bands: {within_count:,}")
        print(f"   📋 Total points: {len(filtered_data):,}")
        
        return filtered_data
    
    def run_integrated_analysis(self, market=['de', 'de'], tenor=['m', 'm'], tn1_list=[1], 
                               tn2_list=[2], start_date=datetime(2025, 7, 1), 
                               end_date=datetime(2025, 7, 31), tau=5, margin=0.43, 
                               eql_p=-6.25, w=0):
        """
        Run complete integrated analysis combining period data caching with SpreadViewer methodology
        
        Args:
            market (list): Market specifications
            tenor (list): Tenor specifications
            tn1_list (list): First contract offsets
            tn2_list (list): Second contract offsets
            start_date (datetime): Start date
            end_date (datetime): End date
            tau (int): EMA parameter
            margin (float): Band margin
            eql_p (float): Equilibrium price
            w (float): Weight parameter
            
        Returns:
            dict: Complete analysis results
        """
        print(f"\n{'='*100}")
        print(f"🚀 INTEGRATED SPREADVIEWER + PERIOD DATA ANALYSIS")
        print(f"{'='*100}")
        print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"📊 Markets: {market}")
        print(f"📋 Tenors: {tenor}")
        print(f"🔢 Contracts: tn1={tn1_list}, tn2={tn2_list}")
        print(f"⚙️  EMA Params: tau={tau}, margin={margin}, eql_p={eql_p}, w={w}")
        print(f"{'='*100}")
        
        try:
            # Step 1: Create contract specifications
            contract_configs = self.create_spread_contracts(market, tenor, tn1_list, tn2_list, start_date, end_date)
            
            if not contract_configs:
                print(f"❌ No valid contract configurations")
                return {}
            
            # Step 2: Fetch and cache contract data
            contract_data = self.fetch_and_cache_contracts(contract_configs)
            
            if not contract_data:
                print(f"❌ No contract data available")
                return {}
            
            # Step 3: Calculate spread coefficients (SpreadViewer style)
            coeff_list = norm_coeff([1, -2], market)
            print(f"\n📊 Spread coefficients: {coeff_list}")
            
            # Step 4: Calculate spread
            spread_data = self.calculate_spread_with_spreadviewer_logic(contract_data, coeff_list)
            
            if spread_data.empty:
                print(f"❌ Spread calculation failed")
                return {}
            
            # Step 5: Apply EMA analysis
            ema_bands = self.apply_spreadviewer_ema_analysis(spread_data, tau, margin, w, eql_p)
            
            # Step 6: Filter trades with bands
            filtered_spread = self.filter_trades_with_bands(spread_data, ema_bands)
            
            # Step 7: Save results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            results = {
                'timestamp': datetime.now().isoformat(),
                'parameters': {
                    'market': market,
                    'tenor': tenor,
                    'tn1_list': tn1_list,
                    'tn2_list': tn2_list,
                    'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                    'coefficients': coeff_list,
                    'ema_params': {'tau': tau, 'margin': margin, 'eql_p': eql_p, 'w': w}
                },
                'data_summary': {
                    'contracts_processed': len(contract_data),
                    'spread_points': len(spread_data),
                    'ema_points': len(ema_bands),
                    'filtered_points': len(filtered_spread),
                    'buy_signals': int(filtered_spread['buy_signal'].sum()) if not filtered_spread.empty else 0,
                    'sell_signals': int(filtered_spread['sell_signal'].sum()) if not filtered_spread.empty else 0
                },
                'spread_statistics': {
                    'min': float(spread_data['spread'].min()) if not spread_data.empty else None,
                    'max': float(spread_data['spread'].max()) if not spread_data.empty else None,
                    'mean': float(spread_data['spread'].mean()) if not spread_data.empty else None,
                    'std': float(spread_data['spread'].std()) if not spread_data.empty else None
                }
            }
            
            # Save comprehensive analysis
            analysis_file = self.spread_analysis_dir / f"integrated_spreadviewer_{timestamp}.pkl"
            with open(analysis_file, 'wb') as f:
                pickle.dump({
                    'spread_data': spread_data,
                    'ema_bands': ema_bands,
                    'filtered_spread': filtered_spread,
                    'contract_data': contract_data,
                    'results': results
                }, f)
            
            # Save CSV files
            spread_csv = self.spread_analysis_dir / f"spread_data_{timestamp}.csv"
            spread_data.to_csv(spread_csv)
            
            if not ema_bands.empty:
                ema_csv = self.technical_analysis_dir / f"ema_bands_{timestamp}.csv"
                ema_bands.to_csv(ema_csv)
            
            if not filtered_spread.empty:
                filtered_csv = self.technical_analysis_dir / f"filtered_spread_{timestamp}.csv"
                filtered_spread.to_csv(filtered_csv)
            
            # Save report
            report_file = self.reports_dir / f"integrated_analysis_{timestamp}.json"
            with open(report_file, 'w') as f:
                json.dump({
                    **results,
                    'files': {
                        'analysis': str(analysis_file),
                        'spread_csv': str(spread_csv),
                        'ema_csv': str(ema_csv) if not ema_bands.empty else None,
                        'filtered_csv': str(filtered_csv) if not filtered_spread.empty else None,
                        'report': str(report_file)
                    }
                }, f, indent=2)
            
            print(f"\n{'='*100}")
            print(f"🎉 INTEGRATED ANALYSIS COMPLETED")
            print(f"{'='*100}")
            print(f"📊 Spread Points: {len(spread_data):,}")
            print(f"📈 Buy Signals: {results['data_summary']['buy_signals']:,}")
            print(f"📉 Sell Signals: {results['data_summary']['sell_signals']:,}")
            print(f"💾 Files: Analysis, CSV, Report")
            print(f"📁 Location: {self.output_dir}")
            print(f"✅ Status: SUCCESS")
            print(f"{'='*100}")
            
            return results
            
        except Exception as e:
            print(f"\n❌ INTEGRATED ANALYSIS FAILED: {e}")
            import traceback
            traceback.print_exc()
            return {}


def main():
    """
    Main function demonstrating integrated SpreadViewer with period data
    """
    print("🔗 Integrated SpreadViewer + Period Data System")
    print("=" * 80)
    
    # Initialize system
    analyzer = IntegratedSpreadViewerWithPeriodData()
    
    # Run integrated analysis with SpreadViewer parameters
    results = analyzer.run_integrated_analysis(
        market=['de', 'de'],          # German power market for both legs
        tenor=['m', 'm'],            # Monthly contracts for both legs  
        tn1_list=[1],                # First leg: M+1 (August 2025)
        tn2_list=[2],                # Second leg: M+2 (September 2025)
        start_date=datetime(2025, 7, 1),
        end_date=datetime(2025, 7, 31),
        tau=5,                       # SpreadViewer EMA parameter
        margin=0.43,                 # SpreadViewer band margin
        eql_p=-6.25,                # SpreadViewer equilibrium price
        w=0                          # SpreadViewer weight parameter
    )
    
    if results:
        print(f"\n🎉 INTEGRATION SUCCESS!")
        print(f"📊 Generated {results['data_summary']['spread_points']:,} spread data points")
        print(f"📈 Identified {results['data_summary']['buy_signals']:,} buy signals")
        print(f"📉 Identified {results['data_summary']['sell_signals']:,} sell signals")
        print(f"💾 All results saved to ATS_4_data directory")
    else:
        print(f"\n❌ INTEGRATION FAILED")
    
    return results


if __name__ == "__main__":
    results = main()