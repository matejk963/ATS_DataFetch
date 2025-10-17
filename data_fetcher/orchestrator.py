"""
Data Fetch Orchestrator
========================

Main orchestration class that coordinates all data fetching operations.
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

# Add paths for core imports
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
from src.core.data_fetcher import DataFetcher, TPDATA_AVAILABLE

from .contracts import ContractSpec, parse_absolute_contract, create_contract_config_from_spec
from .spreadviewer_integration import fetch_synthetic_spread_multiple_periods
from .merger import (
    merge_spread_data,
    create_unified_spreadviewer_data,
    create_unified_real_spread_data
)
from .validators import BidAskValidator


class DataFetchOrchestrator:
    """
    Main orchestrator for unified data fetching integrating DataFetcher and SpreadViewer.
    
    Provides a clean API for fetching single contracts, spreads, and hybrid combinations
    of real and synthetic data.
    """
    
    def __init__(self, output_base: str = None):
        """
        Initialize the orchestrator.
        
        Args:
            output_base: Base directory for output files
        """
        # Set output base based on platform
        if output_base is None:
            if os.name == 'nt':
                self.output_base = r'C:\Users\krajcovic\Documents\Testing Data\RawData'
            else:
                self.output_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData'
        else:
            self.output_base = output_base
            
        # Check dependencies
        if not TPDATA_AVAILABLE:
            raise RuntimeError("TPData not available - cannot initialize orchestrator")
    
    def integrated_fetch(self, config: Dict) -> Dict:
        """
        Main integrated fetch function with unified input processing
        
        Config format:
        {
            'contracts': ['demb07_25', 'demb08_25'],  # 1=single, 2=spread
            'coefficients': [1, -1],  # Only for spreads
            'period': {
                'start_date': '2025-02-03',
                'end_date': '2025-06-02'
            },
            'options': {
                'include_real_spread': True,
                'include_synthetic_spread': True,
                'include_individual_legs': False
            },
            'n_s': 3
        }
        """
        contracts = config['contracts']
        period = config['period']
        options = config.get('options', {})
        coefficients = config.get('coefficients', [1, -1])
        n_s = config.get('n_s', 3)
        
        # Parse absolute contracts
        parsed_contracts = [parse_absolute_contract(c) for c in contracts]
        
        start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
        
        results = {
            'metadata': {
                'contracts': contracts,
                'parsed_contracts': [
                    {
                        'contract': c.market + c.product[0] + c.tenor + c.contract,
                        'market': c.market, 'product': c.product, 'tenor': c.tenor,
                        'delivery_date': c.delivery_date.isoformat()
                    } for c in parsed_contracts
                ],
                'period': period,
                'n_s': n_s,
                'mode': 'single_leg' if len(contracts) == 1 else 'spread'
            }
        }
        
        if len(parsed_contracts) == 1:
            # SINGLE LEG MODE
            print(f"🔍 SINGLE LEG MODE: {contracts[0]}")
            contract = parsed_contracts[0]
            
            # Use DataFetcher for individual contract
            fetcher = DataFetcher(allowed_broker_ids=[1441])
            
            # Create contract config for DataFetcher
            contract_config = {
                'market': contract.market,
                'tenor': contract.tenor,
                'contract': contract.contract,
                'start_date': period['start_date'],
                'end_date': period['end_date'],
                'prod': contract.product
            }
            
            single_data = fetcher.fetch_contract_data(
                contract_config,
                include_trades=True,
                include_orders=True
            )
            
            results['single_leg_data'] = single_data
            
        elif len(parsed_contracts) == 2:
            # SPREAD MODE
            print(f"🔍 SPREAD MODE: {contracts[0]} vs {contracts[1]}")
            contract1, contract2 = parsed_contracts
            
            # Real spread via DataFetcher (if requested)
            if options.get('include_real_spread', True):
                print("📈 Fetching real spread contract...")
                try:
                    fetcher = DataFetcher(allowed_broker_ids=[1441])
                    
                    # Create contract configs for both legs
                    contract1_config = create_contract_config_from_spec(contract1, period)
                    contract2_config = create_contract_config_from_spec(contract2, period)
                    
                    # For cross-market spreads, DataFetcher needs combined market string
                    if contract1.market != contract2.market:
                        combined_market = f"{contract1.market}_{contract2.market}"
                        print(f"   Cross-market spread detected: {contract1.market} + {contract2.market} → market='{combined_market}'")
                        contract1_config['market'] = combined_market
                        contract2_config['market'] = combined_market
                    
                    real_spread_data = fetcher.fetch_spread_contract_data(
                        contract1_config, contract2_config,
                        include_trades=True, include_orders=True
                    )
                    results['real_spread_data'] = real_spread_data
                    print(f"   ✅ Real spread: {len(real_spread_data.get('spread_orders', pd.DataFrame()))} orders, {len(real_spread_data.get('spread_trades', pd.DataFrame()))} trades")
                    
                    # Process real spread data into unified format
                    unified_real_data = create_unified_real_spread_data(real_spread_data)
                    results['real_spread_data']['unified_spread_data'] = unified_real_data['unified_spread_data']
                    
                    # Save unified real spread data
                    print("💾 Saving unified real spread data...")
                    self.save_unified_results(results, config['contracts'], config['period'], 'real_only', config.get('test_mode', False))
                except Exception as e:
                    print(f"   ❌ Real spread failed: {e}")
                    results['real_spread_error'] = str(e)
            
            # Synthetic spread via SpreadViewer (if requested) 
            if options.get('include_synthetic_spread', True):
                print("🔧 Fetching synthetic spread...")
                try:
                    synthetic_spread_data = fetch_synthetic_spread_multiple_periods(
                        contract1, contract2, start_date, end_date, coefficients, n_s
                    )
                    # Store raw synthetic data first
                    results['synthetic_spread_data'] = synthetic_spread_data
                    
                    # Create unified DataFrame from synthetic data
                    unified_synthetic = create_unified_spreadviewer_data(synthetic_spread_data)
                    results['synthetic_spread_data']['unified_spread_data'] = unified_synthetic['unified_spread_data']
                    print(f"   ✅ Synthetic spread: {len(synthetic_spread_data.get('spread_orders', pd.DataFrame()))} orders, {len(synthetic_spread_data.get('spread_trades', pd.DataFrame()))} trades")
                    print(f"   🎉 Unified synthetic data: {len(unified_synthetic.get('unified_spread_data', pd.DataFrame()))} total records")
                    
                    # Save unified synthetic spread data
                    print("💾 Saving unified synthetic spread data...")
                    self.save_unified_results(results, config['contracts'], config['period'], 'synthetic_only', config.get('test_mode', False))
                except Exception as e:
                    print(f"   ❌ Synthetic spread failed: {e}")
                    results['synthetic_spread_error'] = str(e)
            
            # Merge real and synthetic spread data (if both are requested)
            if (options.get('include_real_spread', True) and 
                options.get('include_synthetic_spread', True) and
                'real_spread_data' in results and 
                'synthetic_spread_data' in results):
                print("🔗 Merging real and synthetic spread data...")
                try:
                    merged_spread_data = merge_spread_data(
                        results['real_spread_data'],
                        results['synthetic_spread_data']
                    )
                    results['merged_spread_data'] = merged_spread_data
                    print(f"   ✅ Merged spread: {len(merged_spread_data.get('unified_spread_data', pd.DataFrame()))} total records")
                    
                    # Save unified merged spread data
                    print("💾 Saving unified merged spread data...")
                    self.save_unified_results(results, config['contracts'], config['period'], 'merged', config.get('test_mode', False))
                except Exception as e:
                    print(f"   ❌ Spread merging failed: {e}")
                    results['spread_merge_error'] = str(e)
            
            # Individual legs (if requested)
            if options.get('include_individual_legs', False):
                print("📊 Fetching individual leg data...")
                fetcher = DataFetcher(allowed_broker_ids=[1441])
                
                for i, contract in enumerate(parsed_contracts):
                    contract_config = {
                        'market': contract.market,
                        'tenor': contract.tenor, 
                        'contract': contract.contract,
                        'start_date': period['start_date'],
                        'end_date': period['end_date'],
                        'prod': contract.product
                    }
                    
                    try:
                        leg_data = fetcher.fetch_contract_data(
                            contract_config, include_trades=True, include_orders=True
                        )
                        results[f'leg_{i+1}_data'] = leg_data
                        print(f"   ✅ Leg {i+1}: {len(leg_data.get('orders', pd.DataFrame()))} orders, {len(leg_data.get('trades', pd.DataFrame()))} trades")
                    except Exception as e:
                        print(f"   ❌ Leg {i+1} failed: {e}")
                        results[f'leg_{i+1}_error'] = str(e)
        else:
            raise ValueError("Only 1 or 2 contracts supported")
        
        return results
    
    def save_unified_results(self, results: Dict, contracts: List[str], period: Dict, stage: str = 'unified', test_mode: bool = False) -> None:
        """Save unified spread data with format options based on test mode
        
        Args:
            test_mode: If True, saves all formats (parquet, csv, json) to RawData/test/
                      If False, saves only parquet to RawData/
        """
        output_dir = self.output_base
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Check for unified data in different possible keys
        unified_data = None
        data_source = 'unknown'
        
        if 'merged_spread_data' in results and 'unified_spread_data' in results['merged_spread_data']:
            unified_data = results['merged_spread_data']['unified_spread_data']
            data_source = 'merged'
        elif 'synthetic_spread_data' in results and 'unified_spread_data' in results['synthetic_spread_data']:
            unified_data = results['synthetic_spread_data']['unified_spread_data']  
            data_source = 'synthetic_only'
        elif 'real_spread_data' in results and 'unified_spread_data' in results['real_spread_data']:
            unified_data = results['real_spread_data']['unified_spread_data']
            data_source = 'real_only'
        
        if unified_data is None or (hasattr(unified_data, 'empty') and unified_data.empty):
            print(f"   ⚠️  No unified data to save")
            return
        
        # Generate filename based on number of contracts
        if len(contracts) == 1:
            # Single contract: contract_tr_ba_data
            filename = f"{contracts[0]}_tr_ba_data"
        elif len(contracts) == 2:
            # Spread: contract1_contract2_tr_ba_data  
            filename = f"{contracts[0]}_{contracts[1]}_tr_ba_data"
        else:
            # Fallback for multiple contracts
            contract_names = '_'.join(contracts)
            filename = f"{contract_names}_tr_ba_data"
        
        # Validate bid-ask spreads before saving
        print(f"   🔍 Final validation: Checking for negative bid-ask spreads...")
        validator = BidAskValidator(strict_mode=True, log_filtered=True)
        validated_data = validator.validate_merged_data(unified_data, "FinalData")
        
        # Log validation summary
        stats = validator.get_stats()
        if stats['total_processed'] > 0:
            print(f"      📊 Final validation: {stats['filtered_count']}/{stats['total_processed']} "
                  f"negative spreads filtered ({stats['filter_rate']:.1f}%)")
        
        # Always save as parquet
        parquet_path = os.path.join(output_dir, f'{filename}.parquet')
        validated_data.to_parquet(parquet_path)
        print(f"   📁 Saved validated spread data: {parquet_path}")
        
        # In test mode, also save CSV and pickle formats
        if test_mode:
            # Save as CSV
            csv_path = os.path.join(output_dir, f'{filename}.csv')
            validated_data.to_csv(csv_path)
            print(f"   📁 Saved validated spread data: {csv_path}")
            
            # Save as pickle
            pkl_path = os.path.join(output_dir, f'{filename}.pkl')
            validated_data.to_pickle(pkl_path)
            print(f"   📁 Saved validated spread data: {pkl_path}")
        
        # Save metadata
        metadata = {
            'stage': stage,
            'timestamp': timestamp,
            'contracts': contracts,
            'period': period,
            'n_s': results.get('metadata', {}).get('n_s', 3),
            'data_source': data_source,
            'unified_data_info': {
                'total_records': len(unified_data),
                'columns': list(unified_data.columns),
                'date_range': {
                    'start': str(unified_data.index.min()) if not unified_data.empty else None,
                    'end': str(unified_data.index.max()) if not unified_data.empty else None
                }
            }
        }
        
        # Add source statistics if available
        for key in ['merged_spread_data', 'synthetic_spread_data', 'real_spread_data']:
            if key in results and 'source_stats' in results[key]:
                metadata[f'{key}_stats'] = results[key]['source_stats']
        
        # In test mode, also save metadata
        if test_mode:
            metadata_path = os.path.join(output_dir, f'{filename}_metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            print(f"   📁 Saved metadata: {metadata_path}")
        
        print(f"   ✅ Unified data summary: {len(unified_data):,} records, {unified_data.shape[1]} columns")
        print(f"   📊 Sample structure: {list(unified_data.columns)}")