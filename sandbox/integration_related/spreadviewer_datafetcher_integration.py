# -*- coding: utf-8 -*-
r"""
SpreadViewer to DataFetcher Integration
Takes SpreadViewer's relative contracts and uses DataFetcher to cache data

This script:
1. Uses SpreadViewer's relative contract logic (M+1, M+2) 
2. Uses DataFetcher for robust data retrieval
3. Saves cached data to C:\Users\krajcovic\Documents\Testing Data\ATS_data\test
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
from src.core.data_fetcher import DataFetcher


def convert_spreadviewer_to_datafetcher_contracts(market, tenor, tn1_list, tn2_list, start_date, end_date):
    """
    Convert SpreadViewer relative contract specifications to DataFetcher format
    
    Args:
        market (list): Market specifications from SpreadViewer
        tenor (list): Tenor specifications from SpreadViewer  
        tn1_list (list): First contract offsets from SpreadViewer
        tn2_list (list): Second contract offsets from SpreadViewer
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        list: Contract configurations for DataFetcher
    """
    print("🔄 Converting SpreadViewer contracts to DataFetcher format...")
    print(f"📊 Markets: {market}")
    print(f"📋 Tenors: {tenor}")
    print(f"🔢 tn1_list: {tn1_list}, tn2_list: {tn2_list}")
    print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    contracts = []
    
    # Process first leg contracts (tn1_list)
    for i, offset in enumerate(tn1_list):
        if i < len(market) and i < len(tenor):
            # Calculate contract date based on offset from start_date
            contract_date = start_date + pd.DateOffset(months=offset-1)
            contract_spec = f"{contract_date.month:02d}_{str(contract_date.year)[2:]}"
            
            contract_config = {
                'market': market[i],
                'tenor': tenor[i], 
                'contract': contract_spec,
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'spreadviewer_offset': offset,
                'leg': 'first',
                'label': f"{market[i].upper()}_M+{offset}"
            }
            contracts.append(contract_config)
            print(f"   📋 First Leg: M+{offset} → {contract_config['market']} {contract_spec} ({contract_config['label']})")
    
    # Process second leg contracts (tn2_list)
    for i, offset in enumerate(tn2_list):
        if i < len(market) and i < len(tenor):
            # Calculate contract date based on offset from start_date
            contract_date = start_date + pd.DateOffset(months=offset-1)
            contract_spec = f"{contract_date.month:02d}_{str(contract_date.year)[2:]}"
            
            contract_config = {
                'market': market[i],
                'tenor': tenor[i],
                'contract': contract_spec, 
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'spreadviewer_offset': offset,
                'leg': 'second',
                'label': f"{market[i].upper()}_M+{offset}"
            }
            contracts.append(contract_config)
            print(f"   📋 Second Leg: M+{offset} → {contract_config['market']} {contract_spec} ({contract_config['label']})")
    
    return contracts


def cache_spreadviewer_data_with_datafetcher(output_dir=r"C:\Users\krajcovic\Documents\Testing Data\ATS_data\test"):
    """
    Cache SpreadViewer data using DataFetcher
    
    Args:
        output_dir (str): Output directory for cached data
        
    Returns:
        dict: Results of data caching
    """
    print("🚀 SPREADVIEWER DATA CACHING WITH DATAFETCHER")
    print("=" * 80)
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"📁 Output Directory: {output_path}")
    
    # SpreadViewer parameters (from test_spreadviewer.py)
    start_date = datetime(2025, 7, 1)
    end_date = datetime(2025, 7, 31)
    market = ['de', 'de']
    tenor = ['m', 'm'] 
    tn1_list = [1]  # M+1 (August 2025)
    tn2_list = [2]  # M+2 (September 2025)
    
    print(f"📅 SpreadViewer Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"📊 SpreadViewer Markets: {market}")
    print(f"📋 SpreadViewer Tenors: {tenor}")
    print(f"🔢 SpreadViewer Contracts: tn1={tn1_list}, tn2={tn2_list}")
    
    try:
        # Step 1: Convert SpreadViewer contracts to DataFetcher format
        contracts = convert_spreadviewer_to_datafetcher_contracts(
            market, tenor, tn1_list, tn2_list, start_date, end_date
        )
        
        if not contracts:
            print("❌ No contracts generated")
            return {'status': 'failed', 'reason': 'no_contracts'}
        
        print(f"✅ Generated {len(contracts)} contract configurations")
        
        # Step 2: Initialize DataFetcher
        print("\n📦 Initializing DataFetcher...")
        data_fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
        print("✅ DataFetcher initialized")
        
        # Step 3: Fetch and cache data for each contract
        print("\n🔄 Fetching and caching data for each contract...")
        
        cached_files = []
        results = {}
        
        for contract in contracts:
            try:
                print(f"\n📊 Processing {contract['label']} ({contract['market']} {contract['contract']})...")
                
                # Fetch data using DataFetcher
                print("🔄 Calling DataFetcher...")
                data_result = data_fetcher.fetch_contract_data(contract)
                
                if data_result:
                    # Save to pickle file
                    cache_filename = f"spreadviewer_{contract['label']}_{contract['start_date']}_{contract['end_date']}.pkl"
                    cache_path = output_path / cache_filename
                    
                    # Prepare data for caching
                    cache_data = {
                        'contract_config': contract,
                        'data': data_result,
                        'trades': data_result.get('trades', pd.DataFrame()),
                        'orders': data_result.get('orders', pd.DataFrame()),
                        'mid_prices': data_result.get('mid_prices', pd.Series(dtype=float)),
                        'metadata': {
                            'cached_at': datetime.now().isoformat(),
                            'source': 'DataFetcher',
                            'spreadviewer_compatible': True
                        }
                    }
                    
                    # Save to pickle
                    with open(cache_path, 'wb') as f:
                        pickle.dump(cache_data, f)
                    
                    cached_files.append(str(cache_path))
                    
                    # Store results
                    trades_count = len(data_result.get('trades', []))
                    orders_count = len(data_result.get('orders', []))
                    mid_count = len(data_result.get('mid_prices', []))
                    
                    results[contract['label']] = {
                        'config': contract,
                        'cache_file': str(cache_path),
                        'trades_count': trades_count,
                        'orders_count': orders_count,
                        'mid_prices_count': mid_count,
                        'file_size': cache_path.stat().st_size,
                        'status': 'success'
                    }
                    
                    print(f"✅ {contract['label']}: Cached {trades_count:,} trades, {orders_count:,} orders")
                    print(f"   💾 File: {cache_filename} ({cache_path.stat().st_size:,} bytes)")
                    
                else:
                    results[contract['label']] = {
                        'config': contract,
                        'cache_file': None,
                        'status': 'failed',
                        'error': 'DataFetcher returned no data'
                    }
                    print(f"❌ {contract['label']}: DataFetcher returned no data")
                    
            except Exception as e:
                results[contract['label']] = {
                    'config': contract,
                    'cache_file': None,
                    'status': 'failed',
                    'error': str(e)
                }
                print(f"❌ {contract['label']}: Exception - {e}")
        
        # Step 4: Create summary and metadata
        print("\n" + "=" * 80)
        print("📋 DATA CACHING SUMMARY")
        print("=" * 80)
        
        successful = [k for k, v in results.items() if v['status'] == 'success']
        failed = [k for k, v in results.items() if v['status'] == 'failed']
        
        print(f"✅ Successful: {len(successful)}/{len(contracts)}")
        for contract_label in successful:
            result = results[contract_label]
            print(f"   ✅ {contract_label}: {result['trades_count']:,} trades, {result['orders_count']:,} orders")
        
        if failed:
            print(f"❌ Failed: {len(failed)}/{len(contracts)}")
            for contract_label in failed:
                error = results[contract_label].get('error', 'Unknown error')
                print(f"   ❌ {contract_label}: {error}")
        
        # Step 5: Save summary metadata
        summary_file = output_path / f"spreadviewer_cache_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        summary_data = {
            'timestamp': datetime.now().isoformat(),
            'spreadviewer_params': {
                'market': market,
                'tenor': tenor, 
                'tn1_list': tn1_list,
                'tn2_list': tn2_list,
                'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            },
            'results': results,
            'summary': {
                'total_contracts': len(contracts),
                'successful_contracts': len(successful),
                'failed_contracts': len(failed),
                'cached_files': cached_files,
                'output_directory': str(output_path)
            }
        }
        
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2)
        
        print(f"\n📁 Files in output directory:")
        all_files = list(output_path.iterdir())
        for file_path in sorted(all_files):
            file_size = file_path.stat().st_size if file_path.exists() else 0
            print(f"   📄 {file_path.name} ({file_size:,} bytes)")
        
        # Final results
        final_results = {
            'status': 'success' if successful else 'partial' if len(successful) > 0 else 'failed',
            'contracts_processed': len(contracts),
            'successful_contracts': len(successful),
            'failed_contracts': len(failed),
            'output_directory': str(output_path),
            'cached_files': cached_files,
            'contract_results': results,
            'summary_file': str(summary_file),
            'spreadviewer_params': {
                'market': market,
                'tenor': tenor,
                'tn1_list': tn1_list,
                'tn2_list': tn2_list,
                'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            }
        }
        
        print("\n🎉 DATA CACHING COMPLETED")
        print(f"📊 Status: {final_results['status'].upper()}")
        print(f"📁 Output: {output_path}")
        print("✅ Ready for SpreadViewer analysis with cached data!")
        print("=" * 80)
        
        return final_results
        
    except Exception as e:
        print(f"\n❌ DATA CACHING FAILED: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'reason': 'exception', 'error': str(e)}


def load_cached_spreadviewer_data(cache_file_path):
    """
    Load cached SpreadViewer data from pickle file
    
    Args:
        cache_file_path (str): Path to cached pickle file
        
    Returns:
        dict: Cached data
    """
    try:
        with open(cache_file_path, 'rb') as f:
            cached_data = pickle.load(f)
        
        print(f"📂 Loaded cached data: {cache_file_path}")
        print(f"   📊 Contract: {cached_data['contract_config']['label']}")
        print(f"   📈 Trades: {len(cached_data['trades']):,}")
        print(f"   📋 Orders: {len(cached_data['orders']):,}")
        print(f"   💾 Cached: {cached_data['metadata']['cached_at']}")
        
        return cached_data
        
    except Exception as e:
        print(f"❌ Failed to load cached data: {e}")
        return None


def main():
    """
    Main function to run SpreadViewer data caching with DataFetcher
    """
    print("🔗 SpreadViewer → DataFetcher Caching Integration")
    print("Extracting relative contracts from SpreadViewer and caching with DataFetcher")
    print("=" * 80)
    
    # Run the caching
    results = cache_spreadviewer_data_with_datafetcher()
    
    if results.get('status') == 'success':
        print("\n🎉 SUCCESS! SpreadViewer data cached successfully")
        print(f"📊 Processed: {results['successful_contracts']}/{results['contracts_processed']} contracts")
        print(f"📁 Files saved to: {results['output_directory']}")
        print("🔗 Ready for SpreadViewer analysis!")
        
        # Demonstrate loading cached data
        if results['cached_files']:
            print(f"\n📋 Example: Loading first cached file...")
            first_cache = results['cached_files'][0]
            cached_data = load_cached_spreadviewer_data(first_cache)
            
        return results
        
    elif results.get('status') == 'partial':
        print("\n⚠️  PARTIAL SUCCESS")
        print(f"📊 Processed: {results['successful_contracts']}/{results['contracts_processed']} contracts")
        print(f"❌ {results['failed_contracts']} contracts failed")
        print(f"📁 Files saved to: {results['output_directory']}")
        
    else:
        print("\n❌ FAILED")
        print(f"📋 Reason: {results.get('reason', 'unknown')}")
        if results.get('error'):
            print(f"❌ Error: {results['error']}")
    
    return results


if __name__ == "__main__":
    results = main()