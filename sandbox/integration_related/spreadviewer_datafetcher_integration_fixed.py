# -*- coding: utf-8 -*-
"""
SpreadViewer to DataFetcher Integration - WSL/Windows Compatible
Takes SpreadViewer's relative contracts and uses DataFetcher to cache data

This script:
1. Uses SpreadViewer's relative contract logic (M+1, M+2) 
2. Uses DataFetcher for robust data retrieval
3. Saves cached data with WSL/Windows path compatibility
4. Auto-detects environment and uses appropriate paths
"""

import sys
import os
import platform
from datetime import datetime, time
from pathlib import Path, PureWindowsPath, PurePosixPath
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


def get_cross_platform_path(windows_path):
    """
    Convert Windows path to cross-platform compatible path
    
    Args:
        windows_path (str): Windows path like 'C:\\Users\\...'
        
    Returns:
        Path: Cross-platform Path object
    """
    # Detect if we're in WSL
    is_wsl = 'microsoft' in platform.uname().release.lower() or 'wsl' in platform.uname().release.lower()
    
    if is_wsl:
        # Convert Windows path to WSL path
        if windows_path.startswith('C:'):
            wsl_path = windows_path.replace('C:', '/mnt/c').replace('\\', '/')
            return Path(wsl_path)
        else:
            return Path(windows_path.replace('\\', '/'))
    else:
        # Native Windows or other platform
        return Path(windows_path)


def ensure_directory_exists(path_obj):
    """
    Ensure directory exists with proper error handling
    
    Args:
        path_obj (Path): Path object
        
    Returns:
        Path: Verified path object
    """
    try:
        path_obj.mkdir(parents=True, exist_ok=True)
        print(f"✅ Directory confirmed: {path_obj}")
        return path_obj
    except Exception as e:
        print(f"❌ Failed to create directory {path_obj}: {e}")
        # Fallback to current directory
        fallback = Path.cwd() / "ats_data_test"
        fallback.mkdir(parents=True, exist_ok=True)
        print(f"📁 Using fallback directory: {fallback}")
        return fallback


def save_with_cross_platform_compat(data, file_path):
    """
    Save data with cross-platform path compatibility
    
    Args:
        data: Data to save
        file_path (Path): File path
        
    Returns:
        bool: Success status
    """
    try:
        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save based on file extension
        if file_path.suffix == '.pkl':
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
        elif file_path.suffix == '.json':
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        # Verify file was created and get size
        if file_path.exists():
            file_size = file_path.stat().st_size
            print(f"✅ Saved: {file_path.name} ({file_size:,} bytes)")
            return True
        else:
            print(f"❌ File not created: {file_path}")
            return False
            
    except Exception as e:
        print(f"❌ Save failed for {file_path.name}: {e}")
        return False


def load_with_cross_platform_compat(file_path):
    """
    Load data with cross-platform path compatibility
    
    Args:
        file_path (Path): File path
        
    Returns:
        Data or None
    """
    try:
        if not file_path.exists():
            print(f"❌ File not found: {file_path}")
            return None
        
        if file_path.suffix == '.pkl':
            with open(file_path, 'rb') as f:
                return pickle.load(f)
        elif file_path.suffix == '.json':
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            print(f"❌ Unsupported file format: {file_path.suffix}")
            return None
            
    except Exception as e:
        print(f"❌ Load failed for {file_path.name}: {e}")
        return None


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


def cache_spreadviewer_data_with_datafetcher(windows_output_dir=r"C:\Users\krajcovic\Documents\Testing Data\ATS_data\test"):
    """
    Cache SpreadViewer data using DataFetcher with cross-platform path support
    
    Args:
        windows_output_dir (str): Windows-style output directory path
        
    Returns:
        dict: Results of data caching
    """
    print("🚀 SPREADVIEWER DATA CACHING WITH DATAFETCHER (WSL/Windows Compatible)")
    print("=" * 90)
    
    # Handle cross-platform paths
    print(f"🔧 Environment Detection:")
    is_wsl = 'microsoft' in platform.uname().release.lower() or 'wsl' in platform.uname().release.lower()
    print(f"   🖥️  Platform: {platform.system()}")
    print(f"   🐧 WSL Detected: {is_wsl}")
    print(f"   📁 Requested Windows Path: {windows_output_dir}")
    
    # Convert to cross-platform path
    output_path = get_cross_platform_path(windows_output_dir)
    output_path = ensure_directory_exists(output_path)
    
    print(f"   ✅ Using Path: {output_path}")
    print(f"   📁 Absolute Path: {output_path.absolute()}")
    
    # SpreadViewer parameters (from test_spreadviewer.py)
    start_date = datetime(2025, 7, 1)
    end_date = datetime(2025, 7, 31)
    market = ['de', 'de']
    tenor = ['m', 'm'] 
    tn1_list = [1]  # M+1 (August 2025)
    tn2_list = [2]  # M+2 (September 2025)
    
    print(f"\n📅 SpreadViewer Parameters:")
    print(f"   📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"   📊 Markets: {market}")
    print(f"   📋 Tenors: {tenor}")
    print(f"   🔢 Contracts: tn1={tn1_list}, tn2={tn2_list}")
    
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
                    # Prepare file path with cross-platform compatibility
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
                            'spreadviewer_compatible': True,
                            'platform': platform.system(),
                            'is_wsl': is_wsl,
                            'windows_path': windows_output_dir,
                            'actual_path': str(output_path)
                        }
                    }
                    
                    # Save with cross-platform compatibility
                    success = save_with_cross_platform_compat(cache_data, cache_path)
                    
                    if success:
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
                            'file_size': cache_path.stat().st_size if cache_path.exists() else 0,
                            'status': 'success'
                        }
                        
                        print(f"✅ {contract['label']}: Cached {trades_count:,} trades, {orders_count:,} orders")
                    else:
                        results[contract['label']] = {
                            'config': contract,
                            'cache_file': None,
                            'status': 'failed',
                            'error': 'Failed to save cache file'
                        }
                        print(f"❌ {contract['label']}: Failed to save cache file")
                    
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
        print("\n" + "=" * 90)
        print("📋 DATA CACHING SUMMARY")
        print("=" * 90)
        
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
        
        # Step 5: Save summary metadata with cross-platform compatibility
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_filename = f"spreadviewer_cache_summary_{timestamp}.json"
        summary_path = output_path / summary_filename
        
        summary_data = {
            'timestamp': datetime.now().isoformat(),
            'platform_info': {
                'system': platform.system(),
                'is_wsl': is_wsl,
                'requested_windows_path': windows_output_dir,
                'actual_path': str(output_path.absolute()),
                'python_version': platform.python_version()
            },
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
        
        # Save summary
        summary_success = save_with_cross_platform_compat(summary_data, summary_path)
        
        if summary_success:
            print(f"\n📋 Summary saved: {summary_filename}")
        
        # List all files in output directory
        print(f"\n📁 Files in output directory ({output_path}):")
        try:
            all_files = list(output_path.iterdir())
            for file_path in sorted(all_files):
                if file_path.is_file():
                    file_size = file_path.stat().st_size
                    print(f"   📄 {file_path.name} ({file_size:,} bytes)")
                else:
                    print(f"   📁 {file_path.name}/")
        except Exception as e:
            print(f"   ❌ Error listing files: {e}")
        
        # Final results
        final_results = {
            'status': 'success' if successful else 'partial' if len(successful) > 0 else 'failed',
            'contracts_processed': len(contracts),
            'successful_contracts': len(successful),
            'failed_contracts': len(failed),
            'output_directory': str(output_path),
            'windows_path': windows_output_dir,
            'cached_files': cached_files,
            'contract_results': results,
            'summary_file': str(summary_path) if summary_success else None,
            'platform_info': {
                'system': platform.system(),
                'is_wsl': is_wsl,
                'python_version': platform.python_version()
            },
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
        print(f"🖥️  Platform: {platform.system()} {'(WSL)' if is_wsl else ''}")
        print(f"📁 Windows Path: {windows_output_dir}")
        print(f"📁 Actual Path: {output_path}")
        print("✅ Ready for SpreadViewer analysis with cached data!")
        print("=" * 90)
        
        return final_results
        
    except Exception as e:
        print(f"\n❌ DATA CACHING FAILED: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'reason': 'exception', 'error': str(e)}


def load_cached_spreadviewer_data(cache_file_path):
    """
    Load cached SpreadViewer data from pickle file with cross-platform support
    
    Args:
        cache_file_path (str or Path): Path to cached pickle file
        
    Returns:
        dict: Cached data or None
    """
    try:
        # Convert to Path object
        if isinstance(cache_file_path, str):
            cache_path = get_cross_platform_path(cache_file_path)
        else:
            cache_path = Path(cache_file_path)
        
        # Load data
        cached_data = load_with_cross_platform_compat(cache_path)
        
        if cached_data:
            print(f"📂 Loaded cached data: {cache_path.name}")
            print(f"   📊 Contract: {cached_data['contract_config']['label']}")
            print(f"   📈 Trades: {len(cached_data['trades']):,}")
            print(f"   📋 Orders: {len(cached_data['orders']):,}")
            print(f"   💾 Cached: {cached_data['metadata']['cached_at']}")
            print(f"   🖥️  Platform: {cached_data['metadata'].get('platform', 'unknown')}")
        
        return cached_data
        
    except Exception as e:
        print(f"❌ Failed to load cached data: {e}")
        return None


def main():
    """
    Main function to run SpreadViewer data caching with DataFetcher
    Cross-platform compatible version
    """
    print("🔗 SpreadViewer → DataFetcher Integration (WSL/Windows Compatible)")
    print("Extracting relative contracts from SpreadViewer and caching with DataFetcher")
    print("=" * 90)
    
    # Run the caching with Windows path (will be converted automatically)
    results = cache_spreadviewer_data_with_datafetcher(
        windows_output_dir=r"C:\Users\krajcovic\Documents\Testing Data\ATS_data\test"
    )
    
    if results.get('status') == 'success':
        print("\n🎉 SUCCESS! SpreadViewer data cached successfully")
        print(f"📊 Processed: {results['successful_contracts']}/{results['contracts_processed']} contracts")
        print(f"🖥️  Platform: {results['platform_info']['system']} {'(WSL)' if results['platform_info']['is_wsl'] else ''}")
        print(f"📁 Windows Path: {results['windows_path']}")
        print(f"📁 Actual Path: {results['output_directory']}")
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