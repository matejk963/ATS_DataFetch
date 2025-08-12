r"""
SpreadViewer + DataFetcher Integration Script
===========================================

This script takes SpreadViewer inputs (relative contracts) and produces
period data using generate_period_data for the absolute contracts that
SpreadViewer would fetch.

Usage:
    python integration_script.py

Output:
    Saves period data to C:/Users/krajcovic/Documents/Testing Data/ATS_data/test/
"""

import sys
import os
from datetime import datetime, time
from typing import Dict, List
import pandas as pd
import numpy as np
import json

# Cross-platform project root
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
    output_base = r'C:\Users\krajcovic\Documents\Testing Data\ATS_data\test'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'
    output_base = '/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test'

sys.path.append(project_root)
sys.path.append(energy_trading_path)

from src.core.data_fetcher import DataFetcher, TPDATA_AVAILABLE

try:
    from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData
    from Database.TPData import TPData
    SPREADVIEWER_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SpreadViewer imports failed: {e}")
    SPREADVIEWER_AVAILABLE = False


def extract_absolute_contracts_from_spreadviewer(spreadviewer_config: Dict) -> List[Dict]:
    """
    Extract the absolute contracts that SpreadViewer would fetch
    based on relative contract inputs
    
    Args:
        spreadviewer_config: SpreadViewer configuration with relative contracts
        
    Returns:
        List of absolute contract configurations for DataFetcher
    """
    if not SPREADVIEWER_AVAILABLE:
        raise ImportError("SpreadViewer not available")
    
    # Extract SpreadViewer parameters
    markets = spreadviewer_config['markets']
    tenors = spreadviewer_config['tenors']
    tn1_list = spreadviewer_config['tn1_list']
    tn2_list = spreadviewer_config.get('tn2_list', [])
    brk_list = spreadviewer_config.get('brk_list', ['eex'])
    n_s = spreadviewer_config.get('n_s', 3)
    
    # Date range for contract calculation
    start_date = datetime.strptime(spreadviewer_config['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(spreadviewer_config['end_date'], '%Y-%m-%d')
    dates = pd.date_range(start_date, end_date, freq='B')
    
    print(f"🔍 Analyzing SpreadViewer configuration:")
    print(f"   Markets: {markets}")
    print(f"   Tenors: {tenors}")
    print(f"   Relative contracts: tn1_list={tn1_list}, tn2_list={tn2_list}")
    print(f"   Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Initialize SpreadViewer class to get absolute contracts
    spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, brk_list)
    
    # ⭐ KEY: Get the absolute contract dates that SpreadViewer calculates
    product_dates = spread_class.product_dates(dates, n_s)
    tenors_list = spread_class.tenors_list
    
    print(f"\n📅 SpreadViewer will fetch these absolute contracts:")
    for tenor_info in tenors_list:
        print(f"   {tenor_info}")
    
    print(f"\n📊 Product dates calculated by SpreadViewer:")
    print(f"   Product dates list length: {len(product_dates)}")
    for i, date_range in enumerate(product_dates):
        print(f"   Contract {i+1}: {date_range}")
    
    # Convert SpreadViewer's internal format to DataFetcher format
    absolute_contracts = []
    
    # Parse tenors_list and product_dates together
    for i, tenor_info in enumerate(tenors_list):
        print(f"\n🔍 Processing tenor: {tenor_info}")
        
        # Get corresponding product dates
        if i < len(product_dates) and product_dates[i] is not None:
            contract_dates = product_dates[i]
            print(f"   Product dates: {contract_dates}")
            
            # Extract market and tenor from tenor_info
            # tenor_info format is like 'm_1', 'm_2' 
            parts = tenor_info.split('_')
            if len(parts) >= 2:
                tenor = parts[0]  # 'm'
                offset = int(parts[1])  # 1, 2
                
                # Get market from the configuration
                # For each tenor, we need to match with markets
                market_idx = i % len(markets)  # Handle multiple markets
                market = markets[market_idx]
                
                print(f"   Parsed: market={market}, tenor={tenor}, offset={offset}")
                
                # Get the first contract date from the product_dates
                if hasattr(contract_dates, '__iter__') and len(contract_dates) > 0:
                    first_contract_date = contract_dates[0]
                    print(f"   First contract date: {first_contract_date}")
                    
                    # Convert to contract string
                    if isinstance(first_contract_date, pd.Timestamp) or isinstance(first_contract_date, datetime):
                        if tenor == 'm':  # Monthly
                            contract_str = f"{first_contract_date.month:02d}_{str(first_contract_date.year)[2:]}"
                        elif tenor == 'q':  # Quarterly
                            quarter = (first_contract_date.month - 1) // 3 + 1
                            contract_str = f"{quarter}_{str(first_contract_date.year)[2:]}"
                        elif tenor == 'y':  # Yearly
                            contract_str = str(first_contract_date.year)[2:]
                        else:
                            contract_str = first_contract_date.strftime('%m_%y')
                    else:
                        contract_str = str(first_contract_date)
                    
                    absolute_contract = {
                        'market': market,
                        'tenor': tenor,
                        'contract': contract_str,
                        'spreadviewer_tenor': tenor_info,
                        'offset': offset,
                        'product_dates': contract_dates
                    }
                    
                    absolute_contracts.append(absolute_contract)
                    print(f"   ✅ Extracted: {market}_{tenor}_{contract_str} (offset {offset})")
    
    return absolute_contracts


def generate_period_data_for_contracts(absolute_contracts: List[Dict], 
                                     output_dir: str,
                                     trading_hours: tuple = (9, 17),
                                     lookback_days: int = 90) -> Dict:
    """
    Generate period data for the absolute contracts extracted from SpreadViewer
    
    Args:
        absolute_contracts: List of absolute contract configurations
        output_dir: Directory to save the data
        trading_hours: Trading hours tuple
        lookback_days: Default lookback period
        
    Returns:
        Dictionary with results for each contract
    """
    if not TPDATA_AVAILABLE:
        raise ImportError("TPData not available - cannot generate period data")
    
    print(f"\n🔄 Generating period data for {len(absolute_contracts)} contracts...")
    
    # Initialize DataFetcher
    fetcher = DataFetcher(
        trading_hours=trading_hours,
        allowed_broker_ids=[1441]  # EEX broker
    )
    
    results = {}
    
    for i, contract in enumerate(absolute_contracts, 1):
        contract_key = f"{contract['market']}_{contract['tenor']}_{contract['contract']}"
        print(f"\n📊 Processing contract {i}/{len(absolute_contracts)}: {contract_key}")
        
        try:
            # Calculate appropriate date range for this contract
            from src.core.data_fetcher import DeliveryDateCalculator, DateRangeResolver
            
            calc = DeliveryDateCalculator()
            resolver = DateRangeResolver()
            
            # Get delivery date
            delivery_date = calc.calc_delivery_date(contract['tenor'], contract['contract'])
            print(f"   📅 Delivery date: {delivery_date.strftime('%Y-%m-%d')}")
            
            # Calculate lookback period
            start_date, end_date = resolver.resolve_date_range(delivery_date, lookback_days)
            print(f"   📈 Data period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # Create contract config for DataFetcher
            contract_config = {
                'market': contract['market'],
                'tenor': contract['tenor'],
                'contract': contract['contract'],
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d')
            }
            
            # Fetch the data
            contract_data = fetcher.fetch_contract_data(
                contract_config,
                include_trades=True,
                include_orders=True
            )
            
            # Store results
            results[contract_key] = {
                'config': contract_config,
                'data': contract_data,
                'spreadviewer_tenor': contract['spreadviewer_tenor'],
                'delivery_date': delivery_date,
                'data_period': (start_date, end_date)
            }
            
            # Save individual contract data
            contract_dir = os.path.join(output_dir, contract_key)
            os.makedirs(contract_dir, exist_ok=True)
            
            # Save each data type
            for data_type, df in contract_data.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    file_path = os.path.join(contract_dir, f"{data_type}.csv")
                    df.to_csv(file_path)
                    print(f"   💾 Saved {data_type}: {len(df)} rows → {file_path}")
                elif isinstance(df, pd.Series) and not df.empty:
                    file_path = os.path.join(contract_dir, f"{data_type}.csv")
                    df.to_csv(file_path)
                    print(f"   💾 Saved {data_type}: {len(df)} values → {file_path}")
            
            # Save metadata
            metadata = {
                'contract_key': contract_key,
                'market': contract['market'],
                'tenor': contract['tenor'],
                'contract': contract['contract'],
                'delivery_date': delivery_date.isoformat(),
                'data_start': start_date.isoformat(),
                'data_end': end_date.isoformat(),
                'spreadviewer_tenor': contract['spreadviewer_tenor'],
                'lookback_days': lookback_days,
                'trading_hours': trading_hours
            }
            
            metadata_path = os.path.join(contract_dir, "metadata.json")
            import json
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2)
            print(f"   📋 Saved metadata → {metadata_path}")
            
            print(f"   ✅ {contract_key} completed successfully")
            
        except Exception as e:
            print(f"   ❌ {contract_key} failed: {e}")
            results[contract_key] = {'error': str(e)}
    
    return results


def generate_synthetic_spread_data(absolute_contracts: List[Dict],
                                 coefficients: List[float],
                                 individual_results: Dict,
                                 output_dir: str) -> Dict:
    """
    Generate synthetic spread data by applying coefficients to individual contracts
    
    Args:
        absolute_contracts: List of absolute contract configurations
        coefficients: Spread coefficients (e.g., [1, -2])
        individual_results: Results from generate_period_data_for_contracts
        output_dir: Directory to save spread data
        
    Returns:
        Dictionary with synthetic spread results
    """
    print(f"🔗 Generating synthetic spread with coefficients: {coefficients}")
    
    if len(absolute_contracts) != len(coefficients):
        raise ValueError(f"Contracts count ({len(absolute_contracts)}) must match coefficients count ({len(coefficients)})")
    
    # Collect individual contract data
    contract_data = {}
    valid_contracts = []
    
    for i, contract in enumerate(absolute_contracts):
        contract_key = f"{contract['market']}_{contract['tenor']}_{contract['contract']}"
        
        if contract_key in individual_results and 'error' not in individual_results[contract_key]:
            result = individual_results[contract_key]
            contract_data[i] = {
                'key': contract_key,
                'contract': contract,
                'coefficient': coefficients[i],
                'data': result['data']
            }
            valid_contracts.append(contract)
            print(f"   ✅ Contract {i+1}: {contract_key} (coeff: {coefficients[i]})")
        else:
            print(f"   ❌ Contract {i+1}: {contract_key} - no valid data")
    
    if len(valid_contracts) < 2:
        print("⚠️  Need at least 2 valid contracts for spread generation")
        return {}
    
    # Find common time intersection
    print(f"\n🕐 Finding common time periods...")
    all_indices = []
    
    for i, contract_info in contract_data.items():
        orders_data = contract_info['data'].get('orders', pd.DataFrame())
        if not orders_data.empty:
            all_indices.append(orders_data.index)
            print(f"   Contract {contract_info['key']}: {len(orders_data)} order points")
    
    if not all_indices:
        print("❌ No order data available for spread generation")
        return {}
    
    # Get intersection of all time indices
    common_index = all_indices[0]
    for idx in all_indices[1:]:
        common_index = common_index.intersection(idx)
    
    print(f"   📊 Common time points: {len(common_index)}")
    
    if len(common_index) == 0:
        print("❌ No common time points between contracts")
        return {}
    
    # Generate synthetic spread order book
    print(f"\n📈 Generating synthetic spread order book...")
    
    spread_bid = pd.Series(0.0, index=common_index)
    spread_ask = pd.Series(0.0, index=common_index)
    
    for i, contract_info in contract_data.items():
        orders_data = contract_info['data']['orders']
        coeff = contract_info['coefficient']
        
        # Align to common index
        orders_aligned = orders_data.reindex(common_index)
        
        if coeff > 0:  # Buy this contract
            # For buying: pay ask price, receive bid price
            spread_bid += coeff * orders_aligned.get('b_price', 0)  # What we can sell the spread for
            spread_ask += coeff * orders_aligned.get('a_price', 0)  # What we pay to buy the spread
        else:  # Sell this contract (negative coefficient)
            # For selling: receive bid price, pay ask price  
            spread_bid += coeff * orders_aligned.get('a_price', 0)  # Negative * ask = negative contribution
            spread_ask += coeff * orders_aligned.get('b_price', 0)  # Negative * bid = negative contribution
        
        print(f"   🔢 Applied coefficient {coeff} to {contract_info['key']}")
    
    # Create spread DataFrame
    spread_orders = pd.DataFrame({
        'spread_bid': spread_bid,
        'spread_ask': spread_ask,
        'spread_mid': (spread_bid + spread_ask) / 2
    }).dropna()
    
    print(f"   ✅ Generated {len(spread_orders)} spread order points")
    print(f"   📊 Spread range: {spread_orders['spread_mid'].min():.3f} to {spread_orders['spread_mid'].max():.3f}")
    print(f"   📈 Spread mean: {spread_orders['spread_mid'].mean():.3f}")
    
    # Generate synthetic spread trades (simplified)
    print(f"\n💹 Generating synthetic spread trades...")
    
    spread_trades = pd.DataFrame(index=common_index[:100])  # Limit for demo
    spread_trades['buy_price'] = spread_orders['spread_ask'][:100]  # Price to buy spread
    spread_trades['sell_price'] = spread_orders['spread_bid'][:100]  # Price to sell spread
    spread_trades['spread_value'] = spread_orders['spread_mid'][:100]
    spread_trades = spread_trades.dropna()
    
    print(f"   ✅ Generated {len(spread_trades)} synthetic trade opportunities")
    
    # Save spread data
    spread_dir = os.path.join(output_dir, 'synthetic_spread')
    os.makedirs(spread_dir, exist_ok=True)
    
    # Save spread orders (market data)
    spread_orders_file = os.path.join(spread_dir, 'spread_orders.csv')
    spread_orders.to_csv(spread_orders_file)
    print(f"   💾 Saved spread orders: {spread_orders_file}")
    
    # Save spread trades
    spread_trades_file = os.path.join(spread_dir, 'spread_trades.csv')
    spread_trades.to_csv(spread_trades_file)
    print(f"   💾 Saved spread trades: {spread_trades_file}")
    
    # Save spread configuration
    spread_config = {
        'contracts': [c['spreadviewer_tenor'] for c in valid_contracts],
        'coefficients': coefficients,
        'absolute_contracts': [f"{c['market']}_{c['tenor']}_{c['contract']}" for c in valid_contracts],
        'spread_type': 'synthetic',
        'generation_time': datetime.now().isoformat(),
        'common_time_points': len(common_index),
        'spread_statistics': {
            'min': float(spread_orders['spread_mid'].min()),
            'max': float(spread_orders['spread_mid'].max()),
            'mean': float(spread_orders['spread_mid'].mean()),
            'std': float(spread_orders['spread_mid'].std())
        }
    }
    
    config_file = os.path.join(spread_dir, 'spread_config.json')
    with open(config_file, 'w') as f:
        json.dump(spread_config, f, indent=2)
    print(f"   📋 Saved spread config: {config_file}")
    
    return {
        'spread_orders': spread_orders,
        'spread_trades': spread_trades,
        'spread_config': spread_config,
        'valid_contracts': valid_contracts
    }


def generate_spread_parquet_files(absolute_contracts: List[Dict],
                                 individual_results: Dict,
                                 spreadviewer_config: Dict,
                                 spread_results: Dict,
                                 output_dir: str) -> Dict:
    """
    Generate concise spread parquet files with computed spread data only
    
    Creates spread.parquet and spread_metadata.parquet files with concise naming
    
    Args:
        absolute_contracts: List of absolute contract configurations
        individual_results: Results from generate_period_data_for_contracts
        spreadviewer_config: Original SpreadViewer configuration
        spread_results: Results from generate_synthetic_spread_data
        output_dir: Directory to save parquet files
        
    Returns:
        Dictionary with parquet generation results
    """
    print(f"📦 Generating concise spread parquet files...")
    
    if not spread_results or 'spread_orders' not in spread_results:
        print("❌ No spread data available")
        return {}
    
    # Create parquet subdirectory
    parquet_dir = os.path.join(output_dir, 'parquet_files')
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Generate concise spread filename: LEG1-LEG2_spread.parquet
    if len(absolute_contracts) >= 2:
        leg1 = absolute_contracts[0]
        leg2 = absolute_contracts[1]
        
        # Format: DEM0725-DEM0825_spread.parquet
        leg1_name = f"{leg1['market'].upper()}{leg1['tenor'].upper()}{leg1['contract'].replace('_', '')}"
        leg2_name = f"{leg2['market'].upper()}{leg2['tenor'].upper()}{leg2['contract'].replace('_', '')}"
        
        spread_base_name = f"{leg1_name}-{leg2_name}_spread"
        
        print(f"   📊 Creating spread files: {spread_base_name}")
        
        # Main spread data file
        spread_data = spread_results['spread_orders']
        spread_filename = f"{spread_base_name}.parquet"
        spread_path = os.path.join(parquet_dir, spread_filename)
        
        try:
            spread_data.to_parquet(spread_path)
            spread_size = os.path.getsize(spread_path) / (1024*1024)  # MB
            print(f"   💾 Saved spread data: {spread_filename} ({spread_size:.1f} MB)")
            print(f"      📈 Spread points: {len(spread_data)}")
            print(f"      📊 Columns: {list(spread_data.columns)}")
        except Exception as e:
            print(f"   ❌ Failed to save {spread_filename}: {e}")
            return {}
        
        # Metadata file
        metadata = {
            'spread_name': spread_base_name,
            'leg1_contract': f"{leg1['market']}_{leg1['tenor']}_{leg1['contract']}",
            'leg2_contract': f"{leg2['market']}_{leg2['tenor']}_{leg2['contract']}",
            'leg1_delivery': leg1.get('product_dates', 'N/A'),
            'leg2_delivery': leg2.get('product_dates', 'N/A'),
            'coefficients': spreadviewer_config.get('coefficients', [1, -1]),
            'markets': spreadviewer_config.get('markets', []),
            'tenors': spreadviewer_config.get('tenors', []),
            'date_range': f"{spreadviewer_config.get('start_date')} to {spreadviewer_config.get('end_date')}",
            'generation_time': datetime.now().isoformat(),
            'spread_statistics': spread_results.get('spread_config', {}).get('spread_statistics', {})
        }
        
        # Convert metadata to DataFrame for parquet format
        metadata_df = pd.DataFrame([metadata])
        metadata_filename = f"{spread_base_name}_metadata.parquet"
        metadata_path = os.path.join(parquet_dir, metadata_filename)
        
        try:
            metadata_df.to_parquet(metadata_path)
            metadata_size = os.path.getsize(metadata_path) / 1024  # KB
            print(f"   💾 Saved spread metadata: {metadata_filename} ({metadata_size:.1f} KB)")
        except Exception as e:
            print(f"   ❌ Failed to save {metadata_filename}: {e}")
        
        print(f"\n✅ Generated concise spread files:")
        print(f"   📈 {spread_filename} - Computed spread data ({len(spread_data)} points)")
        print(f"   📋 {metadata_filename} - Spread configuration")
        
        return {
            'spread_file': spread_path,
            'metadata_file': metadata_path,
            'spread_name': spread_base_name,
            'parquet_dir': parquet_dir,
            'data_points': len(spread_data)
        }
    
    else:
        print("❌ Need at least 2 contracts for spread generation")
        return {}


def map_relative_to_absolute_contracts(spread_config: Dict) -> List[str]:
    """
    Map relative contract offsets to absolute contract strings based on data date range
    
    Args:
        spread_config: Single spread configuration with markets, tn1_list, and date range
        
    Returns:
        List of absolute contract strings (e.g., ['dem07_25', 'dem08_25'])
    """
    from datetime import datetime, timedelta
    import dateutil.relativedelta as rd
    
    start_date = datetime.strptime(spread_config['start_date'], '%Y-%m-%d')
    
    absolute_contracts = []
    
    # For each relative offset in tn1_list
    for offset in spread_config['tn1_list'][0]:  # [1, 2] for M1/M2
        # Calculate absolute contract month (M1 = start_date + 1 month, M2 = start_date + 2 months, etc.)
        contract_date = start_date + rd.relativedelta(months=offset)
        
        # Format as absolute contract
        market = spread_config['markets'][0][0]  # 'de'
        contract_str = f"{market}{contract_date.month:02d}_{str(contract_date.year)[2:]}"
        absolute_contracts.append(contract_str)
    
    return absolute_contracts


def generate_multi_spread_data(config: Dict, output_dir: str) -> Dict:
    """
    Generate data for multiple spread configurations and merge overlapping absolute contracts
    
    Args:
        config: Configuration with multiple spread_configs
        output_dir: Output directory
        
    Returns:
        Dict with results including merged data for overlapping absolute contracts
    """
    print(f"🔗 MULTI-SPREAD MODE: Processing {len(config['spread_configs'])} spread configurations...")
    
    # Step 1: Map each spread config to absolute contracts
    spread_mappings = {}
    absolute_contract_groups = {}
    
    for i, spread_config in enumerate(config['spread_configs']):
        print(f"\n📊 Spread {i+1}: {spread_config['name']}")
        print(f"   Relative: {spread_config['tn1_list'][0]} (M{spread_config['tn1_list'][0][0]}/M{spread_config['tn1_list'][0][1]})")
        print(f"   Date range: {spread_config['start_date']} to {spread_config['end_date']}")
        
        # Map to absolute contracts
        absolute_contracts = map_relative_to_absolute_contracts(spread_config)
        spread_key = f"{absolute_contracts[0]}-{absolute_contracts[1]}"
        
        print(f"   Absolute: {absolute_contracts} → {spread_key}")
        
        # Store mapping
        spread_mappings[spread_config['name']] = {
            'config': spread_config,
            'absolute_contracts': absolute_contracts,
            'spread_key': spread_key
        }
        
        # Group by absolute contract pairs
        if spread_key not in absolute_contract_groups:
            absolute_contract_groups[spread_key] = []
        absolute_contract_groups[spread_key].append(spread_config['name'])
    
    # Step 2: Show grouping results
    print(f"\n🎯 ABSOLUTE CONTRACT GROUPING:")
    for spread_key, spread_names in absolute_contract_groups.items():
        print(f"   {spread_key} ← {spread_names}")
        if len(spread_names) > 1:
            print(f"      🔗 OVERLAPPING: Will merge {len(spread_names)} spreads")
    
    # Step 3: Generate data for each spread configuration
    all_spread_results = {}
    
    for spread_name, mapping in spread_mappings.items():
        print(f"\n📈 Generating data for {spread_name}...")
        
        # Create temporary single-spread config for existing function
        single_config = {
            'mode': 'spread',
            'markets': mapping['config']['markets'][0],  # ['de', 'de']
            'tenors': mapping['config']['tenors'][0],    # ['m', 'm']
            'tn1_list': mapping['config']['tn1_list'][0], # [1, 2]
            'coefficients': mapping['config']['coefficients'][0], # [1, -1]
            'brk_list': config['brk_list'],
            'n_s': config['n_s'],
            'add_trades': config['add_trades'],
            'start_date': mapping['config']['start_date'],
            'end_date': mapping['config']['end_date']
        }
        
        # Generate data using existing function
        spread_results = generate_spreadviewer_data(single_config, output_dir)
        all_spread_results[spread_name] = {
            'spread_key': mapping['spread_key'],
            'absolute_contracts': mapping['absolute_contracts'],
            'data': spread_results
        }
    
    # Step 4: Merge overlapping spreads
    merged_results = {}
    
    if config.get('merge_overlapping', False):
        print(f"\n🔗 MERGING OVERLAPPING SPREADS:")
        
        for spread_key, spread_names in absolute_contract_groups.items():
            if len(spread_names) == 1:
                # No overlap - just copy
                spread_name = spread_names[0]
                merged_results[spread_key] = all_spread_results[spread_name]['data']
                print(f"   {spread_key}: Single spread ({spread_name}) - no merge needed")
            else:
                # Multiple spreads - merge them
                print(f"   {spread_key}: Merging {len(spread_names)} spreads...")
                
                merged_orders = pd.DataFrame()
                merged_trades = pd.DataFrame()
                
                for spread_name in spread_names:
                    spread_data = all_spread_results[spread_name]['data']
                    
                    if 'spread_orders' in spread_data and not spread_data['spread_orders'].empty:
                        merged_orders = pd.concat([merged_orders, spread_data['spread_orders']], axis=0)
                        print(f"      Added orders from {spread_name}: {len(spread_data['spread_orders'])} rows")
                    
                    if 'spread_trades' in spread_data and not spread_data['spread_trades'].empty:
                        merged_trades = pd.concat([merged_trades, spread_data['spread_trades']], axis=0)
                        print(f"      Added trades from {spread_name}: {len(spread_data['spread_trades'])} rows")
                
                # Sort by timestamp and remove duplicates
                if not merged_orders.empty:
                    merged_orders = merged_orders.sort_index().drop_duplicates()
                if not merged_trades.empty:
                    merged_trades = merged_trades.sort_index().drop_duplicates()
                
                merged_results[spread_key] = {
                    'spread_orders': merged_orders,
                    'spread_trades': merged_trades
                }
                
                print(f"      ✅ Merged result: {len(merged_orders)} orders, {len(merged_trades)} trades")
                print(f"      📅 Combined period: {merged_orders.index.min()} to {merged_orders.index.max()}")
    else:
        # No merging - keep separate
        for spread_name, results in all_spread_results.items():
            merged_results[results['spread_key']] = results['data']
    
    return {
        'spread_mappings': spread_mappings,
        'absolute_contract_groups': absolute_contract_groups,
        'individual_results': all_spread_results,
        'merged_results': merged_results
    }


def generate_merged_spread_parquet(spread_key: str, merged_data: Dict, output_dir: str) -> Dict:
    """
    Generate parquet files for merged spread data
    
    Args:
        spread_key: Absolute contract pair key (e.g., 'dem07_25-dem08_25')
        merged_data: Dict containing merged spread_orders and spread_trades
        output_dir: Output directory
        
    Returns:
        Dict with parquet file paths and metadata
    """
    print(f"📦 Generating parquet files for merged spread: {spread_key}")
    
    parquet_dir = os.path.join(output_dir, 'parquet_files')
    os.makedirs(parquet_dir, exist_ok=True)
    
    results = {}
    
    # Generate orders parquet
    if 'spread_orders' in merged_data and not merged_data['spread_orders'].empty:
        orders_df = merged_data['spread_orders']
        orders_file = f"{spread_key.upper()}_orders_merged.parquet"
        orders_path = os.path.join(parquet_dir, orders_file)
        
        try:
            orders_df.to_parquet(orders_path, engine='pyarrow', compression='snappy')
            print(f"   ✅ Orders parquet: {orders_file} ({len(orders_df):,} rows)")
            results['orders_parquet'] = orders_path
        except Exception as e:
            print(f"   ❌ Orders parquet failed: {e}")
            results['orders_error'] = str(e)
    
    # Generate trades parquet
    if 'spread_trades' in merged_data and not merged_data['spread_trades'].empty:
        trades_df = merged_data['spread_trades']
        trades_file = f"{spread_key.upper()}_trades_merged.parquet"
        trades_path = os.path.join(parquet_dir, trades_file)
        
        try:
            trades_df.to_parquet(trades_path, engine='pyarrow', compression='snappy')
            print(f"   ✅ Trades parquet: {trades_file} ({len(trades_df):,} rows)")
            results['trades_parquet'] = trades_path
        except Exception as e:
            print(f"   ❌ Trades parquet failed: {e}")
            results['trades_error'] = str(e)
    
    # Generate combined merged file (like the fix_merge_orders_trades.py output)
    if 'spread_orders' in merged_data and 'spread_trades' in merged_data:
        print(f"   🔗 Creating combined merged file...")
        
        try:
            # Create combined file using same logic as fix_merge_orders_trades.py
            orders_df = merged_data['spread_orders'].rename(columns={'bid': 'b_price', 'ask': 'a_price'})
            orders_df['0'] = (orders_df['b_price'] + orders_df['a_price']) / 2
            
            trades_df = merged_data['spread_trades']
            
            # Process trades with action logic
            trades_processed = pd.DataFrame(index=trades_df.index)
            trades_processed['price'] = np.where(
                trades_df['buy'].notna(), 
                trades_df['buy'],
                trades_df['sell']
            )
            trades_processed['action'] = np.where(
                trades_df['buy'].notna() & trades_df['sell'].isna(), 1.0,
                np.where(trades_df['sell'].notna() & trades_df['buy'].isna(), -1.0,
                         np.where((trades_df['buy'].notna()) & (trades_df['sell'].notna()), 1.0, np.nan))
            )
            
            # Add trade metadata
            trades_processed['volume'] = 1
            trades_processed['broker_id'] = 1441.0
            trades_processed['count'] = 1
            trades_processed['tradeid'] = [f"merged_trade_{i:06d}" for i in range(len(trades_processed))]
            trades_processed = trades_processed.dropna(subset=['price', 'action'])
            
            # Create union and merge
            all_timestamps = orders_df.index.union(trades_processed.index)
            orders_aligned = orders_df.reindex(all_timestamps).ffill()
            
            merged_df = pd.DataFrame(index=all_timestamps)
            for col in ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid']:
                merged_df[col] = trades_processed.reindex(all_timestamps)[col]
            for col in ['b_price', 'a_price', '0']:
                merged_df[col] = orders_aligned[col]
            
            # Set column order and clean
            reference_columns = ['price', 'volume', 'action', 'broker_id', 'count', 'tradeid', 'b_price', 'a_price', '0']
            merged_df = merged_df[reference_columns]
            merged_df = merged_df.dropna(subset=['b_price', 'a_price', '0'])
            
            # Save combined file
            combined_file = f"{spread_key}_spread_tr_ba_data_merged.parquet"
            combined_path = os.path.join(parquet_dir, combined_file)
            merged_df.to_parquet(combined_path, engine='pyarrow', compression='snappy')
            
            print(f"   ✅ Combined file: {combined_file} ({merged_df.shape[0]:,} rows)")
            print(f"      Trade rows: {merged_df['price'].notna().sum():,}")
            print(f"      Order-only rows: {merged_df['price'].isna().sum():,}")
            print(f"      📅 Period: {merged_df.index.min()} to {merged_df.index.max()}")
            
            results['combined_parquet'] = combined_path
            results['combined_stats'] = {
                'total_rows': len(merged_df),
                'trade_rows': merged_df['price'].notna().sum(),
                'order_rows': merged_df['price'].isna().sum(),
                'period_start': merged_df.index.min(),
                'period_end': merged_df.index.max()
            }
            
        except Exception as e:
            print(f"   ❌ Combined file failed: {e}")
            results['combined_error'] = str(e)
    
    return results


def generate_spreadviewer_data(config: Dict, output_dir: str) -> Dict:
    """
    Generate spread data using SpreadViewer directly with relative inputs
    
    Args:
        config: Spread configuration with relative contracts
        output_dir: Output directory
        
    Returns:
        Dictionary with spread results including both orders and trades
    """
    if not SPREADVIEWER_AVAILABLE:
        raise ImportError("SpreadViewer not available")
    
    # Extract parameters
    markets = config['markets']
    tenors = config['tenors']
    tn1_list = config['tn1_list']
    tn2_list = config.get('tn2_list', [])
    brk_list = config.get('brk_list', ['eex'])
    n_s = config.get('n_s', 3)
    coefficients = config.get('coefficients', [1, -1])
    add_trades = config.get('add_trades', True)
    
    start_date = datetime.strptime(config['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(config['end_date'], '%Y-%m-%d')
    dates = pd.date_range(start_date, end_date, freq='B')
    
    # SpreadViewer parameters (from test_spreadviewer.py)
    start_time = time(9, 0, 0, 0)
    end_time = time(17, 25, 0, 0)
    gran = None
    gran_s = '1s'  # 1-second granularity for trades
    mm_bool = [True, True]  # Market maker flags for both legs
    
    print(f"🔄 Using SpreadViewer with relative inputs:")
    print(f"   Markets: {markets}, Tenors: {tenors}")
    print(f"   Relative contracts: tn1={tn1_list}, tn2={tn2_list}")
    print(f"   Coefficients: {coefficients}")
    print(f"   Include trades: {add_trades}")
    
    # Initialize SpreadViewer classes
    spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, brk_list)
    data_class = SpreadViewerData()
    db_class = TPData()
    tenors_list = spread_class.tenors_list
    
    # Step 1: Load order book data
    print("📡 Loading order book data via SpreadViewer...")
    data_class.load_best_order_otc(markets, tenors_list,
                                   spread_class.product_dates(dates, n_s),
                                   db_class,
                                   start_time=start_time, end_time=end_time)
    print("✅ Order book data loaded")
    
    # Step 2: Load trade data (MISSING PART!)
    data_class_tr = None
    if add_trades:
        print("💹 Loading trade data via SpreadViewer...")
        data_class_tr = SpreadViewerData()
        data_class_tr.load_trades_otc(markets, tenors_list, db_class,
                                      start_time=start_time, end_time=end_time)
        print("✅ Trade data loaded")
    
    # Step 3: Process daily data (both orders and trades)
    print("📈 Generating spread data via SpreadViewer...")
    sm_all = pd.DataFrame([])  # Spread orders (bid/ask)
    tm_all = pd.DataFrame([])  # Spread trades
    
    for i, d in enumerate(dates):
        print(f"🗓️  Processing day {i+1}/{len(dates)}: {d.strftime('%Y-%m-%d')}")
        d_range = pd.date_range(d, d)
        
        # Generate spread order book for this day
        data_dict = spread_class.aggregate_data(data_class, d_range, n_s, gran=gran,
                                               start_time=start_time, end_time=end_time)
        
        sm = spread_class.spread_maker(data_dict, coefficients, trade_type=['cmb', 'cmb']).dropna()
        print(f"   📊 Spread orders: {len(sm)} points")
        
        # Accumulate spread order data
        sm_all = pd.concat([sm_all, sm], axis=0)
        
        # Generate spread trades for this day (MISSING PART!)
        if add_trades and not sm.empty and data_class_tr is not None:
            col_list = ['bid', 'ask', 'volume', 'broker_id']
            
            # Aggregate trade data
            trade_dict = spread_class.aggregate_data(data_class_tr, d_range, n_s, gran=gran_s,
                                                     start_time=start_time, end_time=end_time,
                                                     col_list=col_list, data_dict=data_dict)
            
            # Add trades to spread
            tm = spread_class.add_trades(data_dict, trade_dict, coefficients, mm_bool)
            print(f"   💹 Spread trades: {len(tm)} trades")
            
            # Accumulate spread trade data
            tm_all = pd.concat([tm_all, tm], axis=0)
    
    print(f"✅ SpreadViewer generated:")
    print(f"   📊 Spread orders: {len(sm_all)} points")
    print(f"   💹 Spread trades: {len(tm_all)} trades")
    
    return {
        'spread_orders': sm_all,  # Order book data (bid/ask)
        'spread_trades': tm_all,  # Trade data 
        'spread_class': spread_class,
        'coefficients': coefficients,
        'absolute_contracts': spread_class.tenors_list,
        'parameters': {
            'markets': markets,
            'tenors': tenors,
            'tn1_list': tn1_list,
            'tn2_list': tn2_list,
            'coefficients': coefficients,
            'add_trades': add_trades,
            'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        }
    }


def generate_individual_contract_data(config: Dict, output_dir: str) -> Dict:
    """
    Generate individual contract data using DataFetcher with absolute inputs
    
    Args:
        config: Individual contract configuration with absolute contracts
        output_dir: Output directory
        
    Returns:
        Dictionary with individual contract results
    """
    if not TPDATA_AVAILABLE:
        raise ImportError("TPData not available - cannot fetch data")
    
    print(f"🔄 Using DataFetcher with absolute contracts:")
    
    # Initialize DataFetcher
    fetcher = DataFetcher(
        trading_hours=(9, 17),
        allowed_broker_ids=[1441]  # EEX broker
    )
    
    results = {}
    
    for i, contract in enumerate(config['contracts'], 1):
        contract_key = f"{contract['market']}_{contract['tenor']}_{contract['contract']}"
        print(f"📊 Processing contract {i}/{len(config['contracts'])}: {contract_key}")
        
        try:
            # Create contract config for DataFetcher
            contract_config = {
                'market': contract['market'],
                'tenor': contract['tenor'],
                'contract': contract['contract'],
                'start_date': config['start_date'],
                'end_date': config['end_date']
            }
            
            # Fetch the data
            contract_data = fetcher.fetch_contract_data(
                contract_config,
                include_trades=True,
                include_orders=True
            )
            
            results[contract_key] = {
                'config': contract_config,
                'data': contract_data
            }
            
            print(f"   ✅ {contract_key}: Fetched data successfully")
            
        except Exception as e:
            print(f"   ❌ {contract_key} failed: {e}")
            results[contract_key] = {'error': str(e)}
    
    return results


def generate_spread_parquet_from_spreadviewer(config: Dict, spread_results: Dict, output_dir: str) -> Dict:
    """
    Generate spread parquet files from SpreadViewer results (both orders and trades)
    """
    print("📦 Generating spread parquet from SpreadViewer data...")
    
    if 'spread_orders' not in spread_results:
        print("❌ No spread orders from SpreadViewer")
        return {}
    
    parquet_dir = os.path.join(output_dir, 'parquet_files')
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Generate filename from absolute contracts that SpreadViewer used
    absolute_contracts = spread_results['absolute_contracts']
    if len(absolute_contracts) >= 2:
        # Convert tenor_list format to readable contracts
        leg1_str = absolute_contracts[0].replace('_', '')  # e.g., 'm_1' -> 'm1'
        leg2_str = absolute_contracts[1].replace('_', '')  # e.g., 'm_2' -> 'm2'
        
        spread_base_name = f"DE{leg1_str.upper()}-DE{leg2_str.upper()}"
        
        saved_files = []
        
        # Save spread orders (bid/ask data)
        spread_orders = spread_results['spread_orders']
        if not spread_orders.empty:
            orders_file = os.path.join(parquet_dir, f"{spread_base_name}_orders.parquet")
            spread_orders.to_parquet(orders_file)
            print(f"💾 Saved orders: {spread_base_name}_orders.parquet ({len(spread_orders)} points)")
            saved_files.append(orders_file)
        
        # Save spread trades
        spread_trades = spread_results.get('spread_trades', pd.DataFrame())
        if not spread_trades.empty:
            trades_file = os.path.join(parquet_dir, f"{spread_base_name}_trades.parquet")
            spread_trades.to_parquet(trades_file)
            print(f"💾 Saved trades: {spread_base_name}_trades.parquet ({len(spread_trades)} trades)")
            saved_files.append(trades_file)
        else:
            print("⚠️  No spread trades generated")
        
        # Save combined data (orders + trades) for backward compatibility
        if not spread_orders.empty:
            # Use orders as the main data (this is the spread bid/ask prices)
            combined_file = os.path.join(parquet_dir, f"{spread_base_name}_spread.parquet")
            spread_orders.to_parquet(combined_file)
            print(f"💾 Saved combined: {spread_base_name}_spread.parquet (main spread data)")
            saved_files.append(combined_file)
        
        # Save metadata
        metadata = {
            'spread_name': spread_base_name,
            'orders_count': len(spread_orders),
            'trades_count': len(spread_trades),
            'parameters': spread_results.get('parameters', {}),
            'absolute_contracts': absolute_contracts,
            'generation_time': datetime.now().isoformat()
        }
        
        metadata_df = pd.DataFrame([metadata])
        metadata_file = os.path.join(parquet_dir, f"{spread_base_name}_metadata.parquet")
        metadata_df.to_parquet(metadata_file)
        print(f"💾 Saved metadata: {spread_base_name}_metadata.parquet")
        saved_files.append(metadata_file)
        
        return {
            'spread_name': spread_base_name,
            'saved_files': saved_files,
            'orders_count': len(spread_orders),
            'trades_count': len(spread_trades)
        }
    
    return {}


def generate_individual_parquet_files(config: Dict, results: Dict, output_dir: str) -> Dict:
    """
    Generate individual contract parquet files from DataFetcher results
    """
    print("📦 Generating individual contract parquet files...")
    
    parquet_dir = os.path.join(output_dir, 'parquet_files')
    os.makedirs(parquet_dir, exist_ok=True)
    
    parquet_files = []
    
    for contract_key, result in results.items():
        if 'error' in result:
            continue
        
        # Generate individual parquet for each contract
        contract_data = result['data']
        if 'orders' in contract_data and not contract_data['orders'].empty:
            filename = f"{contract_key.replace('_', '').upper()}_data.parquet"
            file_path = os.path.join(parquet_dir, filename)
            contract_data['orders'].to_parquet(file_path)
            parquet_files.append(filename)
            print(f"💾 Saved: {filename}")
    
    return {'parquet_files': parquet_files}


def main():
    """
    Main integration script - supports both individual and spread modes
    """
    print("🚀 SpreadViewer + DataFetcher Integration Script")
    print("=" * 60)
    
    # Check dependencies  
    if not SPREADVIEWER_AVAILABLE:
        print("❌ SpreadViewer not available - cannot run integration")
        return False
    
    if not TPDATA_AVAILABLE:
        print("❌ TPData not available - cannot fetch data")
        return False
    
    # ⭐ INTEGRATION MODE CONFIGURATION ⭐
    # Set mode: 'individual' or 'spread'
    mode = 'spread'  # Change to 'individual' for DataFetcher mode
    
    if mode == 'spread':
        # ENHANCED: Multiple SpreadViewer configurations with overlapping absolute contracts
        config = {
            'mode': 'spread',
            'spread_configs': [
                {
                    'name': 'M1M2_Extended',
                    'markets': [['de', 'de']],         # German M1/M2 spread
                    'tenors': [['m', 'm']],
                    'tn1_list': [[1, 2]],              # M1/M2 relative offsets
                    'coefficients': [[1, -1]],         # Buy M1, Sell M2
                    'start_date': '2025-06-01',        # June to July data
                    'end_date': '2025-07-31'
                },
                {
                    'name': 'M2M3_Extended',
                    'markets': [['de', 'de']],         # German M2/M3 spread
                    'tenors': [['m', 'm']],
                    'tn1_list': [[2, 3]],              # M2/M3 relative offsets
                    'coefficients': [[1, -1]],         # Buy M2, Sell M3
                    'start_date': '2025-06-01',        # June to July data
                    'end_date': '2025-07-31'           # (M2/M3 = different absolute contracts by month)
                }
            ],
            'brk_list': ['eex'],                       # EEX exchange
            'n_s': 3,                                  # SpreadViewer parameter
            'add_trades': True,                        # Include trade data generation
            'merge_overlapping': True                   # Merge spreads with same absolute contracts
        }
    else:
        # DataFetcher configuration with absolute contracts
        config = {
            'mode': 'individual',
            'contracts': [
                {'market': 'de', 'tenor': 'm', 'contract': '07_25'},  # Absolute contract
                {'market': 'de', 'tenor': 'm', 'contract': '08_25'}   # Absolute contract
            ],
            'start_date': '2025-06-01',
            'end_date': '2025-07-15'
        }
    
    print(f"📋 Configuration Mode: {config['mode'].upper()}")
    if config['mode'] == 'spread':
        if 'spread_configs' in config:
            print(f"   Multi-Spread Mode: {len(config['spread_configs'])} configurations")
            for i, spread_config in enumerate(config['spread_configs']):
                print(f"     {i+1}. {spread_config['name']}: {spread_config['tn1_list'][0]} ({spread_config['start_date']} to {spread_config['end_date']})")
            print(f"   Merge overlapping: {config.get('merge_overlapping', False)}")
        else:
            print(f"   Markets: {config['markets']}")
            print(f"   Tenors: {config['tenors']}")
            print(f"   Relative contracts: {config['tn1_list']}")
            print(f"   Coefficients: {config['coefficients']}")
            print(f"   Period: {config['start_date']} to {config['end_date']}")
    else:
        print(f"   Absolute contracts: {len(config['contracts'])}")
        for i, contract in enumerate(config['contracts']):
            print(f"     {i+1}: {contract['market']}_{contract['tenor']}_{contract['contract']}")
        print(f"   Period: {config['start_date']} to {config['end_date']}")
    
    try:
        output_dir = output_base
        print(f"\n📁 Output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        
        if config['mode'] == 'spread':
            # Check if we have multiple spread configurations
            if 'spread_configs' in config:
                # MULTI-SPREAD MODE: Handle multiple spreads with overlapping absolute contracts
                print(f"\n🔍 MULTI-SPREAD MODE: Processing multiple spread configurations...")
                multi_spread_results = generate_multi_spread_data(config, output_dir)
                
                # Generate parquet files for each merged spread
                print(f"\n📦 Generating merged spread parquet files...")
                parquet_results = {}
                
                for spread_key, merged_data in multi_spread_results['merged_results'].items():
                    print(f"\n📊 Processing merged spread: {spread_key}")
                    
                    # Create temporary config for parquet generation
                    temp_config = {
                        'mode': 'spread',
                        'spread_key': spread_key,
                        'merge_overlapping': True
                    }
                    
                    # Generate parquet files for this merged spread
                    spread_parquet_results = generate_merged_spread_parquet(
                        spread_key=spread_key,
                        merged_data=merged_data,
                        output_dir=output_dir
                    )
                    parquet_results[spread_key] = spread_parquet_results
                    
                spread_results = multi_spread_results  # For summary reporting
                    
            else:
                # SINGLE SPREAD MODE: Use SpreadViewer directly with relative inputs
                print(f"\n🔍 SINGLE SPREAD MODE: Using SpreadViewer with relative contracts...")
                spread_results = generate_spreadviewer_data(config, output_dir)
                
                # Generate concise spread parquet files
                print(f"\n📦 Generating concise spread parquet files...")
                parquet_results = generate_spread_parquet_from_spreadviewer(
                    config=config,
                    spread_results=spread_results,
                    output_dir=output_dir
                )
            
        else:
            # INDIVIDUAL MODE: Use DataFetcher with absolute contracts
            print(f"\n🔍 INDIVIDUAL MODE: Using DataFetcher with absolute contracts...")
            results = generate_individual_contract_data(config, output_dir)
            
            # Generate individual parquet files
            print(f"\n📦 Generating individual contract parquet files...")
            parquet_results = generate_individual_parquet_files(
                config=config,
                results=results,
                output_dir=output_dir
            )
        
        # Step 3: Generate summary report
        print(f"\n📊 Step 3: Integration Summary")
        print("=" * 50)
        
        if config['mode'] == 'spread':
            # Spread mode summary
            if 'spread_results' in locals() and spread_results:
                if 'spread_configs' in config:
                    # Multi-spread mode summary
                    merged_results = spread_results.get('merged_results', {})
                    print(f"✅ Multi-Spread Generation Successful")
                    print(f"   📊 Total merged spreads: {len(merged_results)}")
                    for spread_key, data in merged_results.items():
                        orders_count = len(data.get('spread_orders', pd.DataFrame()))
                        trades_count = len(data.get('spread_trades', pd.DataFrame()))
                        print(f"      {spread_key}: {orders_count:,} orders, {trades_count:,} trades")
                    print(f"   📁 Output: {len(parquet_results)} parquet groups generated")
                else:
                    # Single spread mode summary
                    spread_orders = spread_results.get('spread_orders', pd.DataFrame())
                    spread_trades = spread_results.get('spread_trades', pd.DataFrame())
                    print(f"✅ Spread Generation Successful")
                    print(f"   📊 Spread orders: {len(spread_orders):,} points")
                    print(f"   💹 Spread trades: {len(spread_trades):,} trades")
                    print(f"   📅 Period: {config['start_date']} to {config['end_date']}")
                    print(f"   📁 Output: {parquet_results.get('spread_name', 'N/A')}")
                successful = 1
                failed = 0
            else:
                print(f"❌ Spread Generation Failed")
                successful = 0
                failed = 1
        
        else:
            # Individual mode summary  
            if 'results' in locals() and results:
                successful = 0
                failed = 0
                summary_data = []
                
                for contract_key, result in results.items():
                    if 'error' in result:
                        print(f"❌ {contract_key}: {result['error']}")
                        failed += 1
                    else:
                        print(f"✅ {contract_key}")
                        contract_config = result['config']
                        data = result['data']
                        
                        # Count data points
                        trades_count = len(data.get('trades', pd.DataFrame()))
                        orders_count = len(data.get('orders', pd.DataFrame()))
                        
                        print(f"   📈 Data: {trades_count} trades, {orders_count} orders")
                        print(f"   📅 Period: {contract_config['start_date']} to {contract_config['end_date']}")
                        
                        summary_data.append({
                            'contract_key': contract_key,
                            'market': contract_config['market'],
                            'tenor': contract_config['tenor'],  
                            'contract': contract_config['contract'],
                            'data_start': contract_config['start_date'],
                            'data_end': contract_config['end_date'],
                            'trades_count': trades_count,
                            'orders_count': orders_count,
                            'total_points': trades_count + orders_count
                        })
                        
                        successful += 1
                
                # Save individual summary
                if summary_data:
                    summary_df = pd.DataFrame(summary_data)
                    summary_path = os.path.join(output_dir, "integration_summary.csv")
                    summary_df.to_csv(summary_path, index=False)
                    print(f"\n📋 Individual summary saved: {summary_path}")
            else:
                print(f"❌ Individual Contract Generation Failed")
                successful = 0
                failed = 1
        print(f"\n🎯 Final Results:")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📁 Output directory: {output_dir}")
        
        return successful > 0
        
    except Exception as e:
        print(f"❌ Integration script failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    """
    Run the integration script
    
    This will:
    1. Take SpreadViewer inputs (relative contracts)
    2. Extract the absolute contracts SpreadViewer would fetch
    3. Use DataFetcher to get period data for those contracts
    4. Save everything to the specified directory
    """
    success = main()
    
    if success:
        print("\n🎉 INTEGRATION COMPLETED SUCCESSFULLY!")
        print(f"📁 Check {output_base} for results")
    else:
        print("\n💥 INTEGRATION FAILED!")
        print("🔧 Check error messages above for troubleshooting")
    
    print("\n" + "=" * 60)