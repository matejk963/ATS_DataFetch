# -*- coding: utf-8 -*-
"""
SpreadViewer to Period Data Integration
Takes relative contracts from SpreadViewer pipeline and generates cached period data

This script:
1. Uses SpreadViewer's relative contract logic (M+1, M+2)
2. Converts to generate_period_data format
3. Saves cached data to C:\Users\krajcovic\Documents\Testing Data\ATS_data\test
"""

import sys
import os
from datetime import datetime, time
from pathlib import Path
import pandas as pd
import numpy as np

# Add paths for imports
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

# Import required modules
from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData, norm_coeff
from Database.TPData import TPData, TPDataDa
from src.core.generate_period_data import PeriodDataGenerator


def convert_spreadviewer_to_period_contracts(market, tenor, tn1_list, tn2_list, start_date, end_date):
    """
    Convert SpreadViewer relative contract specifications to period data format
    
    Args:
        market (list): Market specifications from SpreadViewer
        tenor (list): Tenor specifications from SpreadViewer  
        tn1_list (list): First contract offsets from SpreadViewer
        tn2_list (list): Second contract offsets from SpreadViewer
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        list: Contract configurations for generate_period_data
    """
    print(f"🔄 Converting SpreadViewer contracts to period data format...")
    print(f"📊 Markets: {market}")
    print(f"📋 Tenors: {tenor}")
    print(f"🔢 tn1_list: {tn1_list}, tn2_list: {tn2_list}")
    print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    contracts = []
    
    # Process first leg contracts (tn1_list)
    for i, offset in enumerate(tn1_list):
        if i < len(market) and i < len(tenor):
            # Calculate contract date based on offset
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
            # Calculate contract date based on offset
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


def generate_period_data_from_spreadviewer(output_dir="C:/Users/krajcovic/Documents/Testing Data/ATS_data/test"):
    """
    Generate period data using SpreadViewer's relative contract specifications
    
    Args:
        output_dir (str): Output directory for cached period data
        
    Returns:
        dict: Results of period data generation
    """
    print(f"🚀 SPREADVIEWER TO PERIOD DATA INTEGRATION")
    print(f"=" * 80)
    
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
        # Step 1: Convert SpreadViewer contracts to period data format
        contracts = convert_spreadviewer_to_period_contracts(
            market, tenor, tn1_list, tn2_list, start_date, end_date
        )
        
        if not contracts:
            print("❌ No contracts generated")
            return {'status': 'failed', 'reason': 'no_contracts'}
        
        print(f"✅ Generated {len(contracts)} contract configurations")
        
        # Step 2: Initialize PeriodDataGenerator with custom output directory
        print(f"\n📦 Initializing PeriodDataGenerator...")
        period_generator = PeriodDataGenerator(str(output_path))
        print(f"✅ PeriodDataGenerator initialized with output: {output_path}")
        
        # Step 3: Generate period data for each contract
        print(f"\n🔄 Generating period data for each contract...")
        
        generated_files = []
        results = {}
        
        for contract in contracts:
            try:
                print(f"\n📊 Processing {contract['label']} ({contract['market']} {contract['contract']})...")
                
                # Create contract list for generate_period_data
                contract_list = [contract['contract']]
                
                # Generate period data
                print(f"🔄 Calling generate_period_data...")
                period_result = period_generator.generate_period_data(
                    contracts=contract_list,
                    start_date=contract['start_date'],
                    end_date=contract['end_date']
                )
                
                if period_result:
                    results[contract['label']] = {
                        'config': contract,
                        'result': period_result,
                        'status': 'success'
                    }
                    print(f"✅ {contract['label']}: Period data generated successfully")
                    
                    # Try to find generated files
                    cache_pattern = f"*{contract['contract']}*{contract['start_date']}*{contract['end_date']}*"
                    generated_files.extend(list(output_path.glob(cache_pattern)))
                    
                else:
                    results[contract['label']] = {
                        'config': contract,
                        'result': None,
                        'status': 'failed',
                        'error': 'generate_period_data returned None'
                    }
                    print(f"❌ {contract['label']}: Period data generation failed")
                    
            except Exception as e:
                results[contract['label']] = {
                    'config': contract,
                    'result': None,
                    'status': 'failed',
                    'error': str(e)
                }
                print(f"❌ {contract['label']}: Exception - {e}")
        
        # Step 4: Summary and file listing
        print(f"\n{'=' * 80}")
        print(f"📋 PERIOD DATA GENERATION SUMMARY")
        print(f"{'=' * 80}")
        
        successful = [k for k, v in results.items() if v['status'] == 'success']
        failed = [k for k, v in results.items() if v['status'] == 'failed']
        
        print(f"✅ Successful: {len(successful)}/{len(contracts)}")
        for contract_label in successful:
            print(f"   ✅ {contract_label}")
        
        if failed:
            print(f"❌ Failed: {len(failed)}/{len(contracts)}")
            for contract_label in failed:
                error = results[contract_label].get('error', 'Unknown error')
                print(f"   ❌ {contract_label}: {error}")
        
        # List generated files
        print(f"\n📁 Files in output directory:")
        all_files = list(output_path.iterdir())
        if all_files:
            for file_path in sorted(all_files):
                file_size = file_path.stat().st_size if file_path.exists() else 0
                print(f"   📄 {file_path.name} ({file_size:,} bytes)")
        else:
            print(f"   📭 No files found in {output_path}")
        
        # Final results
        final_results = {
            'status': 'success' if successful else 'partial' if len(successful) > 0 else 'failed',
            'contracts_processed': len(contracts),
            'successful_contracts': len(successful),
            'failed_contracts': len(failed),
            'output_directory': str(output_path),
            'generated_files': [str(f) for f in all_files],
            'contract_results': results,
            'spreadviewer_params': {
                'market': market,
                'tenor': tenor,
                'tn1_list': tn1_list,
                'tn2_list': tn2_list,
                'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            }
        }
        
        print(f"\n🎉 INTEGRATION COMPLETED")
        print(f"📊 Status: {final_results['status'].upper()}")
        print(f"📁 Output: {output_path}")
        print(f"✅ Ready for SpreadViewer analysis with cached data!")
        print(f"=" * 80)
        
        return final_results
        
    except Exception as e:
        print(f"\n❌ INTEGRATION FAILED: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'failed', 'reason': 'exception', 'error': str(e)}


def main():
    """
    Main function to run SpreadViewer to period data integration
    """
    print("🔗 SpreadViewer → Period Data Integration")
    print("Extracting relative contracts from SpreadViewer and generating cached period data")
    print("=" * 80)
    
    # Run the integration
    results = generate_period_data_from_spreadviewer()
    
    if results.get('status') == 'success':
        print(f"\n🎉 SUCCESS! Period data generated for SpreadViewer contracts")
        print(f"📊 Processed: {results['successful_contracts']}/{results['contracts_processed']} contracts")
        print(f"📁 Files saved to: {results['output_directory']}")
        print(f"🔗 Ready for SpreadViewer analysis!")
        
    elif results.get('status') == 'partial':
        print(f"\n⚠️  PARTIAL SUCCESS")
        print(f"📊 Processed: {results['successful_contracts']}/{results['contracts_processed']} contracts")
        print(f"❌ {results['failed_contracts']} contracts failed")
        print(f"📁 Files saved to: {results['output_directory']}")
        
    else:
        print(f"\n❌ FAILED")
        print(f"📋 Reason: {results.get('reason', 'unknown')}")
        if results.get('error'):
            print(f"❌ Error: {results['error']}")
    
    return results


if __name__ == "__main__":
    results = main()