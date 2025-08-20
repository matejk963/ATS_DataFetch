#!/usr/bin/env python3
"""
Inspect the actual data structure returned by integration
"""

import sys
import os
import pandas as pd

# Set environment variable for database config
os.environ['PROJECT_CONFIG'] = '/mnt/192.168.10.91/EnergyTrading/configDB.json'

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def inspect_data_structure():
    """Inspect the actual data structure"""
    try:
        from integration_script_v2 import integrated_fetch
        
        print("🔍 INSPECTING INTEGRATION DATA STRUCTURE")
        print("=" * 50)
        
        # Config for individual leg
        config = {
            'contracts': ['debm01_25'],
            'period': {
                'start_date': '2024-12-02',
                'end_date': '2024-12-06'
            },
            'n_s': 3,
            'mode': 'individual'
        }
        
        print("📡 Fetching individual leg...")
        result = integrated_fetch(config)
        
        print(f"\n📊 RESULT STRUCTURE:")
        print(f"Type: {type(result)}")
        print(f"Keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
        
        for key, value in result.items():
            print(f"\n🔑 Key: '{key}'")
            print(f"   Type: {type(value)}")
            
            if isinstance(value, dict):
                print(f"   Dict keys: {list(value.keys())}")
                for subkey, subvalue in value.items():
                    print(f"     '{subkey}': {type(subvalue)}")
                    if isinstance(subvalue, pd.DataFrame):
                        print(f"       DataFrame shape: {subvalue.shape}")
                        print(f"       DataFrame columns: {list(subvalue.columns)}")
                    elif isinstance(subvalue, dict):
                        print(f"       Dict keys: {list(subvalue.keys())}")
                        for subsubkey, subsubvalue in subvalue.items():
                            print(f"         '{subsubkey}': {type(subsubvalue)}")
                            if isinstance(subsubvalue, pd.DataFrame):
                                print(f"           DataFrame shape: {subsubvalue.shape}")
                                print(f"           DataFrame columns: {list(subsubvalue.columns)}")
            elif isinstance(value, pd.DataFrame):
                print(f"   DataFrame shape: {value.shape}")
                print(f"   DataFrame columns: {list(value.columns)}")
            elif isinstance(value, str):
                print(f"   String value: {value[:100]}..." if len(value) > 100 else f"   String value: {value}")
            else:
                print(f"   Value: {value}")
        
        print(f"\n🎯 LOOKING FOR ACTUAL DATA:")
        
        # Try to find the actual dataframes
        def find_dataframes(obj, path=""):
            """Recursively find DataFrames in nested structure"""
            if isinstance(obj, pd.DataFrame) and not obj.empty:
                print(f"   📊 Found DataFrame at '{path}': {obj.shape} - columns: {list(obj.columns)}")
                return [(path, obj)]
            elif isinstance(obj, dict):
                results = []
                for key, value in obj.items():
                    results.extend(find_dataframes(value, f"{path}.{key}" if path else key))
                return results
            else:
                return []
        
        dataframes = find_dataframes(result)
        print(f"\n📈 Found {len(dataframes)} DataFrames:")
        for path, df in dataframes:
            print(f"   {path}: {df.shape}")
            if len(df) > 0:
                print(f"     Sample: {df.head(1).to_dict()}")
        
    except Exception as e:
        print(f"❌ Inspection failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    inspect_data_structure()