#!/usr/bin/env python3
"""
Inspect the reference file structure to understand the desired format
"""

import pandas as pd

def inspect_reference():
    """Inspect the reference file structure"""
    try:
        # Load the reference file
        ref_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/dem07_25_tr_ba_data.parquet'
        
        print("📊 Inspecting Reference File Structure")
        print("=" * 45)
        print(f"File: {ref_path}")
        
        df = pd.read_parquet(ref_path)
        
        print(f"\n📋 STRUCTURE:")
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Index: {df.index.name} ({type(df.index)})")
        print(f"   Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        
        print(f"\n📋 DATA TYPES:")
        print(df.dtypes)
        
        print(f"\n📋 SAMPLE DATA (first 10 rows):")
        print("=" * 100)
        print(df.head(10))
        
        print(f"\n📋 UNIQUE VALUES:")
        for col in df.columns:
            if df[col].dtype == 'object' or df[col].nunique() < 20:
                print(f"   {col}: {df[col].unique()}")
            else:
                print(f"   {col}: {df[col].nunique()} unique values")
        
        if 'type' in df.columns or any('trade' in col.lower() for col in df.columns):
            print(f"\n📊 TRADE/ORDER BREAKDOWN:")
            if 'type' in df.columns:
                print(df['type'].value_counts())
            
        print(f"\n📅 TIME RANGE:")
        print(f"   Start: {df.index.min()}")
        print(f"   End: {df.index.max()}")
        print(f"   Duration: {df.index.max() - df.index.min()}")
        
        print(f"\n💡 KEY INSIGHTS:")
        print(f"   This appears to be the target structure for spread data")
        print(f"   Need to replicate this format for our spread output")
        
        return df
        
    except Exception as e:
        print(f"❌ Inspection failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    df = inspect_reference()