#!/usr/bin/env python3
"""
Fix the corrupted parquet file by recreating it with proper format
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import traceback

# File paths
input_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test/parquet_files/DEM1-DEM2_spread.parquet'
output_file = '/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test/parquet_files/DEM1-DEM2_spread_fixed.parquet'

print("🔧 Fixing corrupted parquet file...")
print(f"📁 Input: {input_file}")
print(f"📁 Output: {output_file}")

try:
    # Method 1: Try reading with different engines
    print("\n📊 Attempting Method 1: Different engines...")
    
    try:
        # Try fastparquet engine
        df = pd.read_parquet(input_file, engine='fastparquet')
        print("✅ Successfully read with fastparquet engine")
    except Exception as e1:
        print(f"❌ Fastparquet failed: {e1}")
        
        try:
            # Try with pyarrow but different options
            df = pd.read_parquet(input_file, engine='pyarrow', use_nullable_dtypes=False)
            print("✅ Successfully read with pyarrow (nullable=False)")
        except Exception as e2:
            print(f"❌ PyArrow nullable=False failed: {e2}")
            
            try:
                # Try direct PyArrow table read
                table = pq.read_table(input_file, use_threads=False)
                df = table.to_pandas()
                print("✅ Successfully read with PyArrow direct (no threads)")
            except Exception as e3:
                print(f"❌ PyArrow direct failed: {e3}")
                
                # Method 2: Regenerate from source
                print("\n🔄 Method 2: Regenerating from integration script...")
                raise Exception("All read methods failed, need to regenerate")
    
    print(f"📊 Data shape: {df.shape}")
    print(f"📊 Columns: {list(df.columns)}")
    print(f"📊 Index type: {type(df.index)}")
    print(f"📊 Data types: {df.dtypes}")
    
    # Clean up the data
    print("\n🧹 Cleaning data...")
    
    # Ensure proper datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'datetime' in df.columns:
            df = df.set_index('datetime')
        else:
            print("⚠️  No datetime index found")
    
    # Ensure proper column names and types
    if 'bid' not in df.columns or 'ask' not in df.columns:
        print("⚠️  Missing bid/ask columns")
        print(f"Available columns: {list(df.columns)}")
        
        # Try to map columns
        col_mapping = {}
        for col in df.columns:
            if 'bid' in col.lower() or col == '0':
                col_mapping[col] = 'bid'
            elif 'ask' in col.lower() or col == '1':
                col_mapping[col] = 'ask'
        
        if col_mapping:
            df = df.rename(columns=col_mapping)
            print(f"✅ Mapped columns: {col_mapping}")
    
    # Ensure numeric types
    for col in ['bid', 'ask']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove any NaN rows
    original_len = len(df)
    df = df.dropna()
    if len(df) < original_len:
        print(f"🧹 Removed {original_len - len(df)} rows with NaN values")
    
    print(f"✅ Cleaned data shape: {df.shape}")
    
    # Save the fixed parquet file
    print(f"\n💾 Saving fixed parquet file...")
    df.to_parquet(output_file, engine='pyarrow', compression='snappy')
    
    # Verify the fix
    test_df = pd.read_parquet(output_file)
    print(f"✅ Verification successful: {test_df.shape}")
    
    # Save sample as CSV for backup
    csv_file = output_file.replace('.parquet', '_sample.csv')
    df.head(1000).to_csv(csv_file)
    print(f"💾 Sample CSV saved: {csv_file}")
    
    print("\n🎉 Parquet file fixed successfully!")
    print(f"📁 Fixed file: {output_file}")
    print(f"📊 Final shape: {df.shape}")
    print(f"📅 Date range: {df.index.min()} to {df.index.max()}")
    
except Exception as e:
    print(f"\n❌ All methods failed: {e}")
    print("\n📋 Error details:")
    traceback.print_exc()
    
    print(f"\n🔄 Solution: Re-run the integration script to regenerate clean data")
    print(f"   The original SpreadViewer data generation was successful (192,632 points)")
    print(f"   The issue is likely in the parquet writing/reading process")