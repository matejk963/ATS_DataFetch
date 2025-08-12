#!/usr/bin/env python3
"""
Fix all corrupted parquet files from the integration script
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import traceback
import os

# File paths
data_dir = '/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test/parquet_files'
files_to_fix = [
    'DEM1-DEM2_orders.parquet',
    'DEM1-DEM2_trades.parquet', 
    'DEM1-DEM2_spread.parquet',
    'DEM1-DEM2_metadata.parquet'
]

print("🔧 Fixing all corrupted parquet files...")
print(f"📁 Directory: {data_dir}")
print("=" * 60)

for filename in files_to_fix:
    input_file = os.path.join(data_dir, filename)
    output_file = os.path.join(data_dir, filename.replace('.parquet', '_fixed.parquet'))
    
    print(f"\n🔄 Processing: {filename}")
    
    if not os.path.exists(input_file):
        print(f"   ❌ File not found: {filename}")
        continue
    
    try:
        # Try different read methods
        df = None
        
        # Method 1: fastparquet
        try:
            df = pd.read_parquet(input_file, engine='fastparquet')
            print(f"   ✅ Read with fastparquet: {df.shape}")
        except Exception as e1:
            print(f"   ❌ Fastparquet failed: {str(e1)[:100]}...")
            
            # Method 2: pyarrow direct
            try:
                table = pq.read_table(input_file, use_threads=False)
                df = table.to_pandas()
                print(f"   ✅ Read with PyArrow direct: {df.shape}")
            except Exception as e2:
                print(f"   ❌ PyArrow direct failed: {str(e2)[:100]}...")
                
                # Method 3: pyarrow with different options
                try:
                    df = pd.read_parquet(input_file, engine='pyarrow', use_nullable_dtypes=False)
                    print(f"   ✅ Read with PyArrow (nullable=False): {df.shape}")
                except Exception as e3:
                    print(f"   ❌ All methods failed for {filename}")
                    continue
        
        if df is not None:
            # Clean and validate data
            print(f"   📊 Original shape: {df.shape}")
            print(f"   📊 Columns: {list(df.columns)}")
            
            # Remove any completely empty rows
            original_len = len(df)
            df = df.dropna(how='all')
            if len(df) < original_len:
                print(f"   🧹 Removed {original_len - len(df)} empty rows")
            
            # Ensure proper data types
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except:
                        pass
            
            # Save fixed version
            df.to_parquet(output_file, engine='pyarrow', compression='snappy')
            
            # Verify the fix
            try:
                test_df = pd.read_parquet(output_file)
                print(f"   ✅ Fixed file verified: {test_df.shape}")
                
                # Save sample as CSV backup
                csv_file = output_file.replace('.parquet', '_sample.csv')
                sample_size = min(1000, len(df))
                df.head(sample_size).to_csv(csv_file)
                print(f"   💾 Sample CSV saved: {Path(csv_file).name}")
                
            except Exception as e:
                print(f"   ❌ Verification failed: {e}")
        
    except Exception as e:
        print(f"   ❌ Failed to process {filename}: {e}")
        traceback.print_exc()

print(f"\n" + "=" * 60)
print("🎉 Parquet fixing completed!")
print(f"📁 Check directory: {data_dir}")
print("📝 Look for files ending with '_fixed.parquet'")
print("📋 CSV backups available as '_fixed_sample.csv'")