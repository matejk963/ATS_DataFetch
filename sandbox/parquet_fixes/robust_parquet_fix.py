#!/usr/bin/env python3
"""
Robust parquet fix that addresses the index serialization issue
"""

import pandas as pd
import numpy as np
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

print("🔧 Robust Parquet Fix - Addressing Index Issues")
print(f"📁 Directory: {data_dir}")
print("=" * 70)

def safe_read_parquet(file_path):
    """Try multiple methods to read a parquet file safely"""
    print(f"   🔄 Attempting to read: {Path(file_path).name}")
    
    # Method 1: Try fastparquet without index
    try:
        df = pd.read_parquet(file_path, engine='fastparquet')
        print(f"   ✅ Read with fastparquet: {df.shape}")
        return df, 'fastparquet'
    except Exception as e1:
        print(f"   ❌ Fastparquet failed: {str(e1)[:80]}...")
        
        # Method 2: Try pyarrow
        try:
            df = pd.read_parquet(file_path, engine='pyarrow')
            print(f"   ✅ Read with pyarrow: {df.shape}")
            return df, 'pyarrow'
        except Exception as e2:
            print(f"   ❌ PyArrow failed: {str(e2)[:80]}...")
            
            # Method 3: Force reset index and try again
            try:
                import pyarrow.parquet as pq
                table = pq.read_table(file_path)
                df = table.to_pandas()
                
                # Reset index completely to avoid serialization issues
                if not isinstance(df.index, pd.RangeIndex):
                    df = df.reset_index()
                    print(f"   🔄 Reset index, now: {df.shape}")
                
                print(f"   ✅ Read with PyArrow table: {df.shape}")
                return df, 'pyarrow_table'
            except Exception as e3:
                print(f"   ❌ All methods failed: {str(e3)[:80]}...")
                return None, None

def clean_and_save_data(df, file_type, output_path):
    """Clean data and save with proper index handling"""
    print(f"   🧹 Cleaning {file_type} data...")
    
    # Remove any completely empty rows/columns
    original_shape = df.shape
    df = df.dropna(how='all')
    
    # Handle different file types
    if file_type in ['orders', 'spread']:
        # Orders/spread should have datetime index and bid/ask columns
        if 'index' in df.columns:
            df['datetime'] = pd.to_datetime(df['index'])
            df = df.drop('index', axis=1)
            df = df.set_index('datetime')
        elif not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except:
                print(f"   ⚠️  Could not convert index to datetime")
        
        # Ensure we have the right columns
        expected_cols = ['bid', 'ask']
        for col in expected_cols:
            if col not in df.columns:
                print(f"   ⚠️  Missing column: {col}")
        
    elif file_type == 'trades':
        # Trades should have datetime index and buy/sell columns
        if 'index' in df.columns:
            df['datetime'] = pd.to_datetime(df['index'])
            df = df.drop('index', axis=1)
            df = df.set_index('datetime')
        elif not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except:
                print(f"   ⚠️  Could not convert index to datetime")
        
        # Ensure we have the right columns
        expected_cols = ['buy', 'sell']
        for col in expected_cols:
            if col not in df.columns:
                print(f"   ⚠️  Missing column: {col}")
                
    elif file_type == 'metadata':
        # Metadata is just a single row with config info
        pass
    
    # Convert numeric columns properly
    for col in df.columns:
        if col not in ['datetime'] and df[col].dtype == 'object':
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass
    
    # Final cleanup
    df = df.dropna(how='all')
    
    if df.shape != original_shape:
        print(f"   🧹 Shape changed: {original_shape} → {df.shape}")
    
    print(f"   📊 Final data: {df.shape}, columns: {list(df.columns)}")
    print(f"   📅 Index type: {type(df.index)}")
    
    # Save with explicit index handling
    try:
        # Save as parquet with proper index serialization
        df.to_parquet(output_path, engine='pyarrow', compression='snappy', index=True)
        print(f"   💾 Saved parquet: {Path(output_path).name}")
        
        # Verify the save worked
        test_df = pd.read_parquet(output_path, engine='pyarrow')
        print(f"   ✅ Verified: {test_df.shape}")
        
        # Save CSV backup
        csv_path = str(output_path).replace('.parquet', '.csv')
        sample_size = min(1000, len(df))
        df.head(sample_size).to_csv(csv_path)
        print(f"   💾 CSV backup: {Path(csv_path).name}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Save failed: {e}")
        return False

# Process each file
success_count = 0
total_files = len(files_to_fix)

for filename in files_to_fix:
    input_path = os.path.join(data_dir, filename)
    output_path = os.path.join(data_dir, filename.replace('.parquet', '_robust.parquet'))
    
    print(f"\n{'='*50}")
    print(f"🔄 Processing: {filename}")
    
    if not os.path.exists(input_path):
        print(f"   ❌ File not found: {filename}")
        continue
    
    # Determine file type from name
    if 'orders' in filename:
        file_type = 'orders'
    elif 'trades' in filename:
        file_type = 'trades'
    elif 'spread' in filename and 'orders' not in filename and 'trades' not in filename:
        file_type = 'spread'
    elif 'metadata' in filename:
        file_type = 'metadata'
    else:
        file_type = 'unknown'
    
    # Try to read the file
    df, method = safe_read_parquet(input_path)
    
    if df is not None:
        # Clean and save the data
        if clean_and_save_data(df, file_type, output_path):
            success_count += 1
            print(f"   ✅ {filename} → {Path(output_path).name}")
        else:
            print(f"   ❌ Failed to save {filename}")
    else:
        print(f"   ❌ Could not read {filename}")

print(f"\n" + "=" * 70)
print(f"🎉 Robust Parquet Fix Complete!")
print(f"✅ Successfully fixed: {success_count}/{total_files} files")
print(f"📁 Look for files ending with '_robust.parquet'")
print(f"📁 CSV backups available as '_robust.csv'")
print(f"💡 Use engine='pyarrow' when reading the robust files")