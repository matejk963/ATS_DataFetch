#!/usr/bin/env python3
"""
Fetch both DataFetcher and SpreadViewer data with corrected relative tenor logic for comparison
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/src')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from core.data_fetcher import DataFetcher
import subprocess
import time
from pathlib import Path

print("🔄 FETCHING BOTH DATA SOURCES WITH CORRECTED RELATIVE TENOR LOGIC")
print("=" * 80)

# Configuration
contracts = ['debq4_25', 'frbq4_25']
period = '2025-07-01,2025-07-02'  # Just 2 days for quick testing
n_s = 3

print(f"📋 Configuration:")
print(f"   📊 Contracts: {contracts}")
print(f"   📅 Period: {period}")
print(f"   🔧 Expected relative tenor: q_1 (corrected from q_3)")
print()

# Fetch DataFetcher-only data
print("🚀 STEP 1: Fetching DataFetcher-only data...")
print(f"   📝 This should fetch real exchange data with broker_id=1441.0")
try:
    cmd1 = [
        'python', 'data_fetch_engine.py',
        '-c', ','.join(contracts),
        '-p', period,
        '-n_s', str(n_s),
        '-m', 'datafetcher_only'
    ]
    
    result1 = subprocess.run(cmd1, cwd='engines', capture_output=True, text=True, timeout=120)
    
    if result1.returncode == 0:
        print(f"   ✅ DataFetcher-only fetch completed!")
        # Look for the output file
        datafetcher_files = list(Path("/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test").glob("debq4_25_frbq4_25_DATAFETCHER_ONLY*"))
        if datafetcher_files:
            print(f"   📁 Output file: {datafetcher_files[0].name}")
        else:
            print(f"   ⚠️  No DataFetcher output file found")
    else:
        print(f"   ❌ DataFetcher-only fetch failed:")
        print(f"      {result1.stderr}")
        
except subprocess.TimeoutExpired:
    print(f"   ⏰ DataFetcher-only fetch timed out (2 minutes)")
except Exception as e:
    print(f"   ❌ DataFetcher-only fetch error: {e}")

# Wait a bit
print(f"\n⏳ Waiting 5 seconds before next fetch...")
time.sleep(5)

# Fetch SpreadViewer-only data  
print("🚀 STEP 2: Fetching SpreadViewer-only data...")
print(f"   📝 This should fetch synthetic data with broker_id=9999.0")
print(f"   📝 Database queries should use 'q_1' (corrected from 'q_3')")
try:
    cmd2 = [
        'python', 'data_fetch_engine.py',
        '-c', ','.join(contracts),
        '-p', period,
        '-n_s', str(n_s),
        '-m', 'synthetic_only'
    ]
    
    result2 = subprocess.run(cmd2, cwd='engines', capture_output=True, text=True, timeout=120)
    
    if result2.returncode == 0:
        print(f"   ✅ SpreadViewer-only fetch completed!")
        # Look for the output file
        spreadviewer_files = list(Path("/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test").glob("debq4_25_frbq4_25_SPREADVIEWER_ONLY*"))
        if spreadviewer_files:
            print(f"   📁 Output file: {spreadviewer_files[0].name}")
        else:
            print(f"   ⚠️  No SpreadViewer output file found")
            
        # Check for the corrected relative tenor in output
        if 'q_1' in result2.stdout:
            print(f"   ✅ CONFIRMED: Relative tenor calculation uses 'q_1' (corrected!)")
        if 'q_3' in result2.stdout:
            print(f"   ⚠️  WARNING: Still seeing 'q_3' in output")
            
    else:
        print(f"   ❌ SpreadViewer-only fetch failed:")
        print(f"      {result2.stderr}")
        
except subprocess.TimeoutExpired:
    print(f"   ⏰ SpreadViewer-only fetch timed out (2 minutes)")
except Exception as e:
    print(f"   ❌ SpreadViewer-only fetch error: {e}")

print(f"\n🎯 SUMMARY:")
print(f"   📊 Both datasets should now be available for comparison")
print(f"   ✅ SpreadViewer should now use correct relative tenor (q_1 instead of q_3)")
print(f"   📈 Next step: Create side-by-side comparison plot")