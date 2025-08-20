#!/usr/bin/env python3
"""
Verify that the corrected relative tenor logic is working by running a targeted fetch
"""

import subprocess
import sys
import os
from pathlib import Path

print("🔍 VERIFYING CORRECTED RELATIVE TENOR LOGIC")
print("=" * 70)

# Clear any old files first
test_dir = Path("/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/test")
old_files = list(test_dir.glob("*corrected*"))
for f in old_files:
    print(f"   🗑️  Removing old file: {f.name}")
    f.unlink()

print(f"📋 Test Configuration:")
print(f"   📊 Contracts: debq4_25, frbq4_25 (Q4 2025)")
print(f"   📅 Period: July 1-2, 2025 (Q3 2025)")
print(f"   🎯 Expected: q_1 (Q4 2025 is +1 quarter from Q3 2025)")
print(f"   🚫 Should NOT see: q_3 (old incorrect logic)")
print()

# Run a focused synthetic-only fetch with output capture
print("🚀 Running SpreadViewer fetch with corrected logic...")
try:
    cmd = [
        'python', 'data_fetch_engine.py',
        '-c', 'debq4_25,frbq4_25',
        '-p', '2025-07-01,2025-07-02',
        '-n_s', '3',
        '-m', 'synthetic_only'
    ]
    
    print(f"   📝 Command: {' '.join(cmd)}")
    print(f"   ⏳ Running (timeout: 3 minutes)...")
    
    result = subprocess.run(
        cmd, 
        cwd='engines', 
        capture_output=True, 
        text=True, 
        timeout=180  # 3 minutes
    )
    
    # Analyze the output
    print(f"\n📊 OUTPUT ANALYSIS:")
    print(f"   Return code: {result.returncode}")
    
    # Check for key indicators in output
    output_text = result.stdout + result.stderr
    
    # Look for relative tenor calculation
    if 'q_1' in output_text:
        print(f"   ✅ FOUND 'q_1' in output - corrected logic is working!")
        q1_count = output_text.count('q_1')
        print(f"      Occurrences of 'q_1': {q1_count}")
    else:
        print(f"   ❌ NO 'q_1' found in output")
        
    if 'q_3' in output_text:
        print(f"   ⚠️  FOUND 'q_3' in output - old logic might still be active")
        q3_count = output_text.count('q_3')
        print(f"      Occurrences of 'q_3': {q3_count}")
    else:
        print(f"   ✅ No 'q_3' found - old logic not being used")
    
    # Look for the debug output from our fix
    if 'Quarters difference:' in output_text:
        print(f"   ✅ FOUND debug output from corrected calculation")
        # Extract the quarters difference line
        lines = output_text.split('\n')
        for line in lines:
            if 'Quarters difference:' in line:
                print(f"      {line.strip()}")
    else:
        print(f"   ❌ No debug output from corrected calculation")
    
    # Look for product dates (should be October 2025, not April 2026)
    if '2025-10-01' in output_text:
        print(f"   ✅ FOUND October 2025 dates - correct product dates!")
        oct_count = output_text.count('2025-10-01')
        print(f"      Occurrences of '2025-10-01': {oct_count}")
    else:
        print(f"   ❌ No October 2025 dates found")
        
    if '2026-04-01' in output_text:
        print(f"   ⚠️  FOUND April 2026 dates - might be using old logic")
        apr_count = output_text.count('2026-04-01')
        print(f"      Occurrences of '2026-04-01': {apr_count}")
    else:
        print(f"   ✅ No April 2026 dates - not using old logic")
    
    # Look for database queries
    db_lines = [line for line in output_text.split('\n') if 'de //' in line or 'fr //' in line]
    if db_lines:
        print(f"   📊 Database queries found:")
        for line in db_lines[:5]:  # Show first 5
            print(f"      {line.strip()}")
    
    # Show a sample of the output for manual inspection
    print(f"\n📝 SAMPLE OUTPUT (first 20 lines):")
    output_lines = output_text.split('\n')
    for i, line in enumerate(output_lines[:20]):
        if line.strip():
            print(f"   {i+1:2}: {line}")
    
    if result.returncode == 0:
        print(f"\n✅ FETCH COMPLETED SUCCESSFULLY")
        
        # Check for output files
        new_files = list(test_dir.glob("*debq4_25_frbq4_25*"))
        recent_files = [f for f in new_files if f.stat().st_mtime > (import_datetime().timestamp() - 300)]  # Last 5 minutes
        
        if recent_files:
            print(f"   📁 New output files:")
            for f in recent_files:
                print(f"      {f.name} ({f.stat().st_size} bytes)")
        else:
            print(f"   ⚠️  No recent output files found")
            
    else:
        print(f"\n❌ FETCH FAILED")
        if result.stderr:
            print(f"   Error: {result.stderr[:500]}...")
    
except subprocess.TimeoutExpired:
    print(f"   ⏰ TIMEOUT: Fetch took longer than 3 minutes")
    print(f"   📝 This might indicate database connection issues")
    
except Exception as e:
    print(f"   ❌ ERROR: {e}")

print(f"\n🎯 VERIFICATION SUMMARY:")
print(f"   The output above should show:")
print(f"   ✅ 'q_1' in database queries (corrected)")
print(f"   ✅ '2025-10-01' in product dates (corrected)")
print(f"   ✅ 'Quarters difference: 8103 - 8102 = 1' in debug output")
print(f"   ❌ Should NOT see 'q_3' or '2026-04-01' (old logic)")

def import_datetime():
    from datetime import datetime
    return datetime.now()