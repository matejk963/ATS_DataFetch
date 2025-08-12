#!/usr/bin/env python3
"""
Verify where SpreadViewer cache files are actually saved
"""

import os
from pathlib import Path

def check_cache_locations():
    """Check various possible cache locations"""
    
    locations_to_check = [
        r"C:\Users\krajcovic\Documents\Testing Data\ATS_data\test",
        "/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data/test",
        os.getcwd(),
        "./test",
        "../test"
    ]
    
    print("🔍 Checking cache file locations...")
    print("=" * 60)
    
    for location in locations_to_check:
        try:
            path = Path(location)
            print(f"\n📁 Checking: {path}")
            
            if path.exists():
                print(f"   ✅ Directory exists")
                files = list(path.iterdir())
                if files:
                    print(f"   📄 Files found: {len(files)}")
                    for file in sorted(files):
                        size = file.stat().st_size if file.is_file() else 0
                        file_type = "📄" if file.is_file() else "📁"
                        print(f"      {file_type} {file.name} ({size:,} bytes)")
                else:
                    print(f"   📭 Directory empty")
            else:
                print(f"   ❌ Directory does not exist")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("🔍 Checking for any spreadviewer files...")
    
    # Check current working directory for any files
    cwd = Path.cwd()
    print(f"📁 Current working directory: {cwd}")
    
    spreadviewer_files = list(cwd.glob("*spreadviewer*"))
    if spreadviewer_files:
        print("   ✅ Found spreadviewer files in current directory:")
        for file in spreadviewer_files:
            print(f"      📄 {file.name} ({file.stat().st_size:,} bytes)")
    else:
        print("   📭 No spreadviewer files in current directory")

if __name__ == "__main__":
    check_cache_locations()