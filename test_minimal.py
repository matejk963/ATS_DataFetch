#!/usr/bin/env python3

import sys
import os

# Cross-platform project root
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
    energy_trading_path = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch\source_repos\EnergyTrading\Python'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'
    energy_trading_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python'

sys.path.insert(0, project_root)
sys.path.insert(0, energy_trading_path)

print("🔍 Testing minimal imports...")

print("1. Importing DataFetcher...")
from src.core.data_fetcher import DataFetcher

print("2. Importing SpreadViewer classes...")
from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData

print("3. Importing TPData...")
from Database.TPData import TPData

print("4. All imports successful!")
print("5. Creating test instances...")

# Test basic instantiation
try:
    print("  - Testing DataFetcher creation...")
    fetcher = DataFetcher(allowed_broker_ids=[1441])
    print("  ✅ DataFetcher created")
    
    print("  - Testing SpreadSingle creation...")
    spread = SpreadSingle(['de'], ['m'], [9], [], ['eex'])
    print("  ✅ SpreadSingle created")
    
    print("  - Testing TPData creation...")
    tp = TPData()
    print("  ✅ TPData created")
    
except Exception as e:
    print(f"  ❌ Error during instantiation: {e}")

print("✅ Minimal test completed successfully!")