#!/usr/bin/env python3
"""
Trace the exact database error with correct method signature
"""

import sys
import os
from datetime import datetime, time
import pandas as pd

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def trace_database_exact():
    """Trace exact database error"""
    try:
        from Database.TPData import TPData
        
        # Direct database connection test
        db_class = TPData()
        
        # Test basic connection
        db_class.create_connection('PostgreSQL')
        print("Database connection successful")
        
    except Exception as e:
        import traceback
        print("EXACT DATABASE CONNECTION ERROR:")
        traceback.print_exc()

if __name__ == "__main__":
    trace_database_exact()