#!/usr/bin/env python3
"""
Trace the exact database error without any print statements from integration
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
        
        # Direct database call without any wrappers
        db_class = TPData()
        
        # Try the exact same call that SpreadViewer makes
        result = db_class.get_best_ob_data(
            'de', 'base', 'm', 
            '2024-12-02', '2024-12-03',
            start_time='09:00:00', 
            end_time='17:00:00'
        )
        
        print(f"Success: {result}")
        
    except Exception as e:
        import traceback
        print("EXACT DATABASE ERROR:")
        traceback.print_exc()

if __name__ == "__main__":
    trace_database_exact()