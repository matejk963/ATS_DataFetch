#!/usr/bin/env python3
"""
Trace the exact error without print statements to see where JSON parsing fails
"""

import sys
import os
from datetime import datetime

sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def trace_exact_error():
    """Trace exact error location"""
    try:
        from integration_script_v2 import integrated_fetch
        
        test_config = {
            'contracts': ['debm09_25', 'debm10_25'],
            'period': {
                'start_date': '2024-12-02',
                'end_date': '2024-12-06'
            },
            'n_s': 3,
            'mode': 'spread'
        }
        
        result = integrated_fetch(test_config)
        
    except Exception as e:
        import traceback
        print("EXACT ERROR TRACEBACK:")
        traceback.print_exc()

if __name__ == "__main__":
    trace_exact_error()