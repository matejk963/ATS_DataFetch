#!/usr/bin/env python3
"""
Debug the quarter calculation in our synchronized function
"""

from datetime import datetime

def debug_quarter_calculation():
    print("🔍 DEBUGGING QUARTER CALCULATION")
    print("=" * 50)
    
    # June 26, 2025 scenario
    current_date = datetime(2025, 6, 26).date()
    
    # Reference quarter calculation
    ref_period = ((current_date.month - 1) // 3) + 1  # Q2
    ref_year = current_date.year  # 2025
    
    print(f"📅 Current date: {current_date}")
    print(f"📊 Reference period: Q{ref_period} {ref_year}")
    
    # Since June 26 is in transition period, we use NEXT quarter perspective
    in_transition = True  # We established this from DataFetcher logic
    
    if in_transition:
        if ref_period == 4:
            calc_period = 1
            calc_year = ref_year + 1
        else:
            calc_period = ref_period + 1
            calc_year = ref_year
    else:
        calc_period = ref_period
        calc_year = ref_year
    
    print(f"📊 Calculation perspective: Q{calc_period} {calc_year} (transition mode)")
    print()
    
    # Test relative periods
    for tn in [1, 2]:
        print(f"🔄 Processing q_{tn}:")
        
        # Calculate target quarter
        target_quarter = calc_period + tn - 1
        target_year = calc_year
        
        print(f"   📊 Initial calculation: Q{calc_period} + {tn} - 1 = Q{target_quarter} {target_year}")
        
        # Handle quarter overflow
        overflow_count = 0
        while target_quarter > 4:
            target_quarter -= 4
            target_year += 1
            overflow_count += 1
        
        if overflow_count > 0:
            print(f"   ⚡ Quarter overflow: {overflow_count} year(s) forward")
        
        # Calculate delivery date (first month of quarter)
        delivery_month = (target_quarter - 1) * 3 + 1
        delivery_date = datetime(target_year, delivery_month, 1)
        
        print(f"   📅 Final result: Q{target_quarter} {target_year}")
        print(f"   📦 Delivery date: {delivery_date.date()}")
        print()
    
    print("🎯 EXPECTED RESULTS:")
    print("=" * 50)
    print("Using Q3 2025 perspective (July-Sept):")
    print("   q_1 = Q3+1 = Q4 2025 (Oct-Dec) ✅")
    print("   q_2 = Q3+2 = Q5 → Q1 2026 (Jan-Mar)")
    print()
    print("Q5 wraps around: Q5 = Q1 of next year")
    print("So q_2 should deliver in January 2026, not October 2025")

debug_quarter_calculation()