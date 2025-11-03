#!/usr/bin/env python3
"""
Quarter Shift Logic Analysis
============================

Figure out how many quarters ahead a contract is from a given datetime
considering n_s business day shifts at quarter boundaries
"""

import pandas as pd
from datetime import datetime, timedelta
import sys

def get_last_business_day(date):
    """Get last business day of given date"""
    while date.weekday() > 4:  # 0=Monday, 6=Sunday
        date -= timedelta(days=1)
    return date

def get_quarter_end(year, quarter):
    """Get quarter end date"""
    quarter_ends = {
        1: datetime(year, 3, 31),
        2: datetime(year, 6, 30), 
        3: datetime(year, 9, 30),
        4: datetime(year, 12, 31)
    }
    return quarter_ends[quarter]

def get_quarter_from_date(date):
    """Get quarter number (1-4) from date"""
    return ((date.month - 1) // 3) + 1

def calculate_transition_date(year, quarter, n_s=3):
    """Calculate transition start date (n_s business days before quarter end)"""
    quarter_end = get_quarter_end(year, quarter)
    last_bday = get_last_business_day(quarter_end)
    
    # Go back n_s business days
    transition_start = last_bday
    for _ in range(n_s - 1):
        transition_start -= timedelta(days=1)
        while transition_start.weekday() > 4:
            transition_start -= timedelta(days=1)
    
    return transition_start, last_bday

def analyze_quarterly_perspective(current_date, delivery_quarter, delivery_year, n_s=3):
    """
    Analyze which quarter perspective to use for relative period calculation
    
    Args:
        current_date: The trading date
        delivery_quarter: Quarter of contract delivery (1-4)
        delivery_year: Year of contract delivery
        n_s: Business day transition parameter
    
    Returns:
        tuple: (perspective_quarter, perspective_year, relative_offset)
    """
    current_quarter = get_quarter_from_date(current_date)
    current_year = current_date.year
    
    print(f"\n🔍 ANALYZING: {current_date.strftime('%Y-%m-%d')} → Q{delivery_quarter} {delivery_year}")
    print(f"   Current position: Q{current_quarter} {current_year}")
    
    # Check if current date is in transition period for current quarter
    transition_start, quarter_end = calculate_transition_date(current_year, current_quarter, n_s)
    in_transition = transition_start.date() <= current_date.date() <= quarter_end.date()
    
    print(f"   Q{current_quarter} {current_year} transition: {transition_start.strftime('%Y-%m-%d')} to {quarter_end.strftime('%Y-%m-%d')}")
    print(f"   In transition: {in_transition}")
    
    if in_transition:
        # Use NEXT quarter perspective
        if current_quarter == 4:
            perspective_quarter = 1
            perspective_year = current_year + 1
        else:
            perspective_quarter = current_quarter + 1
            perspective_year = current_year
        print(f"   → Using NEXT quarter perspective: Q{perspective_quarter} {perspective_year}")
    else:
        # Use CURRENT quarter perspective
        perspective_quarter = current_quarter
        perspective_year = current_year
        print(f"   → Using CURRENT quarter perspective: Q{perspective_quarter} {perspective_year}")
    
    # Calculate relative offset
    perspective_quarters = perspective_year * 4 + (perspective_quarter - 1)
    delivery_quarters = delivery_year * 4 + (delivery_quarter - 1)
    relative_offset = delivery_quarters - perspective_quarters
    
    print(f"   Perspective quarters: {perspective_quarters} (Q{perspective_quarter} {perspective_year})")
    print(f"   Delivery quarters: {delivery_quarters} (Q{delivery_quarter} {delivery_year})")
    print(f"   Relative offset: {relative_offset} → q_{relative_offset}")
    
    return perspective_quarter, perspective_year, relative_offset

def test_quarter_transitions():
    """Test quarter transition logic with specific dates"""
    
    print("🔍 QUARTER TRANSITION ANALYSIS")
    print("=" * 60)
    
    # Test contract: debq2_25 (Q2 2025 delivery)
    delivery_quarter = 2
    delivery_year = 2025
    n_s = 3
    
    print(f"\n📊 Testing contract: debq2_25 (Q{delivery_quarter} {delivery_year} delivery)")
    print(f"📊 Using n_s = {n_s} business days")
    
    # Test key dates that showed price jumps
    test_dates = [
        datetime(2024, 12, 26),  # Before Dec 27 jump
        datetime(2024, 12, 27),  # Dec 27 jump date
        datetime(2024, 12, 30),  # After Dec 27
        datetime(2025, 1, 15),   # Mid Q1
        datetime(2025, 2, 26),   # Before Feb 27 jump  
        datetime(2025, 2, 27),   # Feb 27 jump date
        datetime(2025, 2, 28),   # After Feb 27
        datetime(2025, 3, 26),   # Before Q1 end transition
        datetime(2025, 3, 27),   # Q1 transition start
        datetime(2025, 3, 31),   # Q1 end
    ]
    
    results = []
    for test_date in test_dates:
        perspective_q, perspective_y, relative_offset = analyze_quarterly_perspective(
            test_date, delivery_quarter, delivery_year, n_s
        )
        results.append({
            'date': test_date,
            'perspective': f"Q{perspective_q} {perspective_y}",
            'relative_offset': relative_offset
        })
    
    # Show results table
    print(f"\n📋 RESULTS SUMMARY:")
    print(f"{'Date':<12} {'Perspective':<8} {'Relative':<8} {'Notes'}")
    print("-" * 50)
    
    for i, result in enumerate(results):
        date_str = result['date'].strftime('%Y-%m-%d')
        perspective = result['perspective']
        relative = f"q_{result['relative_offset']}"
        
        notes = ""
        if i > 0 and result['relative_offset'] != results[i-1]['relative_offset']:
            notes = "← JUMP!"
            
        print(f"{date_str:<12} {perspective:<8} {relative:<8} {notes}")
    
    # Analysis
    print(f"\n🔍 ANALYSIS:")
    unique_offsets = list(set(r['relative_offset'] for r in results))
    print(f"   Unique relative offsets seen: {unique_offsets}")
    
    # Find transition points
    print(f"\n📅 TRANSITION POINTS:")
    for i in range(1, len(results)):
        if results[i]['relative_offset'] != results[i-1]['relative_offset']:
            prev_date = results[i-1]['date'].strftime('%Y-%m-%d')
            curr_date = results[i]['date'].strftime('%Y-%m-%d')
            prev_offset = results[i-1]['relative_offset']
            curr_offset = results[i]['relative_offset']
            print(f"   {prev_date} (q_{prev_offset}) → {curr_date} (q_{curr_offset})")

def main():
    test_quarter_transitions()

if __name__ == "__main__":
    main()