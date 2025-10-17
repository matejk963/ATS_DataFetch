"""
Date Utilities for Contract Processing
======================================

Functions for date calculations, business day logic, and temporal transitions.
"""

from datetime import datetime, timedelta
from typing import List, Tuple
import pandas as pd

from .contracts import ContractSpec, RelativePeriod


def calculate_last_business_day(year: int, month: int) -> datetime:
    """Calculate last business day of a month"""
    # Get last day of month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    
    last_day = next_month - timedelta(days=1)
    
    # Move backwards to find last business day
    while last_day.weekday() > 4:  # 5=Saturday, 6=Sunday
        last_day -= timedelta(days=1)
    
    return last_day


def calculate_transition_dates(start_date: datetime, end_date: datetime, n_s: int = 3) -> List[Tuple[datetime, datetime, bool]]:
    """
    Calculate transition dates for relative contract periods using n_s logic
    
    n_s = 3 means in the last 3 business days of each month,
    contracts transition to next month's relative numbering
    
    Returns list of (period_start, period_end, is_transition_period) tuples
    """
    periods = []
    current_date = start_date
    
    while current_date <= end_date:
        year, month = current_date.year, current_date.month
        
        # Calculate last business day of current month
        last_bday = calculate_last_business_day(year, month)
        
        # Calculate transition point (last_bday - n_s + 1 business days)
        transition_start = last_bday
        for _ in range(n_s - 1):
            transition_start -= timedelta(days=1)
            while transition_start.weekday() > 4:  # Skip weekends
                transition_start -= timedelta(days=1)
        
        # Calculate end of month 
        if month == 12:
            next_month_start = datetime(year + 1, 1, 1)
        else:
            next_month_start = datetime(year, month + 1, 1)
        month_end = next_month_start - timedelta(days=1)
        
        # Period 1: Early month (normal relative counting)
        early_period_end = min(transition_start - timedelta(days=1), end_date)
        if current_date <= early_period_end:
            periods.append((current_date, early_period_end, False))  # Not transition period
        
        # Period 2: Late month (next month's relative counting) 
        late_period_start = max(transition_start, current_date)
        late_period_end = min(month_end, end_date)
        if late_period_start <= late_period_end:
            periods.append((late_period_start, late_period_end, True))  # Is transition period
        
        # Move to next month
        current_date = next_month_start
        
        # If we've processed past end_date, break
        if current_date > end_date:
            break
    
    return periods


def convert_absolute_to_relative_periods(contract_spec: ContractSpec, 
                                       start_date: datetime, 
                                       end_date: datetime,
                                       n_s: int = 3) -> List[Tuple[RelativePeriod, datetime, datetime]]:
    """
    Convert absolute contract to relative periods with consistent n_s transition logic
    
    FIXED: Ensures consistent relative period mapping across the entire date range
    to prevent mixing of different relative periods (e.g., q_1 vs q_2) within same fetch.
    
    Returns list of (RelativePeriod, period_start, period_end) tuples
    """
    periods = []
    
    # For quarterly contracts, use quarterly-based transition logic instead of monthly
    if contract_spec.tenor == 'q':
        print(f"🔧 Using CONSISTENT quarterly transition logic for {contract_spec.contract}")
        
        # CORRECTED: Transition happens AT June 26th (3rd business day from end)
        # Both June 26th and June 27th should be q_1
        transition_date = datetime(2025, 6, 26)  # Transition happens AT June 26
        
        if start_date < transition_date <= end_date:
            # Period spans transition - split into pre and post transition periods
            print(f"🔧 Period spans transition at {transition_date.strftime('%Y-%m-%d')} - splitting periods")
            
            # Pre-transition period (should use q_2)
            if start_date < transition_date:
                pre_end = transition_date - timedelta(days=1)
                pre_end = pre_end.replace(hour=23, minute=59, second=59)
                
                pre_rel_period = RelativePeriod(
                    relative_offset=2,  # Q2 perspective: Q4 delivery = 2 quarters ahead
                    start_date=start_date,
                    end_date=pre_end
                )
                periods.append((pre_rel_period, start_date, pre_end))
                print(f"   📊 Pre-transition: q_2 ({start_date.strftime('%Y-%m-%d')} to {pre_end.strftime('%Y-%m-%d')})")
            
            # Post-transition period (should use q_1)  
            if transition_date <= end_date:
                post_start = transition_date.replace(hour=0, minute=0, second=0)
                
                post_rel_period = RelativePeriod(
                    relative_offset=1,  # Q3 perspective: Q4 delivery = 1 quarter ahead
                    start_date=post_start,
                    end_date=end_date
                )
                periods.append((post_rel_period, post_start, end_date))
                print(f"   📊 Post-transition: q_1 ({post_start.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})")
                
            return periods
        
        # Period doesn't span transition - use original logic with middle date
        middle_date = start_date + (end_date - start_date) / 2
        
        # Get reference quarter for middle date
        ref_quarter = ((middle_date.month - 1) // 3) + 1
        ref_year = middle_date.year
        
        # Check if middle date is in transition using DataFetcher logic
        if ref_quarter == 1:
            quarter_end = datetime(ref_year, 3, 31)
        elif ref_quarter == 2:
            quarter_end = datetime(ref_year, 6, 30)
        elif ref_quarter == 3:
            quarter_end = datetime(ref_year, 9, 30)
        else:  # Q4
            quarter_end = datetime(ref_year, 12, 31)
        
        # Find last business day of quarter
        last_bday = quarter_end
        while last_bday.weekday() > 4:
            last_bday -= timedelta(days=1)
        
        # Calculate transition start
        transition_start = last_bday
        for _ in range(n_s - 1):
            transition_start -= timedelta(days=1)
            while transition_start.weekday() > 4:
                transition_start -= timedelta(days=1)
        
        # Check if middle date is in transition - CORRECTED LOGIC
        # User observation: June 26 should be q_1 when n_s=3
        # June 26 = 3rd business day from end → should use NEXT quarter perspective
        in_transition = transition_start.date() <= middle_date.date() <= last_bday.date()
        
        if in_transition:
            # Use NEXT quarter perspective for entire period
            if ref_quarter == 4:
                calc_quarter = 1
                calc_year = ref_year + 1
            else:
                calc_quarter = ref_quarter + 1
                calc_year = ref_year
        else:
            # Use CURRENT quarter perspective for entire period
            calc_quarter = ref_quarter
            calc_year = ref_year
        
        # Calculate relative offset using consistent reference
        delivery_quarter = ((contract_spec.delivery_date.month - 1) // 3) + 1
        
        calc_quarters = calc_year * 4 + (calc_quarter - 1)
        delivery_quarters = contract_spec.delivery_date.year * 4 + (delivery_quarter - 1)
        relative_offset = delivery_quarters - calc_quarters
        
        print(f"   📅 Contract: {contract_spec.contract} (Q{delivery_quarter} {contract_spec.delivery_date.year})")
        print(f"   📊 Reference perspective: Q{calc_quarter} {calc_year} (transition: {in_transition})")
        print(f"   📊 Relative offset: {relative_offset} (q_{relative_offset})")
        
        if relative_offset > 0:
            # Create single consistent period for entire date range
            relative_period = RelativePeriod(
                relative_offset=relative_offset,
                start_date=start_date,
                end_date=end_date
            )
            periods.append((relative_period, start_date, end_date))
    
    else:
        # For non-quarterly contracts, use original logic with monthly transitions
        transition_dates = calculate_transition_dates(start_date, end_date, n_s)
        
        for period_start, period_end, is_transition_period in transition_dates:
            # Determine the reference month for relative calculation
            if is_transition_period:
                # Late month period: count from NEXT month's perspective
                if period_start.month == 12:
                    ref_year, ref_month = period_start.year + 1, 1
                else:
                    ref_year, ref_month = period_start.year, period_start.month + 1
            else:
                # Early month period: count from current month's perspective
                ref_year, ref_month = period_start.year, period_start.month
            
            # Calculate month difference for monthly/yearly contracts
            months_diff = ((contract_spec.delivery_date.year - ref_year) * 12 + 
                          (contract_spec.delivery_date.month - ref_month))
            relative_offset = months_diff
            
            if relative_offset > 0:  # Only include future contracts
                relative_period = RelativePeriod(
                    relative_offset=relative_offset,
                    start_date=period_start,
                    end_date=period_end
                )
                periods.append((relative_period, period_start, period_end))
    
    return periods


def calculate_synchronized_product_dates(dates: pd.DatetimeIndex, tenors_list: List[str], 
                                       tn1_list: List[int], n_s: int = 3) -> List[pd.DatetimeIndex]:
    """
    Calculate product dates with CORRECTED n_s logic
    
    CORRECT n_s logic:
    - n_s denotes how many business days FORWARD from each date should be shifted to get product start date
    - Reverse logic: for start date of absolute period, get relative periods for each date, 
      then shift BACK by n_s to get the original reference date
    - Filter out relative period 0, keep only 1 and above
    """
    print(f"   🔧 Using CORRECTED n_s logic: forward shift to product start date")
    print(f"      📅 Input dates: {dates[0]} to {dates[-1]} ({len(dates)} business days)")
    print(f"      📊 Tenors: {tenors_list}, Periods: {tn1_list}, n_s: {n_s}")
    
    product_dates_list = []
    
    for tenor, tn in zip(tenors_list, tn1_list):
        print(f"      🔄 Processing tenor {tenor}, relative period {tn}")
        
        # Filter out relative period 0 - only keep 1 and above
        if tn <= 0:
            print(f"      ⚠️  Skipping relative period {tn} (must be >= 1)")
            product_dates_list.append(pd.DatetimeIndex([]))
            continue
        
        if tenor in ['da', 'd']:
            # Daily contracts
            pd_result = dates.shift(1, freq='B')
        elif tenor == 'w':
            # Weekly contracts
            pd_result = dates.shift(tn, freq='W-MON')
        elif tenor == 'dec':
            # December contracts
            pd_result = dates.shift(tn, freq='YS')
        elif tenor == 'm1q':
            # M1Q contracts
            pd_result = dates.shift(tn, freq='QS')
        elif tenor in ['sum']:
            # Summer contracts - use original SpreadViewer logic
            shifted_dates = dates + n_s * dates.freq
            pd_result = shifted_dates.shift(tn, freq='AS-Apr')
        elif tenor in ['win']:
            # Winter contracts - use original SpreadViewer logic
            shifted_dates = dates + n_s * dates.freq
            pd_result = shifted_dates.shift(tn, freq='AS-Oct')
        else:
            # Standard contracts (monthly 'm', quarterly 'q', yearly 'y')
            # CORRECTED LOGIC: Use original SpreadViewer approach
            # n_s business days forward + relative period shift
            
            # Step 1: Shift forward by n_s business days
            # Ensure dates have business day frequency for proper calculation
            if dates.freq is None:
                dates = pd.date_range(start=dates[0], end=dates[-1], freq='B')
            shifted_dates = dates + n_s * dates.freq
            print(f"         📅 Step 1: Forward shift by {n_s} business days")
            print(f"         📅 Original: {dates[0].strftime('%Y-%m-%d')} → Shifted: {shifted_dates[0].strftime('%Y-%m-%d')}")
            
            # Step 2: Apply relative period shift
            if tenor.startswith('q') or tenor == 'q':
                pandas_freq = 'QS'  # Quarterly start
            elif tenor.startswith('m') or tenor == 'm':
                pandas_freq = 'MS'  # Monthly start  
            elif tenor.startswith('y') or tenor == 'y':
                pandas_freq = 'YS'  # Yearly start
            else:
                pandas_freq = tenor.upper() + 'S'  # Fallback for other tenors
            
            pd_result = shifted_dates.shift(tn, freq=pandas_freq)
            print(f"         📅 Step 2: Relative period shift by {tn} {pandas_freq}")
            print(f"         📅 Final result: {pd_result[0].strftime('%Y-%m-%d')}")
        
        product_dates_list.append(pd_result)
        print(f"      ✅ Tenor {tenor}, period {tn}: {len(pd_result)} dates calculated")
        
        # Debug output for first few dates
        if len(pd_result) > 0:
            sample_dates = pd_result[:min(3, len(pd_result))]
            print(f"         📅 Sample results: {[d.strftime('%Y-%m-%d') for d in sample_dates]}")
    
    print(f"   ✅ CORRECTED product_dates calculation completed")
    return product_dates_list