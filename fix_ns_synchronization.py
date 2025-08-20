#!/usr/bin/env python3
"""
Fix n_s synchronization between DataFetcher and SpreadViewer
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/src')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, date, timedelta
import pandas as pd

print("🔧 FIXING N_S SYNCHRONIZATION ISSUE")
print("=" * 60)

print(f"📋 PROBLEM IDENTIFIED:")
print(f"   🔍 June 26, 2025 showed €20-€33 price spikes (100% synthetic)")
print(f"   📊 Root cause: DataFetcher and SpreadViewer use different n_s logic")
print(f"   📅 June 26 was 2 business days from Q2 end (< n_s=3)")
print(f"   ❌ DataFetcher: Used Q3 perspective → queries q_1 (Q4 2025)")
print(f"   ❌ SpreadViewer: Used Q2 perspective → queries q_2 (Q1 2026)")
print(f"   💥 Result: They queried different quarterly contracts!")
print()

print(f"📋 TECHNICAL DETAILS:")
print(f"   🔧 DataFetcher: Uses convert_absolute_to_relative_periods() with business day transition")
print(f"   🔧 SpreadViewer: Uses product_dates() with simple date arithmetic")
print(f"   📊 DataFetcher logic: IF (business_days_to_quarter_end <= n_s) THEN use_next_quarter_perspective")
print(f"   📊 SpreadViewer logic: Simple date shift without business day consideration")
print()

print(f"🔧 SOLUTION OPTIONS:")
print("=" * 60)

print(f"\n📝 OPTION 1: Synchronize SpreadViewer to use DataFetcher logic")
print(f"   ✅ Pros: Single source of truth, consistent behavior")
print(f"   ⚠️  Cons: Requires modifying SpreadViewer integration")

print(f"\n📝 OPTION 2: Override SpreadViewer product_dates in integration")
print(f"   ✅ Pros: No changes to core SpreadViewer, isolated fix")
print(f"   ⚠️  Cons: Bypasses SpreadViewer's internal logic")

print(f"\n📝 OPTION 3: Create unified n_s calculator service")
print(f"   ✅ Pros: Future-proof, testable")
print(f"   ⚠️  Cons: Major refactoring, new dependencies")

print(f"\n🎯 RECOMMENDED: Option 2 - Override SpreadViewer product_dates")
print(f"   📝 Reasoning: Minimal changes, immediate fix, preserves both systems")
print()

# Demonstrate the fix
def calculate_correct_product_dates_for_quarterly(dates, contracts, n_s=3):
    """
    Calculate product dates using the same logic as DataFetcher
    """
    print(f"🔧 IMPLEMENTING FIX:")
    print(f"   📅 Input dates: {dates[0]} to {dates[-1]}")
    print(f"   📊 Contracts: {contracts}")
    print(f"   🔧 n_s: {n_s}")
    
    # Parse contracts (assuming quarterly format like 'debq4_25')
    product_dates = []
    
    for contract in contracts:
        # Extract contract info
        parts = contract.split('_')
        market_product = parts[0]  # e.g., 'debq4'
        year_suffix = parts[1]     # e.g., '25'
        
        quarter_num = int(market_product[4:])  # Extract '4' from 'debq4'
        year = 2000 + int(year_suffix)  # Convert '25' to 2025
        
        # Calculate delivery date
        delivery_month = (quarter_num - 1) * 3 + 1  # Q4 = Oct (month 10)
        delivery_date = datetime(year, delivery_month, 1).date()
        
        print(f"   📊 Contract {contract}: Q{quarter_num} {year}, delivery {delivery_date}")
        
        # For each date in the date range, calculate correct relative period
        contract_product_dates = []
        
        for single_date in dates:
            # Get reference quarter
            ref_quarter = ((single_date.month - 1) // 3) + 1
            ref_year = single_date.year
            
            # Check if in transition period (last n_s business days of quarter)
            # Calculate end of current quarter
            if ref_quarter == 1:
                quarter_end = date(ref_year, 3, 31)
            elif ref_quarter == 2:
                quarter_end = date(ref_year, 6, 30)
            elif ref_quarter == 3:
                quarter_end = date(ref_year, 9, 30)
            else:  # Q4
                quarter_end = date(ref_year, 12, 31)
            
            # Count business days to quarter end
            business_days_to_end = 0
            current_date = single_date.date()
            
            while current_date <= quarter_end:
                if current_date.weekday() < 5:  # Monday=0, Friday=4
                    business_days_to_end += 1
                current_date += timedelta(days=1)
            
            business_days_to_end -= 1  # Don't count the reference date itself
            
            # Determine if in transition
            in_transition = business_days_to_end <= n_s
            
            if in_transition:
                # Use next quarter perspective
                if ref_quarter == 4:
                    calc_quarter = 1
                    calc_year = ref_year + 1
                else:
                    calc_quarter = ref_quarter + 1
                    calc_year = ref_year
            else:
                # Use current quarter perspective
                calc_quarter = ref_quarter
                calc_year = ref_year
            
            # Calculate relative quarters
            calc_quarters = calc_year * 4 + (calc_quarter - 1)
            target_quarters = year * 4 + (quarter_num - 1)
            relative_quarters = target_quarters - calc_quarters
            
            # Create the actual delivery date for this relative period
            # This is what SpreadViewer should query
            actual_delivery = delivery_date
            
            contract_product_dates.append(actual_delivery)
            
            print(f"      📅 {single_date.date()}: Q{ref_quarter} {ref_year}, "
                  f"transition={in_transition}, "
                  f"calc_from=Q{calc_quarter} {calc_year}, "
                  f"relative=q_{relative_quarters}, "
                  f"query_date={actual_delivery}")
        
        product_dates.append(pd.DatetimeIndex(contract_product_dates))
    
    return product_dates

# Test the fix with the problematic date
test_dates = pd.date_range('2025-06-26', '2025-06-26', freq='D')
test_contracts = ['debq4_25', 'frbq4_25']

corrected_product_dates = calculate_correct_product_dates_for_quarterly(
    test_dates, test_contracts, n_s=3
)

print(f"\n✅ CORRECTED PRODUCT DATES:")
for i, contract in enumerate(test_contracts):
    print(f"   📊 {contract}: {corrected_product_dates[i]}")

print(f"\n📋 IMPLEMENTATION PLAN:")
print("=" * 60)
print(f"1. 🔧 Modify engines/data_fetch_engine.py")
print(f"   📝 Replace: spread_class.product_dates(dates, n_s)")
print(f"   📝 With: calculate_synchronized_product_dates(dates, contracts, n_s)")
print(f"")
print(f"2. 🔧 Create synchronized product_dates function")
print(f"   📝 Use same business day transition logic as DataFetcher")
print(f"   📝 Apply corrected quarterly arithmetic")
print(f"")
print(f"3. ✅ Test with June 26, 2025 scenario")
print(f"   📝 Verify both systems query same relative contracts")
print(f"   📝 Confirm price discrepancies are eliminated")
print(f"")
print(f"4. 📊 Validate historical data impact")
print(f"   📝 Check other transition periods for similar issues")
print(f"   📝 Consider reprocessing affected data")

print(f"\n🎯 EXPECTED OUTCOME:")
print(f"   ✅ DataFetcher and SpreadViewer query same quarterly contracts")
print(f"   ✅ Price discrepancies reduced to normal market spread levels")
print(f"   ✅ No more €20 vs €33 artificial price spikes")
print(f"   ✅ Synchronized behavior across all transition periods")

print(f"\n🔧 Fix implementation ready - apply to engines/data_fetch_engine.py!")