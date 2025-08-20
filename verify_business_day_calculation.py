#!/usr/bin/env python3
"""
Verify the business day calculation for June 26, 2025 scenario
"""

from datetime import date, timedelta

def count_business_days_detailed(start_date, end_date):
    """Count business days between two dates with detailed logging"""
    print(f"📅 Counting business days from {start_date} to {end_date}")
    
    business_days = 0
    current_date = start_date
    dates_list = []
    
    while current_date <= end_date:
        weekday = current_date.weekday()  # Monday=0, Sunday=6
        is_business_day = weekday < 5  # Monday-Friday
        
        if is_business_day:
            business_days += 1
            
        dates_list.append({
            'date': current_date,
            'weekday': current_date.strftime('%A'),
            'is_business_day': is_business_day,
            'running_count': business_days if is_business_day else business_days
        })
        
        current_date += timedelta(days=1)
    
    # Print detailed breakdown
    for day_info in dates_list:
        status = "✅ BUSINESS" if day_info['is_business_day'] else "❌ WEEKEND"
        print(f"   {day_info['date']} ({day_info['weekday']}): {status} - Count: {day_info['running_count']}")
    
    return business_days, dates_list

print("🔍 VERIFYING BUSINESS DAY CALCULATION FOR JUNE 26, 2025")
print("=" * 70)

# Test the critical date
test_date = date(2025, 6, 26)  # Thursday
q2_end = date(2025, 6, 30)     # Monday

print(f"📋 Scenario:")
print(f"   📅 Reference date: {test_date} ({test_date.strftime('%A')})")
print(f"   📅 Q2 2025 end: {q2_end} ({q2_end.strftime('%A')})")
print(f"   🔧 n_s parameter: 3 business days")
print()

# Method 1: Include the reference date
print("🔧 METHOD 1: Including reference date in count")
total_days_including_ref, days_list = count_business_days_detailed(test_date, q2_end)
print(f"   📊 Total business days (including {test_date}): {total_days_including_ref}")
print()

# Method 2: Exclude the reference date (commonly used in transition logic)
print("🔧 METHOD 2: Excluding reference date from count (transition logic)")
if test_date < q2_end:
    start_next_day = test_date + timedelta(days=1)
    total_days_excluding_ref, days_list_excl = count_business_days_detailed(start_next_day, q2_end)
    print(f"   📊 Business days from {start_next_day} to {q2_end}: {total_days_excluding_ref}")
else:
    total_days_excluding_ref = 0
    print(f"   📊 Reference date equals end date - 0 days remaining")
print()

# Method 3: "Days remaining" interpretation
print("🔧 METHOD 3: 'Days remaining until transition' interpretation")
remaining_business_days = max(0, total_days_excluding_ref)
print(f"   📊 Business days remaining after {test_date}: {remaining_business_days}")
print()

# Compare with n_s threshold
n_s = 3
print("🎯 TRANSITION ANALYSIS:")
print("=" * 70)

print(f"🔧 n_s threshold: {n_s} business days")

for method, count, description in [
    ("Method 1 (inclusive)", total_days_including_ref, "includes reference date"),
    ("Method 2 (exclusive)", total_days_excluding_ref, "excludes reference date"),
    ("Method 3 (remaining)", remaining_business_days, "days remaining interpretation")
]:
    in_transition = count <= n_s
    print(f"   {method}: {count} days ({description})")
    print(f"      ⚡ In transition (≤ {n_s}): {in_transition}")
    if in_transition:
        print(f"      ✅ Would use NEXT quarter perspective (Q3 2025)")
    else:
        print(f"      ❌ Would use CURRENT quarter perspective (Q2 2025)")
    print()

print("🔍 WHAT DOES 'PERSPECTIVE' MEAN?")
print("=" * 70)
print("When counting relative periods (q_1, q_2, etc.), we need a reference point:")
print()
print("📊 Q2 2025 PERSPECTIVE (current quarter):")
print("   📅 Reference: Q2 2025 (Apr-Jun)")
print("   📊 q_1 = Q2+1 = Q3 2025 (Jul-Sep)")  
print("   📊 q_2 = Q2+2 = Q4 2025 (Oct-Dec)")
print("   📊 q_3 = Q2+3 = Q1 2026 (Jan-Mar)")
print()
print("📊 Q3 2025 PERSPECTIVE (next quarter - transition mode):")
print("   📅 Reference: Q3 2025 (Jul-Sep)") 
print("   📊 q_1 = Q3+1 = Q4 2025 (Oct-Dec)")
print("   📊 q_2 = Q3+2 = Q1 2026 (Jan-Mar)")
print("   📊 q_3 = Q3+3 = Q2 2026 (Apr-Jun)")
print()

print("💥 THIS IS WHY WE SAW PRICE DISCREPANCIES:")
print("=" * 70)
print("❌ BEFORE FIX:")
print("   DataFetcher: Used business day logic → Q3 perspective → q_1=Q4_2025")
print("   SpreadViewer: Used simple date logic → Q2 perspective → q_1=Q3_2025") 
print("   💥 Result: Different contracts queried → €20 vs €33 prices")
print()
print("✅ AFTER FIX:")
print("   DataFetcher: Uses business day logic → Q3 perspective → q_1=Q4_2025")
print("   SpreadViewer: Uses synchronized logic → Q3 perspective → q_1=Q4_2025")
print("   🎉 Result: Same contracts queried → Consistent prices")

print()
print("🎯 RECOMMENDATION:")
print("=" * 70)
print("The exact business day counting method should match DataFetcher's implementation.")
print("Need to verify which method DataFetcher actually uses in convert_absolute_to_relative_periods()")