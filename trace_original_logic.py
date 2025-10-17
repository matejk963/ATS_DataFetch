#!/usr/bin/env python3
"""
Trace the original logic before our n_s adjustments to understand what SpreadViewer was actually doing
"""

import sys
import os
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines')

from datetime import datetime, timedelta

print("🔍 TRACING ORIGINAL LOGIC BEFORE N_S ADJUSTMENTS")
print("=" * 55)

print("📋 ORIGINAL PROBLEM SUMMARY:")
print("   • User observed price spikes around €32.5 on June 26")
print("   • Original analysis showed €9+ price difference")
print("   • SpreadViewer was generating different prices than DataFetcher")
print("   • Root cause identified: n_s parameter mismatch")

print(f"\n🤔 WHAT WAS THE ORIGINAL SPREADVIEWER LOGIC?")
print("=" * 50)

# The original SpreadViewer logic (before our fixes)
print("📊 Original SpreadViewer product_dates function:")
print("   (dates + n_s * dates.freq).shift(tn, freq=t.upper() + 'S')")
print("   This was forward-looking: adds n_s business days FORWARD")

# Test with June 26, 2025
june_26 = datetime(2025, 6, 26)
n_s = 3

print(f"\n🧪 ORIGINAL LOGIC TEST for June 26, 2025:")
print(f"   📅 Current date: {june_26.strftime('%Y-%m-%d')}")
print(f"   🔧 n_s: {n_s}")

# SpreadViewer's original forward calculation
print(f"\n📈 SpreadViewer Original Forward Logic:")
forward_date = june_26
for i in range(n_s):
    forward_date += timedelta(days=1)
    while forward_date.weekday() > 4:  # Skip weekends
        forward_date += timedelta(days=1)

forward_quarter = ((forward_date.month - 1) // 3) + 1
forward_year = forward_date.year

print(f"   📅 June 26 + {n_s} business days = {forward_date.strftime('%Y-%m-%d')}")
print(f"   📊 Forward date quarter: Q{forward_quarter} {forward_year}")

# For Q4 2025 contracts from this perspective
if forward_quarter == 3 and forward_year == 2025:
    original_sv_mapping = "q_1"  # Q4 from Q3 perspective = 1 quarter ahead
    print(f"   📊 Q4 2025 from Q3 2025 perspective = q_1")
elif forward_quarter == 2 and forward_year == 2025:
    original_sv_mapping = "q_2"  # Q4 from Q2 perspective = 2 quarters ahead
    print(f"   📊 Q4 2025 from Q2 2025 perspective = q_2")
else:
    original_sv_mapping = "q_?"

print(f"   🔵 Original SpreadViewer would use: {original_sv_mapping}")

# DataFetcher's logic (backward-looking)
print(f"\n📊 DataFetcher Logic (backward-looking):")
q2_end = datetime(2025, 6, 30)  # Last day of Q2
last_bday = q2_end
while last_bday.weekday() > 4:
    last_bday -= timedelta(days=1)

business_days_to_end = 0
temp_date = june_26
while temp_date <= last_bday:
    if temp_date.weekday() < 5:
        business_days_to_end += 1
    temp_date += timedelta(days=1)

print(f"   📅 Q2 last business day: {last_bday.strftime('%Y-%m-%d')}")
print(f"   📊 Business days from June 26 to Q2 end: {business_days_to_end}")
print(f"   🔧 n_s threshold: {n_s}")

if business_days_to_end <= n_s:
    original_df_mapping = "q_1"  # Next quarter perspective
    print(f"   📊 {business_days_to_end} ≤ {n_s}, so use NEXT quarter (Q3) perspective")
    print(f"   📊 Q4 2025 from Q3 2025 perspective = q_1")
else:
    original_df_mapping = "q_2"  # Current quarter perspective
    print(f"   📊 {business_days_to_end} > {n_s}, so use CURRENT quarter (Q2) perspective")
    print(f"   📊 Q4 2025 from Q2 2025 perspective = q_2")

print(f"   🟢 Original DataFetcher would use: {original_df_mapping}")

print(f"\n🤯 ORIGINAL MISMATCH:")
print("=" * 25)
if original_sv_mapping == original_df_mapping:
    print(f"   ✅ Both systems originally used: {original_df_mapping}")
    print(f"   🤔 So the original problem wasn't relative period mismatch?")
else:
    print(f"   ❌ Original mismatch: DataFetcher={original_df_mapping}, SpreadViewer={original_sv_mapping}")
    print(f"   💡 This explains the original €9+ price difference")

print(f"\n🔍 WHAT DID OUR CHANGES DO?")
print("=" * 35)

print("🔧 Our Changes:")
print("   1. Modified transition boundary: <= became <")
print("   2. Added period splitting for June 24-July 1")
print("   3. Forced specific relative period mappings")

print(f"\n📊 Current Results Analysis:")
print("   • June 26: DataFetcher €20.25, SpreadViewer €8.69 (€11.56 diff)")
print("   • June 27: DataFetcher €20.03, SpreadViewer €19.88 (€0.15 diff)")

print(f"\n🎯 HYPOTHESIS:")
print("=" * 15)
print("The €8.69 SpreadViewer price suggests it's fetching:")
print("   • Possibly q_3 or q_4 relative period (much shorter timeframe)")
print("   • Or wrong n_s parameter (n_s=4 or 5?)")
print("   • Or different contract altogether")

print(f"\n💭 QUESTIONS TO INVESTIGATE:")
print("=" * 35)
print("1. What relative period is SpreadViewer actually fetching for June 26?")
print("2. Did our period splitting accidentally push June 26 to wrong relative offset?") 
print("3. Is the hardcoded transition_date = datetime(2025, 6, 27) correct?")
print("4. Should we revert to original logic and fix only the n_s interpretation?")

print(f"\n🚨 CRITICAL INSIGHT:")
print("=" * 25)
print("We may have 'fixed' the symptom but created a new bug.")
print("The original SpreadViewer bug might have been simpler to address.")
print("We need to understand what SpreadViewer is ACTUALLY fetching now.")