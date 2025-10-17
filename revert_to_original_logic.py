#!/usr/bin/env python3
"""
Revert the data_fetch_engine.py to original logic by removing our complex modifications
"""

print("🔄 REVERTING TO ORIGINAL LOGIC")
print("=" * 35)

# First, let me backup the current modified version
import shutil
import os

engine_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines/data_fetch_engine.py"
backup_file = "/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/engines/data_fetch_engine_modified_backup.py"

print(f"📋 Backing up modified version...")
shutil.copy2(engine_file, backup_file)
print(f"✅ Backup saved: {backup_file}")

print(f"\n🔄 Key reversions needed:")
print("1. Remove period splitting logic")
print("2. Remove hardcoded transition dates") 
print("3. Remove custom n_s boundary modifications")
print("4. Restore original SpreadViewer product_dates usage")
print("5. Remove calculate_synchronized_product_dates function")

print(f"\n📝 Manual steps required:")
print("1. Simplify convert_absolute_to_relative_periods()")
print("2. Remove calculate_synchronized_product_dates() calls")
print("3. Restore original SpreadViewer configuration")

# Let's create a simplified version of convert_absolute_to_relative_periods
simplified_function = '''
def convert_absolute_to_relative_periods(contract_spec: ContractSpec, 
                                       start_date: datetime, 
                                       end_date: datetime,
                                       n_s: int = 3) -> List[Tuple[RelativePeriod, datetime, datetime]]:
    """
    Convert absolute contract to relative periods - REVERTED TO SIMPLE LOGIC
    
    Uses middle date to determine relative offset without complex transition handling.
    """
    periods = []
    
    # Use middle date to determine perspective
    middle_date = start_date + (end_date - start_date) / 2
    
    if contract_spec.tenor == 'q':
        # Get reference quarter for middle date
        ref_quarter = ((middle_date.month - 1) // 3) + 1
        ref_year = middle_date.year
        
        # Calculate relative offset from reference to delivery
        delivery_quarter = ((contract_spec.delivery_date.month - 1) // 3) + 1
        delivery_year = contract_spec.delivery_date.year
        
        # Simple quarters difference calculation
        ref_quarters = ref_year * 4 + (ref_quarter - 1)
        delivery_quarters = delivery_year * 4 + (delivery_quarter - 1)
        relative_offset = delivery_quarters - ref_quarters
        
        print(f"📊 {contract_spec.contract}: Q{ref_quarter} {ref_year} → Q{delivery_quarter} {delivery_year} = q_{relative_offset}")
        
        if relative_offset > 0:
            relative_period = RelativePeriod(
                relative_offset=relative_offset,
                start_date=start_date,
                end_date=end_date
            )
            periods.append((relative_period, start_date, end_date))
    
    return periods
'''

print(f"\n🔧 Simplified function created")
print("📋 To complete reversion:")
print("1. Replace the complex convert_absolute_to_relative_periods with the simplified version")
print("2. Remove calculate_synchronized_product_dates function entirely")
print("3. Restore original SpreadViewer calls without synchronization")

print(f"\n⚠️  After reversion, we should see:")
print("   • Both DataFetcher and SpreadViewer using same relative periods")
print("   • Original €32 vs €20 price difference returns")
print("   • This will help isolate the REAL bug (not relative period mismatch)")

print(f"\n✅ Reversion plan ready - now need to implement the actual changes")