#!/usr/bin/env python3
"""
Test basic database connectivity to diagnose the JSON parsing errors
"""

import sys
import os
import json

# Set up paths
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

def test_database_connections():
    """Test basic database connections to diagnose issues"""
    print("🔍 TESTING DATABASE CONNECTIONS")
    print("=" * 50)
    
    try:
        from Database.TPData import TPData
        
        print("✅ TPData imported successfully")
        
        # Test database connection
        db_class = TPData()
        print("✅ TPData class instantiated")
        
        # Test Oracle connection
        print("\n📡 Testing Oracle connection...")
        try:
            db_class.create_connection('OracleSQL')
            print("✅ Oracle connection successful")
        except Exception as e:
            print(f"❌ Oracle connection failed: {e}")
        
        # Test PostgreSQL connection  
        print("\n📡 Testing PostgreSQL connection...")
        try:
            db_class.create_connection('PostgreSQL')
            print("✅ PostgreSQL connection successful")
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            
        # Test a simple data query
        print("\n📊 Testing simple data query...")
        try:
            # Try the same query that SpreadViewer would use
            # This should help identify what's causing the JSON parsing error
            result = db_class.get_best_ob_data(
                'de', 'base', 'm', '2024-12-02', '2024-12-03', 
                start_time='09:00:00', end_time='17:00:00'
            )
            
            if result is not None and not result.empty:
                print(f"✅ Data query successful: {len(result)} records")
                print(f"📋 Columns: {list(result.columns)}")
                print(f"📋 Sample data:")
                print(result.head(2))
            else:
                print("⚠️  Query returned empty result")
                
        except Exception as e:
            print(f"❌ Data query failed: {e}")
            print(f"   Exception type: {type(e)}")
            if "Expecting value" in str(e):
                print("   🎯 This is the JSON parsing error we've been seeing!")
    
    except ImportError as e:
        print(f"❌ Import failed: {e}")
    except Exception as e:
        print(f"❌ General error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_database_connections()