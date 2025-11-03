#!/usr/bin/env python3
"""
Test DEBY Contract Parsing
==========================

Debug why deby1_25 contracts result in 0 fetched data
"""

import sys
import os

# Add paths
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')
sys.path.insert(0, '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

from src.core.data_fetcher import DataFetcher, DeliveryDateCalculator

def test_deby_contract_parsing():
    """Test how deby contracts are parsed"""
    
    print("🔍 TESTING DEBY CONTRACT PARSING")
    print("=" * 40)
    
    # Test deby1_25 contract
    test_contract = "deby1_25"
    
    # Split the contract like the engine does
    if test_contract.startswith('de'):
        market = 'de'
        product_part = test_contract[2:]  # Remove 'de' prefix
        
        print(f"📊 Original contract: {test_contract}")
        print(f"   Market: {market}")
        print(f"   Product part: {product_part}")
        
        # Extract product and tenor
        if product_part.startswith('b'):
            product = 'b'  # Base
            tenor_part = product_part[1:]  # Remove 'b'
        elif product_part.startswith('p'):
            product = 'p'  # Peak
            tenor_part = product_part[1:]  # Remove 'p'
        else:
            product = 'b'  # Default
            tenor_part = product_part
            
        print(f"   Product: {product}")
        print(f"   Tenor part: {tenor_part}")
        
        # Extract tenor and contract spec
        if tenor_part.startswith('y'):
            tenor = 'y'
            contract_spec = tenor_part[1:]  # Remove 'y'
            print(f"   Tenor: {tenor} (yearly)")
            print(f"   Contract spec: {contract_spec}")
            
            # Test delivery date calculation
            try:
                delivery_date = DeliveryDateCalculator.calc_delivery_date(tenor, contract_spec)
                print(f"   ✅ Delivery date: {delivery_date}")
                
                # Test with data fetcher
                print("\n🔄 Testing DataFetcher initialization...")
                fetcher = DataFetcher()
                
                contract_config = {
                    'market': market,
                    'product': product,
                    'tenor': tenor,
                    'contract': contract_spec,
                    'lookback_days': 30
                }
                
                print(f"   Contract config: {contract_config}")
                
                # Test contract validation
                from src.core.data_fetcher import ContractValidator
                is_valid = ContractValidator.validate_contract(contract_config)
                print(f"   ✅ Contract validation: {is_valid}")
                
                # Try to actually fetch data
                print("\n📡 Attempting to fetch actual data...")
                try:
                    result = fetcher.fetch_contract_data(contract_config, include_trades=True, include_orders=False)
                    
                    if 'trades' in result and not result['trades'].empty:
                        trades_df = result['trades']
                        print(f"   ✅ Trades fetched: {len(trades_df)} records")
                        print(f"   📅 Date range: {trades_df.index.min()} to {trades_df.index.max()}")
                        print(f"   💰 Price range: {trades_df['price'].min():.2f} to {trades_df['price'].max():.2f}")
                    else:
                        print(f"   ⚠️  No trades data returned - this is the problem!")
                        if 'trades' in result:
                            print(f"      Trades DataFrame shape: {result['trades'].shape}")
                        else:
                            print(f"      No 'trades' key in result: {list(result.keys())}")
                            
                except Exception as fetch_error:
                    print(f"   ❌ Error fetching data: {fetch_error}")
                    import traceback
                    traceback.print_exc()
                
            except Exception as e:
                print(f"   ❌ Error in parsing: {e}")
                import traceback
                traceback.print_exc()

def main():
    test_deby_contract_parsing()

if __name__ == "__main__":
    main()