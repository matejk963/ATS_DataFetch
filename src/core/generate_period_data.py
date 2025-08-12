"""
Period Data Generator Helper Script

This script extracts the period data generation process (steps 1-3) from the main pipeline
and saves the result as pickle files for faster development and testing.

This eliminates the need to fetch from database repeatedly and reduces dependency
during development of the rest of the pipeline.
"""

import pandas as pd
import pickle
import os
import sys
from pathlib import Path
from datetime import datetime

# Add EnergyTrading to Python path for imports  
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

# Add project root to path
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch')

# Default configuration if not available from main file
DEFAULT_DATE_RANGE = {
    'start': '2025-04-01',
    'end': '2025-06-30'
}

DEFAULT_CONTRACT_COMBINATIONS = [
    ['2025-05', '2025-06'],
    ['2025-07', '2025-08'],
    ['2025-09', '2025-10']
]

# Try to import configuration from the main file
try:
    from get_combos_simple import DATE_RANGE, CONTRACT_COMBINATIONS
except ImportError:
    print("⚠️  Could not import configuration from get_combos_simple, using defaults")
    DATE_RANGE = DEFAULT_DATE_RANGE
    CONTRACT_COMBINATIONS = DEFAULT_CONTRACT_COMBINATIONS


class PeriodDataGenerator:
    """
    Helper class to generate and save period data for development/testing.
    """
    
    def __init__(self, output_dir="/mnt/c/Users/krajcovic/Documents/Testing Data/ATS_data"):
        """
        Initialize the period data generator.
        
        Args:
            output_dir (str): Directory to save pickle files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectory for period data
        self.period_data_dir = self.output_dir / "period_data"
        self.period_data_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"📁 Period data will be saved to: {self.period_data_dir}")
    
    def _import_data_tools(self):
        """
        Import data processing tools (same as original).
        """
        try:
            from prefect_data_orchestrator.datamart_inject.strategy_predictors_workflow_tools import (
                process_relative_contracts_to_periods,
                compute_macd_from_period_data,
                compute_swing_points_from_period_data,
                compute_atr_from_period_data
            )
            return (
                process_relative_contracts_to_periods,
                compute_macd_from_period_data,
                compute_swing_points_from_period_data,
                compute_atr_from_period_data
            )
        except ImportError as e:
            print(f"⚠️  Could not import data processing tools: {e}")
            print("📋 Consider using DataFetcher from data_fetcher.py for data retrieval")
            return None, None, None, None
    
    def _fetch_period_data(self, contracts, start_date, end_date):
        """
        Fetch period data from database (step 1-2 from original).
        
        Args:
            contracts (list): List of contract names
            start_date (str): Start date
            end_date (str): End date
            
        Returns:
            dict: Raw period data
        """
        print(f"📊 Fetching period data for {contracts} ({start_date} to {end_date})...")
        
        # Import data tools
        (process_contracts, _, _, _) = self._import_data_tools()
        
        if process_contracts is None:
            # Fallback to DataFetcher if prefect tools not available
            print("🔄 Using DataFetcher as fallback...")
            return self._fetch_with_data_fetcher(contracts, start_date, end_date)
        
        # Fetch the data
        period_data = process_contracts(contracts, start_date, end_date)
        
        print(f"✅ Fetched period data with {len(period_data)} periods")
        return period_data
    
    def _fetch_with_data_fetcher(self, contracts, start_date, end_date):
        """
        Fallback method using DataFetcher when prefect tools are not available.
        
        Args:
            contracts (list): List of contract names  
            start_date (str): Start date
            end_date (str): End date
            
        Returns:
            dict: Period data dictionary
        """
        try:
            from data_fetcher import DataFetcher
            
            print("📡 Using DataFetcher to retrieve period data...")
            
            # Initialize DataFetcher
            fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
            
            # Convert contracts to DataFetcher format
            contract_configs = []
            for contract in contracts:
                # Parse contract string (assumes format like '2025-05' for monthly)
                try:
                    year, month = contract.split('-')
                    contract_configs.append({
                        'market': 'de',  # Default to German market
                        'tenor': 'm',    # Monthly tenor
                        'contract': f"{month}_{year[2:]}",  # Convert to MM_YY format
                        'start_date': start_date,
                        'end_date': end_date
                    })
                except ValueError:
                    print(f"⚠️  Could not parse contract format: {contract}")
                    continue
            
            # Fetch data for all contracts
            if contract_configs:
                results = fetcher.fetch_multiple_contracts(contract_configs)
                
                # Convert to period data format
                period_data = {}
                for contract_key, data in results.items():
                    if 'trades' in data and not data['trades'].empty:
                        # Use trade prices as period data
                        period_data[contract_key] = data['trades']['price']
                
                return period_data
            else:
                return {}
                
        except Exception as e:
            print(f"❌ DataFetcher fallback failed: {e}")
            return {}
    
    def _filter_period_data(self, period_data):
        """
        Filter periods to include only those spanning at least 2 months (step 3 from original).
        
        Args:
            period_data (dict): Raw period data
            
        Returns:
            dict: Filtered period data
        """
        print(f"🔍 Filtering periods (original count: {len(period_data)})...")
        
        # Filter periods with less than two months of data
        filtered_period_data = {}
        
        for period, data in period_data.items():
            if isinstance(data, pd.Series) and len(data) > 0:
                # Check if data spans multiple months
                if hasattr(data.index, 'month'):
                    start_month = data.index[0].month
                    start_year = data.index[0].year
                    end_month = data.index[-1].month
                    end_year = data.index[-1].year
                    
                    if start_month != end_month or start_year != end_year:
                        filtered_period_data[period] = data
                else:
                    # If no datetime index, keep the data
                    filtered_period_data[period] = data
        
        print(f"✅ Filtered to {len(filtered_period_data)} periods spanning ≥2 months")
        
        # Print period details
        for period, data in filtered_period_data.items():
            if hasattr(data.index, 'strftime'):
                start_date = data.index[0]
                end_date = data.index[-1]
                print(f"   📅 {period}: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({len(data):,} rows)")
            else:
                print(f"   📅 {period}: {len(data):,} rows")
        
        return filtered_period_data
    
    def _generate_cache_key(self, contracts, start_date, end_date):
        """
        Generate cache key for the data combination.
        
        Args:
            contracts (list): List of contract names
            start_date (str): Start date  
            end_date (str): End date
            
        Returns:
            str: Cache key
        """
        return f"{'-'.join(sorted(contracts))}_{start_date}_{end_date}"
    
    def _save_period_data(self, period_data, cache_key):
        """
        Save period data as pickle file.
        
        Args:
            period_data (dict): Filtered period data
            cache_key (str): Cache key for filename
        """
        filename = f"period_data_{cache_key}.pkl"
        filepath = self.period_data_dir / filename
        
        print(f"💾 Saving period data to: {filename}")
        
        # Save as pickle
        with open(filepath, 'wb') as f:
            pickle.dump(period_data, f)
        
        # Save metadata
        metadata = {
            'cache_key': cache_key,
            'generated_at': datetime.now().isoformat(),
            'periods': list(period_data.keys()),
            'total_rows': sum(len(df) if isinstance(df, (pd.Series, pd.DataFrame)) else 0 for df in period_data.values()),
            'date_ranges': {}
        }
        
        # Add date range info if data has datetime index
        for period, df in period_data.items():
            if isinstance(df, (pd.Series, pd.DataFrame)) and len(df) > 0:
                if hasattr(df.index, 'strftime'):
                    metadata['date_ranges'][period] = {
                        'start': df.index[0].isoformat(),
                        'end': df.index[-1].isoformat(),
                        'rows': len(df)
                    }
                else:
                    metadata['date_ranges'][period] = {
                        'start': 'N/A',
                        'end': 'N/A', 
                        'rows': len(df)
                    }
        
        metadata_file = self.period_data_dir / f"metadata_{cache_key}.json"
        import json
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        file_size = filepath.stat().st_size / 1024 / 1024  # MB
        print(f"✅ Saved {filename} ({file_size:.1f} MB)")
        print(f"📋 Metadata saved to: metadata_{cache_key}.json")
        
        return filepath
    
    def generate_single_combination(self, contracts, start_date, end_date):
        """
        Generate period data for a single contract/date combination.
        
        Args:
            contracts (list): List of contract names
            start_date (str): Start date
            end_date (str): End date
            
        Returns:
            str: Path to saved pickle file
        """
        print(f"\n{'='*60}")
        print(f"🚀 Generating Period Data")
        print(f"📊 Contracts: {contracts}")
        print(f"📅 Date Range: {start_date} to {end_date}")
        print(f"{'='*60}")
        
        try:
            # Step 1-2: Fetch period data
            raw_period_data = self._fetch_period_data(contracts, start_date, end_date)
            
            # Step 3: Filter period data  
            filtered_period_data = self._filter_period_data(raw_period_data)
            
            # Generate cache key and save
            cache_key = self._generate_cache_key(contracts, start_date, end_date)
            filepath = self._save_period_data(filtered_period_data, cache_key)
            
            print(f"\n✅ Period data generation complete!")
            print(f"📁 Saved to: {filepath}")
            
            return str(filepath)
            
        except Exception as e:
            print(f"❌ Error generating period data: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def generate_all_combinations(self):
        """
        Generate period data for all contract combinations from the configuration.
        
        Returns:
            list: List of saved file paths
        """
        print(f"\n🚀 Generating Period Data for All Combinations")
        print(f"{'='*60}")
        print(f"📅 Date Range: {DATE_RANGE['start']} to {DATE_RANGE['end']}")
        print(f"📊 Contract Combinations: {len(CONTRACT_COMBINATIONS)}")
        print(f"{'='*60}")
        
        saved_files = []
        failed_combinations = []
        
        for i, contracts in enumerate(CONTRACT_COMBINATIONS):
            print(f"\n🎯 Processing combination {i+1}/{len(CONTRACT_COMBINATIONS)}")
            
            try:
                filepath = self.generate_single_combination(
                    list(contracts),
                    DATE_RANGE['start'],
                    DATE_RANGE['end']
                )
                
                if filepath:
                    saved_files.append(filepath)
                else:
                    failed_combinations.append(contracts)
                    
            except Exception as e:
                print(f"❌ Failed combination {contracts}: {str(e)}")
                failed_combinations.append(contracts)
        
        # Summary
        print(f"\n{'='*60}")
        print(f"🎉 Period Data Generation Complete!")
        print(f"✅ Successful: {len(saved_files)}")
        print(f"❌ Failed: {len(failed_combinations)}")
        print(f"📁 Output directory: {self.period_data_dir}")
        
        if failed_combinations:
            print(f"\n❌ Failed combinations:")
            for combo in failed_combinations:
                print(f"   {combo}")
        
        return saved_files
    
    def load_period_data(self, cache_key):
        """
        Load previously saved period data.
        
        Args:
            cache_key (str): Cache key for the data
            
        Returns:
            dict: Period data dictionary
        """
        filename = f"period_data_{cache_key}.pkl"
        filepath = self.period_data_dir / filename
        
        if not filepath.exists():
            raise FileNotFoundError(f"Period data file not found: {filename}")
        
        print(f"📂 Loading period data from: {filename}")
        
        with open(filepath, 'rb') as f:
            period_data = pickle.load(f)
        
        print(f"✅ Loaded period data with {len(period_data)} periods")
        return period_data
    
    def list_available_data(self):
        """
        List all available period data files.
        
        Returns:
            list: List of available cache keys
        """
        pickle_files = list(self.period_data_dir.glob("period_data_*.pkl"))
        cache_keys = [f.stem.replace("period_data_", "") for f in pickle_files]
        
        print(f"\n📋 Available Period Data Files:")
        print(f"{'='*50}")
        
        if not cache_keys:
            print("   No period data files found.")
            return []
        
        for i, cache_key in enumerate(cache_keys):
            metadata_file = self.period_data_dir / f"metadata_{cache_key}.json"
            if metadata_file.exists():
                import json
                try:
                    with open(metadata_file, 'r') as f:
                        metadata = json.load(f)
                    
                    print(f"{i+1:2d}. {cache_key}")
                    print(f"    Generated: {metadata['generated_at']}")
                    print(f"    Periods: {len(metadata['periods'])}")
                    print(f"    Total Rows: {metadata['total_rows']:,}")
                    print()
                except json.JSONDecodeError:
                    print(f"{i+1:2d}. {cache_key} (corrupted metadata)")
            else:
                print(f"{i+1:2d}. {cache_key} (no metadata)")
        
        return cache_keys


def main():
    """
    Main function with usage examples.
    """
    print("📊 Period Data Generator")
    print("=" * 50)
    
    try:
        generator = PeriodDataGenerator()
        
        print("\n🎯 Usage Examples:")
        print("=" * 50)
        print("1. Generate single combination:")
        print("   generator.generate_single_combination(['2025-05', '2025-06'], '2025-04-01', '2025-06-30')")
        print("\n2. Generate all combinations:")
        print("   generator.generate_all_combinations()")
        print("\n3. List available data:")
        print("   generator.list_available_data()")
        print("\n4. Load existing data:")
        print("   data = generator.load_period_data('2025-05-2025-06_2025-04-01_2025-06-30')")
        
        # Show available data first
        print(f"\n📋 Checking for existing data...")
        available = generator.list_available_data()
        
        if available:
            print(f"\n✨ Found {len(available)} existing period data files")
        else:
            print(f"\n🚀 No existing data found. You can run:")
            print("   python src/core/generate_period_data.py")
            print("   to generate period data for all combinations")
        
        return generator
        
    except Exception as e:
        print(f"❌ Error initializing generator: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    generator = main()
    
    if generator:
        print("\n✨ Period Data Generator ready!")
        print("💡 Uncomment the following line to generate all combinations:")
        print("   # results = generator.generate_all_combinations()")
        
        # Uncomment to generate all combinations:
        # results = generator.generate_all_combinations()