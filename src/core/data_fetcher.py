"""
Data Fetcher Implementation for ATS_3

Clean interface with flexible contract-specific date handling for trading data.
Based on ATS_2 data_fetch.py patterns with modern improvements.
"""

import sys
import os
from datetime import datetime, time, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional, Union, Tuple
import pandas as pd
import numpy as np

# Add EnergyTrading to path for TPData imports
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

try:
    from Database.TPData import TPData, TPDataDa
    TPDATA_AVAILABLE = True
except ImportError as e:
    print(f"Warning: TPData import failed: {e}")
    TPDATA_AVAILABLE = False


class BidAskValidator:
    """
    Validator for filtering negative bid-ask spreads.
    
    Prevents financially impossible data where ask price < bid price from 
    entering the system.
    """
    
    def __init__(self, strict_mode: bool = True, log_filtered: bool = True):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, remove invalid records. If False, mark them.
            log_filtered: If True, log details of filtered records.
        """
        self.strict_mode = strict_mode
        self.log_filtered = log_filtered
        self.filtered_count = 0
        self.total_processed = 0
    
    def validate_merged_data(self, df: pd.DataFrame, source_name: str = "DataFetcher") -> pd.DataFrame:
        """
        Validate bid-ask spreads in merged data (trades + orders format).
        
        Args:
            df: DataFrame with 'b_price' and 'a_price' columns
            source_name: Name of data source for logging
            
        Returns:
            Validated DataFrame (filtered or marked invalid records)
        """
        if df.empty:
            return df
        
        # Only validate if both bid and ask columns exist and have data
        if 'b_price' not in df.columns or 'a_price' not in df.columns:
            if self.log_filtered:
                print(f"   ⚠️  No bid/ask columns in {source_name} data - skipping validation")
            return df
        
        # Count records with both bid and ask data
        has_both_prices = df['b_price'].notna() & df['a_price'].notna()
        valid_records = df[has_both_prices]
        
        if valid_records.empty:
            if self.log_filtered:
                print(f"   ⚠️  No records with both bid/ask in {source_name} - skipping validation")
            return df
        
        # Identify negative spreads (ask < bid)
        negative_mask = valid_records['a_price'] < valid_records['b_price']
        negative_count = negative_mask.sum()
        
        self.total_processed += len(valid_records)
        self.filtered_count += negative_count
        
        if negative_count > 0:
            if self.log_filtered:
                worst_spread = (valid_records.loc[negative_mask, 'a_price'] - 
                               valid_records.loc[negative_mask, 'b_price']).min()
                print(f"   🚫 {source_name}: Found {negative_count} negative spreads "
                      f"({negative_count/len(valid_records)*100:.1f}%) - worst: {worst_spread:.3f}")
                
                # Sample of problematic records  
                sample_records = valid_records[negative_mask].head(3)
                for idx, row in sample_records.iterrows():
                    spread_val = row['a_price'] - row['b_price']
                    print(f"      🔍 {idx}: bid={row['b_price']:.3f}, ask={row['a_price']:.3f}, "
                          f"spread={spread_val:.3f}")
            
            if self.strict_mode:
                # Remove records with negative spreads
                invalid_indices = valid_records[negative_mask].index
                df_filtered = df.drop(invalid_indices)
                print(f"      ✅ {source_name}: Removed {negative_count} invalid records "
                      f"({len(df)} → {len(df_filtered)})")
                return df_filtered
            else:
                # Mark invalid records
                df.loc[valid_records[negative_mask].index, 'is_valid'] = False
                print(f"      ⚠️  {source_name}: Marked {negative_count} records as invalid")
        else:
            if self.log_filtered:
                print(f"      ✅ {source_name}: All {len(valid_records)} records have valid spreads")
        
        return df
    
    def get_stats(self) -> Dict:
        """Get validation statistics."""
        return {
            'total_processed': self.total_processed,
            'filtered_count': self.filtered_count,
            'filter_rate': self.filtered_count / max(1, self.total_processed) * 100
        }


class DeliveryDateCalculator:
    """Calculate first delivery dates from tenor/contract specifications"""
    
    @staticmethod
    def calc_delivery_date(tenor: str, contract: str) -> datetime:
        """
        Convert tenor/contract to first delivery date
        
        Args:
            tenor: 'd', 'w', 'm', 'q', 'y', 'da'
            contract: Contract specification (e.g., '07_25', '2_25', '1')
            
        Returns:
            First delivery date
        """
        base_year = 2000
        current_year = datetime.now().year
        
        if tenor.lower() == 'da':
            # Day-ahead: contract is days offset
            days_offset = int(contract)
            return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_offset)
        
        elif tenor.lower() == 'd':
            # Daily: contract is specific date
            return datetime.strptime(contract, '%Y-%m-%d')
        
        elif tenor.lower() == 'w':
            # Weekly: contract is week number_year
            week_str, year_str = contract.split('_')
            year = base_year + int(year_str) if int(year_str) < 50 else 1900 + int(year_str)
            return datetime.strptime(f'{year}-W{week_str}-1', '%Y-W%W-%w')
        
        elif tenor.lower() == 'm':
            # Monthly: contract is MM_YY format
            month_str, year_str = contract.split('_')
            year = base_year + int(year_str) if int(year_str) < 50 else 1900 + int(year_str)
            return datetime(year, int(month_str), 1)
        
        elif tenor.lower() == 'q':
            # Quarterly: contract is Q_YY format
            quarter_str, year_str = contract.split('_')
            year = base_year + int(year_str) if int(year_str) < 50 else 1900 + int(year_str)
            quarter = int(quarter_str)
            month = (quarter - 1) * 3 + 1
            return datetime(year, month, 1)
        
        elif tenor.lower() == 'y':
            # Yearly: contract is YY format
            year = base_year + int(contract) if int(contract) < 50 else 1900 + int(contract)
            return datetime(year, 1, 1)
        
        else:
            raise ValueError(f"Unknown tenor: {tenor}")


class DateRangeResolver:
    """Convert lookback days to start/end dates from delivery date"""
    
    @staticmethod
    def resolve_date_range(delivery_date: datetime, lookback_days: int) -> Tuple[datetime, datetime]:
        """
        Calculate start and end dates based on lookback from delivery date or today
        
        For future contracts (delivery date > today), calculates from today instead
        to ensure we have actual trading data available.
        
        Args:
            delivery_date: First delivery date
            lookback_days: Number of business days to look back
            
        Returns:
            (start_date, end_date) tuple
        """
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # If delivery date is in the future, use today as reference point
        if delivery_date > today:
            end_date = today - timedelta(days=1)  # Yesterday (last complete day)
        else:
            end_date = delivery_date - timedelta(days=1)  # Day before delivery
        
        # Calculate business days backwards
        current_date = end_date
        business_days_counted = 0
        
        while business_days_counted < lookback_days:
            current_date = current_date - timedelta(days=1)
            # Monday=0, Sunday=6; business days are 0-4
            if current_date.weekday() < 5:
                business_days_counted += 1
        
        start_date = current_date
        return start_date, end_date


class ContractValidator:
    """Validate contract specifications"""
    
    @staticmethod
    def validate_contract(contract_config: Dict) -> bool:
        """
        Validate a single contract configuration
        
        Args:
            contract_config: Dictionary containing contract specification
            
        Returns:
            True if valid, raises ValueError if invalid
        """
        required_fields = ['market', 'tenor', 'contract']
        
        # Check required fields
        for field in required_fields:
            if field not in contract_config:
                raise ValueError(f"Missing required field: {field}")
        
        # Check market is valid (including cross-market combinations)
        base_markets = ['de', 'fr', 'hu', 'it', 'es', 'ttf', 'the', 'eua']
        valid_markets = base_markets.copy()
        
        # Add cross-market combinations for spreads (market1_market2 format)
        for market1 in base_markets:
            for market2 in base_markets:
                if market1 != market2:
                    valid_markets.append(f"{market1}_{market2}")
        
        if contract_config['market'] not in valid_markets:
            raise ValueError(f"Invalid market: {contract_config['market']}")
        
        # Check tenor is valid
        valid_tenors = ['da', 'd', 'w', 'm', 'q', 'y']
        if contract_config['tenor'] not in valid_tenors:
            raise ValueError(f"Invalid tenor: {contract_config['tenor']}")
        
        # Check date configuration - either explicit dates or lookback
        has_explicit_dates = 'start_date' in contract_config and 'end_date' in contract_config
        has_lookback = 'lookback_days' in contract_config
        
        if not (has_explicit_dates or has_lookback):
            raise ValueError("Contract must specify either explicit dates (start_date/end_date) or lookback_days")
        
        if has_explicit_dates and has_lookback:
            raise ValueError("Contract cannot specify both explicit dates and lookback_days")
        
        return True


class DataFetcher:
    """
    Main data fetcher class with flexible contract-specific date handling
    
    Features:
    - Contract-specific date ranges (explicit dates or lookback-based)
    - Unified contract configuration for trades and orders
    - Parallel processing with data alignment
    - Clean interface preserving legacy functionality
    """
    
    def __init__(self, trading_hours: Tuple[int, int] = (9, 17), 
                 allowed_broker_ids: Optional[List[int]] = None):
        """
        Initialize DataFetcher
        
        Args:
            trading_hours: (start_hour, end_hour) for filtering
            allowed_broker_ids: List of allowed broker IDs for trade filtering
        """
        self.trading_hours = trading_hours
        self.allowed_broker_ids = allowed_broker_ids or [1441]  # Default to EEX
        
        # Initialize TPData connections
        self.data_class_oracle = None
        self.data_class_pg = None
        self.data_class_da = None
        
        if not TPDATA_AVAILABLE:
            raise RuntimeError("TPData not available. Cannot initialize DataFetcher.")
    
    def _init_connections(self):
        """Initialize database connections"""
        if not self.data_class_oracle:
            self.data_class_oracle = TPData()
            self.data_class_oracle.create_connection('OracleSQL')
        
        if not self.data_class_pg:
            self.data_class_pg = TPData()
            self.data_class_pg.create_connection('PostgreSQL')
        
        if not self.data_class_da:
            self.data_class_da = TPDataDa()
    
    def _resolve_contract_dates(self, contract_config: Dict) -> Tuple[datetime, datetime]:
        """
        Resolve contract configuration to start/end dates
        
        Args:
            contract_config: Contract configuration dictionary
            
        Returns:
            (start_date, end_date) tuple
        """
        ContractValidator.validate_contract(contract_config)
        
        if 'start_date' in contract_config and 'end_date' in contract_config:
            # Explicit dates
            start_date = pd.to_datetime(contract_config['start_date']).to_pydatetime()
            end_date = pd.to_datetime(contract_config['end_date']).to_pydatetime()
            return start_date, end_date
        
        elif 'lookback_days' in contract_config:
            # Lookback from delivery date
            delivery_date = DeliveryDateCalculator.calc_delivery_date(
                contract_config['tenor'], contract_config['contract']
            )
            return DateRangeResolver.resolve_date_range(
                delivery_date, contract_config['lookback_days']
            )
        
        else:
            raise ValueError("Invalid contract configuration")
    
    def fetch_contract_data(self, contract_config: Dict, 
                          include_trades: bool = True, 
                          include_orders: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for a single contract
        
        Args:
            contract_config: Contract configuration
            include_trades: Whether to fetch trade data
            include_orders: Whether to fetch order book data
            
        Returns:
            Dictionary containing fetched data
        """
        self._init_connections()
        
        start_date, end_date = self._resolve_contract_dates(contract_config)
        
        market = contract_config['market']
        tenor = contract_config['tenor']
        contract = contract_config['contract']
        prod = contract_config.get('prod', 'base')
        venue_list = contract_config.get('venue_list', ['eex'])
        
        # Calculate product delivery date
        product_date = DeliveryDateCalculator.calc_delivery_date(tenor, contract)
        
        # Trading hours
        start_time = time(self.trading_hours[0], 0, 0)
        end_time = time(self.trading_hours[1], 0, 0)
        
        result = {}
        
        # Generate business days in the range
        dates = pd.date_range(start_date, end_date, freq='B')
        
        if include_trades:
            result['trades'] = self._fetch_trades(
                market, tenor, venue_list, product_date, 
                dates, start_time, end_time, prod
            )
        
        if include_orders:
            result['orders'] = self._fetch_orders(
                market, tenor, venue_list, product_date,
                dates, start_time, end_time, prod
            )
            result['mid_prices'] = self._calculate_mid_prices(result['orders'])
        
        return result
    
    def fetch_spread_contract_data(self, contract1_config: Dict, contract2_config: Dict,
                                 include_trades: bool = True, 
                                 include_orders: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for a spread contract using two delivery dates
        
        Args:
            contract1_config: First contract configuration
            contract2_config: Second contract configuration  
            include_trades: Whether to fetch trade data
            include_orders: Whether to fetch order book data
            
        Returns:
            Dictionary containing spread contract data
        """
        self._init_connections()
        
        # Resolve dates for both contracts
        start_date1, end_date1 = self._resolve_contract_dates(contract1_config)
        start_date2, end_date2 = self._resolve_contract_dates(contract2_config)
        
        # Use overlapping date range
        start_date = max(start_date1, start_date2)
        end_date = min(end_date1, end_date2)
        
        # Extract contract parameters (using first contract as primary)
        market = contract1_config['market']
        tenor = contract1_config['tenor']
        venue_list = contract1_config.get('venue_list', ['eex'])
        prod = contract1_config.get('prod', 'base')
        
        # Calculate product delivery dates
        product_date1 = DeliveryDateCalculator.calc_delivery_date(
            contract1_config['tenor'], contract1_config['contract']
        )
        product_date2 = DeliveryDateCalculator.calc_delivery_date(
            contract2_config['tenor'], contract2_config['contract']
        )
        
        # Trading hours
        start_time = time(self.trading_hours[0], 0, 0)
        end_time = time(self.trading_hours[1], 0, 0)
        
        result = {}
        
        # Generate business days in the range
        dates = pd.date_range(start_date, end_date, freq='B')
        
        if include_trades:
            result['spread_trades'] = self._fetch_trades(
                market, tenor, venue_list, product_date1, 
                dates, start_time, end_time, prod, product_date2
            )
        
        if include_orders:
            result['spread_orders'] = self._fetch_orders(
                market, tenor, venue_list, product_date1,
                dates, start_time, end_time, prod, product_date2
            )
            result['spread_mid_prices'] = self._calculate_mid_prices(result['spread_orders'])
        
        return result
    
    def _fetch_trades(self, market: str, tenor: str, venue_list: List[str], 
                     product_date: datetime, dates: pd.DatetimeIndex,
                     start_time: time, end_time: time, prod: str, 
                     product_date2: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch trade data following legacy pattern"""
        df_tr = pd.DataFrame([])
        
        agg_dict = {'price': 'sum', 'volume': 'sum', 'action': 'median',
                   'broker_id': 'median', 'count': 'sum', 'tradeid': 'first'}
        
        series = pd.Series(product_date, index=dates)
        
        for p_d, ds in series.groupby(series).groups.items():
            bT = datetime.combine(ds[0], start_time)
            eT = datetime.combine(ds[-1], end_time)
            
            # Trades
            try:
                if '_' in market:
                    # Cross-market spread: same delivery date, different markets (identified by market="de_fr")
                    df_tr_aux = self.data_class_oracle.get_trades(market, tenor, venue_list, p_d, bT, eT, prod)
                elif product_date2 is not None:
                    # Spread contract: different delivery dates
                    df_tr_aux = self.data_class_oracle.get_trades(market, tenor, venue_list, p_d, bT, eT, prod, product_date2)
                else:
                    # Single contract: original behavior
                    df_tr_aux = self.data_class_oracle.get_trades(market, tenor, venue_list, p_d, bT, eT, prod)
            except AttributeError as e:
                if "microsecond" in str(e):
                    print(f"⚠️  Pandas index microsecond compatibility issue, returning empty trades")
                    df_tr_aux = pd.DataFrame()
                else:
                    raise e
            
            # Filter by broker
            if self.allowed_broker_ids and tenor != 'da' and not df_tr_aux.empty:
                df_tr_aux = df_tr_aux[df_tr_aux['broker_id'].isin(self.allowed_broker_ids)]
            
            try:
                df_tr_aux = df_tr_aux.between_time(start_time, end_time)
            except(TypeError):
                pass
            
            # Group trades (only if not empty)
            if not df_tr_aux.empty:
                df_tr_aux['count'] = 1
                df_tr_aux['price'] *= df_tr_aux['volume']
                df_tr_aux = df_tr_aux.groupby(df_tr_aux.index).agg(agg_dict)
                df_tr_aux['price'] /= df_tr_aux['volume']
                
                df_tr = pd.concat([df_tr, df_tr_aux])
            del df_tr_aux
        
        return df_tr
    
    def _fetch_orders(self, market: str, tenor: str, venue_list: List[str],
                     product_date: datetime, dates: pd.DatetimeIndex,
                     start_time: time, end_time: time, prod: str,
                     product_date2: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch order book data following legacy pattern"""
        df_ba = pd.DataFrame([])
        
        series = pd.Series(product_date, index=dates)
        
        for p_d, ds in series.groupby(series).groups.items():
            bT = datetime.combine(ds[0], start_time)
            eT = datetime.combine(ds[-1], end_time)
            
            # Order book data
            try:
                if '_' in market:
                    # Cross-market spread: same delivery date, different markets (identified by market="de_fr")
                    df_ba_aux = self.data_class_pg.get_best_ob_data(market, tenor, venue_list, p_d, bT, eT, prod, None, False)
                
                elif product_date2 is not None:
                    # Spread contract: different delivery dates
                    df_ba_aux = self.data_class_pg.get_best_ob_data(market, tenor, venue_list, p_d, bT, eT, prod, product_date2, False)
                
                else:
                    # Single contract: original behavior  
                    df_ba_aux = self.data_class_pg.get_best_ob_data(market, tenor, venue_list, p_d, bT, eT, prod, None, False)
            except AttributeError as e:
                if "microsecond" in str(e):
                    print(f"⚠️  Pandas index microsecond compatibility issue, returning empty orders")
                    df_ba_aux = pd.DataFrame()
                else:
                    raise e
            if not df_ba_aux.empty:
                df_ba_aux = df_ba_aux.rename(columns={'bidbestprice': 'b_price', 'askbestprice': 'a_price'})
                
                try:
                    df_ba_aux = df_ba_aux.between_time(start_time, end_time)
                except(TypeError):
                    pass
                
                df_ba = pd.concat([df_ba, df_ba_aux])
            del df_ba_aux
        
        return df_ba
    
    def _calculate_mid_prices(self, orders_df: pd.DataFrame) -> pd.Series:
        """Calculate mid prices from order book data"""
        if 'b_price' in orders_df.columns and 'a_price' in orders_df.columns:
            return 0.5 * (orders_df['b_price'] + orders_df['a_price'])
        return pd.Series(dtype=float)
    
    def fetch_multiple_contracts(self, contracts: List[Dict], 
                               include_trades: bool = True,
                               include_orders: bool = True) -> Dict[str, Dict]:
        """
        Fetch data for multiple contracts
        
        Args:
            contracts: List of contract configurations
            include_trades: Whether to fetch trade data
            include_orders: Whether to fetch order book data
            
        Returns:
            Dictionary keyed by contract identifiers
        """
        results = {}
        
        for contract_config in contracts:
            # Generate contract key
            contract_key = f"{contract_config['market']}{contract_config['tenor']}{contract_config['contract']}"
            
            try:
                results[contract_key] = self.fetch_contract_data(
                    contract_config, include_trades, include_orders
                )
                print(f"Successfully fetched data for {contract_key}")
            except Exception as e:
                print(f"Error fetching data for {contract_key}: {e}")
                results[contract_key] = {}
        
        return results
    
    def export_to_parquet(self, contract_data: Dict[str, Dict], output_dir: str):
        """
        Export contract data to parquet files
        
        Args:
            contract_data: Result from fetch_multiple_contracts
            output_dir: Output directory path
        """
        os.makedirs(output_dir, exist_ok=True)
        
        for contract_key, data in contract_data.items():
            if not data:
                continue
            
            # Merge trades, orders, and mid prices
            merged_data = pd.DataFrame()
            
            if 'trades' in data and not data['trades'].empty:
                merged_data = pd.concat([merged_data, data['trades']], axis=1, join='outer')
            
            if 'orders' in data and not data['orders'].empty:
                merged_data = pd.concat([merged_data, data['orders']], axis=1, join='outer')
            
            if 'mid_prices' in data and not data['mid_prices'].empty:
                merged_data = pd.concat([merged_data, data['mid_prices']], axis=1, join='outer')
            
            if not merged_data.empty:
                # Validate bid-ask spreads before saving
                print(f"   🔍 Validating bid-ask spreads for {contract_key}...")
                validator = BidAskValidator(strict_mode=True, log_filtered=True)
                validated_data = validator.validate_merged_data(merged_data, contract_key)
                
                # Log validation summary
                stats = validator.get_stats()
                if stats['total_processed'] > 0:
                    print(f"      📊 Validation summary: {stats['filtered_count']}/{stats['total_processed']} "
                          f"negative spreads filtered ({stats['filter_rate']:.1f}%)")
                
                file_path = os.path.join(output_dir, f'{contract_key}_tr_ba_data.parquet')
                
                # Reset index to avoid datetime index metadata issues
                simple_data = validated_data.reset_index()
                
                # Save with minimal options to prevent corruption
                simple_data.to_parquet(file_path, index=False)
                print(f"Exported {contract_key} to {file_path}")

    def fetch_spread_data(self, 
                         spread_config: Dict,
                         ema_params: Optional[Dict] = None,
                         include_raw_data: bool = False) -> Dict:
        """
        Fetch synthetic spread data using SpreadViewer methodology
        
        Args:
            spread_config: Spread configuration with contracts and coefficients
                {
                    'contracts': [
                        {'market': 'de', 'tenor': 'm', 'contract': '08_25'},
                        {'market': 'de', 'tenor': 'm', 'contract': '09_25'}
                    ],
                    'coefficients': [1, -2],  # Buy contract 1, sell 2x contract 2
                    'period': {
                        'start_date': '2025-07-01',
                        'end_date': '2025-07-31'
                    }
                }
            ema_params: EMA filtering parameters
                {
                    'tau': 5,           # EMA time constant
                    'margin': 0.43,     # Band width
                    'eql_p': -6.25,     # Equilibrium price
                    'w': 0              # Weight (0 = pure EMA)
                }
            include_raw_data: Whether to include individual contract data
            
        Returns:
            Dictionary containing spread data:
            {
                'spread_market_data': DataFrame,    # sm_all - synthetic spread order book
                'spread_trade_data': DataFrame,     # tm_all - synthetic spread trades  
                'spread_filtered_trades': DataFrame, # tm_filtered - EMA filtered trades
                'ema_bands': DataFrame,             # EMA bands for visualization
                'parameters': Dict,                 # Configuration used
                'raw_contracts': Dict               # Individual contract data (if requested)
            }
        """
        if not TPDATA_AVAILABLE:
            raise ImportError("TPData not available - cannot fetch spread data")
        
        # Import SpreadViewer components
        sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')
        from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData, norm_coeff
        from Math.accumfeatures import EMA
        
        # Set default EMA parameters  
        if ema_params is None:
            ema_params = {
                'tau': 5,
                'margin': 0.43,
                'eql_p': -6.25,
                'w': 0
            }
        
        print(f"🔄 Fetching spread data for {len(spread_config['contracts'])} contracts...")
        
        # Extract contract configurations
        contracts = spread_config['contracts']
        coefficients = spread_config['coefficients']
        period = spread_config['period']
        
        # Validate configuration
        if len(contracts) != len(coefficients):
            raise ValueError(f"Contracts count ({len(contracts)}) must match coefficients count ({len(coefficients)})")
        
        # Prepare SpreadViewer parameters
        markets = [c['market'] for c in contracts]
        tenors = [c['tenor'] for c in contracts]
        
        # Convert period to date range first (needed for relative calculation)
        start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
        dates = pd.date_range(start_date, end_date, freq='B')
        
        # Calculate relative tenor offsets for SpreadViewer
        tn1_list = []
        tn2_list = []
        
        # Get the quarter/month of the date range (use start date as reference)
        reference_date = start_date
        
        print(f"🔍 RELATIVE TENOR CALCULATION DEBUG:")
        print(f"   📅 Reference date: {reference_date}")
        print(f"   📅 Date range quarter: Q{((reference_date.month-1)//3)+1} {reference_date.year}")
        
        for contract in contracts:
            contract_spec = contract['contract']
            
            if contract['tenor'] == 'm':  # Monthly
                # Extract month and year from contract (format: 'm1_25' = January 2025)
                parts = contract_spec.split('_')
                target_month = int(parts[0][1:])  # Extract '1' from 'm1'
                target_year = 2000 + int(parts[1])  # Convert '25' to 2025
                
                # Calculate month difference
                ref_months = reference_date.year * 12 + reference_date.month
                target_months = target_year * 12 + target_month
                relative_months = target_months - ref_months
                
                print(f"   📊 Monthly: {contract_spec} → Target M{target_month} {target_year} → Relative: {relative_months} months")
                tn1_list.append(relative_months)
                
            elif contract['tenor'] == 'q':  # Quarterly
                # Extract quarter and year from contract (format: 'q4_25' = Q4 2025)
                parts = contract_spec.split('_')
                target_quarter = int(parts[0][1:])  # Extract '4' from 'q4'
                target_year = 2000 + int(parts[1])  # Convert '25' to 2025
                
                # Calculate current quarter from reference date
                reference_quarter = ((reference_date.month-1)//3) + 1
                reference_year = reference_date.year
                
                # Calculate quarter difference
                ref_quarters = reference_year * 4 + (reference_quarter - 1)
                target_quarters = target_year * 4 + (target_quarter - 1)
                relative_quarters = target_quarters - ref_quarters
                
                print(f"   📊 Quarterly: {contract_spec} → Target Q{target_quarter} {target_year} → Relative: {relative_quarters} quarters")
                print(f"      Reference: Q{reference_quarter} {reference_year}")
                print(f"      Target: Q{target_quarter} {target_year}")
                tn1_list.append(relative_quarters)
                
            elif contract['tenor'] == 'y':  # Yearly
                # Extract year from contract
                target_year = int(contract_spec)
                relative_years = target_year - reference_date.year
                print(f"   📊 Yearly: {contract_spec} → Relative: {relative_years} years")
                tn1_list.append(relative_years)
        
        print(f"   ✅ Final tn1_list (relative tenors): {tn1_list}")
        print(f"   📝 Expected database queries for: {['q_' + str(abs(t)) if t >= 0 else 'q_-' + str(abs(t)) for t in tn1_list]}")
        
        # Initialize SpreadViewer classes
        spread_class = SpreadSingle(markets, tenors, tn1_list, tn2_list, ['eex'])
        data_class = SpreadViewerData()
        data_class_tr = SpreadViewerData()
        db_class = self.tpdata
        
        # Normalize coefficients
        coeff_list = norm_coeff(coefficients, markets)
        
        print(f"📊 Processing {len(dates)} business days...")
        
        # Load order book data
        tenors_list = spread_class.tenors_list
        data_class.load_best_order_otc(
            markets, tenors_list,
            spread_class.product_dates(dates, 3),
            db_class,
            start_time=time(self.trading_hours[0], 0),
            end_time=time(self.trading_hours[1], 25)
        )
        
        # Load trade data
        data_class_tr.load_trades_otc(
            markets, tenors_list, db_class,
            start_time=time(self.trading_hours[0], 0),
            end_time=time(self.trading_hours[1], 25)
        )
        
        # Process daily data
        sm_all = pd.DataFrame([])  # Spread market data
        tm_all = pd.DataFrame([])  # Spread trade data
        
        for d in dates:
            d_range = pd.date_range(d, d)
            
            # Aggregate order book data
            data_dict = spread_class.aggregate_data(
                data_class, d_range, 3,
                start_time=time(self.trading_hours[0], 0),
                end_time=time(self.trading_hours[1], 25)
            )
            
            # Create spread market data
            sm = spread_class.spread_maker(data_dict, coeff_list, trade_type=['cmb', 'cmb']).dropna()
            sm_all = pd.concat([sm_all, sm], axis=0)
            
            # Create spread trade data
            if not sm.empty:
                col_list = ['bid', 'ask', 'volume', 'broker_id']
                trade_dict = spread_class.aggregate_data(
                    data_class_tr, d_range, 3, gran='1s',
                    start_time=time(self.trading_hours[0], 0),
                    end_time=time(self.trading_hours[1], 25),
                    col_list=col_list, data_dict=data_dict
                )
                
                tm = spread_class.add_trades(data_dict, trade_dict, coeff_list, [True] * len(contracts))
                tm_all = pd.concat([tm_all, tm], axis=0)
        
        print(f"✅ Generated {len(sm_all)} spread market points, {len(tm_all)} trade points")
        
        # Calculate EMA bands and filter trades
        em_bands = None
        tm_filtered = pd.DataFrame([])
        
        if not sm_all.empty:
            # Calculate EMA bands (using SpreadViewer logic)
            mid_ser = 0.5 * (sm_all.iloc[:, 0] + sm_all.iloc[:, 1]) if sm_all.shape[1] >= 2 else sm_all.iloc[:, 0]
            mid_list = mid_ser.values
            dif_list = [0.001]
            dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
            
            model = EMA(ema_params['tau'], mid_list[0])
            ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
            ema_list = [ema_params['w'] * ema_params['eql_p'] + (1 - ema_params['w']) * x for x in ema_list]
            
            bands = [[x - ema_params['margin'], x, x + ema_params['margin']] for x in ema_list]
            em_bands = pd.DataFrame(bands, index=mid_ser.index, columns=['lower_band', 'ema_center', 'upper_band'])
            
            # Filter trades using EMA bands
            if not tm_all.empty:
                # Align timestamps
                timestamp = tm_all.index
                ts_new = em_bands.index.union(timestamp)
                em_aligned = em_bands.reindex(ts_new).ffill().reindex(timestamp)
                
                # Apply filtering logic
                tm_filtered = tm_all.copy()
                lb = em_aligned['lower_band']
                ub = em_aligned['upper_band']
                
                tm_filtered.loc[tm_filtered['buy'] > ub, 'buy'] = np.nan
                tm_filtered.loc[tm_filtered['sell'] < lb, 'sell'] = np.nan
                tm_filtered = tm_filtered.dropna(how='all')
                
                print(f"🔍 Filtered to {len(tm_filtered)} valid trading opportunities")
        
        # Prepare result
        result = {
            'spread_market_data': sm_all,
            'spread_trade_data': tm_all,
            'spread_filtered_trades': tm_filtered,
            'ema_bands': em_bands,
            'parameters': {
                'spread_config': spread_config,
                'ema_params': ema_params,
                'coefficients_normalized': coeff_list,
                'trading_hours': self.trading_hours,
                'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            }
        }
        
        # Include raw contract data if requested
        if include_raw_data:
            raw_contracts = {}
            for i, contract in enumerate(contracts):
                contract_key = f"{contract['market']}{contract['tenor']}{contract['contract']}"
                try:
                    raw_data = self.fetch_contract_data(contract, include_trades=True, include_orders=True)
                    raw_contracts[contract_key] = raw_data
                except Exception as e:
                    print(f"⚠️  Could not fetch raw data for {contract_key}: {e}")
            
            result['raw_contracts'] = raw_contracts
        
        return result


def test_tpdata_connectivity():
    """Test basic TPData connectivity"""
    print("Testing TPData connectivity...")
    
    if not TPDATA_AVAILABLE:
        print("❌ TPData not available for import")
        return False
    
    try:
        # Test basic instantiation
        data_class = TPData()
        print("✅ TPData instantiated successfully")
        
        # Test connection creation (without actually connecting)
        print("✅ TPData basic functionality available")
        
        data_class_da = TPDataDa()
        print("✅ TPDataDa instantiated successfully")
        
        return True
        
    except Exception as e:
        print(f"❌ TPData connectivity test failed: {e}")
        return False


if __name__ == "__main__":
    # Run connectivity test
    test_tpdata_connectivity()
    
    # Test delivery date calculator
    print("\nTesting DeliveryDateCalculator...")
    calc = DeliveryDateCalculator()
    
    test_cases = [
        ('m', '07_25'),  # July 2025
        ('q', '2_25'),   # Q2 2025
        ('y', '25'),     # Year 2025
    ]
    
    for tenor, contract in test_cases:
        try:
            delivery_date = calc.calc_delivery_date(tenor, contract)
            print(f"✅ {tenor}:{contract} -> {delivery_date}")
        except Exception as e:
            print(f"❌ {tenor}:{contract} -> {e}")
    
    # Test date range resolver
    print("\nTesting DateRangeResolver...")
    resolver = DateRangeResolver()
    
    delivery_date = datetime(2025, 7, 1)
    start_date, end_date = resolver.resolve_date_range(delivery_date, 90)
    print(f"✅ 90 days before {delivery_date} -> {start_date} to {end_date}")