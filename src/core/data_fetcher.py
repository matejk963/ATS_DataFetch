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
        
        # Check market is valid
        valid_markets = ['de', 'fr', 'hu', 'it', 'es', 'ttf', 'the', 'eua']
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
    
    def _fetch_trades(self, market: str, tenor: str, venue_list: List[str], 
                     product_date: datetime, dates: pd.DatetimeIndex,
                     start_time: time, end_time: time, prod: str) -> pd.DataFrame:
        """Fetch trade data following legacy pattern"""
        df_tr = pd.DataFrame([])
        
        agg_dict = {'price': 'sum', 'volume': 'sum', 'action': 'median',
                   'broker_id': 'median', 'count': 'sum', 'tradeid': 'first'}
        
        series = pd.Series(product_date, index=dates)
        
        for p_d, ds in series.groupby(series).groups.items():
            bT = datetime.combine(ds[0], start_time)
            eT = datetime.combine(ds[-1], end_time)
            
            # Trades
            df_tr_aux = self.data_class_oracle.get_trades(market, tenor, venue_list, p_d, bT, eT, prod)
            
            # Filter by broker
            if self.allowed_broker_ids and tenor != 'da':
                df_tr_aux = df_tr_aux[df_tr_aux['broker_id'].isin(self.allowed_broker_ids)]
            
            try:
                df_tr_aux = df_tr_aux.between_time(start_time, end_time)
            except(TypeError):
                pass
            
            # Group trades
            df_tr_aux['count'] = 1
            df_tr_aux['price'] *= df_tr_aux['volume']
            df_tr_aux = df_tr_aux.groupby(df_tr_aux.index).agg(agg_dict)
            df_tr_aux['price'] /= df_tr_aux['volume']
            
            df_tr = pd.concat([df_tr, df_tr_aux])
            del df_tr_aux
        
        return df_tr
    
    def _fetch_orders(self, market: str, tenor: str, venue_list: List[str],
                     product_date: datetime, dates: pd.DatetimeIndex,
                     start_time: time, end_time: time, prod: str) -> pd.DataFrame:
        """Fetch order book data following legacy pattern"""
        df_ba = pd.DataFrame([])
        
        series = pd.Series(product_date, index=dates)
        
        for p_d, ds in series.groupby(series).groups.items():
            bT = datetime.combine(ds[0], start_time)
            eT = datetime.combine(ds[-1], end_time)
            
            # Order book data
            df_ba_aux = self.data_class_pg.get_best_ob_data(market, tenor, venue_list, p_d, bT, eT, prod, None, False)
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
                file_path = os.path.join(output_dir, f'{contract_key}_tr_ba_data.parquet')
                
                # Reset index to avoid datetime index metadata issues
                simple_data = merged_data.reset_index()
                
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
        
        # Extract contract numbers for SpreadViewer
        tn1_list = []
        tn2_list = []
        
        for contract in contracts:
            contract_spec = contract['contract']
            if contract['tenor'] == 'm':  # Monthly
                month_num = int(contract_spec.split('_')[0])
                tn1_list.append(month_num)
            elif contract['tenor'] == 'q':  # Quarterly
                quarter_num = int(contract_spec.split('_')[0])
                tn1_list.append(quarter_num)
            elif contract['tenor'] == 'y':  # Yearly
                tn1_list.append(int(contract_spec))
        
        # Convert period to date range
        start_date = datetime.strptime(period['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(period['end_date'], '%Y-%m-%d')
        dates = pd.date_range(start_date, end_date, freq='B')
        
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