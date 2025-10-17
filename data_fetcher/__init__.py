"""
Data Fetcher Module
==================

Core components for unified data fetching integrating DataFetcher and SpreadViewer.
"""

from .validators import BidAskValidator
from .contracts import ContractSpec, RelativePeriod, parse_absolute_contract
from .date_utils import (
    calculate_last_business_day,
    calculate_transition_dates,
    convert_absolute_to_relative_periods,
    calculate_synchronized_product_dates
)
from .spreadviewer_integration import (
    fetch_synthetic_spread_multiple_periods,
    create_spreadviewer_config_for_period,
    fetch_spreadviewer_for_period
)
from .data_transformers import (
    transform_orders_to_target_format,
    transform_trades_to_target_format,
    detect_price_outliers
)
from .merger import (
    merge_spread_data,
    create_unified_spreadviewer_data,
    create_unified_real_spread_data
)
from .orchestrator import DataFetchOrchestrator
from .data_fetch_engine import DataFetcher, DeliveryDateCalculator, integrated_fetch

__all__ = [
    'BidAskValidator',
    'ContractSpec',
    'RelativePeriod',
    'parse_absolute_contract',
    'calculate_last_business_day',
    'calculate_transition_dates',
    'convert_absolute_to_relative_periods',
    'calculate_synchronized_product_dates',
    'fetch_synthetic_spread_multiple_periods',
    'create_spreadviewer_config_for_period',
    'fetch_spreadviewer_for_period',
    'transform_orders_to_target_format',
    'transform_trades_to_target_format',
    'detect_price_outliers',
    'merge_spread_data',
    'create_unified_spreadviewer_data',
    'create_unified_real_spread_data',
    'DataFetchOrchestrator',
    'DataFetcher',
    'DeliveryDateCalculator',
    'integrated_fetch'
]