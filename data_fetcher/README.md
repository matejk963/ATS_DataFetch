# Data Fetcher Module

A modular, production-ready data fetching system that integrates DataFetcher (real spreads) and SpreadViewer (synthetic spreads) for unified trading data retrieval.

## Architecture

The module is organized into logical components:

```
data_fetcher/
├── __init__.py                    # Main exports
├── validators.py                  # BidAskValidator for data quality
├── contracts.py                   # Contract specifications and parsing
├── date_utils.py                  # Date calculations and n_s logic
├── spreadviewer_integration.py    # SpreadViewer synthetic spread integration
├── data_transformers.py           # Data format transformations
├── merger.py                      # Advanced data merging algorithms
├── orchestrator.py               # Main API orchestrator
├── example.py                    # Usage example
└── README.md                     # This file
```

## Quick Start

```python
from data_fetcher import DataFetchOrchestrator

# Initialize
orchestrator = DataFetchOrchestrator()

# Configure spread fetching
config = {
    'contracts': ['debq4_25', 'frbq4_25'],  # German vs French Q4 2025
    'coefficients': [1, -1],                # Buy German, Sell French
    'period': {
        'start_date': '2025-06-24',
        'end_date': '2025-07-01'
    },
    'options': {
        'include_real_spread': True,         # Fetch real market spreads
        'include_synthetic_spread': True,    # Calculate synthetic spreads
        'include_individual_legs': False     # Skip individual legs
    },
    'n_s': 3                                # Business day transition
}

# Execute
results = orchestrator.integrated_fetch(config)
```

## Key Features

### 🔄 **Unified Integration**
- Seamlessly combines DataFetcher (real spreads) and SpreadViewer (synthetic spreads)
- Sophisticated n_s business day transition logic ensures temporal synchronization
- Advanced 4-stage merging pipeline with best bid/ask selection

### 🔍 **Data Quality Assurance**  
- **BidAskValidator**: Filters financially impossible negative bid-ask spreads
- **Outlier Detection**: Rolling z-score analysis removes extreme price movements
- **Validation Pipeline**: Multi-stage validation before data output

### 📊 **Flexible Contract Handling**
- **Single Contracts**: Individual contract data fetching
- **Spread Contracts**: Real and synthetic spread combinations  
- **Cross-Market Spreads**: Different markets (e.g., DE vs FR)
- **Product Encoding**: Supports base/peak products with 2-3 letter market codes

### ⚡ **Production Ready**
- **Modular Architecture**: Clean separation of concerns
- **Error Handling**: Graceful degradation with detailed error reporting
- **Multiple Formats**: Parquet, CSV, PKL output with metadata
- **Cross-Platform**: Windows/Linux compatibility

## Components

### Validators (`validators.py`)
```python
from data_fetcher import BidAskValidator

validator = BidAskValidator(strict_mode=True)
clean_data = validator.validate_orders(raw_data, "DataSource")
```

### Contract Parsing (`contracts.py`)
```python
from data_fetcher import parse_absolute_contract

contract = parse_absolute_contract('debq4_25')
# ContractSpec(market='de', product='base', tenor='q', 
#              contract='4_25', delivery_date=datetime(2025, 10, 1))
```

### Date Utilities (`date_utils.py`)
```python
from data_fetcher import convert_absolute_to_relative_periods

periods = convert_absolute_to_relative_periods(
    contract_spec, start_date, end_date, n_s=3
)
```

### Data Transformation (`data_transformers.py`)
```python
from data_fetcher import transform_orders_to_target_format, detect_price_outliers

# Format conversion
orders = transform_orders_to_target_format(raw_orders, 'spreadviewer')

# Outlier detection
clean_trades = detect_price_outliers(trades_df, z_threshold=5.0)
```

### Advanced Merging (`merger.py`)
```python
from data_fetcher import merge_spread_data

# Sophisticated 4-stage merging
unified = merge_spread_data(real_data, synthetic_data)
```

## Configuration Options

### Contract Specifications
- **Format**: `{market}{product}{tenor}{contract}`
- **Markets**: `de`, `fr`, `ttf`, `nbp`, `peg`, etc.
- **Products**: `b` (base), `p` (peak)  
- **Tenors**: `d`, `w`, `m`, `q`, `y`, `da`
- **Examples**: `debq4_25`, `ttfpm07_25`, `frbm12_24`

### Fetch Options
```python
'options': {
    'include_real_spread': True,      # Market spread contracts via DataFetcher
    'include_synthetic_spread': True, # Calculated spreads via SpreadViewer  
    'include_individual_legs': False  # Individual contract data
}
```

### Output Modes
- **Production**: `test_mode=False` → Parquet only
- **Development**: `test_mode=True` → Parquet + CSV + PKL + Metadata JSON

## Advanced Features

### n_s Business Logic
Sophisticated temporal synchronization ensures DataFetcher and SpreadViewer use identical relative period mappings:

- **Quarterly Example**: June 26th with n_s=3 uses Q3 perspective (q_1) for Q4 delivery
- **Monthly Transitions**: Last 3 business days of each month switch to next month's perspective
- **Consistent Mapping**: Prevents mixing different relative periods within single fetch

### Outlier Detection
Multi-layer outlier detection with:
- **Rolling Z-Score**: Dynamic threshold based on recent volatility
- **Percentage Limits**: Hard caps on price movements  
- **Time Gap Adjustment**: Larger moves allowed for trades further apart
- **Conservative Defaults**: 5.0 z-score threshold, 8% price change limit

### Merging Algorithm
Advanced 4-stage pipeline:
1. **Format Transformation**: Normalize all data to unified schema
2. **Trade Merging**: Simple union of all trade data
3. **Order Merging**: Best bid/ask selection across sources
4. **Final Union**: Combine trades + orders with temporal alignment

## Error Handling

The system provides graceful degradation:
- **Missing Dependencies**: Clear error messages for SpreadViewer/TPData issues
- **Data Source Failures**: Continues with available sources if one fails
- **Validation Errors**: Detailed logging of filtered records
- **Configuration Errors**: Helpful validation of input parameters

## Example Output

```
🔄 Merging real and synthetic spread data (unified pipeline)...
   🔄 Stage 1: Transforming data to target format
      ✅ Formatted: 15,432 real orders, 2,108 real trades
      ✅ Formatted: 18,291 synthetic orders, 1,847 synthetic trades
   🔍 Stage 1.5: Validating bid-ask spreads
      ✅ DataFetcher: All 15,432 records have valid spreads
      ✅ SpreadViewer: All 18,291 records have valid spreads
   📊 Stage 2: Merging trades (union)
      ✅ Merged trades (after outlier filtering): 3,955 records
   🎯 Stage 3: Merging orders (best bid/ask selection)
      ✅ Merged orders: 33,723 records
   🎉 Stage 4: Final union merge (trades + orders → unified DataFrame)
      ✅ Unified dataset: 37,678 total records (trades + orders)
   📁 Saved validated spread data: debq4_25_frbq4_25_tr_ba_data.parquet
```

## Integration with Original Engine

The modular components can be imported and used in the original `data_fetch_engine.py`:

```python
# Replace monolithic functions with modular imports
from data_fetcher import (
    DataFetchOrchestrator,
    BidAskValidator,
    parse_absolute_contract,
    merge_spread_data
)

# Use in existing code
orchestrator = DataFetchOrchestrator()
results = orchestrator.integrated_fetch(config)
```

This modular approach maintains all functionality while improving:
- **Maintainability**: Clear separation of concerns
- **Testability**: Individual components can be unit tested
- **Reusability**: Components can be used independently
- **Extensibility**: Easy to add new data sources or features