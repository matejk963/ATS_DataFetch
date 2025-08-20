# Data Fetch Engines

This directory contains production-ready data fetching engines for the ATS_DataFetch system.

## 📁 Available Engines

### `data_fetch_engine.py` - **Primary Data Fetching Engine**

Production-ready engine that integrates SpreadViewer (synthetic) and DataFetcher (real) data sources.

#### 🚀 **Key Features:**
- **Cross-market spread support** (e.g., DE-FR, DE-IT, FR-ES)
- **Multiple data sources**: Real spread data + Synthetic spread data
- **Unified output format**: Single DataFrame with trades + orders
- **Advanced merging**: Best bid/ask selection for order books
- **Multiple output formats**: Parquet, CSV, JSON metadata
- **n_s transition logic**: Handles relative contract periods correctly
- **Error handling**: Robust error recovery and logging

#### 📊 **Output Files:**
- `{contracts}_tr_ba_data.parquet` - Main unified data (Parquet format)
- `{contracts}_tr_ba_data.csv` - Human-readable data (CSV format) 
- `{contracts}_tr_ba_data_metadata.json` - Schema and statistics
- `integration_results_v2.json` - Summary results

#### 💻 **Usage Examples:**

```bash
# Cross-market spread (DE-FR)
python engines/data_fetch_engine.py --contracts debm08_25 frbm08_25 --start-date 2025-07-01 --end-date 2025-07-05 --n-s 3 --mode spread

# Single leg contract
python engines/data_fetch_engine.py --contracts debm08_25 --start-date 2025-07-01 --end-date 2025-07-05 --mode single

# Custom output directory
python engines/data_fetch_engine.py --contracts debm08_25 frbm08_25 --start-date 2025-07-01 --end-date 2025-07-05 --mode spread --output-base /path/to/output
```

#### 🔧 **Configuration Options:**

- `--contracts`: Contract list (e.g., `debm08_25 frbm08_25`)
- `--start-date`: Start date (YYYY-MM-DD format)
- `--end-date`: End date (YYYY-MM-DD format)
- `--n-s`: Business day transition parameter (default: 3)
- `--mode`: Processing mode (`spread` or `single`)
- `--coefficients`: Spread coefficients (default: `[1, -1]`)

#### 📈 **Data Sources:**

1. **DataFetcher (Real Data)**:
   - Real executed spread trades
   - Cross-market spread order book data
   - Direct database queries (Oracle + PostgreSQL)

2. **SpreadViewer (Synthetic Data)**:
   - Synthetic spread construction from individual legs
   - Order book synthesis with bid/ask optimization
   - Multiple relative period processing

#### 🎯 **Output Schema:**

| Column | Type | Description |
|--------|------|-------------|
| `price` | float | Trade execution price (EUR/MWh) |
| `volume` | int | Trade volume (MWh) |
| `action` | int | Trade direction (1=buy, -1=sell) |
| `broker_id` | int | Broker identifier |
| `count` | int | Number of trades aggregated |
| `tradeid` | str | Unique trade identifier |
| `b_price` | float | Best bid price (EUR/MWh) |
| `a_price` | float | Best ask price (EUR/MWh) |
| `0` | float | Reserved column |

#### ⚡ **Performance:**
- **Typical runtime**: 30-60 seconds for 4-day periods
- **Memory usage**: ~500MB for 40K records
- **Database connections**: Automatic connection pooling
- **Error recovery**: Graceful handling of connection issues

---

## 🔄 **Development Workflow**

1. **Development**: Use `sandbox/integration_related/` for testing
2. **Testing**: Verify with small date ranges first
3. **Production**: Use `engines/data_fetch_engine.py` for production runs
4. **Monitoring**: Check output files and metadata for validation

## 📚 **Dependencies**

- Python 3.8+
- pandas, numpy, matplotlib
- Database drivers (Oracle, PostgreSQL)
- SpreadViewer (EnergyTrading repository)
- DataFetcher (ATS_DataFetch/src/core)

## 🛠️ **Troubleshooting**

- **No data returned**: Check database connectivity and contract dates
- **Cross-market validation**: Ensure market combinations exist in database
- **Memory issues**: Reduce date range or enable data chunking
- **SpreadViewer errors**: Verify EnergyTrading repository integration