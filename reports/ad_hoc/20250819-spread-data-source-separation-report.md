# Ad-Hoc Analysis: Spread Data Source Separation
**Date:** 2025-08-19  
**Question:** How to separate DataFetcher vs SpreadViewer trades within the merged dataset  
**Time spent:** 45 minutes

## Summary
- **Clear data structure pattern identified:** 96% of rows contain bid/ask data (SpreadViewer), 4% contain trade data (DataFetcher)
- **Primary separation method:** Use null/non-null patterns in `price`, `volume`, `action` columns to distinguish sources
- **Data quality issue detected:** 90.5% duplicate rows indicate potential data processing problems

## Analysis

### Data Structure Overview
The dataset contains **15,286 rows** with **9 columns**:
- `price`, `volume`, `action`, `broker_id`, `count`, `tradeid` - Trade-related fields
- `b_price`, `a_price` - Bid/Ask price fields  
- `0` - Computed mid-price field

### Key Finding: Complementary Data Patterns
The analysis reveals a clear complementary pattern between two data types:

**Pattern 1: Trade Data (DataFetcher) - 605 rows (3.96%)**
- Has values in: `price`, `volume`, `action`, `broker_id`, `count`, `tradeid`
- Missing values in: `b_price`, `a_price` (mostly)
- Represents actual trade executions

**Pattern 2: Bid/Ask Data (SpreadViewer) - 14,681 rows (96.04%)**  
- Has values in: `b_price`, `a_price`, `0` (mid-price)
- Missing values in: `price`, `volume`, `action`, `broker_id`, `count`, `tradeid`
- Represents market quotes/order book data

## Results

### Data Source Separation Method
```python
# Separate DataFetcher trades (actual executions)
datafetcher_trades = df[df['price'].notna()]

# Separate SpreadViewer data (bid/ask quotes)  
spreadviewer_data = df[df['b_price'].notna()]
```

### Trade Data Characteristics (DataFetcher)
- **Volume distribution:** Mostly 1.0 (94.2%), with some 2.0, 3.0, 5.0, 6.0
- **Action values:** 1.0 (buy, 150 trades) vs -1.0 (sell, 455 trades)
- **Price range:** $5.90 - $11.35, mean $8.73
- **Broker ID:** Consistent 1441.0 across all trades
- **Time span:** May 2, 2025 to August 14, 2025

### Bid/Ask Data Characteristics (SpreadViewer)
- **Bid prices:** $5.75 - $11.20, mean $8.51
- **Ask prices:** $7.50 - $15.00, mean $10.11  
- **Spread:** Average ~$1.58 ($10.11 - $8.51)
- **Coverage:** Some missing ask prices (8.7% nulls vs 4% bid nulls)

### Data Quality Issues
- **90.5% duplicate rows** - Major concern requiring investigation
- **Missing timestamps** - No explicit datetime column found in index
- **Inconsistent coverage** - Different null patterns between bid/ask

## Limitations
- **No explicit source identifiers** in the data columns
- **Temporal analysis limited** by lack of explicit timestamp column (using index)
- **Data quality issues** may affect reliability of separation method
- **Sample represents only Q1 2025** spread data

## Next Steps (if applicable)

### Immediate Actions
1. **Implement separation logic** using the null/non-null pattern method
2. **Investigate duplicate data** - 90.5% duplication needs resolution
3. **Add explicit source tagging** during data collection/merging process
4. **Validate timestamp handling** - confirm datetime index represents actual trade times

### Recommended Data Structure Improvements
```python
# Suggested additional columns for future data collection
df['data_source'] = 'DataFetcher' | 'SpreadViewer'  # Explicit source tagging
df['record_type'] = 'trade' | 'quote'               # Record type classification  
df['collection_timestamp'] = pd.Timestamp           # When data was collected
```

### Plotting Strategy
```python
# Separate the data sources
trades = df[df['price'].notna()].copy()
quotes = df[df['b_price'].notna()].copy()

# Plot trades from DataFetcher
plt.scatter(trades.index, trades['price'], label='DataFetcher Trades', alpha=0.8)

# Plot bid/ask from SpreadViewer  
plt.scatter(quotes.index, quotes['b_price'], label='SpreadViewer Bids', alpha=0.6)
plt.scatter(quotes.index, quotes['a_price'], label='SpreadViewer Asks', alpha=0.6)
```

### Data Quality Investigation
- **Root cause analysis** of 13,845 duplicate rows out of 15,286 total
- **Timestamp validation** to ensure proper chronological ordering
- **Data lineage review** to understand merging process that created this file

## Visualization Files Generated
1. `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250819-spread-data-source-dist-1.png` - Action distribution
2. `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250819-spread-data-source-dist-2.png` - Broker ID distribution  
3. `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250819-spread-data-trading-dist-3.png` - Price distribution

## Conclusion
The data can be reliably separated using the complementary null patterns in trade vs quote fields. However, the 90.5% duplicate rate suggests significant data quality issues that should be addressed before production use.