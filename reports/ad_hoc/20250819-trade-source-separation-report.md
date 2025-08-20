# Ad-Hoc Analysis: DataFetcher vs SpreadViewer Trade Separation

**Date:** 2025-08-19  
**Question:** How to separate DataFetcher trades from SpreadViewer trades in merged debq4_25_frbq4_25 dataset  
**Time spent:** 45 minutes

## Summary

- **Clear separation method identified**: Use `broker_id` field
- **Two distinct broker IDs found**: 9999.0 (1432 trades) and 1441.0 (771 trades)  
- **Trade volume**: 2,203 total trade records (5.93% of dataset with price values)

## Analysis

### Dataset Overview
- **Total records**: 37,121
- **Trade records (with price)**: 2,203 (5.93%)
- **Columns available**: price, volume, action, broker_id, count, tradeid, b_price, a_price, '0'
- **Price range**: 7.26 to 42.30
- **Volume range**: 1 to 5 (mostly volume=1)

### Key Findings for Trade Separation

#### 1. BROKER_ID - Primary Separation Method ✓
**CRITICAL FINDING**: Two distinct broker IDs provide clear separation:
- **broker_id = 9999.0**: 1,432 trades (65.0%)
- **broker_id = 1441.0**: 771 trades (35.0%)

This is the **most reliable separation method** as it provides:
- Clean binary classification
- Complete coverage (no missing values)
- Logical source separation

#### 2. ACTION Field - Secondary Indicator
- **action = -1.0**: 1,326 trades (60.2%) 
- **action = 1.0**: 877 trades (39.8%)
- Could represent buy/sell directions from different sources

#### 3. TRADEID Patterns - Format-based Separation
All trade IDs follow Eurex format: `Eurex T7/DEBQF7BQ102025-YYYYMMDD/number/leg`
- **Length patterns**: 30 chars (739), 29 chars (693), 39 chars (681)
- Different lengths might indicate different sources or leg configurations
- All appear to be from same exchange system

#### 4. Volume Patterns
- **Extremely consistent**: 97.7% have volume = 1
- Small variations: volume 2-5 (only 50 trades total)
- Not suitable for source separation

#### 5. Price Precision
- **Mixed decimal precision**: 15 decimals (938), 2 decimals (550), 1 decimal (546)
- Different precision levels might indicate different data sources
- Higher precision (15 decimals) vs standard precision (1-2 decimals)

## Results

### Recommended Separation Strategy

**PRIMARY METHOD - Broker ID Separation:**
```python
def separate_trades_by_broker(df):
    """Separate DataFetcher vs SpreadViewer trades using broker_id"""
    trades = df[df['price'].notna() & (df['price'] > 0)]
    
    # Assuming broker_id 9999 = DataFetcher, 1441 = SpreadViewer
    datafetcher_trades = trades[trades['broker_id'] == 9999.0]
    spreadviewer_trades = trades[trades['broker_id'] == 1441.0]
    
    return datafetcher_trades, spreadviewer_trades
```

**SECONDARY METHOD - Combined Approach:**
```python
def separate_trades_combined(df):
    """Use multiple fields for robust separation"""
    trades = df[df['price'].notna() & (df['price'] > 0)]
    
    # Primary: broker_id
    # Secondary: price precision as validation
    trades['price_decimals'] = trades['price'].astype(str).str.split('.').str[1].str.len().fillna(0)
    
    # High precision (15 decimals) likely from one source
    high_precision_mask = trades['price_decimals'] >= 10
    
    # Cross-validate broker_id with precision patterns
    datafetcher_trades = trades[
        (trades['broker_id'] == 9999.0) | 
        ((trades['broker_id'].isna()) & high_precision_mask)
    ]
    
    spreadviewer_trades = trades[
        (trades['broker_id'] == 1441.0) | 
        ((trades['broker_id'].isna()) & ~high_precision_mask)
    ]
    
    return datafetcher_trades, spreadviewer_trades
```

### Validation Statistics
- **Total trades to separate**: 2,203
- **Source 1 (broker_id 9999)**: 1,432 trades (65.0%)
- **Source 2 (broker_id 1441)**: 771 trades (35.0%)
- **Coverage**: 100% (all trades have broker_id)

## Limitations

1. **No timestamp analysis**: Missing timestamp column prevented temporal pattern analysis
2. **Source assumption**: Cannot definitively confirm which broker_id corresponds to which system without additional metadata
3. **No duplicate analysis**: Could not check for overlapping trades between sources
4. **Single symbol**: Analysis limited to debq4_25_frbq4_25 spread

## Next Steps (if applicable)

1. **Validate broker_id mapping**: Confirm which broker_id corresponds to DataFetcher vs SpreadViewer
2. **Cross-reference with logs**: Check system logs to validate the broker_id assignments
3. **Test on other symbols**: Apply same separation logic to other spread data files
4. **Implement monitoring**: Create automated checks to ensure consistent separation in future data
5. **Add timestamp analysis**: If timestamps become available, analyze temporal patterns for additional validation

## Implementation Code

```python
import pandas as pd

def load_and_separate_trades(file_path):
    """Complete function to load and separate trades"""
    # Load data
    df = pd.read_parquet(file_path)
    
    # Filter for trade records only
    trades = df[df['price'].notna() & (df['price'] > 0)].copy()
    
    print(f"Total trade records: {len(trades)}")
    print(f"Broker ID distribution:")
    print(trades['broker_id'].value_counts())
    
    # Separate by broker_id
    datafetcher_trades = trades[trades['broker_id'] == 9999.0].copy()
    spreadviewer_trades = trades[trades['broker_id'] == 1441.0].copy()
    
    print(f"\nDataFetcher trades (broker_id 9999): {len(datafetcher_trades)}")
    print(f"SpreadViewer trades (broker_id 1441): {len(spreadviewer_trades)}")
    
    return datafetcher_trades, spreadviewer_trades, trades

# Usage example:
file_path = "/path/to/debq4_25_frbq4_25_tr_ba_data.parquet"
df_trades, sv_trades, all_trades = load_and_separate_trades(file_path)
```