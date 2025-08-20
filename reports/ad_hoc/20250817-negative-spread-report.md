# Ad-Hoc Analysis: Negative Bid-Ask Spreads in DEBM09_25_FRBM09_25

**Date:** 2025-08-17
**Question:** Investigate negative bid-ask spreads where ask price < bid price
**Time spent:** 45 minutes

## Summary

**CRITICAL DATA QUALITY ISSUE IDENTIFIED:**
- **57,969 negative spreads** found out of 243,986 records with both bid/ask prices
- **23.76% negative spread rate** - This is extremely high and indicates serious data quality problems
- Most severe negative spread: **-3.15** (bid 33.15, ask 30.0)
- Negative spreads concentrated on specific dates, particularly **July 17, 2025** (94.0% negative rate)

## Analysis

### Data Overview
- Total records: 254,619
- Records with both bid/ask prices: 243,986 (95.8%)
- Time period: July 1, 2025 to August 14, 2025
- Bid price range: 24.43 to 35.17
- Ask price range: 24.79 to 36.18

### Negative Spread Statistics
- **Count:** 57,969 negative spreads
- **Rate:** 23.76% of all bid/ask pairs
- **Severity range:** -3.15 to -0.000001
- **Mean negative spread:** -0.631

### Temporal Patterns

#### By Hour (Trading Hours)
Most problematic hours:
- **Hour 13 (1 PM):** 39.6% negative spread rate (10,792 / 27,286)
- **Hour 11 (11 AM):** 30.6% negative spread rate (8,434 / 27,571)
- **Hour 12 (12 PM):** 28.0% negative spread rate (6,691 / 23,895)

Better hours:
- **Hour 15 (3 PM):** 13.2% negative spread rate
- **Hour 09 (9 AM):** 17.7% negative spread rate

#### By Date - Worst Offenders
1. **July 17, 2025:** 94.0% negative (20,191 / 21,470) - EXTREMELY BAD
2. **July 14, 2025:** 74.5% negative (13,139 / 17,641)
3. **July 8, 2025:** 76.3% negative (3,018 / 3,955)
4. **July 2, 2025:** 41.5% negative (7,456 / 17,986)
5. **July 9, 2025:** 40.5% negative (7,158 / 17,691)

### Sample Anomalous Records

Most severe negative spreads (July 7, 2025 around 9:10 AM):
```
Timestamp                   Bid     Ask     Spread
2025-07-07 09:10:25.744    33.15   30.0    -3.15
2025-07-07 09:10:50.656    33.15   30.0    -3.15
2025-07-07 09:10:39.609    33.14   30.0    -3.14
2025-07-07 09:10:47.479    33.14   30.0    -3.14
```

### Spread Distribution
- **1st percentile:** -1.50 (negative)
- **5th percentile:** -0.89 (negative)
- **10th percentile:** -0.69 (negative)
- **25th percentile:** 0.00 (zero)
- **50th percentile:** 0.13 (positive)
- **75th percentile:** 0.33 (positive)

## Root Cause Analysis

### Likely Causes
1. **Data feed issues:** Different bid/ask sources with timing misalignment
2. **System clock synchronization:** Bid and ask updates arriving out of sequence
3. **Data processing errors:** Incorrect assignment of bid/ask labels
4. **Market disruption:** Extreme volatility causing quote inversions
5. **Software bugs:** Issues in data capture or transformation logic

### Evidence Supporting Data Feed Issues
- **Temporal clustering:** Bad dates suggest systematic problems on specific days
- **Intraday patterns:** Consistent hourly patterns suggest recurring system issues
- **Magnitude consistency:** Similar negative spread values (-3.15, -3.14) suggest systematic offset

## Limitations

- Analysis limited to existing data structure
- No access to original source timestamps or data provenance
- Cannot determine if issue is in capture, processing, or storage
- No comparison with external reference data

## Recommendations

### Immediate Actions
1. **STOP using this data** for any production trading or risk calculations
2. **Investigate data pipeline** for July 17, 2025 (94% negative rate)
3. **Check data source configuration** for bid/ask feed mapping
4. **Verify system clocks** across all data collection components

### Data Quality Fixes
1. **Implement real-time validation** to reject negative spreads
2. **Add data quality monitoring** with alerts for negative spread rates > 1%
3. **Cross-reference with alternative data sources** for validation
4. **Implement spread reasonableness checks** (e.g., spreads > 5% of mid-price should be flagged)

### Investigation Priorities
1. **July 17, 2025:** Investigate what caused 94% negative spread rate
2. **Hour 13 patterns:** Why is 1 PM consistently problematic?
3. **Data source audit:** Verify bid/ask feed configurations
4. **Historical comparison:** Check if this issue exists in other date ranges

## Next Steps

1. **Formal data quality assessment** of the entire pipeline
2. **Source system investigation** for the most problematic dates
3. **Alternative data validation** using external market data feeds
4. **Data reprocessing** once root cause is identified and fixed

**CRITICAL:** This data should not be used for any financial decisions or risk calculations until these issues are resolved.