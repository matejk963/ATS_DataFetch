# Ad-Hoc Analysis: Negative Bid-Ask Spreads Check

**Date:** 2025-08-17  
**Question:** Do negative bid-ask spreads still exist after the latest data fetch?  
**Time spent:** 15 minutes

## Summary

**YES** - Negative bid-ask spreads still exist in significant numbers in the latest data file.

- **57,968 negative spreads** found out of 243,985 valid bid-ask pairs
- **23.76% of all valid pairs** show negative spreads (ask < bid)
- File timestamp: 2025-08-17 18:53:57 (today's data)

## Analysis

### File Information
- **File:** `/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_frbm09_25_tr_ba_data.parquet`
- **Last Modified:** 2025-08-17 18:53:57
- **Size:** 2,937,127 bytes
- **Total Records:** 254,597
- **Date Range:** 2025-07-01 09:00:00 to 2025-08-14 17:23:25

### Data Structure
- **Columns:** price, volume, action, broker_id, count, tradeid, b_price, a_price, 0
- **Bid Price Column:** `b_price` (244,267 non-null values)
- **Ask Price Column:** `a_price` (243,985 non-null values)
- **Valid Bid-Ask Pairs:** 243,985 (both bid and ask present)

## Results

### Negative Spread Analysis
- **Total Negative Spreads:** 57,968
- **Percentage of Valid Pairs:** 23.7588%
- **Spread Range:** -3.15 to +5.22
- **Mean Spread:** 0.1215
- **Median Spread:** 0.1300

### Sample Negative Spreads
```
2025-07-01 10:40:13.151: bid=33.4500, ask=33.4400, spread=-0.0100
2025-07-01 10:40:13.333: bid=33.4500, ask=33.4400, spread=-0.0100
2025-07-01 10:40:13.587: bid=33.4500, ask=33.4400, spread=-0.0100
2025-07-01 10:40:14.129: bid=33.4500, ask=33.4400, spread=-0.0100
2025-07-01 10:40:14.577: bid=33.4500, ask=33.4400, spread=-0.0100
```

### Key Observations
1. **High Frequency:** Nearly 1 in 4 bid-ask pairs show negative spreads
2. **Consistent Pattern:** Negative spreads appear consistently throughout the data
3. **Small Magnitudes:** Most negative spreads are small (0.01-0.02), but some reach -3.15
4. **Data Quality Issue:** This indicates a systematic data quality problem

## Limitations

- Analysis assumes `b_price` = bid and `a_price` = ask based on column naming
- Did not analyze temporal patterns or specific time periods
- Did not investigate root cause of negative spreads
- Did not compare with previous data versions for trend analysis

## Next Steps

1. **Immediate:** Investigate data source and processing pipeline for bid-ask price assignment
2. **Root Cause:** Analyze whether this is a data feed issue, processing error, or timing mismatch
3. **Data Cleaning:** Implement validation rules to flag/correct negative spreads
4. **Monitoring:** Set up alerts for negative spread detection in future data fetches

## Conclusion

**ANSWER: YES** - Negative bid-ask spreads still exist in large numbers (57,968 instances, 23.76% of valid pairs) in the latest data file from 2025-08-17. This represents a significant data quality issue that requires immediate attention.