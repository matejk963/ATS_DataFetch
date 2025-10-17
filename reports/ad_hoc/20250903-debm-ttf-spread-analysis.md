# Ad-Hoc Analysis: DEBM09_25 vs TTFBM09_25 Spread Analysis

**Date:** 2025-09-03  
**Question:** Comprehensive analysis of spread data between German Base September 2025 (DEBM09_25) and TTF Base September 2025 (TTFBM09_25)  
**Time spent:** 45 minutes  

## Summary

- **Dataset contains 117,674 records** spanning 27 days (July 30 - August 26, 2025)
- **Market shows tight spread** with mean bid-ask spread of 0.06 EUR/MWh
- **Active trading** with 6,660 trades executed, balanced between buy (3,290) and sell (3,370) orders
- **Price range**: 49.35 - 55.44 EUR/MWh with mean of 52.26 EUR/MWh

## Analysis

### Data Structure
The parquet file contains both **order book data** (111,014 records with bid/ask prices) and **trade data** (6,660 executed trades). Key columns include:
- `'0'`: Mid-price (always available)
- `b_price`, `a_price`: Bid and ask prices (order book data)
- `price`, `volume`, `action`: Trade execution data (1=buy, -1=sell)

### Market Characteristics
1. **Tight spread market**: Mean bid-ask spread of only 6 basis points indicates high liquidity
2. **Balanced order flow**: Nearly equal buy/sell trade distribution suggests efficient price discovery
3. **Stable pricing**: Relatively low volatility (1.47 EUR/MWh standard deviation)
4. **Consistent activity**: Continuous market data over 27-day period

## Results

### Key Metrics
- **Mid Price Statistics:**
  - Mean: 52.260 EUR/MWh
  - Standard Deviation: 1.472 EUR/MWh
  - Range: 49.345 - 55.438 EUR/MWh

- **Bid-Ask Spread:**
  - Mean: 0.0599 EUR/MWh (0.11% of mid price)
  - Standard Deviation: 0.0485 EUR/MWh
  - Maximum: 0.885 EUR/MWh

- **Trading Activity:**
  - Total Trades: 6,660
  - Buy Orders: 3,290 (49.4%)
  - Sell Orders: 3,370 (50.6%)
  - Total Volume: 6,660 MWh (1 MWh per trade standard)

### Generated Visualizations

1. **Market Overview** (`20250903-debm-ttf-spread-overview.png`)
   - Time series of mid-prices with trade markers
   - Bid-ask spread evolution over time
   - Hourly trading volume and trade count

2. **Price Distribution Analysis** (`20250903-debm-ttf-distribution-analysis.png`)
   - Mid-price histogram showing normal-like distribution
   - Trade price distribution
   - Bid-ask spread distribution (right-skewed)
   - Intraday price patterns by hour

3. **Market Microstructure** (`20250903-debm-ttf-microstructure.png`)
   - Rolling 1-hour volatility analysis
   - Order book visualization (recent 1000 records)
   - Trade size distribution (uniform 1 MWh lots)
   - Price impact analysis by trade direction

## Limitations

- **Trade volume uniformity**: All trades appear to be 1 MWh standard lots, limiting volume-weighted analysis
- **Single broker data**: All activity from broker_id=1, may not represent full market depth
- **Limited time horizon**: 27-day period may not capture seasonal or longer-term patterns
- **No external factors**: Analysis doesn't incorporate fundamental drivers (weather, storage, etc.)

## Next Steps (if applicable)

For deeper analysis, consider:

1. **Fundamental Analysis**: Correlate price movements with weather data, storage levels, and German/Dutch supply-demand fundamentals
2. **Comparative Studies**: Analyze against historical September contract data or other month spreads
3. **Intraday Patterns**: More granular analysis of trading patterns during European market hours
4. **Market Impact Studies**: Investigate larger order impact if multi-MWh trades become available
5. **Cross-Market Analysis**: Compare with other European gas hub spreads for arbitrage opportunities

## Technical Notes

- Data source: `/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm09_25_ttfbm09_25_tr_ba_data.parquet`
- Analysis script: `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/reports/ad_hoc/20250903-debm-ttf-spread-analysis.py`
- Output plots: `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/`
- Time zone: UTC (assumed)
- Price unit: EUR/MWh
- Volume unit: MWh