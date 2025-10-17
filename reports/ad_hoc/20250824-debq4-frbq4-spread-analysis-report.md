# Ad-Hoc Analysis: DEBQ4_25 vs FRBQ4_25 Spread Analysis
**Date:** 2025-08-24  
**Question:** Comprehensive analysis of spread data for DEBQ4_25 vs FRBQ4_25 energy futures  
**Time spent:** 45 minutes  

## Summary
* **Dataset covers 45 days** (July 1 - August 15, 2025) with 120,268 total records
* **Primary data is bid/ask quotes** (117,602 records) with limited trade data (2,666 trades)
* **Average spread is 0.305 points** (162 basis points), indicating moderate liquidity
* **Price range from 16.74 to 20.99** showing significant price movement over the period
* **Clear intraday patterns** with wider spreads during market open/close

## Analysis

### Data Structure
- **Total Records:** 120,268
- **Quote Records:** 117,602 (97.8% of data)
- **Trade Records:** 2,666 (2.2% of data)
- **Time Period:** July 1, 2025 to August 15, 2025 (45 days)
- **Instruments:** DEBQ4_25 (December German Power) vs FRBQ4_25 (December French Power)

### Data Quality
The dataset is primarily composed of bid/ask quotes with synthetic trade markers. The high quote-to-trade ratio (44:1) suggests this is primarily market-making data rather than active trading data.

## Results

### Spread Statistics
- **Mean Spread:** 0.305 points
- **Median Spread:** 0.190 points (indicating right-skewed distribution)
- **Standard Deviation:** 0.367 points
- **Range:** 0.000 to 4.430 points
- **Average Spread (basis points):** 162 bps
- **Median Spread (basis points):** 101 bps

### Price Dynamics
- **Average Mid Price:** 18.79 points
- **Price Range:** 16.74 - 20.99 points (~25% total range)
- **Price Volatility:** 0.78 points standard deviation
- **Correlation (Mid Price vs Spread):** Moderate positive correlation observed

### Intraday Patterns
- **Market Hours:** 9:00 - 17:00 CET
- **Spread Pattern:** Wider spreads at market open (9:00-10:00) and close (16:00-17:00)
- **Tightest Spreads:** Mid-day period (11:00-15:00)
- **Volume Concentration:** Limited trade data shows sporadic activity throughout the day

### Trading Activity
- **Total Trades:** 2,666 over 45 days (~59 trades/day)
- **Average Trade Size:** 1.03 lots
- **Trade Size Range:** 1-6 lots
- **Buy/Sell Balance:** Slightly more sells (-0.13 average action)

### Market Microstructure
- **Bid-Ask Bounce:** Minimal, suggesting stable market making
- **Price Impact:** Low correlation between price changes and spread changes
- **Liquidity:** Moderate, with occasional wide spread events (>2 points)

## Key Findings

### 1. Market Liquidity
The spread averages 162 basis points, which is reasonable for European power futures but indicates this is not a highly liquid market. The median spread of 101 bps suggests that half the time, liquidity is quite good.

### 2. Price Discovery
- Strong price movement over the period (16.74 to 20.99)
- Bid-ask spreads generally track with volatility
- Mid-price calculation appears accurate based on synthetic trades

### 3. Intraday Dynamics
- Clear U-shaped intraday spread pattern
- Widest spreads during market open/close
- Most efficient pricing during European market hours (11:00-15:00)

### 4. Data Quality Issues
- Very low trade-to-quote ratio suggests synthetic or test data
- Some extreme spread values (>4 points) may be outliers or data quality issues
- Trade IDs all appear to be synthetic ("synth_buy_", "synth_sell_")

## Limitations
- **Synthetic Data:** Trade data appears to be simulated rather than actual market trades
- **Limited Trade Information:** Low trade frequency limits volume analysis
- **Single Instrument Pair:** Analysis limited to DEBQ4_25 vs FRBQ4_25 spread
- **No External Factors:** No correlation with underlying power prices or market events
- **Data Sampling:** Performance sampling used for visualizations may miss short-term patterns

## Next Steps (if applicable)
1. **Validate Data Source:** Confirm whether this is live market data or simulation
2. **Compare to Historical:** Analyze against historical spread patterns for these instruments
3. **Cross-Market Analysis:** Compare spreads to other European power markets
4. **Risk Analysis:** Assess spread volatility and extreme event frequency
5. **Trading Strategy Development:** Use spread patterns for market-making strategies

## Generated Visualizations
1. **Comprehensive Analysis Dashboard** - 10-panel overview with all key metrics
2. **Spread Time Series** - High-resolution spread evolution over 45 days  
3. **Price & Volume Analysis** - Bid/ask/mid price evolution with volume patterns

### File Locations
- `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-comprehensive-analysis.png`
- `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-spread-timeseries.png`
- `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/figures/ad_hoc/20250824-debq4-frbq4-price-volume.png`
- `/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/reports/ad_hoc/20250824-spread-analysis-headless.py`