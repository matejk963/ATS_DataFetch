# Analysis: tm_filtered Creation and Buy/Sell Values in SpreadViewer

## 🔍 **Overview**

The `tm_filtered` DataFrame in SpreadViewer represents **filtered synthetic spread trades** that fall within acceptable EMA trading bands. Here's exactly how it's created and what the values mean.

---

## 📊 **Step 1: Trade Data Creation (`tm`)**

### **Data Source**
```python
# Raw trade data loaded from database
data_class_tr.load_trades_otc(market, tenors_list, db_class, 
                              start_time=start_time, end_time=end_time)

# Trade data aggregated for each day
trade_dict = spread_class.aggregate_data(data_class_tr, d_range, n_s, gran=gran_s,
                                         start_time=start_time, end_time=end_time,
                                         col_list=['bid', 'ask', 'volume', 'broker_id'])
```

### **Synthetic Spread Trade Creation**
```python
# Creates synthetic spread trades from individual contract trades
tm = spread_class.add_trades(data_dict, trade_dict, coeff_list, mm_bool)
```

**What `add_trades()` Does:**
1. **Takes individual contract trades** (e.g., DE_M+1 and DE_M+2 trades)
2. **Applies spread coefficients** (e.g., `[1, -2]` meaning buy 1 unit M+1, sell 2 units M+2)
3. **Creates synthetic spread prices** using this logic:

```python
# From spreadviewer_class.py lines ~950-960:
# For positive coefficient (buy the contract):
instr_dict[m + '_' + t + '_bid'] = ['bid', c]  # Buy at bid price
instr_dict[m + '_' + t + '_ask'] = ['ask', c]  # Sell at ask price

# For negative coefficient (sell the contract):
instr_dict[m + '_' + t + '_bid'] = ['ask', -c]  # Buy at ask price (reverse)
instr_dict[m + '_' + t + '_ask'] = ['bid', -c]  # Sell at bid price (reverse)

# Final spread trade prices:
data_['bid'] = data_['bid'].max(axis=1).rename('buy')    # Best buy price for spread
data_['ask'] = data_['ask'].min(axis=1).rename('sell')   # Best sell price for spread
```

---

## 💰 **What Buy/Sell Values Mean**

### **`buy` Column**
- **Definition:** The price at which you can **BUY the spread**
- **Meaning:** Execute the spread strategy by paying this price
- **Example:** If `buy = -75.5`, you pay 75.5 EUR/MWh to enter the spread position
- **Trade Direction:** Long the spread (buy M+1, sell M+2 in this case)

### **`sell` Column**  
- **Definition:** The price at which you can **SELL the spread**
- **Meaning:** Exit or reverse the spread strategy by receiving this price
- **Example:** If `sell = -74.8`, you receive 74.8 EUR/MWh to sell the spread
- **Trade Direction:** Short the spread (sell M+1, buy M+2 in this case)

### **Practical Example**
With coefficients `[1, -2]` (DE_M+1 vs DE_M+2):
```
Individual Contract Prices:
- DE_M+1: bid=50.0, ask=50.2
- DE_M+2: bid=62.0, ask=62.3

Spread Calculation:
- Spread Buy Price = 1×50.0 + (-2)×62.3 = 50.0 - 124.6 = -74.6
- Spread Sell Price = 1×50.2 + (-2)×62.0 = 50.2 - 124.0 = -73.8

tm DataFrame:
- buy = -74.6  (price to buy the spread)
- sell = -73.8 (price to sell the spread)
```

---

## 📈 **Step 2: EMA Band Creation (`em`)**

```python
# Calculate EMA bands for all spread data
em = calc_ema_m(sm_all, tau, margin, w, eql_p)
```

**EMA Bands Structure:**
```python
em = DataFrame with columns [0, 1, 2]:
- Column 0: Lower band (EMA - margin)
- Column 1: Center EMA
- Column 2: Upper band (EMA + margin)
```

**Parameters:**
- `tau=5`: EMA time constant
- `margin=0.43`: Band width around EMA
- `eql_p=-6.25`: Equilibrium price adjustment
- `w=0`: Weight between EMA and equilibrium (0 = pure EMA)

---

## 🔍 **Step 3: Trade Filtering (`adjust_trds`)**

```python
def adjust_trds(df_tr, df_em):
    # Align timestamps
    timestamp = df_tr.index
    ts_new = df_em.index.union(timestamp)
    df_em = df_em.reindex(ts_new).ffill().reindex(timestamp)
    
    # Extract band boundaries
    lb = df_em.iloc[:, 0]  # Lower band
    ub = df_em.iloc[:, 2]  # Upper band
    
    # ⭐ CORE FILTERING LOGIC ⭐
    df_tr.loc[df_tr['buy'] > ub, 'buy'] = np.nan    # Remove expensive buys
    df_tr.loc[df_tr['sell'] < lb, 'sell'] = np.nan  # Remove cheap sells
    
    # Keep only rows with at least one valid trade
    return df_tr.dropna(how='all')
```

### **Filtering Logic Explained**

**1. Remove Expensive Buys:**
```python
df_tr.loc[df_tr['buy'] > ub, 'buy'] = np.nan
```
- **Logic:** If buy price > upper band → remove the buy opportunity
- **Reason:** Spread is overpriced, don't buy above the upper band
- **Example:** If upper band = -74.0 and buy = -73.5, remove this buy (spread too expensive)

**2. Remove Cheap Sells:**
```python
df_tr.loc[df_tr['sell'] < lb, 'sell'] = np.nan  
```
- **Logic:** If sell price < lower band → remove the sell opportunity
- **Reason:** Spread is underpriced, don't sell below the lower band
- **Example:** If lower band = -75.0 and sell = -75.5, remove this sell (spread too cheap)

**3. Keep Valid Trades:**
```python
return df_tr.dropna(how='all')
```
- Only keep rows where at least one trade (buy OR sell) is valid
- Remove rows where both buy AND sell were filtered out

---

## 🎯 **Trading Strategy Interpretation**

### **Valid Trades After Filtering**

**1. Valid Buy Signals:**
- `buy` price ≤ upper band
- **Strategy:** Buy the spread when it's reasonably priced
- **Market View:** Spread will move toward EMA center

**2. Valid Sell Signals:**
- `sell` price ≥ lower band  
- **Strategy:** Sell the spread when it's reasonably priced
- **Market View:** Spread will move toward EMA center

**3. Filtered Out Trades:**
- Buys above upper band (too expensive)
- Sells below lower band (too cheap) 
- **Result:** Only trade when spread is within reasonable bounds

---

## 📊 **Example tm_filtered DataFrame**

```python
                        buy     sell    volume  broker_id
2025-07-01 09:15:23   -74.2    -73.8    1000      1441
2025-07-01 09:16:45     NaN    -75.1     500      1441  # Buy filtered out
2025-07-01 09:18:12   -74.8     NaN     2000      1441  # Sell filtered out
2025-07-01 09:20:33   -74.5    -74.0    1500      1441
```

**Interpretation:**
- **Row 1:** Both buy (-74.2) and sell (-73.8) within bands → keep both
- **Row 2:** Buy was > upper band → filtered out, sell (-75.1) valid
- **Row 3:** Sell was < lower band → filtered out, buy (-74.8) valid  
- **Row 4:** Both trades within bands → keep both

---

## 🏁 **Final Result: tm_filtered**

**tm_filtered** contains only the **statistically valid** spread trading opportunities that:
1. ✅ Have prices within the EMA trading bands
2. ✅ Represent reasonable entry/exit points for the spread strategy
3. ✅ Filter out extreme price movements that deviate from the EMA model
4. ✅ Support systematic spread trading based on mean reversion

This filtered dataset is what SpreadViewer uses for **backtesting, risk management, and live trading decisions** on synthetic spreads.