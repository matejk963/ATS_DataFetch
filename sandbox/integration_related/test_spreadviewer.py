# -*- coding: utf-8 -*-
"""
Created on Thu Apr 14 09:36:39 2022

@author: zelenaymar
"""

import sys
import os

# Add EnergyTrading to Python path for imports
sys.path.append('/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/source_repos/EnergyTrading/Python')

import pandas as pd
import numpy as np
from datetime import datetime, time
from SynthSpread.spreadviewer_class import SpreadSingle, SpreadViewerData, norm_coeff
from Database.TPData import TPData, TPDataDa
from Math.accumfeatures import EMA, MA, MSTD


def calc_ema(df_data, tau):
    mid_ser = .5 * (df_data.loc[:, 'bid'] + df_data.loc[:, 'ask'])
    mid_list = mid_ser.values
    dif_list = [0.001]
    dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
    model = EMA(tau, mid_list[0])
    ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
    return pd.Series(ema_list, index=mid_ser.index)


def calc_ema_m(df_data, tau, margin, w, eql_p):
    mid_ser = .5 * (df_data.loc[:, 'bid'] + df_data.loc[:, 'ask'])
    mid_list = mid_ser.values
    dif_list = [0.001]
    dif_list.extend([abs(x - xl) for x, xl in zip(mid_list[1:], mid_list[:-1])])
    model = EMA(tau, mid_list[0])
    ema_list = [model.push(x, dx) for x, dx in zip(mid_list, dif_list)]
    ema_list = [w * eql_p + (1 - w) * x for x in ema_list]
    bands = [[x - margin, x, x + margin] for x in ema_list]
    return pd.DataFrame(bands, index=mid_ser.index)


def adjust_trds(df_tr, df_em):
    timestamp = df_tr.index
    ts_new = df_em.index.union(timestamp)
    df_em = df_em.reindex(ts_new).ffill().reindex(timestamp)
    lb = df_em.iloc[:, 0]
    ub = df_em.iloc[:, 2]
    df_tr.loc[df_tr['buy'] > ub, 'buy'] = np.nan
    df_tr.loc[df_tr['sell'] < lb, 'sell'] = np.nan
    return df_tr.dropna(how='all')


if __name__ == "__main__":
    print("🚀 Running SpreadViewer Data Generation Workflow")
    print("=" * 60)
    
    # SpreadViewer parameters
    n_s = 3
    start_date = datetime(2025, 7, 1)
    end_date = datetime(2025, 7, 31)
    dates = pd.date_range(start_date, end_date, freq='B')
    market = ['de', 'de']
    tenor = ['m', 'm']
    tn1_list = [1, 2]
    tn2_list = []
    brk_list = ['eex']
    mm_bool = [True, True]

    tau = 5
    margin = .43

    start_time = time(9, 0, 0, 0)
    end_time = time(17, 25, 0, 0)
    gran = None
    gran_s = '1s'
    coeff_list = norm_coeff([1, -2], market)

    eql_p = -6.25
    w = 0

    add_trades = True
    ob_data = False

    print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"📊 Markets: {market}")
    print(f"📋 Tenors: {tenor}")
    print(f"🔢 Contract Lists: tn1={tn1_list}, tn2={tn2_list}")
    print(f"⚙️  Coefficients: {coeff_list}")
    print(f"🕐 Trading Hours: {start_time} to {end_time}")
    
    try:
        # Initialize classes
        print("\n📦 Step 1: Initializing Classes")
        spread_class = SpreadSingle(market, tenor, tn1_list, tn2_list, brk_list)
        data_class = SpreadViewerData()
        db_class = TPData()
        tenors_list = spread_class.tenors_list
        
        print("✅ SpreadSingle class initialized")
        print("✅ SpreadViewerData class initialized")
        print("✅ TPData class initialized")
        print(f"📋 Tenors list: {tenors_list}")
        
        # Load order book data
        print(f"\n📡 Step 2: Loading Order Book Data")
        if not ob_data:
            print("🔄 Loading OTC order book data...")
            data_class.load_best_order_otc(market, tenors_list,
                                           spread_class.product_dates(dates, n_s),
                                           db_class,
                                           start_time=start_time, end_time=end_time)
            print("✅ OTC order book data loaded")
        else:
            print("🔄 Loading full order book data...")
            data_class.load_best_ob_tp(market, tenors_list,
                                       spread_class.product_dates(dates, n_s),
                                       db_class,
                                       start_time=start_time, end_time=end_time)
            print("✅ Full order book data loaded")
        
        # Load trade data
        print(f"\n💹 Step 3: Loading Trade Data")
        if add_trades:
            data_class_tr = SpreadViewerData()
            data_class_tr.load_trades_otc(market, tenors_list, db_class,
                                          start_time=start_time, end_time=end_time)
            print("✅ Trade data loaded")
        
        # Process daily data
        print(f"\n📈 Step 4: Daily Data Processing and Spread Generation")
        sm_all = pd.DataFrame([])
        tm_all = pd.DataFrame([])
        
        daily_results = []
        
        for i, d in enumerate(dates):
            print(f"🗓️  Processing day {i+1}/{len(dates)}: {d.strftime('%Y-%m-%d')}")
            
            d_range = pd.date_range(d, d)
            
            # ⭐ CORE DATA GENERATION: Aggregate order book data for this day
            data_dict = spread_class.aggregate_data(data_class, d_range, n_s, gran=gran,
                                                   start_time=start_time, end_time=end_time)
            
            print(f"   📊 Data aggregated for {len(data_dict)} contracts")
            
            # ⭐ CORE DATA GENERATION: Create spread data from aggregated data
            sm = spread_class.spread_maker(data_dict, coeff_list, trade_type=['cmb', 'cmb']).dropna()
            
            print(f"   📈 Spread data: {len(sm)} points")
            if not sm.empty:
                print(f"      Range: {sm.iloc[:, 0].min():.3f} to {sm.iloc[:, 0].max():.3f}")
                print(f"      Mean: {sm.iloc[:, 0].mean():.3f}")
            
            # Accumulate spread data
            sm_all = pd.concat([sm_all, sm], axis=0)
            
            # Process trade data for this day
            if add_trades and not sm.empty:
                col_list = ['bid', 'ask', 'volume', 'broker_id']
                
                # ⭐ CORE DATA GENERATION: Aggregate trade data
                trade_dict = spread_class.aggregate_data(data_class_tr, d_range, n_s, gran=gran_s,
                                                         start_time=start_time, end_time=end_time,
                                                         col_list=col_list, data_dict=data_dict)
                
                # ⭐ CORE DATA GENERATION: Add trades to spread data
                tm = spread_class.add_trades(data_dict, trade_dict, coeff_list, mm_bool)
                print(f"   💹 Trade data: {len(tm)} trades")
                
                tm_all = pd.concat([tm_all, tm], axis=0)
            
            # Store daily results
            daily_results.append({
                'date': d,
                'spread_points': len(sm),
                'trade_points': len(tm) if add_trades and not sm.empty else 0,
                'spread_mean': sm.iloc[:, 0].mean() if not sm.empty else np.nan,
                'spread_std': sm.iloc[:, 0].std() if not sm.empty else np.nan
            })
        
        # Apply EMA analysis to combined data
        print(f"\n📊 Step 5: EMA Analysis on Combined Data")
        print(f"📈 Total spread data: {len(sm_all)} points")
        print(f"💹 Total trade data: {len(tm_all)} points")
        
        if not sm_all.empty:
            # ⭐ CORE DATA GENERATION: Calculate EMA bands for entire dataset
            em = calc_ema_m(sm_all, tau, margin, w, eql_p)
            sm_combined = pd.concat([sm_all, em], axis=1)
            
            print("✅ EMA bands calculated")
            print(f"   📊 EMA data shape: {em.shape}")
            print(f"   📈 Combined shape: {sm_combined.shape}")
            
            # ⭐ CORE DATA GENERATION: Apply trade filtering with EMA bands
            if not tm_all.empty:
                tm_filtered = adjust_trds(tm_all, em)
                print(f"✅ Trades filtered: {len(tm_filtered)} remaining")
            else:
                tm_filtered = pd.DataFrame()
            
            # Final analysis
            print(f"\n📋 Step 6: Final Results Analysis")
            
            # Spread statistics
            spread_col = sm_all.columns[0] if len(sm_all.columns) > 0 else None
            if spread_col is not None:
                spread_data = sm_all[spread_col]
                print(f"📈 Spread Analysis:")
                print(f"   Data Points: {len(spread_data):,}")
                print(f"   Range: {spread_data.min():.3f} to {spread_data.max():.3f}")
                print(f"   Mean: {spread_data.mean():.3f}")
                print(f"   Std Dev: {spread_data.std():.3f}")
                print(f"   Date Range: {spread_data.index[0]} to {spread_data.index[-1]}")
            
            # EMA band statistics
            print(f"📊 EMA Band Analysis:")
            print(f"   Lower Band: {em.iloc[:, 0].mean():.3f} ± {em.iloc[:, 0].std():.3f}")
            print(f"   Center EMA: {em.iloc[:, 1].mean():.3f} ± {em.iloc[:, 1].std():.3f}")
            print(f"   Upper Band: {em.iloc[:, 2].mean():.3f} ± {em.iloc[:, 2].std():.3f}")
            print(f"   Band Width: {(em.iloc[:, 2] - em.iloc[:, 0]).mean():.3f}")
            
            # Daily summary
            print(f"📅 Daily Summary:")
            daily_df = pd.DataFrame(daily_results)
            successful_days = daily_df[daily_df['spread_points'] > 0]
            print(f"   Successful Days: {len(successful_days)}/{len(daily_df)}")
            print(f"   Avg Points/Day: {successful_days['spread_points'].mean():.1f}")
            print(f"   Avg Trades/Day: {successful_days['trade_points'].mean():.1f}")
            
            print(f"\n🎉 SPREADVIEWER DATA GENERATION COMPLETED SUCCESSFULLY")
            print(f"=" * 60)
            
            # Return the generated data for potential use
            results = {
                'spread_data': sm_combined,
                'trade_data': tm_filtered,
                'ema_bands': em,
                'daily_summary': daily_df,
                'parameters': {
                    'markets': market,
                    'tenors': tenor,
                    'contracts': {'tn1': tn1_list, 'tn2': tn2_list},
                    'date_range': f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                    'coefficients': coeff_list,
                    'ema_params': {'tau': tau, 'margin': margin, 'eql_p': eql_p, 'w': w}
                }
            }
            
        else:
            print(f"❌ No spread data generated")
            results = None
            
    except Exception as e:
        print(f"❌ SpreadViewer workflow failed: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        results = None