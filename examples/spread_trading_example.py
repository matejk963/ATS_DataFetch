"""
Spread Trading Example: How to Use fetch_spread_data()
=====================================================

This example demonstrates how to use the integrated SpreadViewer functionality
to generate and analyze synthetic spread trading opportunities.

Real-world use cases:
1. Calendar spread trading (M+1 vs M+2)
2. Cross-market arbitrage (DE vs TTF)
3. Market making on synthetic spreads
4. Statistical arbitrage strategies
"""

import sys
import os

# Cross-platform path setup
if os.name == 'nt':
    project_root = r'C:\Users\krajcovic\Documents\GitHub\ATS_DataFetch'
else:
    project_root = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch'

sys.path.append(project_root)

from src.core.data_fetcher import DataFetcher
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def example_1_calendar_spread():
    """
    Example 1: German Monthly Calendar Spread (M+1 vs M+2)
    
    Strategy: Buy August 2025, Sell 2x September 2025
    Use Case: Trading seasonal price differences
    """
    print("=" * 60)
    print("EXAMPLE 1: German Monthly Calendar Spread")
    print("=" * 60)
    
    # Initialize DataFetcher
    fetcher = DataFetcher(
        trading_hours=(9, 17),
        allowed_broker_ids=[1441]
    )
    
    # Define spread configuration
    spread_config = {
        'contracts': [
            {'market': 'de', 'tenor': 'm', 'contract': '08_25'},  # August 2025
            {'market': 'de', 'tenor': 'm', 'contract': '09_25'}   # September 2025
        ],
        'coefficients': [1, -2],  # Buy 1x Aug, Sell 2x Sep
        'period': {
            'start_date': '2025-07-01',
            'end_date': '2025-07-10'  # Small range for demo
        }
    }
    
    # Custom EMA parameters for this strategy
    ema_params = {
        'tau': 5,           # Faster EMA for short-term trading
        'margin': 0.43,     # Tight bands for precise entries
        'eql_p': -6.25,     # Expected equilibrium price
        'w': 0              # Pure EMA (no equilibrium weighting)
    }
    
    try:
        # Fetch spread data
        print("🔄 Fetching calendar spread data...")
        result = fetcher.fetch_spread_data(
            spread_config=spread_config,
            ema_params=ema_params,
            include_raw_data=True  # Include individual contract data for analysis
        )
        
        # Extract key datasets
        spread_market = result['spread_market_data']        # Order book data
        spread_trades = result['spread_trade_data']         # All possible trades
        filtered_trades = result['spread_filtered_trades']  # EMA-filtered opportunities
        ema_bands = result['ema_bands']                     # Trading bands
        
        print(f"✅ Data fetched successfully!")
        print(f"   📊 Market data points: {len(spread_market):,}")
        print(f"   💹 Total trade opportunities: {len(spread_trades):,}")
        print(f"   🎯 Filtered opportunities: {len(filtered_trades):,}")
        
        # USAGE 1: Trading Signal Generation
        print("\n📈 USAGE 1: Generate Trading Signals")
        if not filtered_trades.empty:
            # Buy signals: where we can buy the spread cheaply
            buy_signals = filtered_trades[filtered_trades['buy'].notna()]
            print(f"   🟢 Buy signals: {len(buy_signals)} opportunities")
            if len(buy_signals) > 0:
                print(f"      Best buy price: {buy_signals['buy'].min():.3f} EUR/MWh")
                print(f"      Avg buy price: {buy_signals['buy'].mean():.3f} EUR/MWh")
            
            # Sell signals: where we can sell the spread expensively  
            sell_signals = filtered_trades[filtered_trades['sell'].notna()]
            print(f"   🔴 Sell signals: {len(sell_signals)} opportunities")
            if len(sell_signals) > 0:
                print(f"      Best sell price: {sell_signals['sell'].max():.3f} EUR/MWh")
                print(f"      Avg sell price: {sell_signals['sell'].mean():.3f} EUR/MWh")
        
        # USAGE 2: Risk Analysis
        print("\n⚖️  USAGE 2: Risk Analysis")
        if not ema_bands.empty:
            band_width = (ema_bands['upper_band'] - ema_bands['lower_band']).mean()
            print(f"   📏 Average band width: {band_width:.3f} EUR/MWh")
            print(f"   📊 EMA center: {ema_bands['ema_center'].mean():.3f} EUR/MWh")
            print(f"   📈 Price volatility: {ema_bands['ema_center'].std():.3f} EUR/MWh")
        
        # USAGE 3: Performance Metrics
        print("\n📊 USAGE 3: Strategy Performance Metrics")
        if not filtered_trades.empty:
            # Calculate potential profit per trade
            valid_pairs = filtered_trades.dropna(subset=['buy', 'sell'])
            if not valid_pairs.empty:
                spreads = valid_pairs['sell'] - valid_pairs['buy']
                print(f"   💰 Potential profit per round-trip:")
                print(f"      Max: {spreads.max():.3f} EUR/MWh")
                print(f"      Mean: {spreads.mean():.3f} EUR/MWh") 
                print(f"      Min: {spreads.min():.3f} EUR/MWh")
        
        # USAGE 4: Export for Further Analysis
        print("\n💾 USAGE 4: Export Data for Analysis")
        output_dir = os.path.join(project_root, "examples", "spread_data")
        os.makedirs(output_dir, exist_ok=True)
        
        # Export filtered trading opportunities
        if not filtered_trades.empty:
            trades_file = os.path.join(output_dir, "calendar_spread_trades.csv")
            filtered_trades.to_csv(trades_file)
            print(f"   📁 Exported trades: {trades_file}")
        
        # Export EMA bands for visualization
        if not ema_bands.empty:
            bands_file = os.path.join(output_dir, "calendar_spread_bands.csv")
            ema_bands.to_csv(bands_file)
            print(f"   📁 Exported bands: {bands_file}")
        
        return result
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def example_2_cross_market_arbitrage():
    """
    Example 2: Cross-Market Arbitrage (DE vs TTF)
    
    Strategy: Exploit price differences between German and Dutch markets
    Use Case: Geographic arbitrage trading
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Cross-Market Arbitrage")
    print("=" * 60)
    
    fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
    
    # Cross-market spread configuration
    spread_config = {
        'contracts': [
            {'market': 'de', 'tenor': 'q', 'contract': '3_25'},   # German Q3 2025
            {'market': 'ttf', 'tenor': 'q', 'contract': '3_25'}  # Dutch Q3 2025
        ],
        'coefficients': [1, -1],  # Long DE, Short TTF
        'period': {
            'start_date': '2025-06-01',
            'end_date': '2025-06-05'
        }
    }
    
    # More conservative EMA for cross-market trades
    ema_params = {
        'tau': 10,          # Slower EMA for stability
        'margin': 0.8,      # Wider bands for cross-market volatility
        'eql_p': 2.5,       # Expected DE-TTF premium
        'w': 0.3            # Some equilibrium weighting
    }
    
    try:
        print("🔄 Fetching cross-market spread data...")
        result = fetcher.fetch_spread_data(spread_config, ema_params)
        
        filtered_trades = result['spread_filtered_trades']
        
        if not filtered_trades.empty:
            print(f"✅ Found {len(filtered_trades)} arbitrage opportunities")
            
            # USAGE: Real-time arbitrage monitoring
            print("\n🚨 USAGE: Real-time Arbitrage Alerts")
            current_time = filtered_trades.index[-1] if len(filtered_trades) > 0 else None
            if current_time:
                latest_trades = filtered_trades.loc[current_time:]
                if not latest_trades.empty:
                    print(f"   📅 Latest opportunity at: {current_time}")
                    if 'buy' in latest_trades.columns and latest_trades['buy'].notna().any():
                        print(f"   🟢 Can buy spread at: {latest_trades['buy'].iloc[0]:.3f}")
                    if 'sell' in latest_trades.columns and latest_trades['sell'].notna().any():
                        print(f"   🔴 Can sell spread at: {latest_trades['sell'].iloc[0]:.3f}")
        
        return result
        
    except Exception as e:
        print(f"❌ Cross-market example failed: {e}")
        return None


def example_3_market_making_strategy():
    """
    Example 3: Market Making on Synthetic Spreads
    
    Strategy: Provide liquidity by quoting both sides of the spread
    Use Case: Systematic market making with statistical edges
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Market Making Strategy")
    print("=" * 60)
    
    # Market making typically uses very tight parameters
    spread_config = {
        'contracts': [
            {'market': 'de', 'tenor': 'm', 'contract': '07_25'},
            {'market': 'de', 'tenor': 'm', 'contract': '08_25'}
        ],
        'coefficients': [1, -1.5],  # Weighted spread
        'period': {
            'start_date': '2025-06-15',
            'end_date': '2025-06-20'
        }
    }
    
    # Tight EMA bands for market making
    market_making_params = {
        'tau': 3,           # Very fast EMA
        'margin': 0.25,     # Tight bands
        'eql_p': -1.8,      # Expected spread level
        'w': 0.1            # Minimal equilibrium adjustment
    }
    
    fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
    
    try:
        print("🔄 Setting up market making parameters...")
        result = fetcher.fetch_spread_data(spread_config, market_making_params)
        
        filtered_trades = result['spread_filtered_trades']
        ema_bands = result['ema_bands']
        
        if not filtered_trades.empty and not ema_bands.empty:
            print(f"✅ Market making setup complete")
            
            # USAGE: Calculate optimal bid/ask quotes
            print("\n💰 USAGE: Optimal Bid/Ask Calculation")
            
            # Use EMA bands to determine quote levels
            latest_ema = ema_bands.iloc[-1] if len(ema_bands) > 0 else None
            if latest_ema is not None:
                center = latest_ema['ema_center']
                band_width = latest_ema['upper_band'] - latest_ema['lower_band']
                
                # Market making quotes (narrower than EMA bands)
                bid_quote = center - (band_width * 0.3)  # Buy 30% below center
                ask_quote = center + (band_width * 0.3)  # Sell 30% above center
                
                print(f"   📊 Current EMA center: {center:.3f}")
                print(f"   📏 Band width: {band_width:.3f}")
                print(f"   🟢 Optimal bid quote: {bid_quote:.3f}")
                print(f"   🔴 Optimal ask quote: {ask_quote:.3f}")
                print(f"   💵 Expected spread capture: {ask_quote - bid_quote:.3f}")
        
        return result
        
    except Exception as e:
        print(f"❌ Market making example failed: {e}")
        return None


def example_4_backtesting_preparation():
    """
    Example 4: Prepare Spread Data for Backtesting
    
    Use Case: Generate clean datasets for strategy backtesting
    """
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Backtesting Data Preparation")
    print("=" * 60)
    
    # Get spread data from previous example
    fetcher = DataFetcher(trading_hours=(9, 17), allowed_broker_ids=[1441])
    
    spread_config = {
        'contracts': [
            {'market': 'de', 'tenor': 'm', 'contract': '08_25'},
            {'market': 'de', 'tenor': 'm', 'contract': '09_25'}
        ],
        'coefficients': [1, -2],
        'period': {
            'start_date': '2025-07-01',
            'end_date': '2025-07-05'
        }
    }
    
    try:
        result = fetcher.fetch_spread_data(spread_config, include_raw_data=True)
        
        # USAGE: Create backtest-ready datasets
        print("🔄 Preparing backtesting datasets...")
        
        backtest_data = {}
        
        # 1. Price series for signal generation
        spread_market = result['spread_market_data']
        if not spread_market.empty:
            # Create OHLC-style data from spread market data
            price_series = spread_market.iloc[:, 0] if len(spread_market.columns) > 0 else pd.Series()
            backtest_data['prices'] = price_series.resample('1T').ohlc()  # 1-minute OHLC
            print(f"   📈 Price series: {len(backtest_data['prices'])} bars")
        
        # 2. Trading signals from filtered trades
        filtered_trades = result['spread_filtered_trades']
        if not filtered_trades.empty:
            # Create buy/sell signal series
            signals = pd.DataFrame(index=filtered_trades.index)
            signals['buy_signal'] = filtered_trades['buy'].notna().astype(int)
            signals['sell_signal'] = filtered_trades['sell'].notna().astype(int)
            signals['buy_price'] = filtered_trades['buy']
            signals['sell_price'] = filtered_trades['sell']
            
            backtest_data['signals'] = signals
            print(f"   🎯 Trading signals: {len(signals)} points")
        
        # 3. Risk metrics from EMA bands
        ema_bands = result['ema_bands']
        if not ema_bands.empty:
            risk_metrics = pd.DataFrame(index=ema_bands.index)
            risk_metrics['volatility'] = (ema_bands['upper_band'] - ema_bands['lower_band'])
            risk_metrics['trend'] = ema_bands['ema_center'].diff()
            risk_metrics['position_size'] = 1.0 / risk_metrics['volatility']  # Inverse volatility sizing
            
            backtest_data['risk_metrics'] = risk_metrics
            print(f"   ⚖️  Risk metrics: {len(risk_metrics)} points")
        
        # Export for external backtesting platforms
        output_dir = os.path.join(project_root, "examples", "backtest_data")
        os.makedirs(output_dir, exist_ok=True)
        
        for dataset_name, dataset in backtest_data.items():
            if not dataset.empty:
                file_path = os.path.join(output_dir, f"spread_{dataset_name}.csv")
                dataset.to_csv(file_path)
                print(f"   💾 Exported {dataset_name}: {file_path}")
        
        print("✅ Backtesting data preparation complete!")
        
        return backtest_data
        
    except Exception as e:
        print(f"❌ Backtesting preparation failed: {e}")
        return None


if __name__ == "__main__":
    """
    Run all spread trading examples
    """
    print("🚀 SPREAD TRADING EXAMPLES")
    print("=" * 60)
    print("Demonstrating how to use fetch_spread_data() for real trading strategies")
    
    # Run examples
    results = {}
    
    print("\n1️⃣  Running Calendar Spread Example...")
    results['calendar'] = example_1_calendar_spread()
    
    print("\n2️⃣  Running Cross-Market Arbitrage Example...")
    results['arbitrage'] = example_2_cross_market_arbitrage()
    
    print("\n3️⃣  Running Market Making Example...")
    results['market_making'] = example_3_market_making_strategy()
    
    print("\n4️⃣  Running Backtesting Preparation Example...")
    results['backtesting'] = example_4_backtesting_preparation()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY: How to Use Spread Data")
    print("=" * 60)
    
    successful_examples = sum(1 for r in results.values() if r is not None)
    print(f"✅ {successful_examples}/4 examples completed successfully")
    
    print("\n🎯 KEY USAGE PATTERNS:")
    print("1. 📊 spread_market_data → Price analysis & charting")
    print("2. 💹 spread_trade_data → All possible trading opportunities")
    print("3. 🎯 spread_filtered_trades → EMA-validated entry/exit points")
    print("4. 📈 ema_bands → Risk management & position sizing")
    print("5. 📁 Export to CSV/Parquet → Integration with other systems")
    
    print("\n🔄 TYPICAL WORKFLOW:")
    print("1. Define spread configuration (contracts + coefficients)")
    print("2. Set EMA parameters for your strategy")
    print("3. Call fetch_spread_data()")
    print("4. Use filtered_trades for entry/exit signals")
    print("5. Use ema_bands for risk management")
    print("6. Export data for backtesting/live trading")
    
    print("\n✨ The spread data is now ready for:")
    print("   • Live trading systems")
    print("   • Strategy backtesting")
    print("   • Risk management")
    print("   • Market analysis")
    print("   • Performance attribution")