#!/usr/bin/env python3
"""
Interactive Footprint Chart with Plotly
========================================

Creates interactive footprint chart with zoom, pan, and scroll capabilities
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta

def load_spread_data(file_path: str) -> pd.DataFrame:
    """Load and prepare spread data"""
    data = pd.read_parquet(file_path)
    data.index = pd.to_datetime(data.index)
    
    # Filter only trades (action = 1 or -1)
    trades = data[data['action'].isin([1.0, -1.0])].copy()
    
    print(f"📊 Loaded {len(trades):,} trades from {trades.index.min()} to {trades.index.max()}")
    print(f"   Price range: {trades['price'].min():.2f} to {trades['price'].max():.2f}")
    print(f"   Action distribution: {trades['action'].value_counts().to_dict()}")
    
    return trades

def prepare_daily_footprint(trades: pd.DataFrame, tick_size: float = 0.1) -> tuple:
    """Prepare data for footprint candles and calculate daily OHLC"""
    
    print(f"\n🔄 Preparing footprint data with {tick_size} tick size...")
    
    # Round prices to nearest tick
    trades['price_tick'] = (trades['price'] / tick_size).round() * tick_size
    
    # Create daily periods
    trades['day'] = trades.index.floor('D')
    
    # Calculate daily OHLC for each day
    daily_ohlc = trades.groupby('day')['price'].agg(['first', 'last', 'min', 'max']).reset_index()
    daily_ohlc.columns = ['day', 'open', 'close', 'low', 'high']
    
    # Separate bid and ask volumes
    trades['bid_volume'] = trades['volume'].where(trades['action'] == -1, 0)
    trades['ask_volume'] = trades['volume'].where(trades['action'] == 1, 0)
    
    # Group by day and price tick, sum volumes
    footprint = trades.groupby(['day', 'price_tick']).agg({
        'bid_volume': 'sum',
        'ask_volume': 'sum',
        'volume': 'sum'
    }).reset_index()
    
    # Calculate delta
    footprint['delta'] = footprint['ask_volume'] - footprint['bid_volume']
    
    print(f"   ✅ Created footprint data: {len(footprint)} day/price combinations")
    print(f"   Days: {footprint['day'].min()} to {footprint['day'].max()}")
    print(f"   Price ticks: {footprint['price_tick'].min():.1f} to {footprint['price_tick'].max():.1f}")
    
    # Merge OHLC data
    footprint = footprint.merge(daily_ohlc, on='day', how='left')
    
    return footprint, daily_ohlc

def create_interactive_footprint(footprint: pd.DataFrame, daily_ohlc: pd.DataFrame, file_path: str):
    """Create interactive footprint chart using Plotly"""
    
    print(f"\n📈 Creating interactive footprint chart...")
    
    # Get unique days
    days = sorted(footprint['day'].unique())
    all_prices = sorted(footprint['price_tick'].unique())
    
    print(f"   📊 Creating interactive chart for {len(days)} days")
    
    # Create subplot with secondary y-axis for delta table
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.8, 0.2],
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('Footprint Chart', 'Delta Analysis')
    )
    
    # Color scales
    bid_colors = ['rgba(255,0,0,0.3)', 'rgba(255,0,0,0.8)']  # Light to dark red
    ask_colors = ['rgba(0,0,255,0.3)', 'rgba(0,0,255,0.8)']  # Light to dark blue
    
    # Prepare data for heatmap-style visualization
    footprint_matrix = []
    hover_text = []
    
    for day_idx, day in enumerate(days):
        day_data = footprint[footprint['day'] == day]
        day_ohlc = daily_ohlc[daily_ohlc['day'] == day]
        
        if not day_ohlc.empty:
            open_price = day_ohlc.iloc[0]['open']
            close_price = day_ohlc.iloc[0]['close']
            is_green = close_price > open_price
        else:
            open_price = close_price = 0
            is_green = True
        
        # Get all price levels for this day with complete coverage
        day_prices = sorted(day_data['price_tick'].unique())
        
        if day_prices:
            # Create complete price range for this day
            price_min = min(day_prices)
            price_max = max(day_prices)
            complete_prices = []
            current_price = price_min
            while current_price <= price_max + 0.05:
                complete_prices.append(round(current_price, 1))
                current_price += 0.1
            
            day_column = []
            day_hover = []
            
            for price_level in complete_prices:
                price_data = day_data[day_data['price_tick'] == price_level]
                
                if not price_data.empty:
                    bid_vol = int(price_data.iloc[0]['bid_volume'])
                    ask_vol = int(price_data.iloc[0]['ask_volume'])
                    total_vol = bid_vol + ask_vol
                    delta = ask_vol - bid_vol
                else:
                    bid_vol = ask_vol = total_vol = delta = 0
                
                # Store for heatmap (use total volume for intensity)
                day_column.append(total_vol)
                
                # Create hover text
                hover_info = (f"Date: {day.strftime('%Y-%m-%d')}<br>"
                            f"Price: {price_level:.1f}<br>"
                            f"Bid Volume: {bid_vol}<br>"
                            f"Ask Volume: {ask_vol}<br>"
                            f"Delta: {delta:+d}<br>"
                            f"Total Volume: {total_vol}")
                day_hover.append(hover_info)
            
            footprint_matrix.append(day_column)
            hover_text.append(day_hover)
    
    # Create main footprint heatmap
    if footprint_matrix:
        # Transpose matrix for proper orientation
        z_matrix = list(map(list, zip(*footprint_matrix)))
        hover_matrix = list(map(list, zip(*hover_text)))
        
        fig.add_trace(
            go.Heatmap(
                z=z_matrix,
                x=[d.strftime('%m/%d') for d in days],
                y=[f"{p:.1f}" for p in sorted(set([p for day_prices in footprint_matrix for p in range(len(day_prices))]))],
                colorscale='Viridis',
                hovertemplate='%{customdata}<extra></extra>',
                customdata=hover_matrix,
                showscale=True,
                colorbar=dict(title="Volume", x=1.05)
            ),
            row=1, col=1
        )
    
    # Add delta analysis in bottom subplot
    delta_stats = []
    weekly_cumulative = 0
    
    for day in days:
        day_data = footprint[footprint['day'] == day]
        
        if not day_data.empty:
            daily_delta = day_data['ask_volume'].sum() - day_data['bid_volume'].sum()
            
            # Weekly cumulative (reset every Monday)
            if day.weekday() == 0:  # Monday
                weekly_cumulative = daily_delta
            else:
                weekly_cumulative += daily_delta
            
            delta_stats.append({
                'day': day,
                'daily_delta': daily_delta,
                'weekly_cumulative': weekly_cumulative
            })
    
    # Add delta traces
    dates = [stats['day'] for stats in delta_stats]
    daily_deltas = [stats['daily_delta'] for stats in delta_stats]
    weekly_deltas = [stats['weekly_cumulative'] for stats in delta_stats]
    
    # Daily delta bar chart
    fig.add_trace(
        go.Bar(
            x=[d.strftime('%m/%d') for d in dates],
            y=daily_deltas,
            name='Daily Delta',
            marker_color=['green' if d > 0 else 'red' for d in daily_deltas],
            hovertemplate='Daily Delta: %{y}<extra></extra>'
        ),
        row=2, col=1
    )
    
    # Weekly cumulative line
    fig.add_trace(
        go.Scatter(
            x=[d.strftime('%m/%d') for d in dates],
            y=weekly_deltas,
            mode='lines+markers',
            name='Weekly Cumulative',
            line=dict(color='orange', width=3),
            hovertemplate='Weekly Cumulative: %{y}<extra></extra>'
        ),
        row=2, col=1
    )
    
    # Update layout
    spread_name = Path(file_path).stem
    fig.update_layout(
        title=f'Interactive Footprint Chart - {spread_name}',
        height=800,
        showlegend=True,
        hovermode='closest'
    )
    
    # Update x-axes
    fig.update_xaxes(title_text="Date", row=2, col=1)
    
    # Update y-axes
    fig.update_yaxes(title_text="Price (EUR/MWh)", row=1, col=1)
    fig.update_yaxes(title_text="Delta", row=2, col=1)
    
    # Save as HTML for full interactivity
    output_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/interactive_footprint.html'
    fig.write_html(output_path)
    print(f"   💾 Saved interactive footprint chart: {output_path}")
    
    # Don't auto-show to avoid browser issues in this environment
    # fig.show()
    
    return fig

def main():
    """Main function"""
    
    print("📈 INTERACTIVE FOOTPRINT CHART")
    print("=" * 40)
    
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/debm11_25_frbm11_25_tr_ba_data.parquet'
    
    try:
        trades = load_spread_data(file_path)
        
        if len(trades) == 0:
            print("❌ No trade data found")
            return
        
        footprint, daily_ohlc = prepare_daily_footprint(trades, tick_size=0.1)
        
        if len(footprint) == 0:
            print("❌ No footprint data created")
            return
        
        fig = create_interactive_footprint(footprint, daily_ohlc, file_path)
        
        print("\n🎯 INTERACTIVE FEATURES:")
        print("   • Zoom: Select area or use zoom tools")
        print("   • Pan: Click and drag to move around")
        print("   • Hover: See detailed volume info")
        print("   • Reset: Double-click to reset view")
        print("   • Download: Use camera icon to save image")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()