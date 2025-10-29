#!/usr/bin/env python3
"""
Footprint Candles Chart
========================

Creates footprint chart where each day is shown as a "candle box" containing
all price levels with bid/ask volumes side by side.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from pathlib import Path
from datetime import datetime, timedelta
import seaborn as sns

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
    
    print(f"\n🔄 Preparing footprint candle data with {tick_size} tick size...")
    
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

def check_imbalance_3x(footprint: pd.DataFrame, day: pd.Timestamp, price: float, side: str) -> bool:
    """Check if volume shows 3x imbalance vs opposite side at adjacent level"""
    
    current_data = footprint[(footprint['day'] == day) & (footprint['price_tick'] == price)]
    if current_data.empty:
        return False
    
    current_row = current_data.iloc[0]
    
    if side == 'bid':
        current_value = current_row['bid_volume']
        # Compare with ask at one level higher
        compare_price = price + 0.1
        compare_data = footprint[(footprint['day'] == day) & (footprint['price_tick'] == compare_price)]
        if compare_data.empty:
            return current_value > 0
        compare_value = compare_data.iloc[0]['ask_volume']
    else:  # ask
        current_value = current_row['ask_volume']
        # Compare with bid at one level lower  
        compare_price = price - 0.1
        compare_data = footprint[(footprint['day'] == day) & (footprint['price_tick'] == compare_price)]
        if compare_data.empty:
            return current_value > 0
        compare_value = compare_data.iloc[0]['bid_volume']
    
    # 3x rule
    if compare_value == 0:
        return current_value > 0
    return current_value >= 3 * compare_value

def create_footprint_candles(footprint: pd.DataFrame, daily_ohlc: pd.DataFrame, file_path: str):
    """Create footprint chart with candle-like boxes for each day"""
    
    print(f"\n📈 Creating footprint candles chart...")
    
    # Get unique days
    days = sorted(footprint['day'].unique())
    
    # Show all days (no limit)
    print(f"   📊 Displaying all {len(days)} days")
    
    print(f"   📅 Creating candles for {len(days)} days")
    
    # Get overall price range
    all_prices = sorted(footprint['price_tick'].unique())
    price_min, price_max = min(all_prices), max(all_prices)
    
    # Create figure with better proportions and horizontal scrolling
    fig_width = max(20, len(days) * 0.8)  # More proportional width
    fig_height = 14  # Taller for better vertical proportions
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    
    candle_width = 0.6  # Slightly narrower for better proportions with more days
    
    for day_idx, day in enumerate(days):
        day_data = footprint[footprint['day'] == day]
        
        if day_data.empty:
            continue
        
        # Get OHLC for this day
        day_ohlc = daily_ohlc[daily_ohlc['day'] == day]
        if not day_ohlc.empty:
            open_price = day_ohlc.iloc[0]['open']
            close_price = day_ohlc.iloc[0]['close']
            is_green = close_price > open_price
        else:
            open_price = close_price = 0
            is_green = True
        
        # Get price levels for this day
        day_prices = sorted(day_data['price_tick'].unique())
        
        if day_idx < 5 or day_idx % 10 == 0:  # Print progress less frequently
            print(f"   📊 Day {day.strftime('%m/%d')}: {len(day_prices)} price levels, O:{open_price:.2f} C:{close_price:.2f}")
        
        # Calculate candle position
        x_center = day_idx
        
        # Draw candle box outline
        day_price_min = min(day_prices)
        day_price_max = max(day_prices)
        
        # Convert prices to y-coordinates
        y_bottom = day_price_min
        y_top = day_price_max
        box_height = y_top - y_bottom + 0.1  # Add small padding
        
        # Draw main candle box
        candle_rect = patches.Rectangle(
            (x_center - candle_width/2, y_bottom - 0.05), 
            candle_width, box_height,
            linewidth=1, edgecolor='black', facecolor='white', alpha=0.9
        )
        ax.add_patch(candle_rect)
        
        # Draw open/close stripe on the left side
        if open_price != close_price:
            stripe_color = 'green' if is_green else 'red'
            stripe_start = min(open_price, close_price)
            stripe_end = max(open_price, close_price)
            stripe_height = stripe_end - stripe_start
            
            stripe_rect = patches.Rectangle(
                (x_center - candle_width/2 - 0.05, stripe_start), 
                0.03, stripe_height,
                linewidth=0, facecolor=stripe_color, alpha=0.8
            )
            ax.add_patch(stripe_rect)
        
        # Get all possible price levels in the overall range for this day's span
        day_price_min = min(day_prices)
        day_price_max = max(day_prices)
        
        # Create all price levels in 0.1 increments within this day's range
        all_day_levels = []
        current_price = day_price_min
        while current_price <= day_price_max + 0.05:  # Small tolerance for floating point
            all_day_levels.append(round(current_price, 1))
            current_price += 0.1
        
        # Find the price level with highest total volume for this day
        max_total_volume = 0
        max_volume_price = None
        
        for price_level in all_day_levels:
            price_data = day_data[day_data['price_tick'] == price_level]
            if not price_data.empty:
                total_vol = int(price_data.iloc[0]['bid_volume']) + int(price_data.iloc[0]['ask_volume'])
                if total_vol > max_total_volume:
                    max_total_volume = total_vol
                    max_volume_price = price_level
        
        # Add price levels inside the candle - including zeros for missing levels
        for price_level in all_day_levels:
            # Get data for this price level (or create empty if not exists)
            price_data = day_data[day_data['price_tick'] == price_level]
            
            if not price_data.empty:
                bid_vol = int(price_data.iloc[0]['bid_volume'])
                ask_vol = int(price_data.iloc[0]['ask_volume'])
            else:
                bid_vol = 0
                ask_vol = 0
            
            # Check if this is the highest volume price level
            is_max_volume = (price_level == max_volume_price and max_total_volume > 0)
            
            # Check for 3x imbalances - only bold if opposite side is non-zero
            bid_bold = False
            ask_bold = False
            
            if bid_vol > 0:
                # Check ask at one level higher
                higher_price = round(price_level + 0.1, 1)
                higher_data = day_data[day_data['price_tick'] == higher_price]
                if not higher_data.empty:
                    higher_ask = int(higher_data.iloc[0]['ask_volume'])
                    if higher_ask > 0 and bid_vol >= 3 * higher_ask:
                        bid_bold = True
            
            if ask_vol > 0:
                # Check bid at one level lower
                lower_price = round(price_level - 0.1, 1)
                lower_data = day_data[day_data['price_tick'] == lower_price]
                if not lower_data.empty:
                    lower_bid = int(lower_data.iloc[0]['bid_volume'])
                    if lower_bid > 0 and ask_vol >= 3 * lower_bid:
                        ask_bold = True
            
            # Position within candle
            y_pos = price_level
            
            # Add orange background highlight for max volume row
            if is_max_volume:
                # Draw orange background rectangle for the max volume row
                highlight_left = x_center - candle_width/2 + 0.02
                highlight_right = x_center + candle_width/2 - 0.02
                highlight_width = highlight_right - highlight_left
                highlight_height = 0.08
                highlight_bottom = y_pos - highlight_height/2
                
                highlight_rect = patches.Rectangle(
                    (highlight_left, highlight_bottom), 
                    highlight_width, highlight_height,
                    linewidth=0, facecolor='orange', alpha=0.6
                )
                ax.add_patch(highlight_rect)
            
            # Add bid volume (left side of candle) - always show, even if 0
            ax.text(x_center - 0.25, y_pos, str(bid_vol), 
                   ha='center', va='center', fontsize=8, color='red',
                   weight='bold' if bid_bold else 'normal')
            
            # Add ask volume (right side of candle) - always show, even if 0
            ax.text(x_center + 0.25, y_pos, str(ask_vol),
                   ha='center', va='center', fontsize=8, color='blue',
                   weight='bold' if ask_bold else 'normal')
            
            # Add price level label (outside candle)
            ax.text(x_center - candle_width/2 - 0.1, y_pos, f"{price_level:.1f}",
                   ha='right', va='center', fontsize=7, color='gray')
        
        # Remove date label (as requested)
        
        # Add daily summary above candle
        day_bid_total = day_data['bid_volume'].sum()
        day_ask_total = day_data['ask_volume'].sum()
        day_delta = day_ask_total - day_bid_total
        
        summary_text = f"B:{day_bid_total:.0f} A:{day_ask_total:.0f}"
        ax.text(x_center, y_top + 0.1, summary_text,
               ha='center', va='bottom', fontsize=7, 
               bbox=dict(boxstyle='round,pad=0.2', facecolor='lightgray', alpha=0.7))
    
    # Customize axes
    ax.set_xlim(-0.5, len(days) - 0.5)
    ax.set_ylim(price_min - 0.5, price_max + 0.5)
    
    # Set x-axis
    ax.set_xticks(range(len(days)))
    ax.set_xticklabels([d.strftime('%m/%d') for d in days], rotation=45)
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    
    # Set y-axis
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12, fontweight='bold')
    
    # Add grid
    ax.grid(True, alpha=0.3)
    
    # Title
    spread_name = Path(file_path).stem.replace('_tr_ba_data_data_fetch_engine_method_real', '')
    ax.set_title(f'Footprint Candles - {spread_name}\nDaily Order Flow in Candle Format', 
                fontsize=14, fontweight='bold', pad=20)
    
    # Add legend
    legend_text = ("FOOTPRINT CANDLES:\n"
                  "• Each box = One trading day\n"
                  "• Red: Bid Volume (left)\n"
                  "• Blue: Ask Volume (right)\n"
                  "• Bold: 3x+ vs non-zero opposite\n"
                  "• Orange highlight: Highest volume price\n"
                  "• 0: No volume at that level\n"
                  "• Left stripe: Open to Close\n"
                  "  - Green: Close > Open\n"
                  "  - Red: Close < Open")
    
    ax.text(0.02, 0.98, legend_text, transform=ax.transAxes, fontsize=10,
           verticalalignment='top', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    # Enable interactive navigation (zoom, pan)
    plt.subplots_adjust(bottom=0.1, top=0.95, left=0.05, right=0.98)
    
    # Add navigation toolbar message
    fig.text(0.5, 0.02, 'Use toolbar to zoom and pan horizontally through all days', 
             ha='center', va='bottom', fontsize=10, style='italic')
    
    # Calculate delta statistics for table
    delta_stats = []
    weekly_cumulative = 0
    
    for day_idx, day in enumerate(days):
        day_data = footprint[footprint['day'] == day]
        
        if not day_data.empty:
            # Daily delta sum (ask vol - bid vol)
            daily_delta = day_data['ask_volume'].sum() - day_data['bid_volume'].sum()
            
            # Min/Max delta at price level within the day
            day_data_copy = day_data.copy()
            day_data_copy['price_delta'] = day_data_copy['ask_volume'] - day_data_copy['bid_volume']
            min_delta = day_data_copy['price_delta'].min()
            max_delta = day_data_copy['price_delta'].max()
            
            # Weekly cumulative (reset every Monday)
            if day.weekday() == 0:  # Monday
                weekly_cumulative = daily_delta
            else:
                weekly_cumulative += daily_delta
            
            delta_stats.append({
                'day': day,
                'daily_delta': daily_delta,
                'min_delta': min_delta,
                'max_delta': max_delta,
                'weekly_cumulative': weekly_cumulative
            })
    
    # Create transposed delta table below the chart
    table_y_start = price_min - 1.0
    row_height = 0.25
    
    # Row labels (on y-axis)
    metrics = ['Daily Δ', 'Min Δ', 'Max Δ', 'Weekly Cum Δ']
    for i, metric in enumerate(metrics):
        row_y = table_y_start - (i * row_height)
        ax.text(-0.8, row_y, metric, ha='right', va='center', fontsize=8, weight='bold')
    
    # Draw separator line between labels and data
    ax.axvline(x=-0.6, ymin=0, ymax=1, color='black', linewidth=1, alpha=0.3)
    
    # Table data columns (aligned with footprint candles)
    for day_idx, stats in enumerate(delta_stats):
        x_pos = day_idx
        
        # Daily delta (color coded)
        daily_color = 'green' if stats['daily_delta'] > 0 else 'red' if stats['daily_delta'] < 0 else 'black'
        ax.text(x_pos, table_y_start, f"{stats['daily_delta']:+.0f}", ha='center', va='center', 
               fontsize=7, color=daily_color, weight='bold' if abs(stats['daily_delta']) > 50 else 'normal')
        
        # Min delta
        min_color = 'red' if stats['min_delta'] < -20 else 'black'
        ax.text(x_pos, table_y_start - row_height, f"{stats['min_delta']:+.0f}", ha='center', va='center', 
               fontsize=7, color=min_color)
        
        # Max delta  
        max_color = 'green' if stats['max_delta'] > 20 else 'black'
        ax.text(x_pos, table_y_start - (2 * row_height), f"{stats['max_delta']:+.0f}", ha='center', va='center', 
               fontsize=7, color=max_color)
        
        # Weekly cumulative delta
        weekly_color = 'green' if stats['weekly_cumulative'] > 0 else 'red' if stats['weekly_cumulative'] < 0 else 'black'
        ax.text(x_pos, table_y_start - (3 * row_height), f"{stats['weekly_cumulative']:+.0f}", ha='center', va='center', 
               fontsize=7, color=weekly_color, weight='bold')
        
        # Draw light grid lines for better readability
        for i in range(len(metrics)):
            row_y = table_y_start - (i * row_height)
            ax.axhline(y=row_y - row_height/2, xmin=max(0, (day_idx-0.5)/len(days)), xmax=min(1, (day_idx+0.5)/len(days)), 
                      color='lightgray', linewidth=0.5, alpha=0.5)
    
    # Adjust y-limits to include table
    ax.set_ylim(table_y_start - 2.0, price_max + 0.5)
    
    # Save chart
    output_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/footprint_candles.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved footprint candles chart: {output_path}")
    
    plt.show()
    
    # Print summary
    print(f"\n📊 FOOTPRINT CANDLES SUMMARY:")
    print(f"   Days displayed: {len(days)}")
    print(f"   Price range: {price_min:.1f} to {price_max:.1f} EUR/MWh")
    
    for day in days[:5]:  # Show first 5 days
        day_data = footprint[footprint['day'] == day]
        day_bid = day_data['bid_volume'].sum()
        day_ask = day_data['ask_volume'].sum()
        print(f"   {day.strftime('%m/%d')}: Bid={day_bid:.0f}, Ask={day_ask:.0f}, Δ={day_ask-day_bid:+.0f}")

def main():
    """Main function"""
    
    print("📈 FOOTPRINT CANDLES CHART")
    print("=" * 35)
    
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
        
        create_footprint_candles(footprint, daily_ohlc, file_path)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()