#!/usr/bin/env python3
"""
Daily Footprint Boxes
======================

Creates individual footprint boxes for each day.
Each box shows bid/ask volumes for all price levels of that specific day.
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

def prepare_daily_footprint(trades: pd.DataFrame, tick_size: float = 0.1) -> pd.DataFrame:
    """Prepare data for daily footprint boxes"""
    
    print(f"\n🔄 Preparing daily footprint data with {tick_size} tick size...")
    
    # Round prices to nearest tick
    trades['price_tick'] = (trades['price'] / tick_size).round() * tick_size
    
    # Create daily periods
    trades['day'] = trades.index.floor('D')
    
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
    
    return footprint

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

def create_daily_boxes(footprint: pd.DataFrame, file_path: str):
    """Create individual footprint boxes for each day"""
    
    print(f"\n📈 Creating daily footprint boxes...")
    
    # Get unique days
    days = sorted(footprint['day'].unique())
    
    # Limit days for visibility
    max_days = 6  # Show 6 days in a 2x3 grid
    if len(days) > max_days:
        print(f"   📊 Limiting to first {max_days} days")
        days = days[:max_days]
        footprint = footprint[footprint['day'].isin(days)]
    
    print(f"   📅 Creating boxes for {len(days)} days")
    
    # Create subplots - 2 rows, 3 columns
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Daily Footprint Boxes - Order Flow by Day', fontsize=16, fontweight='bold')
    
    # Flatten axes for easier iteration
    axes_flat = axes.flatten()
    
    for day_idx, day in enumerate(days):
        if day_idx >= len(axes_flat):
            break
            
        ax = axes_flat[day_idx]
        day_data = footprint[footprint['day'] == day]
        
        if day_data.empty:
            ax.text(0.5, 0.5, 'No Data', ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f"{day.strftime('%Y-%m-%d')}")
            continue
        
        # Get price levels for this day
        price_levels = sorted(day_data['price_tick'].unique())
        
        print(f"   📊 Day {day.strftime('%m/%d')}: {len(price_levels)} price levels")
        
        # Create footprint for this day
        for i, price_level in enumerate(price_levels):
            price_data = day_data[day_data['price_tick'] == price_level].iloc[0]
            
            bid_vol = int(price_data['bid_volume'])
            ask_vol = int(price_data['ask_volume'])
            
            # Check for 3x imbalances
            bid_bold = check_imbalance_3x(footprint, day, price_level, 'bid') if bid_vol > 0 else False
            ask_bold = check_imbalance_3x(footprint, day, price_level, 'ask') if ask_vol > 0 else False
            
            # Draw price level line
            y_pos = len(price_levels) - i - 1  # Invert so highest price is at top
            
            # Add price label on left
            ax.text(-0.1, y_pos, f"{price_level:.1f}", ha='right', va='center', fontsize=8, weight='bold')
            
            # Add bid volume (left side, red)
            if bid_vol > 0:
                ax.text(0.2, y_pos, str(bid_vol), ha='center', va='center', 
                       fontsize=9, color='red', weight='bold' if bid_bold else 'normal')
            
            # Add ask volume (right side, blue)  
            if ask_vol > 0:
                ax.text(0.8, y_pos, str(ask_vol), ha='center', va='center',
                       fontsize=9, color='blue', weight='bold' if ask_bold else 'normal')
            
            # Add separator line
            if i < len(price_levels) - 1:
                ax.axhline(y=y_pos - 0.5, color='lightgray', linewidth=0.5, alpha=0.5)
        
        # Customize this day's box
        ax.set_xlim(-0.3, 1.3)
        ax.set_ylim(-0.5, len(price_levels) - 0.5)
        ax.set_title(f"{day.strftime('%Y-%m-%d')}", fontsize=12, weight='bold')
        
        # Remove ticks
        ax.set_xticks([])
        ax.set_yticks([])
        
        # Add labels
        ax.text(0.2, -0.2, 'BID', ha='center', va='top', fontsize=10, color='red', weight='bold')
        ax.text(0.8, -0.2, 'ASK', ha='center', va='top', fontsize=10, color='blue', weight='bold')
        
        # Add border
        for spine in ax.spines.values():
            spine.set_edgecolor('black')
            spine.set_linewidth(1)
        
        # Calculate and show daily summary
        day_bid_total = day_data['bid_volume'].sum()
        day_ask_total = day_data['ask_volume'].sum()
        day_delta = day_ask_total - day_bid_total
        
        summary_text = f"Bid: {day_bid_total:.0f}\nAsk: {day_ask_total:.0f}\nΔ: {day_delta:+.0f}"
        ax.text(0.5, len(price_levels) + 0.3, summary_text, ha='center', va='bottom', 
               fontsize=8, bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgray', alpha=0.7))
    
    # Hide unused subplots
    for day_idx in range(len(days), len(axes_flat)):
        axes_flat[day_idx].set_visible(False)
    
    # Add overall legend
    legend_text = ("LEGEND:\n"
                  "• Red: Bid Volume (Sellers)\n"
                  "• Blue: Ask Volume (Buyers)\n" 
                  "• Bold: 3x Imbalance\n"
                  "• Δ: Net Delta (Ask - Bid)")
    
    fig.text(0.02, 0.02, legend_text, fontsize=10, 
            bbox=dict(boxstyle='round,pad=0.5', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    
    # Save chart
    output_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/daily_footprint_boxes.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved daily footprint boxes: {output_path}")
    
    plt.show()

def main():
    """Main function"""
    
    print("📈 DAILY FOOTPRINT BOXES")
    print("=" * 30)
    
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm1_25_debm2_25_tr_ba_data_data_fetch_engine_method_real.parquet'
    
    try:
        trades = load_spread_data(file_path)
        
        if len(trades) == 0:
            print("❌ No trade data found")
            return
        
        footprint = prepare_daily_footprint(trades, tick_size=0.1)
        
        if len(footprint) == 0:
            print("❌ No footprint data created")
            return
        
        create_daily_boxes(footprint, file_path)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()