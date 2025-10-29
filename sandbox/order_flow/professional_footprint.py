#!/usr/bin/env python3
"""
Professional Footprint Chart
============================

Replicates professional order flow chart style with:
- Vertical price levels on right
- Horizontal time periods at bottom
- Bid/Ask values in cells
- Bold highlighting for 3x imbalances
- Delta calculations
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

def prepare_footprint_data(trades: pd.DataFrame, tick_size: float = 0.1) -> pd.DataFrame:
    """Prepare data for professional footprint chart"""
    
    print(f"\n🔄 Preparing professional footprint data with {tick_size} tick size...")
    
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
    
    # Calculate delta (net volume)
    footprint['delta'] = footprint['ask_volume'] - footprint['bid_volume']
    footprint['total_volume'] = footprint['ask_volume'] + footprint['bid_volume']
    
    print(f"   ✅ Created footprint data: {len(footprint)} day/price combinations")
    print(f"   Days: {footprint['day'].min()} to {footprint['day'].max()}")
    print(f"   Price ticks: {footprint['price_tick'].min():.1f} to {footprint['price_tick'].max():.1f}")
    
    return footprint

def check_imbalance(footprint: pd.DataFrame, day: pd.Timestamp, price: float, side: str) -> bool:
    """
    Check if current cell shows significant imbalance (3x rule)
    
    Args:
        footprint: Full footprint data
        day: Current day
        price: Current price level
        side: 'bid' or 'ask'
    
    Returns:
        bool: True if should be highlighted as bold
    """
    # Get current cell data
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
    
    # Check if current value is 3x higher than opposite side at adjacent level
    if compare_value == 0:
        return current_value > 0
    
    return current_value >= 3 * compare_value

def create_professional_footprint(footprint: pd.DataFrame, file_path: str):
    """Create professional-style footprint chart"""
    
    print(f"\n📈 Creating professional footprint chart...")
    
    # Get unique days and price levels
    days = sorted(footprint['day'].unique())
    price_levels = sorted(footprint['price_tick'].unique())
    
    # Limit to reasonable number of days
    max_days = 20  # Show fewer days for better readability
    if len(days) > max_days:
        print(f"   📊 Limiting to first {max_days} days for professional view")
        days = days[:max_days]
        footprint = footprint[footprint['day'].isin(days)]
        price_levels = sorted(footprint['price_tick'].unique())
    
    print(f"   📅 Displaying {len(days)} days")
    print(f"   💰 Displaying {len(price_levels)} price levels")
    
    # Create figure with professional proportions
    fig, ax = plt.subplots(figsize=(max(16, len(days) * 0.8), max(12, len(price_levels) * 0.2)))
    
    # Set white background
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    # Create the grid
    cell_width = 1.0
    cell_height = 1.0
    
    for i, day in enumerate(days):
        day_data = footprint[footprint['day'] == day]
        
        for j, price_level in enumerate(price_levels):
            x_pos = i
            y_pos = j
            
            # Draw cell border
            rect = patches.Rectangle(
                (x_pos, y_pos), cell_width, cell_height,
                linewidth=0.5, edgecolor='black', facecolor='white'
            )
            ax.add_patch(rect)
            
            # Find data for this day/price combination
            day_price_data = day_data[day_data['price_tick'] == price_level]
            
            if not day_price_data.empty:
                row = day_price_data.iloc[0]
                bid_vol = int(row['bid_volume'])
                ask_vol = int(row['ask_volume'])
                
                # Check for imbalances
                bid_bold = check_imbalance(footprint, day, price_level, 'bid') if bid_vol > 0 else False
                ask_bold = check_imbalance(footprint, day, price_level, 'ask') if ask_vol > 0 else False
                
                # Add bid volume (bottom part of cell) in red
                if bid_vol > 0:
                    ax.text(
                        x_pos + 0.5, y_pos + 0.25,
                        str(bid_vol), ha='center', va='center',
                        fontsize=8, color='red', 
                        weight='bold' if bid_bold else 'normal'
                    )
                
                # Add ask volume (top part of cell) in blue
                if ask_vol > 0:
                    ax.text(
                        x_pos + 0.5, y_pos + 0.75,
                        str(ask_vol), ha='center', va='center',
                        fontsize=8, color='blue',
                        weight='bold' if ask_bold else 'normal'
                    )
    
    # Customize axes - professional style
    ax.set_xlim(0, len(days))
    ax.set_ylim(0, len(price_levels))
    
    # Remove default ticks
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Add price labels on the RIGHT side (like professional charts)
    price_step = max(1, len(price_levels) // 20)
    for i in range(0, len(price_levels), price_step):
        y_pos = i + 0.5
        price_val = price_levels[i]
        ax.text(len(days) + 0.1, y_pos, f"{price_val:.1f}", 
               ha='left', va='center', fontsize=10, weight='bold')
    
    # Add time labels at the BOTTOM
    for i, day in enumerate(days):
        ax.text(i + 0.5, -0.5, day.strftime('%m/%d'), 
               ha='center', va='top', fontsize=9, rotation=45)
    
    # Add professional title
    spread_name = Path(file_path).stem.replace('_tr_ba_data_data_fetch_engine_method_real', '')
    ax.text(len(days)/2, len(price_levels) + 2, f'ORDER FLOW - {spread_name.upper()}', 
           ha='center', va='bottom', fontsize=16, weight='bold')
    
    # Add professional legend
    legend_text = ("FOOTPRINT LEGEND:\n"
                  "• Blue: Ask Volume (Buyers)\n"
                  "• Red: Bid Volume (Sellers)\n"
                  "• Bold: 3x Imbalance vs Adjacent Level\n"
                  "• Price Levels: 0.1 EUR ticks")
    
    ax.text(len(days) + 2, len(price_levels) * 0.8, legend_text, 
           ha='left', va='top', fontsize=10,
           bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.8))
    
    # Calculate and display volume profile on the right
    price_totals = footprint.groupby('price_tick').agg({
        'bid_volume': 'sum',
        'ask_volume': 'sum',
        'total_volume': 'sum'
    }).reset_index()
    
    max_volume = price_totals['total_volume'].max()
    
    for _, row in price_totals.iterrows():
        price_idx = price_levels.index(row['price_tick'])
        volume_width = (row['total_volume'] / max_volume) * 2  # Scale to 2 units max
        
        # Draw volume bar
        volume_rect = patches.Rectangle(
            (len(days) + 4, price_idx + 0.3), volume_width, 0.4,
            linewidth=0, facecolor='gray', alpha=0.6
        )
        ax.add_patch(volume_rect)
        
        # Add volume number
        if row['total_volume'] > max_volume * 0.1:  # Only show significant volumes
            ax.text(len(days) + 4 + volume_width + 0.1, price_idx + 0.5,
                   f"{int(row['total_volume'])}", ha='left', va='center', fontsize=8)
    
    # Set final axis limits
    ax.set_xlim(-0.5, len(days) + 8)
    ax.set_ylim(-2, len(price_levels) + 4)
    
    # Remove spines for professional look
    for spine in ax.spines.values():
        spine.set_visible(False)
    
    plt.tight_layout()
    
    # Save chart
    output_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/professional_footprint.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"   💾 Saved professional footprint chart: {output_path}")
    
    plt.show()
    
    # Print summary
    print(f"\n📊 PROFESSIONAL FOOTPRINT SUMMARY:")
    total_bid = footprint['bid_volume'].sum()
    total_ask = footprint['ask_volume'].sum()
    total_delta = footprint['delta'].sum()
    
    print(f"   Period: {days[0].strftime('%Y-%m-%d')} to {days[-1].strftime('%Y-%m-%d')}")
    print(f"   Total Bid Volume: {total_bid:,.0f}")
    print(f"   Total Ask Volume: {total_ask:,.0f}")
    print(f"   Net Delta: {total_delta:,.0f}")
    print(f"   Imbalance Ratio: {abs(total_ask/total_bid):.2f}" if total_bid > 0 else "   No bid volume")

def main():
    """Main function to create professional footprint chart"""
    
    print("📈 PROFESSIONAL FOOTPRINT CHART")
    print("=" * 45)
    
    file_path = '/mnt/c/Users/krajcovic/Documents/Testing Data/RawData/spreads/debm1_25_debm2_25_tr_ba_data_data_fetch_engine_method_real.parquet'
    
    try:
        # Load data
        trades = load_spread_data(file_path)
        
        if len(trades) == 0:
            print("❌ No trade data found")
            return
        
        # Prepare footprint data
        footprint = prepare_footprint_data(trades, tick_size=0.1)
        
        if len(footprint) == 0:
            print("❌ No footprint data created")
            return
        
        # Create professional chart
        create_professional_footprint(footprint, file_path)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()