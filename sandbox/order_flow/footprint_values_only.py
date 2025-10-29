#!/usr/bin/env python3
"""
Footprint Chart - Values Only in Boxes
=======================================

Creates clean footprint chart showing only volume values in boxes without colors.
Action: -1 = bid side (sell), 1 = ask side (buy)
Price levels: 10 cents per tick
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
    """
    Prepare data for footprint chart with daily periods and tick-sized price levels
    
    Args:
        trades: DataFrame with trades
        tick_size: Price tick size (default: 0.1 = 10 cents)
    """
    print(f"\n🔄 Preparing footprint data with {tick_size} tick size...")
    
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
    
    # Calculate net volume (positive = more buying, negative = more selling)
    footprint['net_volume'] = footprint['ask_volume'] - footprint['bid_volume']
    footprint['total_volume'] = footprint['ask_volume'] + footprint['bid_volume']
    
    print(f"   ✅ Created footprint data: {len(footprint)} day/price combinations")
    print(f"   Days: {footprint['day'].min()} to {footprint['day'].max()}")
    print(f"   Price ticks: {footprint['price_tick'].min():.1f} to {footprint['price_tick'].max():.1f}")
    
    return footprint

def create_footprint_values_chart(footprint: pd.DataFrame, file_path: str):
    """Create clean footprint chart with values only in boxes"""
    
    print(f"\n📈 Creating values-only footprint chart...")
    
    # Get unique days and price levels
    days = sorted(footprint['day'].unique())
    price_levels = sorted(footprint['price_tick'].unique())
    
    # Limit to reasonable number of days for visibility
    max_days = 60  # Show up to 60 days
    if len(days) > max_days:
        print(f"   📊 Limiting to first {max_days} days for visibility")
        days = days[:max_days]
        footprint = footprint[footprint['day'].isin(days)]
    
    print(f"   📅 Displaying {len(days)} days")
    print(f"   💰 Displaying {len(price_levels)} price levels")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(max(12, len(days) * 0.4), max(10, len(price_levels) * 0.15)))
    
    # Create grid of boxes
    for i, day in enumerate(days):
        day_data = footprint[footprint['day'] == day]
        
        for j, price_level in enumerate(price_levels):
            # Draw box outline
            rect = patches.Rectangle(
                (i, j), 1, 1,
                linewidth=0.5, edgecolor='black', facecolor='white'
            )
            ax.add_patch(rect)
            
            # Find data for this day/price combination
            day_price_data = day_data[day_data['price_tick'] == price_level]
            
            if not day_price_data.empty:
                row = day_price_data.iloc[0]
                bid_vol = int(row['bid_volume'])
                ask_vol = int(row['ask_volume'])
                total_vol = int(row['total_volume'])
                
                # Check for 3x imbalances
                bid_bold = False
                ask_bold = False
                
                if bid_vol > 0 or ask_vol > 0:
                    # Check bid imbalance (vs ask one level higher)
                    if bid_vol > 0 and j + 1 < len(price_levels):
                        higher_price = price_levels[j + 1]
                        higher_data = footprint[(footprint['day'] == day) & (footprint['price_tick'] == higher_price)]
                        if not higher_data.empty:
                            higher_ask = higher_data.iloc[0]['ask_volume']
                            if higher_ask == 0 or bid_vol >= 3 * higher_ask:
                                bid_bold = True
                    
                    # Check ask imbalance (vs bid one level lower)
                    if ask_vol > 0 and j > 0:
                        lower_price = price_levels[j - 1]
                        lower_data = footprint[(footprint['day'] == day) & (footprint['price_tick'] == lower_price)]
                        if not lower_data.empty:
                            lower_bid = lower_data.iloc[0]['bid_volume']
                            if lower_bid == 0 or ask_vol >= 3 * lower_bid:
                                ask_bold = True
                    
                    # Format: BidVol/AskVol
                    text = f"{bid_vol}/{ask_vol}"
                    
                    # Determine text weight based on imbalances
                    weight = 'bold' if (bid_bold or ask_bold) else 'normal'
                    
                    # Color based on which side dominates
                    if ask_vol > bid_vol:
                        text_color = 'blue'  # More buying
                    elif bid_vol > ask_vol:
                        text_color = 'red'   # More selling
                    else:
                        text_color = 'black' # Equal
                    
                    # Add text in center of box
                    ax.text(
                        i + 0.5, j + 0.5,
                        text, ha='center', va='center',
                        fontsize=max(6, min(9, 120 // len(days))), 
                        color=text_color, weight=weight
                    )
    
    # Customize axes
    ax.set_xlim(0, len(days))
    ax.set_ylim(0, len(price_levels))
    
    # Set x-axis (days)
    ax.set_xticks([i + 0.5 for i in range(len(days))])
    ax.set_xticklabels([d.strftime('%m/%d') for d in days], rotation=45, ha='right', fontsize=8)
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    
    # Set y-axis (price levels)
    price_step = max(1, len(price_levels) // 30)  # Show ~30 price labels max
    price_ticks = [i + 0.5 for i in range(0, len(price_levels), price_step)]
    price_labels = [f"{price_levels[i]:.1f}" for i in range(0, len(price_levels), price_step)]
    ax.set_yticks(price_ticks)
    ax.set_yticklabels(price_labels, fontsize=8)
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12, fontweight='bold')
    
    # Title and styling
    spread_name = Path(file_path).stem.replace('_tr_ba_data_data_fetch_engine_method_real', '')
    ax.set_title(f'Footprint Chart - {spread_name}\nOrder Flow Values by Day and Price Level', 
                fontsize=14, fontweight='bold', pad=20)
    
    # Add legend
    legend_text = ("Legend:\n"
                  "• Format: BidVolume/AskVolume\n"
                  "• Red: More selling (bid > ask)\n"
                  "• Blue: More buying (ask > bid)\n" 
                  "• Bold: 3x imbalance vs adjacent level\n"
                  "• Empty: No trading\n"
                  "• 10 cent price ticks")
    
    ax.text(1.02, 0.5, legend_text, transform=ax.transAxes, fontsize=10,
           verticalalignment='center', bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))
    
    # Remove top and right spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    
    # Save chart
    output_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/footprint_values_only.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved values-only footprint chart: {output_path}")
    
    plt.show()
    
    # Print summary statistics
    print(f"\n📊 FOOTPRINT SUMMARY:")
    total_bid = footprint['bid_volume'].sum()
    total_ask = footprint['ask_volume'].sum()
    active_days = len(footprint['day'].unique())
    active_prices = len(footprint['price_tick'].unique())
    
    print(f"   Active trading days: {active_days}")
    print(f"   Active price levels: {active_prices}")
    print(f"   Total Bid Volume: {total_bid:,.0f}")
    print(f"   Total Ask Volume: {total_ask:,.0f}")
    print(f"   Net Volume: {total_ask - total_bid:,.0f} ({'Buying' if total_ask > total_bid else 'Selling'} pressure)")
    print(f"   Volume Ratio (Ask/Bid): {total_ask/total_bid:.2f}" if total_bid > 0 else "   No bid volume")

def main():
    """Main function to create values-only footprint chart"""
    
    print("📈 FOOTPRINT CHART - VALUES ONLY")
    print("=" * 40)
    
    # Use the last file you wanted plotted
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
        
        # Create values-only chart
        create_footprint_values_chart(footprint, file_path)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()