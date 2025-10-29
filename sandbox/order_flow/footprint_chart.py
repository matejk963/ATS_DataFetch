#!/usr/bin/env python3
"""
Footprint Chart Generator
=========================

Creates footprint charts showing order flow at each price level within hourly periods.
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

def create_footprint_chart(footprint: pd.DataFrame, file_path: str):
    """Create footprint chart visualization"""
    
    print(f"\n📈 Creating footprint chart...")
    
    # Get unique days and price levels
    days = sorted(footprint['day'].unique())
    price_levels = sorted(footprint['price_tick'].unique())
    
    # Limit to reasonable number of days for visibility (no need to limit as much)
    max_days = 60  # Show up to 60 days
    if len(days) > max_days:
        print(f"   📊 Limiting to first {max_days} days for visibility")
        days = days[:max_days]
        footprint = footprint[footprint['day'].isin(days)]
    
    print(f"   📅 Displaying {len(days)} days")
    print(f"   💰 Displaying {len(price_levels)} price levels")
    
    # Create figure
    fig, ax = plt.subplots(figsize=(20, 12))
    
    # Define color map for volume intensity
    max_volume = footprint['total_volume'].max()
    
    # Plot each day/price combination
    day_width = 0.8
    price_height = 0.08
    
    for i, day in enumerate(days):
        day_data = footprint[footprint['day'] == day]
        
        for _, row in day_data.iterrows():
            price_tick = row['price_tick']
            bid_vol = row['bid_volume']
            ask_vol = row['ask_volume']
            total_vol = row['total_volume']
            net_vol = row['net_volume']
            
            # Find position in grid
            x_pos = i
            y_pos = price_levels.index(price_tick)
            
            # Color based on net volume (red = selling, green = buying)
            if net_vol > 0:
                color = 'lightgreen'
                edge_color = 'green'
            elif net_vol < 0:
                color = 'lightcoral'
                edge_color = 'red'
            else:
                color = 'lightgray'
                edge_color = 'gray'
            
            # Alpha based on total volume intensity
            alpha = min(0.3 + (total_vol / max_volume) * 0.7, 1.0)
            
            # Draw rectangle for this day/price cell
            rect = patches.Rectangle(
                (x_pos, y_pos), day_width, price_height,
                linewidth=0.5, edgecolor=edge_color, facecolor=color, alpha=alpha
            )
            ax.add_patch(rect)
            
            # Add volume text if significant
            if total_vol > max_volume * 0.01:  # Only show if > 1% of max volume
                # Format: BidVol/AskVol
                text = f"{int(bid_vol)}/{int(ask_vol)}"
                
                # Text color based on which side dominates
                text_color = 'darkgreen' if ask_vol > bid_vol else 'darkred' if bid_vol > ask_vol else 'black'
                
                ax.text(
                    x_pos + day_width/2, y_pos + price_height/2,
                    text, ha='center', va='center',
                    fontsize=8, color=text_color, weight='bold'
                )
    
    # Customize axes
    ax.set_xlim(-0.1, len(days))
    ax.set_ylim(-0.1, len(price_levels))
    
    # Set x-axis (days)
    ax.set_xticks(range(len(days)))
    ax.set_xticklabels([d.strftime('%m/%d\n%Y') for d in days], rotation=45, ha='right')
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    
    # Set y-axis (price levels)
    price_step = max(1, len(price_levels) // 20)  # Show ~20 price labels max
    price_ticks = list(range(0, len(price_levels), price_step))
    ax.set_yticks(price_ticks)
    ax.set_yticklabels([f"{price_levels[i]:.1f}" for i in price_ticks])
    ax.set_ylabel('Price (EUR/MWh)', fontsize=12, fontweight='bold')
    
    # Title and styling
    spread_name = Path(file_path).stem.replace('_tr_ba_data_data_fetch_engine_method_real', '')
    ax.set_title(f'Footprint Chart - {spread_name}\nOrder Flow by Day and Price Level', 
                fontsize=16, fontweight='bold', pad=20)
    
    # Add legend
    legend_elements = [
        patches.Patch(color='lightgreen', label='Net Buying (Ask Volume > Bid Volume)'),
        patches.Patch(color='lightcoral', label='Net Selling (Bid Volume > Ask Volume)'),
        patches.Patch(color='lightgray', label='Balanced (Equal Bid/Ask Volume)')
    ]
    ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    # Add explanation
    explanation = ("Footprint Chart Legend:\n"
                  "• Numbers show: BidVolume/AskVolume\n"
                  "• Color intensity = Total volume\n"
                  "• Green = More buying (action=1)\n"
                  "• Red = More selling (action=-1)\n"
                  "• 10 cent price ticks")
    
    ax.text(1.02, 0.5, explanation, transform=ax.transAxes, fontsize=10,
           verticalalignment='center', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    plt.tight_layout()
    plt.grid(True, alpha=0.3)
    
    # Save chart
    output_path = '/mnt/c/Users/krajcovic/Documents/GitHub/ATS_DataFetch/sandbox/order_flow/footprint_chart.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"   💾 Saved footprint chart: {output_path}")
    
    plt.show()
    
    # Print summary statistics
    print(f"\n📊 FOOTPRINT SUMMARY:")
    total_bid = footprint['bid_volume'].sum()
    total_ask = footprint['ask_volume'].sum()
    print(f"   Total Bid Volume: {total_bid:,.0f}")
    print(f"   Total Ask Volume: {total_ask:,.0f}")
    print(f"   Net Volume: {total_ask - total_bid:,.0f} ({'Buying' if total_ask > total_bid else 'Selling'} pressure)")
    print(f"   Volume Ratio (Ask/Bid): {total_ask/total_bid:.2f}" if total_bid > 0 else "   No bid volume")

def main():
    """Main function to create footprint chart"""
    
    print("📈 FOOTPRINT CHART GENERATOR")
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
        
        # Create chart
        create_footprint_chart(footprint, file_path)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()