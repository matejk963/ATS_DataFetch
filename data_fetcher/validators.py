"""
Data Validation Components
==========================

Validators for ensuring data quality and integrity.
"""

import pandas as pd
from typing import Dict


class BidAskValidator:
    """
    Validator for filtering negative bid-ask spreads.
    
    Prevents financially impossible data where ask price < bid price from 
    entering the system.
    """
    
    def __init__(self, strict_mode: bool = True, log_filtered: bool = True):
        """
        Initialize validator.
        
        Args:
            strict_mode: If True, remove invalid records. If False, mark them.
            log_filtered: If True, log details of filtered records.
        """
        self.strict_mode = strict_mode
        self.log_filtered = log_filtered
        self.filtered_count = 0
        self.total_processed = 0
    
    def validate_orders(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """
        Validate bid-ask spreads in order data.
        
        Args:
            df: DataFrame with 'b_price' and 'a_price' columns
            source_name: Name of data source for logging
            
        Returns:
            Validated DataFrame (filtered or marked invalid records)
        """
        if df.empty:
            return df
        
        # Only validate if both bid and ask columns exist and have data
        if 'b_price' not in df.columns or 'a_price' not in df.columns:
            if self.log_filtered:
                print(f"   ⚠️  No bid/ask columns in {source_name} orders - skipping validation")
            return df
        
        # Count records with both bid and ask data
        has_both_prices = df['b_price'].notna() & df['a_price'].notna()
        valid_records = df[has_both_prices]
        
        if valid_records.empty:
            if self.log_filtered:
                print(f"   ⚠️  No records with both bid/ask in {source_name} - skipping validation")
            return df
        
        # Identify negative spreads (ask < bid)
        negative_mask = valid_records['a_price'] < valid_records['b_price']
        negative_count = negative_mask.sum()
        
        self.total_processed += len(valid_records)
        self.filtered_count += negative_count
        
        if negative_count > 0:
            if self.log_filtered:
                worst_spread = (valid_records.loc[negative_mask, 'a_price'] - 
                               valid_records.loc[negative_mask, 'b_price']).min()
                print(f"   🚫 {source_name}: Found {negative_count} negative spreads "
                      f"({negative_count/len(valid_records)*100:.1f}%) - worst: {worst_spread:.3f}")
                
                # Sample of problematic records
                sample_records = valid_records[negative_mask].head(3)
                for idx, row in sample_records.iterrows():
                    spread_val = row['a_price'] - row['b_price']
                    print(f"      🔍 {idx}: bid={row['b_price']:.3f}, ask={row['a_price']:.3f}, "
                          f"spread={spread_val:.3f}")
            
            if self.strict_mode:
                # Remove records with negative spreads
                invalid_indices = valid_records[negative_mask].index
                df_filtered = df.drop(invalid_indices)
                print(f"      ✅ {source_name}: Removed {negative_count} invalid records "
                      f"({len(df)} → {len(df_filtered)})")
                return df_filtered
            else:
                # Mark invalid records
                df.loc[valid_records[negative_mask].index, 'is_valid'] = False
                print(f"      ⚠️  {source_name}: Marked {negative_count} records as invalid")
        else:
            if self.log_filtered:
                print(f"      ✅ {source_name}: All {len(valid_records)} records have valid spreads")
        
        return df
    
    def validate_merged_data(self, df: pd.DataFrame, source_name: str = "Engine") -> pd.DataFrame:
        """
        Validate bid-ask spreads in merged data (trades + orders format).
        Alias for validate_orders to maintain compatibility.
        """
        return self.validate_orders(df, source_name)
    
    def get_stats(self) -> Dict:
        """Get validation statistics."""
        return {
            'total_processed': self.total_processed,
            'filtered_count': self.filtered_count,
            'filter_rate': self.filtered_count / max(1, self.total_processed) * 100
        }