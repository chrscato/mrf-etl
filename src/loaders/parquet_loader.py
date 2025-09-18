"""
Data loading utilities for parquet files
"""
import pandas as pd
import logging
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

class ParquetLoader:
    """Load processed data to parquet files"""
    
    def save_dataframe(self, df: pd.DataFrame, output_path: Path, 
                      prepare_for_parquet: bool = True) -> None:
        """Save dataframe to parquet with preprocessing"""
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            if prepare_for_parquet:
                df = self._prepare_for_parquet(df)
            
            df.to_parquet(output_path, index=False)
            logger.info(f"Saved {len(df)} rows to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save dataframe to {output_path}: {e}")
            # Fallback to CSV
            csv_path = output_path.with_suffix('.csv')
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved as CSV instead: {csv_path}")
    
    def _prepare_for_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare dataframe for parquet serialization"""
        df = df.copy()
        
        # Convert problematic columns to string to avoid Arrow errors
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if column contains mixed types
                sample_types = df[col].dropna().head(1000).apply(lambda x: type(x)).nunique()
                if sample_types > 1:
                    logger.debug(f"Converting mixed-type column {col} to string")
                    df[col] = df[col].astype(str)
        
        # Handle specific problematic columns from Georgia WC data
        ga_columns = ['GA_PROF_MAR', 'GA_OP_MAR', 'GA_ASC_MAR']
        for col in ga_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df
    
    def append_to_existing(self, new_df: pd.DataFrame, existing_path: Path) -> None:
        """Append new data to existing parquet file"""
        try:
            if existing_path.exists():
                logger.info(f"Loading existing data from {existing_path}")
                existing_df = pd.read_parquet(existing_path)
                
                logger.info(f"Combining {len(existing_df)} existing + {len(new_df)} new rows")
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                combined_df = new_df
            
            self.save_dataframe(combined_df, existing_path)
            
        except Exception as e:
            logger.error(f"Failed to append to existing file: {e}")
            # Save as new file with timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_path = existing_path.with_stem(f"{existing_path.stem}_{timestamp}")
            self.save_dataframe(new_df, new_path)