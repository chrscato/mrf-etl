"""
Georgia Workers' Compensation rate processing
"""
import pandas as pd
import logging
from typing import Iterator
from src.utils.chunk_processor import ChunkProcessor
from src.extractors.excel_extractor import ExcelExtractor

logger = logging.getLogger(__name__)

class GAWCProcessor:
    """Process Georgia Workers' Compensation rates"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
        self.excel_extractor = ExcelExtractor()
    
    def initialize_ga_wc_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Initialize GA WC rate columns with None values"""
        logger.info("Initializing GA WC rate columns...")
        
        def init_ga_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Add GA WC rate columns and initialize with None
            chunk['GA_PROF_MAR'] = None
            chunk['GA_OP_MAR'] = None
            chunk['GA_ASC_MAR'] = None
            return chunk
        
        return self.chunk_processor.filter_chunks(chunks, init_ga_columns)
    
    def merge_ga_wc_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Merge with Georgia WC data"""
        logger.info("Merging with Georgia WC data...")
        
        # Load GA WC data
        ga_wc_data = self.excel_extractor.extract_georgia_wc_data()
        
        if ga_wc_data.empty:
            logger.warning("Georgia WC data not available - skipping merge")
            return chunks
        
        def merge_ga_wc_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns:
                logger.warning("No billing_code column found for GA WC merge")
                return chunk
            
            # Ensure both columns are string type for join
            chunk['billing_code'] = chunk['billing_code'].astype(str)
            ga_wc_data['CODE'] = ga_wc_data['CODE'].astype(str)
            
            # Merge with GA WC data
            merged = pd.merge(
                chunk,
                ga_wc_data,
                left_on='billing_code',
                right_on='CODE',
                how='left'
            )
            
            # Move values from source columns to target columns (only overwrite where src_col is not null)
            rate_mapping = [
                ('PROF MAR', 'GA_PROF_MAR'),
                ('OP MAR', 'GA_OP_MAR'), 
                ('ASC MAR', 'GA_ASC_MAR')
            ]
            
            for src_col, dest_col in rate_mapping:
                if src_col in merged.columns and dest_col in merged.columns:
                    # Only overwrite where src_col is not null
                    merged[dest_col] = merged[src_col].combine_first(merged[dest_col])
            
            # Clean up temporary columns
            cols_to_drop = ['CODE'] + [col for col, _ in rate_mapping if col in merged.columns]
            cols_to_drop.extend(['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 
                               'MOD', 'DESCRIPTION', 'FUD', 'APC', 'SI', 'PI'])
            
            return merged.drop(columns=cols_to_drop, errors='ignore')
        
        return self.chunk_processor.filter_chunks(chunks, merge_ga_wc_chunk)
