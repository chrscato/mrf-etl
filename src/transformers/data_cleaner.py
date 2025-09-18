"""
Data cleaning and validation utilities
"""
import pandas as pd
import logging
from typing import Iterator, Callable
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class DataCleaner:
    """Data cleaning and validation utilities"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
    
    def clean_chunks(self, 
                    chunks: Iterator[pd.DataFrame], 
                    clean_func: Callable[[pd.DataFrame], pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Apply cleaning function to chunks"""
        logger.info(f"DataCleaner.clean_chunks input type: {type(chunks)}")
        if chunks is None:
            logger.error("DataCleaner received None chunks - returning empty iterator")
            return iter([])
        
        result = self.chunk_processor.filter_chunks(chunks, clean_func)
        logger.info(f"DataCleaner.clean_chunks result type: {type(result)}")
        if result is None:
            logger.error("ChunkProcessor.filter_chunks returned None - returning empty iterator")
            return iter([])
        return result
    
    def standardize_data_types(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Standardize data types for common columns"""
        
        # Numeric columns
        numeric_cols = ['rate', 'medicare_prof']
        for col in numeric_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
        
        # String columns  
        string_cols = ['billing_code', 'prov_npi', 'postal_code', 'payer']
        for col in string_cols:
            if col in chunk.columns:
                chunk[col] = chunk[col].astype(str)
        
        # Date columns
        date_cols = ['rate_updated_on']
        for col in date_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
        
        return chunk
    
    def remove_duplicates(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows"""
        initial_rows = len(chunk)
        chunk = chunk.drop_duplicates()
        
        if len(chunk) < initial_rows:
            logger.debug(f"Removed {initial_rows - len(chunk)} duplicate rows")
        
        return chunk
    
    def validate_required_fields(self, chunk: pd.DataFrame, required_fields: list) -> pd.DataFrame:
        """Remove rows with missing required fields"""
        initial_rows = len(chunk)
        
        for field in required_fields:
            if field in chunk.columns:
                chunk = chunk[chunk[field].notna()]
        
        if len(chunk) < initial_rows:
            logger.debug(f"Removed {initial_rows - len(chunk)} rows with missing required fields")
        
        return chunk
    
    def filter_by_billing_codes(self, chunks: Iterator[pd.DataFrame], 
                               billing_codes_file: str = "data/raw/cpt_codes.txt") -> Iterator[pd.DataFrame]:
        """Filter data by billing codes from txt file"""
        logger.info("Filtering by billing codes from cpt_codes.txt...")
        
        # Read billing codes from txt file
        from pathlib import Path
        billing_codes_txt_path = Path(billing_codes_file)
        if not billing_codes_txt_path.exists():
            logger.warning(f"Billing codes file not found: {billing_codes_txt_path}")
            return chunks
        
        with open(billing_codes_txt_path, "r") as f:
            billing_codes_list = [line.strip() for line in f if line.strip()]
        billing_codes_set = set(billing_codes_list)
        
        logger.info(f"Loaded {len(billing_codes_set)} billing codes from file")
        
        def filter_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns:
                logger.warning("No billing_code column found for filtering")
                return chunk
            
            # Filter to only include billing codes in our set
            chunk['billing_code_str'] = chunk['billing_code'].astype(str)
            filtered = chunk[chunk['billing_code_str'].isin(billing_codes_set)]
            filtered = filtered.drop(columns=['billing_code_str'], errors='ignore')
            
            logger.info(f"Filtered chunk: {len(chunk)} -> {len(filtered)} rows")
            return filtered
        
        return self.chunk_processor.filter_chunks(chunks, filter_chunk)
    
    def rename_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Rename columns according to the specified mapping"""
        logger.info("Renaming columns...")
        
        # Define the rename dictionary as specified
        rename_dict = {
            'negotiated_rate': 'rate',
            'last_updated_on_x': 'rate_updated_on',
            'reporting_entity_name_x': 'payer',
            'reporting_entity_type_x': 'payer_type',
            'npi': 'prov_npi',
            'description': 'code_desc'
        }
        
        def rename_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            # Only rename columns that exist
            existing_renames = {old: new for old, new in rename_dict.items() if old in chunk.columns}
            if existing_renames:
                chunk = chunk.rename(columns=existing_renames)
                logger.info(f"Renamed columns: {existing_renames}")
            return chunk
        
        return self.chunk_processor.filter_chunks(chunks, rename_chunk)
    
    def drop_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Drop unwanted columns"""
        logger.info("Dropping unwanted columns...")
        
        # Define columns to drop as specified
        drop_cols = [
            'provider_reference_id', 'version_x', 'provider_group_id', 'reporting_entity_name_y',
            'reporting_entity_type_y', 'last_updated_on_y', 'version_y', 'expiration_date'
        ]
        
        def drop_chunk_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Only drop columns that exist
            existing_drops = [col for col in drop_cols if col in chunk.columns]
            if existing_drops:
                chunk = chunk.drop(columns=existing_drops, errors='ignore')
                logger.info(f"Dropped columns: {existing_drops}")
            return chunk
        
        return self.chunk_processor.filter_chunks(chunks, drop_chunk_columns)
    
    def drop_nppes_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Drop NPPES-specific columns after enrichment"""
        logger.info("Dropping NPPES columns...")
        
        # Define NPPES columns to drop as specified
        nppes_drop_cols = ['npi', 'error']
        
        def drop_nppes_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Only drop columns that exist
            existing_drops = [col for col in nppes_drop_cols if col in chunk.columns]
            if existing_drops:
                chunk = chunk.drop(columns=existing_drops, errors='ignore')
                logger.info(f"Dropped NPPES columns: {existing_drops}")
            return chunk
        
        return self.chunk_processor.filter_chunks(chunks, drop_nppes_columns)