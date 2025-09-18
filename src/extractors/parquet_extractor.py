"""
Extract and preprocess data from parquet files
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Set, Iterator
from config.settings import FilePaths, ValidationConfig
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class ParquetExtractor:
    """Extract and filter data from rates and providers parquet files"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
        self.billing_codes = self._load_billing_codes()
    
    def _load_billing_codes(self) -> Set[str]:
        """Load billing codes from text file"""
        try:
            with open(FilePaths.CPT_CODES_TXT, "r") as f:
                codes = {line.strip() for line in f if line.strip()}
            logger.info(f"Loaded {len(codes)} billing codes")
            return codes
        except FileNotFoundError:
            logger.warning(f"Billing codes file not found: {FilePaths.CPT_CODES_TXT}")
            return set()
    
    def extract_rates_data(self) -> Iterator[pd.DataFrame]:
        """Extract rates data in chunks"""
        logger.info("Extracting rates data...")
        
        def filter_rates(chunk: pd.DataFrame) -> pd.DataFrame:
            # Validate required columns
            missing_cols = set(ValidationConfig.REQUIRED_COLUMNS['rates']) - set(chunk.columns)
            if missing_cols:
                logger.warning(f"Missing required columns in rates: {missing_cols}")
            
            # Filter by billing codes if available
            if self.billing_codes and 'billing_code' in chunk.columns:
                initial_rows = len(chunk)
                chunk = chunk[chunk['billing_code'].astype(str).isin(self.billing_codes)]
                logger.debug(f"Filtered rates: {initial_rows} -> {len(chunk)} rows")
            
            return chunk
        
        chunks = self.chunk_processor.read_parquet_chunks(FilePaths.RATES_PARQUET)
        return self.chunk_processor.filter_chunks(chunks, filter_rates)
    
    def extract_providers_data(self) -> pd.DataFrame:
        """Extract providers data (typically smaller, load fully)"""
        logger.info("Extracting providers data...")
        
        try:
            providers_df = pd.read_parquet(FilePaths.PROVIDERS_PARQUET)
            
            # Validate required columns
            missing_cols = set(ValidationConfig.REQUIRED_COLUMNS['providers']) - set(providers_df.columns)
            if missing_cols:
                logger.warning(f"Missing required columns in providers: {missing_cols}")
            
            # Remove rows with null NPI
            initial_rows = len(providers_df)
            providers_df = providers_df[providers_df['npi'].notna()]
            logger.info(f"Providers data: {initial_rows} -> {len(providers_df)} rows (removed null NPIs)")
            
            return providers_df
            
        except Exception as e:
            logger.error(f"Failed to load providers data: {e}")
            return pd.DataFrame()
    
    def merge_rates_providers(self, 
                             rates_chunks: Iterator[pd.DataFrame], 
                             providers_df: pd.DataFrame,
                             left_on: str = 'provider_reference_id',
                             right_on: str = 'provider_group_id',
                             how: str = 'left') -> Iterator[pd.DataFrame]:
        """Merge rates and providers data in chunks"""
        logger.info("Merging rates and providers data...")
        
        return self.chunk_processor.merge_chunks(
            rates_chunks, 
            providers_df, 
            left_on='provider_reference_id',
            right_on='provider_group_id',
            how='left'
        )
    
    def apply_column_transformations(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Apply column renaming and dropping"""
        from config.settings import ColumnMapping
        
        def transform_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Rename columns
            chunk = chunk.rename(columns=ColumnMapping.RENAME_DICT)
            
            # Drop unwanted columns
            chunk = chunk.drop(columns=ColumnMapping.DROP_COLUMNS, errors='ignore')
            
            # Remove rows where prov_npi is null
            if 'prov_npi' in chunk.columns:
                initial_rows = len(chunk)
                chunk = chunk[chunk['prov_npi'].notna()]
                if len(chunk) < initial_rows:
                    logger.debug(f"Removed {initial_rows - len(chunk)} rows with null prov_npi")
            
            return chunk
        
        return self.chunk_processor.filter_chunks(chunks, transform_columns)
    
    def validate_data_quality(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Apply data quality validation"""
        initial_rows = len(chunk)
        
        # Validate rate ranges if rate column exists
        if 'rate' in chunk.columns:
            chunk = chunk[
                (chunk['rate'] >= ValidationConfig.MIN_VALID_RATE) & 
                (chunk['rate'] <= ValidationConfig.MAX_VALID_RATE)
            ]
        
        # Log quality issues
        removed_rows = initial_rows - len(chunk)
        if removed_rows > 0:
            logger.debug(f"Removed {removed_rows} rows due to quality issues")
        
        return chunk