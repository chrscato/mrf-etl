"""
Utility for processing large datasets in chunks
"""
import pandas as pd
import logging
from typing import Callable, Iterator, Optional, List
from pathlib import Path
import pyarrow.parquet as pq
from config.settings import CHUNK_SIZE

logger = logging.getLogger(__name__)

class ChunkProcessor:
    def __init__(self, chunk_size: int = CHUNK_SIZE):
        self.chunk_size = chunk_size
        
    def read_parquet_chunks(self, file_path: Path) -> Iterator[pd.DataFrame]:
        """
        Read parquet file in chunks using pyarrow
        """
        try:
            parquet_file = pq.ParquetFile(file_path)
            total_rows = parquet_file.metadata.num_rows
            logger.info(f"Processing {total_rows} rows from {file_path}")
            
            for batch in parquet_file.iter_batches(batch_size=self.chunk_size):
                yield batch.to_pandas()
                
        except Exception as e:
            logger.error(f"Failed to read parquet file {file_path}: {e}")
            # Try to salvage data by reading row groups individually
            yield from self._salvage_parquet_data(file_path)
    
    def _salvage_parquet_data(self, file_path: Path) -> Iterator[pd.DataFrame]:
        """
        Attempt to salvage data from partially corrupted parquet files
        """
        try:
            parquet_file = pq.ParquetFile(file_path)
            logger.warning(f"Attempting to salvage data from {file_path}")
            
            for i in range(parquet_file.num_row_groups):
                try:
                    batch = parquet_file.read_row_group(i).to_pandas()
                    yield batch
                except Exception as e:
                    logger.warning(f"Failed to read row group {i}: {e}")
                    
        except Exception as e:
            logger.error(f"Could not salvage any data from {file_path}: {e}")
    
    def process_chunks(self, 
                      chunks: Iterator[pd.DataFrame], 
                      transform_func: Callable[[pd.DataFrame], pd.DataFrame],
                      output_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Process chunks through a transformation function and optionally save results
        """
        processed_chunks = []
        chunk_count = 0
        
        if chunks is None:
            logger.warning("Chunks iterator is None - returning empty DataFrame")
            return pd.DataFrame()
        
        for chunk in chunks:
            if chunk is None:
                logger.warning(f"Received None chunk at position {chunk_count} - skipping")
                continue
                
            try:
                processed_chunk = transform_func(chunk)
                processed_chunks.append(processed_chunk)
                chunk_count += 1
                
                if chunk_count % 10 == 0:
                    logger.info(f"Processed {chunk_count} chunks")
                    
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_count}: {e}")
                continue
        
        if not processed_chunks:
            logger.warning("No chunks were successfully processed")
            return pd.DataFrame()
        
        # Combine all chunks
        result = pd.concat(processed_chunks, ignore_index=True)
        logger.info(f"Combined {len(processed_chunks)} chunks into {len(result)} rows")
        
        # Save if output path provided
        if output_path:
            self.save_dataframe(result, output_path)
        
        return result
    
    def save_dataframe(self, df: pd.DataFrame, output_path: Path) -> None:
        """
        Save dataframe to parquet with error handling
        """
        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert problematic columns to string to avoid Arrow errors
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Check if column contains mixed types
                    if df[col].apply(lambda x: type(x)).nunique() > 1:
                        df[col] = df[col].astype(str)
            
            df.to_parquet(output_path, index=False)
            logger.info(f"Saved {len(df)} rows to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to save dataframe to {output_path}: {e}")
            # Fallback to CSV
            csv_path = output_path.with_suffix('.csv')
            df.to_csv(csv_path, index=False)
            logger.info(f"Saved as CSV instead: {csv_path}")
    
    def merge_chunks(self, 
                    left_chunks: Iterator[pd.DataFrame],
                    right_df: pd.DataFrame,
                    left_on: str,
                    right_on: str,
                    how: str = 'left') -> Iterator[pd.DataFrame]:
        """
        Merge chunks with a reference dataframe
        """
        for chunk in left_chunks:
            try:
                merged_chunk = pd.merge(chunk, right_df, left_on=left_on, right_on=right_on, how=how)
                yield merged_chunk
            except Exception as e:
                logger.error(f"Error merging chunk: {e}")
                continue
    
    def filter_chunks(self,
                     chunks: Iterator[pd.DataFrame],
                     filter_func: Callable[[pd.DataFrame], pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """
        Apply filter function to chunks
        """
        logger.info(f"ChunkProcessor.filter_chunks input type: {type(chunks)}")
        if chunks is None:
            logger.error("ChunkProcessor.filter_chunks received None chunks - returning empty iterator")
            return iter([])
        
        chunk_count = 0
        for chunk in chunks:
            chunk_count += 1
            logger.info(f"ChunkProcessor processing chunk {chunk_count} with {len(chunk)} rows")
            try:
                filtered_chunk = filter_func(chunk)
                if not filtered_chunk.empty:
                    logger.info(f"ChunkProcessor yielding filtered chunk {chunk_count} with {len(filtered_chunk)} rows")
                    yield filtered_chunk
                else:
                    logger.warning(f"ChunkProcessor filtered chunk {chunk_count} is empty, skipping")
            except Exception as e:
                logger.error(f"Error filtering chunk {chunk_count}: {e}")
                continue
        
        logger.info(f"ChunkProcessor processed {chunk_count} total chunks")