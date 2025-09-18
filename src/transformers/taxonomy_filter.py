"""
Taxonomy filtering transformer for primary_taxonomy_desc whitelist
"""
import pandas as pd
import logging
from typing import Iterator, Set
from pathlib import Path
from config.settings import FilePaths

logger = logging.getLogger(__name__)

class TaxonomyFilter:
    """Filter data based on primary_taxonomy_desc whitelist"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size
        self.whitelist = self._load_whitelist()
        
    def _load_whitelist(self) -> Set[str]:
        """Load taxonomy descriptions from whitelist file"""
        whitelist_path = FilePaths.TAXONOMY_WHITELIST
        
        if not whitelist_path.exists():
            logger.warning(f"Taxonomy whitelist file not found: {whitelist_path}")
            return set()
        
        whitelist = set()
        
        try:
            with open(whitelist_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if line and not line.startswith('#'):
                        whitelist.add(line.lower())
            
            logger.info(f"Loaded {len(whitelist)} taxonomy descriptions from whitelist")
            
        except Exception as e:
            logger.error(f"Failed to load taxonomy whitelist: {e}")
            return set()
        
        return whitelist
    
    def filter_by_taxonomy(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Filter chunks to only include records with whitelisted taxonomy descriptions"""
        logger.info(f"Taxonomy filter input chunks type: {type(chunks)}")
        
        if chunks is None:
            logger.error("Taxonomy filter received None chunks - returning empty iterator")
            return iter([])
        
        if not self.whitelist:
            logger.warning("No taxonomy whitelist loaded - returning all data")
            for chunk in chunks:
                yield chunk
            return
        
        total_rows_before = 0
        total_rows_after = 0
        
        for chunk in chunks:
            total_rows_before += len(chunk)
            
            # Check if primary_taxonomy_desc column exists
            if 'primary_taxonomy_desc' not in chunk.columns:
                logger.warning("primary_taxonomy_desc column not found - skipping taxonomy filtering")
                yield chunk
                continue
            
            # Filter based on whitelist
            filtered_chunk = self._filter_chunk(chunk)
            total_rows_after += len(filtered_chunk)
            
            if len(filtered_chunk) < len(chunk):
                removed_count = len(chunk) - len(filtered_chunk)
                logger.info(f"Filtered out {removed_count} rows based on taxonomy whitelist")
            
            yield filtered_chunk
        
        if total_rows_before > 0:
            retention_rate = (total_rows_after / total_rows_before) * 100
            logger.info(f"Taxonomy filtering complete: {total_rows_after}/{total_rows_before} rows retained ({retention_rate:.1f}%)")
    
    def _filter_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Filter a single chunk based on taxonomy whitelist"""
        if chunk.empty:
            return chunk
        
        # Create a mask for rows that match the whitelist
        # Handle NaN values and case-insensitive matching
        taxonomy_mask = chunk['primary_taxonomy_desc'].notna()
        
        if taxonomy_mask.any():
            # Convert to lowercase for case-insensitive matching
            taxonomy_lower = chunk.loc[taxonomy_mask, 'primary_taxonomy_desc'].str.lower()
            
            # Check if any part of the taxonomy description matches the whitelist
            # This allows for partial matches (e.g., "Internal Medicine" matches "Internal Medicine - General Practice")
            matches_whitelist = taxonomy_lower.apply(
                lambda desc: any(whitelist_item in desc for whitelist_item in self.whitelist)
            )
            
            # Update the mask to include only rows that match
            taxonomy_mask = taxonomy_mask & matches_whitelist
        
        return chunk[taxonomy_mask].reset_index(drop=True)
    
    def get_whitelist_stats(self, chunks: Iterator[pd.DataFrame]) -> dict:
        """Get statistics about taxonomy distribution in the data"""
        stats = {
            'total_rows': 0,
            'rows_with_taxonomy': 0,
            'whitelisted_rows': 0,
            'unique_taxonomies': set(),
            'whitelisted_taxonomies': set()
        }
        
        for chunk in chunks:
            stats['total_rows'] += len(chunk)
            
            if 'primary_taxonomy_desc' not in chunk.columns:
                continue
            
            # Count rows with taxonomy data
            taxonomy_mask = chunk['primary_taxonomy_desc'].notna()
            stats['rows_with_taxonomy'] += taxonomy_mask.sum()
            
            if taxonomy_mask.any():
                taxonomies = chunk.loc[taxonomy_mask, 'primary_taxonomy_desc'].str.lower()
                stats['unique_taxonomies'].update(taxonomies.unique())
                
                # Count whitelisted rows
                whitelisted_mask = taxonomies.apply(
                    lambda desc: any(whitelist_item in desc for whitelist_item in self.whitelist)
                )
                stats['whitelisted_rows'] += whitelisted_mask.sum()
                stats['whitelisted_taxonomies'].update(taxonomies[whitelisted_mask].unique())
        
        # Convert sets to counts
        stats['unique_taxonomy_count'] = len(stats['unique_taxonomies'])
        stats['whitelisted_taxonomy_count'] = len(stats['whitelisted_taxonomies'])
        
        # Remove sets from final stats
        del stats['unique_taxonomies']
        del stats['whitelisted_taxonomies']
        
        return stats
