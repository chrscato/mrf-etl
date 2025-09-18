# NPPES Memory Efficiency Improvements

## Problem
The NPPES API portion of the ETL pipeline was causing memory crashes because it was not memory efficient. The pipeline was chunked for the main data processing, but the NPPES enrichment was making individual API calls for each row in each chunk, leading to:

1. **Memory Issues**: Each chunk triggered a separate NPPES lookup process
2. **API Inefficiency**: Making individual API calls instead of batching
3. **Rate Limiting**: Hitting API rate limits with too many concurrent requests
4. **No Caching**: Repeated API calls for the same NPIs

## Solution
Implemented a memory-efficient NPPES enrichment system with the following improvements:

### 1. **Batched Processing**
- Collect all unique NPIs across all chunks first
- Process NPIs in batches of 50 to avoid overwhelming the API
- Use fewer concurrent workers (5 instead of 10) to respect rate limits

### 2. **Caching System**
- Implemented persistent caching in `data/cache/nppes_cache.json`
- Avoids duplicate API calls for the same NPIs
- Cache persists between pipeline runs

### 3. **Rate Limiting & Retry Logic**
- Added intelligent rate limiting with exponential backoff
- 2-second delay between batches
- Better error handling and retry logic
- Reduced concurrent workers to avoid API limits

### 4. **Memory Management**
- Process all NPIs in one batch instead of per-chunk
- Reduced memory footprint by avoiding duplicate lookups
- Better error handling to prevent memory leaks

## Key Changes

### New Method: `_memory_efficient_nppes_lookup()`
```python
def _memory_efficient_nppes_lookup(self, npis: list) -> pd.DataFrame:
    """
    Memory-efficient NPPES lookup with batching, caching, and rate limiting.
    """
    # 1. Load existing cache
    nppes_cache = self._load_nppes_cache(cache_file)
    
    # 2. Process NPIs in batches of 50
    batch_size = 50
    npi_batches = [npis[i:i + batch_size] for i in range(0, len(npis), batch_size)]
    
    # 3. Use fewer workers and add delays between batches
    with ThreadPoolExecutor(max_workers=5) as ex:
        # Process each batch with rate limiting
        for batch_idx, npi_batch in enumerate(npi_batches):
            # Process batch...
            if batch_idx < len(npi_batches) - 1:
                time.sleep(2)  # 2 second delay between batches
    
    # 4. Save cache for future runs
    self._save_nppes_cache(nppes_cache, cache_file)
```

### Caching Methods
```python
def _load_nppes_cache(self, cache_file: Path) -> dict:
    """Load NPPES cache from file"""
    
def _save_nppes_cache(self, cache: dict, cache_file: Path) -> None:
    """Save NPPES cache to file"""
```

## Benefits

1. **Memory Efficiency**: 
   - Single batch processing instead of per-chunk processing
   - Caching prevents duplicate API calls
   - Reduced memory footprint

2. **API Efficiency**:
   - Batched processing reduces total API calls
   - Rate limiting prevents API throttling
   - Caching eliminates redundant lookups

3. **Reliability**:
   - Better error handling and retry logic
   - Graceful handling of API failures
   - Persistent caching for recovery

4. **Performance**:
   - Faster subsequent runs due to caching
   - Reduced API wait times
   - Better progress tracking

## Usage

The memory-efficient NPPES enrichment is automatically used when calling:
```python
# This now uses the memory-efficient version
enriched_chunks = geocoder.enrich_with_nppes_data(chunks)
```

## Cache Location
- Cache file: `data/cache/nppes_cache.json`
- Automatically created if it doesn't exist
- Persists between pipeline runs
- Can be safely deleted to force fresh lookups

## Testing
The improvements have been tested with a small dataset and verified to:
- ✅ Successfully enrich data with NPPES information
- ✅ Cache results for future use
- ✅ Handle API errors gracefully
- ✅ Respect rate limits
- ✅ Maintain memory efficiency
