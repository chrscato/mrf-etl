# NPPES Utils Update Summary

## Overview
Updated `utils_nppes.py` to work better within the `prod_etl` folder structure and provide more flexibility for designating custom `dim_npi.parquet` files.

## Key Changes Made

### 1. Enhanced Path Functions
- **Modified `dim_paths()`**: Now accepts an optional `dim_npi_filename` parameter
- **Default behavior**: Still uses `"dim_npi.parquet"` if no custom filename specified
- **Custom filenames**: Can now specify any filename like `"dim_npi_custom.parquet"`

### 2. Updated Public API Functions
All main functions now support custom dim_npi filenames:

- `add_npi_to_dims(npi, silver_dir, dim_npi_filename="dim_npi.parquet")`
- `add_many_npis(npis, silver_dir, sleep=0.0, dim_npi_filename="dim_npi.parquet")`

### 3. New Notebook-Friendly Functions
Added convenience functions for notebook usage:

- `get_npi_info(npi, silver_dir, dim_npi_filename)` - Get NPI info from existing table or fetch from NPPES
- `list_available_npis(silver_dir, dim_npi_filename, limit)` - List NPIs in dim table
- `batch_update_npis_from_fact(silver_dir, dim_npi_filename, sleep)` - Extract NPIs from xref_group_npi and update dim_npi

### 4. Flag-Based Backfill System (NEW!)
Added comprehensive backfill functionality:

- **`nppes_fetched` flag**: Tracks whether each NPI has been fetched from NPPES
- **`nppes_fetch_date`**: Records when the NPI was fetched
- **`add_npi_placeholders()`**: Add placeholder records with `nppes_fetched=False`
- **`backfill_missing_npis()`**: Find and backfill NPIs with `nppes_fetched=False`
- **`sync_npis_from_xref()`**: Complete sync - add placeholders from xref_group_npi, then backfill

### 5. Enhanced CLI
- Added `--dim-npi-filename` parameter to CLI
- Added `--backfill` flag to backfill unfetched NPIs
- Added `--sync` flag for complete sync operation
- Added `--placeholders-only` flag to add placeholders without backfilling
- Maintains backward compatibility with existing usage

## Usage Examples

### From Notebook (ETL_2.ipynb)
```python
# Import utilities
from utils_nppes import add_npi_to_dims, get_npi_info, list_available_npis

# Use custom filename
custom_filename = "dim_npi_custom.parquet"
result = add_npi_to_dims("1295795499", "core/data/silver", dim_npi_filename=custom_filename)

# Query existing data
npis = list_available_npis("core/data/silver", dim_npi_filename=custom_filename)
```

### From Command Line
```bash
# Use default dim_npi.parquet
python utils/utils_nppes.py --silver-dir core/data/silver --add 1295795499

# Use custom filename
python utils/utils_nppes.py --silver-dir core/data/silver --add 1295795499 --dim-npi-filename dim_npi_custom.parquet

# Backfill unfetched NPIs
python utils/utils_nppes.py --silver-dir core/data/silver --backfill --sleep 0.1

# Complete sync (placeholders + backfill)
python utils/utils_nppes.py --silver-dir core/data/silver --sync --sleep 0.1

# Add placeholders only
python utils/utils_nppes.py --silver-dir core/data/silver --placeholders-only
```

### From Python Script
```python
# Batch update with custom filename
found, wrote = batch_update_npis_from_fact(
    silver_dir="core/data/silver",
    dim_npi_filename="dim_npi_batch.parquet",
    sleep=0.1
)

# Flag-based backfill operations
# Add placeholders for NPIs
placeholders_added = add_npi_placeholders(
    ["1295795499", "1639195834"], 
    "core/data/silver"
)

# Backfill unfetched NPIs
found, updated = backfill_missing_npis(
    "core/data/silver", 
    sleep=0.1
)

# Complete sync
placeholders, found, updated = sync_npis_from_xref(
    "core/data/silver", 
    sleep=0.1
)
```

## File Structure
```
prod_etl/
├── utils/
│   └── utils_nppes.py          # Updated utility functions
├── ETL_2.ipynb                 # Example notebook
├── example_nppes_usage.py      # Example script
└── core/data/silver/
    └── dim_npi/
        ├── dim_npi.parquet     # Default file
        ├── dim_npi_custom.parquet  # Custom files
        └── dim_npi_batch.parquet
```

## Benefits

1. **Flexibility**: Can maintain multiple dim_npi files for different purposes
2. **Notebook Integration**: Easy to use from Jupyter notebooks
3. **Backward Compatibility**: Existing code continues to work
4. **Batch Operations**: Can extract NPIs from fact table automatically
5. **Memory Efficient**: Uses DuckDB for large table operations
6. **Flag-Based Tracking**: Track which NPIs have been fetched from NPPES
7. **Incremental Backfill**: Only fetch NPIs that haven't been fetched yet
8. **Placeholder System**: Add NPIs as placeholders and backfill later

## Migration Guide

### Existing Code
No changes needed - all existing code will continue to work with default `dim_npi.parquet`.

### New Features
To use custom filenames, simply add the `dim_npi_filename` parameter:

```python
# Old way (still works)
add_npi_to_dims("1295795499", "core/data/silver")

# New way with custom filename
add_npi_to_dims("1295795499", "core/data/silver", dim_npi_filename="my_custom.parquet")
```

## Next Steps

1. Run `ETL_2.ipynb` to test the new functionality
2. Use `backfill_npis_example.py` to test the backfill system
3. Consider creating different dim_npi files for different data sources or time periods
4. Use `sync_npis_from_xref()` to ensure all referenced NPIs have provider details
5. Monitor `nppes_fetched` flag to track which NPIs need backfilling
6. Use `--backfill` CLI option for regular maintenance of unfetched NPIs
