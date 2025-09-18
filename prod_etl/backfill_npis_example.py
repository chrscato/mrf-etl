#!/usr/bin/env python3
"""
Example script demonstrating the flag-based backfill system for dim_npi.parquet

This script shows how to:
1. Add placeholders for NPIs from xref_group_npi
2. Backfill unfetched NPIs with NPPES data
3. Monitor the status of the dim_npi table
"""

import sys
from pathlib import Path

# Add utils to path
sys.path.append('utils')

from utils_nppes import (
    add_npi_placeholders,
    backfill_missing_npis,
    sync_npis_from_xref
)
import polars as pl

def check_dim_npi_status(silver_dir: str = "core/data/silver"):
    """Check the current status of dim_npi.parquet"""
    dim_path = Path(silver_dir) / "dim_npi" / "dim_npi.parquet"
    
    if not dim_path.exists():
        print("dim_npi.parquet not found")
        return
    
    df = pl.read_parquet(str(dim_path))
    print(f"Total NPIs in dim_npi: {df.height}")
    
    if "nppes_fetched" in df.columns:
        fetched = df.filter(pl.col("nppes_fetched") == True).height
        unfetched = df.filter(
            (pl.col("nppes_fetched") == False) | (pl.col("nppes_fetched").is_null())
        ).height
        print(f"  - Fetched from NPPES: {fetched}")
        print(f"  - Not fetched: {unfetched}")
        
        if unfetched > 0:
            print(f"  - Unfetched NPIs: {unfetched} need backfilling")
    else:
        print("  - nppes_fetched column not found (older version)")

def main():
    print("=== NPPES Backfill Example ===")
    print()
    
    # Check initial status
    print("1. Initial dim_npi status:")
    check_dim_npi_status()
    print()
    
    # Option 1: Add placeholders only
    print("2. Adding placeholders from xref_group_npi...")
    try:
        placeholders_added = add_npi_placeholders(
            ["1295795499", "1639195834", "1477564367"],  # Example NPIs
            silver_dir="core/data/silver"
        )
        print(f"   Added {placeholders_added} placeholder records")
    except Exception as e:
        print(f"   Error adding placeholders: {e}")
    print()
    
    # Check status after placeholders
    print("3. Status after adding placeholders:")
    check_dim_npi_status()
    print()
    
    # Option 2: Backfill unfetched NPIs
    print("4. Backfilling unfetched NPIs...")
    try:
        found, updated = backfill_missing_npis(
            silver_dir="core/data/silver",
            sleep=0.1  # Be respectful to NPPES API
        )
        print(f"   Found: {found}, Updated: {updated}")
    except Exception as e:
        print(f"   Error during backfill: {e}")
    print()
    
    # Check final status
    print("5. Final dim_npi status:")
    check_dim_npi_status()
    print()
    
    # Option 3: Complete sync (commented out to avoid API calls)
    print("6. Complete sync option (commented out to avoid API calls):")
    print("   # Uncomment to run complete sync:")
    print("   # placeholders, found, updated = sync_npis_from_xref(")
    print("   #     silver_dir='core/data/silver', sleep=0.1")
    print("   # )")
    print("   # print(f'Sync complete - Placeholders: {placeholders}, Found: {found}, Updated: {updated}')")
    print()
    
    print("=== Example Complete ===")
    print()
    print("CLI Usage:")
    print("  # Backfill unfetched NPIs:")
    print("  python utils/utils_nppes.py --silver-dir core/data/silver --backfill --sleep 0.1")
    print()
    print("  # Complete sync:")
    print("  python utils/utils_nppes.py --silver-dir core/data/silver --sync --sleep 0.1")

if __name__ == "__main__":
    main()
