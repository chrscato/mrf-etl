#!/usr/bin/env python3
"""
Fix nppes_fetched flags - reset to False for NPIs that don't have actual NPPES data

This script will:
1. Check which NPIs have actual NPPES data (non-null values in key fields)
2. Reset nppes_fetched to False for NPIs without real data
3. Keep nppes_fetched=True only for NPIs with actual provider information
"""

import sys
from pathlib import Path
import polars as pl

def fix_nppes_flags(silver_dir: str = "core/data/silver", dim_npi_filename: str = "dim_npi.parquet"):
    """Fix the nppes_fetched flags based on actual data presence."""
    
    dim_path = Path(silver_dir) / "dim_npi" / dim_npi_filename
    
    if not dim_path.exists():
        print(f"❌ dim_npi file not found at {dim_path}")
        return False
    
    print(f"📁 Reading dim_npi file: {dim_path}")
    df = pl.read_parquet(str(dim_path))
    
    print(f"📊 Total NPIs: {df.height}")
    print(f"📋 Columns: {df.columns}")
    
    # Check which NPIs have actual NPPES data
    # A NPI has real data if any of these key fields are not null:
    key_fields = ['status', 'organization_name', 'first_name', 'last_name', 'primary_taxonomy_code']
    
    # Find NPIs with actual data
    has_data_condition = pl.lit(False)
    for field in key_fields:
        if field in df.columns:
            has_data_condition = has_data_condition | pl.col(field).is_not_null()
    
    # Count current status
    currently_fetched = df.filter(pl.col("nppes_fetched") == True).height
    actually_has_data = df.filter(has_data_condition).height
    
    print(f"🔍 Currently marked as fetched: {currently_fetched}")
    print(f"✅ Actually has NPPES data: {actually_has_data}")
    
    # Fix the flags
    print("🔧 Fixing nppes_fetched flags...")
    
    df_fixed = df.with_columns([
        pl.when(has_data_condition)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("nppes_fetched")
    ])
    
    # Count after fix
    after_fetch = df_fixed.filter(pl.col("nppes_fetched") == True).height
    after_unfetch = df_fixed.filter(pl.col("nppes_fetched") == False).height
    
    print(f"✅ After fix - Fetched: {after_fetch}")
    print(f"⏳ After fix - Not fetched: {after_unfetch}")
    
    # Create backup
    backup_path = dim_path.with_suffix('.parquet.backup')
    df.write_parquet(str(backup_path))
    print(f"💾 Backup created: {backup_path}")
    
    # Write fixed data
    df_fixed.write_parquet(str(dim_path))
    print(f"✅ Fixed dim_npi file written")
    
    print()
    print("🎯 Summary:")
    print(f"   - NPIs with real NPPES data: {after_fetch}")
    print(f"   - NPIs that need fetching: {after_unfetch}")
    print(f"   - Ready for fetch_npi_data.py")
    
    return True

def main():
    print("=== Fix NPPES Fetch Flags ===")
    print()
    
    success = fix_nppes_flags()
    
    if success:
        print()
        print("🎉 Flags fixed successfully!")
        print()
        print("Next steps:")
        print("1. Run: python check_npi_status.py")
        print("2. Run: python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1")
        return 0
    else:
        print("❌ Failed to fix flags")
        return 1

if __name__ == "__main__":
    sys.exit(main())
