#!/usr/bin/env python3
"""
Migration script to update existing dim_npi.parquet to include nppes_fetched flag columns.

This script will:
1. Read your existing dim_npi.parquet file
2. Create a backup
3. Add nppes_fetched and nppes_fetch_date columns
4. Mark existing NPIs as fetched=True (since they already have data)
"""

import polars as pl
from pathlib import Path
import sys

def migrate_dim_npi_to_new_format(silver_dir: str = "core/data/silver", backup: bool = True):
    """
    Migrate existing dim_npi.parquet to include nppes_fetched and nppes_fetch_date columns.
    Existing NPIs will be marked as fetched=True since they already have data.
    """
    dim_path = Path(silver_dir) / "dim_npi" / "dim_npi.parquet"
    
    if not dim_path.exists():
        print(f"❌ dim_npi.parquet not found at {dim_path}")
        return False
    
    print(f"📁 Found dim_npi.parquet at {dim_path}")
    
    # Read existing data
    try:
        df = pl.read_parquet(str(dim_path))
        print(f"📊 Current dim_npi has {df.height} NPIs")
        print(f"📋 Current columns: {df.columns}")
    except Exception as e:
        print(f"❌ Error reading dim_npi.parquet: {e}")
        return False
    
    # Check if already migrated
    if "nppes_fetched" in df.columns:
        print("✅ dim_npi already has nppes_fetched column - no migration needed")
        return True
    
    print("🔄 Starting migration...")
    
    # Create backup if requested
    if backup:
        backup_path = dim_path.with_suffix('.parquet.backup')
        try:
            df.write_parquet(str(backup_path))
            print(f"💾 Backup created: {backup_path}")
        except Exception as e:
            print(f"⚠️  Warning: Could not create backup: {e}")
    
    # Add new columns
    # Existing NPIs are considered "fetched" since they have data
    try:
        df_migrated = df.with_columns([
            pl.lit(True).alias("nppes_fetched"),  # Mark existing NPIs as fetched
            pl.lit(None).alias("nppes_fetch_date")  # No fetch date for existing data
        ])
        
        # Write migrated data
        df_migrated.write_parquet(str(dim_path))
        
        print(f"✅ Migration complete!")
        print(f"   - Added nppes_fetched and nppes_fetch_date columns")
        print(f"   - Existing {df.height} NPIs marked as fetched=True")
        print(f"   - New columns: {df_migrated.columns}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        return False

def main():
    print("=== dim_npi Migration Script ===")
    print()
    
    # Check if we're in the right directory
    if not Path("core/data/silver").exists():
        print("❌ Error: core/data/silver directory not found")
        print("   Please run this script from the prod_etl directory")
        return 1
    
    # Run migration
    success = migrate_dim_npi_to_new_format()
    
    if success:
        print()
        print("🎉 Migration successful!")
        print()
        print("Next steps:")
        print("1. You can now use the new backfill functions in ETL_2.ipynb")
        print("2. Run: python utils/utils_nppes.py --silver-dir core/data/silver --backfill")
        print("3. Or use the sync function to add placeholders and backfill")
        return 0
    else:
        print()
        print("❌ Migration failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
