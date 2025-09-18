#!/usr/bin/env python3
"""
Fetch real NPI data from NPPES for existing NPIs in dim_npi.parquet

This script will:
1. Read all NPIs from your dim_npi.parquet file
2. Fetch real provider data from NPPES for each NPI
3. Update the dim_npi file with full provider information
4. Create/update dim_npi_address.parquet with address data

Usage:
    python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1
    python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --limit 100
"""

import sys
import time
import argparse
from pathlib import Path

# Add utils to path
sys.path.append('utils')

from utils_nppes import (
    fetch_nppes_record, 
    normalize_nppes_result, 
    upsert_dim_npi, 
    upsert_dim_npi_address
)
import polars as pl

def fetch_all_npi_data(silver_dir: str, dim_npi_filename: str = "dim_npi.parquet", 
                      sleep: float = 0.1, limit: int = None):
    """
    Fetch real NPI data from NPPES for all NPIs in dim_npi file.
    """
    dim_path = Path(silver_dir) / "dim_npi" / dim_npi_filename
    
    if not dim_path.exists():
        print(f"❌ dim_npi file not found at {dim_path}")
        return 0, 0
    
    # Read current NPIs
    print(f"📁 Reading NPIs from {dim_path}")
    current_dim = pl.read_parquet(str(dim_path))
    npis_to_fetch = current_dim.select("npi").to_series().to_list()
    
    if limit:
        npis_to_fetch = npis_to_fetch[:limit]
        print(f"🔢 Limiting to first {limit} NPIs")
    
    print(f"🚀 Starting to fetch NPPES data for {len(npis_to_fetch)} NPIs...")
    print(f"⏱️  Sleep between requests: {sleep} seconds")
    print(f"⏰ Estimated time: {len(npis_to_fetch) * sleep / 60:.1f} minutes")
    print()
    
    found = 0
    updated = 0
    errors = 0
    
    for i, npi in enumerate(npis_to_fetch):
        # Progress indicator
        if i % 50 == 0 or i == len(npis_to_fetch) - 1:
            progress = (i + 1) / len(npis_to_fetch) * 100
            print(f"📊 Progress: {i + 1}/{len(npis_to_fetch)} ({progress:.1f}%) - Found: {found}, Updated: {updated}, Errors: {errors}")
        
        try:
            # Fetch from NPPES
            rec = fetch_nppes_record(str(npi))
            if rec:
                found += 1
                # Normalize the data
                dim_df, addr_df = normalize_nppes_result(str(npi), rec, nppes_fetched=True)
                
                # Update dim_npi - handle schema differences
                if dim_path.exists():
                    existing_df = pl.read_parquet(str(dim_path))
                    # Remove the existing NPI from the dataframe
                    existing_df = existing_df.filter(pl.col("npi") != str(npi))
                    
                    # Get all unique columns from both dataframes
                    all_columns = list(set(existing_df.columns + dim_df.columns))
                    
                    # Add missing columns to existing_df
                    for col in all_columns:
                        if col not in existing_df.columns:
                            existing_df = existing_df.with_columns(pl.lit(None).alias(col))
                    
                    # Add missing columns to new data
                    for col in all_columns:
                        if col not in dim_df.columns:
                            dim_df = dim_df.with_columns(pl.lit(None).alias(col))
                    
                    # Reorder columns to match
                    existing_df = existing_df.select(all_columns)
                    dim_df = dim_df.select(all_columns)
                    
                    # Now concatenate with matching schemas
                    merged_df = pl.concat([existing_df, dim_df], how="vertical_relaxed")
                    merged_df.write_parquet(str(dim_path))
                else:
                    dim_df.write_parquet(str(dim_path))
                
                # Update dim_npi_address
                addr_path = dim_path.parent / "dim_npi_address" / "dim_npi_address.parquet"
                upsert_dim_npi_address(addr_df, addr_path)
                
                updated += 1
            else:
                print(f"⚠️  NPI {npi} not found in NPPES")
            
            # Be respectful to NPPES API
            if sleep > 0:
                time.sleep(sleep)
                
        except Exception as e:
            errors += 1
            print(f"❌ Error fetching NPI {npi}: {e}")
            continue
    
    print()
    print("🎉 Fetch complete!")
    print(f"   📈 Total NPIs processed: {len(npis_to_fetch)}")
    print(f"   ✅ Found in NPPES: {found}")
    print(f"   🔄 Updated: {updated}")
    print(f"   ❌ Errors: {errors}")
    
    return found, updated

def main():
    parser = argparse.ArgumentParser(description="Fetch real NPI data from NPPES")
    parser.add_argument("--silver-dir", required=True, help="Path to silver directory")
    parser.add_argument("--dim-npi-filename", default="dim_npi.parquet", help="dim_npi filename")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between requests (seconds)")
    parser.add_argument("--limit", type=int, help="Limit number of NPIs to process (for testing)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not Path("core/data/silver").exists():
        print("❌ Error: core/data/silver directory not found")
        print("   Please run this script from the prod_etl directory")
        return 1
    
    # Confirmation prompt
    if not args.yes:
        dim_path = Path(args.silver_dir) / "dim_npi" / args.dim_npi_filename
        if dim_path.exists():
            current_dim = pl.read_parquet(str(dim_path))
            total_npis = current_dim.height
            limit_text = f" (limited to {args.limit})" if args.limit else ""
            estimated_time = (args.limit or total_npis) * args.sleep / 60
            
            print(f"⚠️  WARNING: This will fetch NPPES data for {total_npis} NPIs{limit_text}")
            print(f"⏰ Estimated time: {estimated_time:.1f} minutes")
            print(f"🌐 This will make {(args.limit or total_npis)} API calls to NPPES")
            print()
            
            response = input("Do you want to continue? (y/N): ")
            if response.lower() != 'y':
                print("❌ Cancelled by user")
                return 0
    
    # Run the fetch
    try:
        found, updated = fetch_all_npi_data(
            silver_dir=args.silver_dir,
            dim_npi_filename=args.dim_npi_filename,
            sleep=args.sleep,
            limit=args.limit
        )
        
        if found > 0:
            print()
            print("✅ Success! Your dim_npi.parquet now has real NPPES data")
            print("   You can now use the updated provider information in your analysis")
        
        return 0
        
    except KeyboardInterrupt:
        print()
        print("⏹️  Interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
