#!/usr/bin/env python3
"""
Quick status checker for NPPES fetch progress

Usage:
    python check_npi_status.py
    python check_npi_status.py --silver-dir core/data/silver
"""

import argparse
import sys
from pathlib import Path
import polars as pl

def check_npi_status(silver_dir: str = "core/data/silver", dim_npi_filename: str = "dim_npi.parquet"):
    """Check the current status of NPPES fetch progress."""
    
    dim_path = Path(silver_dir) / "dim_npi" / dim_npi_filename
    
    if not dim_path.exists():
        print(f"❌ dim_npi file not found at {dim_path}")
        return False
    
    try:
        df = pl.read_parquet(str(dim_path))
        
        total_npis = df.height
        fetched_npis = df.filter(pl.col("nppes_fetched") == True).height
        unfetched_npis = total_npis - fetched_npis
        
        progress_pct = (fetched_npis / total_npis * 100) if total_npis > 0 else 0
        
        print("📊 NPPES Fetch Status")
        print("=" * 40)
        print(f"📁 File: {dim_path}")
        print(f"📈 Total NPIs: {total_npis:,}")
        print(f"✅ Fetched: {fetched_npis:,}")
        print(f"⏳ Remaining: {unfetched_npis:,}")
        print(f"📊 Progress: {progress_pct:.1f}%")
        
        if unfetched_npis > 0:
            print()
            print("🔄 To continue fetching:")
            print(f"   python fetch_npi_data.py --silver-dir {silver_dir} --sleep 0.1")
            print()
            print("🔄 To fetch in batches:")
            print(f"   python fetch_npi_data.py --silver-dir {silver_dir} --sleep 0.1 --limit 1000")
        else:
            print()
            print("🎉 All NPIs have been fetched!")
            print("   Your dim_npi.parquet now has complete NPPES data")
        
        # Show sample of fetched data
        if fetched_npis > 0:
            print()
            print("📋 Sample of fetched data:")
            sample = df.filter(pl.col("nppes_fetched") == True).head(3)
            if "organization_name" in sample.columns:
                org_sample = sample.filter(pl.col("organization_name").is_not_null())
                if org_sample.height > 0:
                    print("   Organizations:")
                    for row in org_sample.iter_rows(named=True):
                        print(f"     - {row['organization_name']} (NPI: {row['npi']})")
            
            if "first_name" in sample.columns:
                ind_sample = sample.filter(pl.col("first_name").is_not_null())
                if ind_sample.height > 0:
                    print("   Individuals:")
                    for row in ind_sample.iter_rows(named=True):
                        name = f"{row['first_name']} {row['last_name']}"
                        print(f"     - {name} (NPI: {row['npi']})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error reading dim_npi file: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Check NPPES fetch status")
    parser.add_argument("--silver-dir", default="core/data/silver", help="Path to silver directory")
    parser.add_argument("--dim-npi-filename", default="dim_npi.parquet", help="dim_npi filename")
    
    args = parser.parse_args()
    
    success = check_npi_status(args.silver_dir, args.dim_npi_filename)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
