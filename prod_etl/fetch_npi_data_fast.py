#!/usr/bin/env python3
"""
Fast NPPES data fetcher with parallel processing

This version uses threading to fetch multiple NPIs simultaneously,
significantly reducing the total time. It reads NPIs from the xref file
and only fetches those that haven't been fetched before.

Usage:
    python fetch_npi_data_fast.py --data-dir core/data --threads 5
    python fetch_npi_data_fast.py --data-dir core/data --threads 10 --limit 1000
"""

import sys
import time
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import polars as pl

# Add utils to path
sys.path.append('utils')

from utils_nppes import (
    fetch_nppes_record, 
    normalize_nppes_result, 
    upsert_dim_npi_address
)

class NPPESFetcher:
    def __init__(self, data_dir: str, dim_npi_filename: str = "dim_npi.parquet", 
                 threads: int = 5, sleep: float = 0.1):
        self.data_dir = data_dir
        self.dim_npi_filename = dim_npi_filename
        self.threads = threads
        self.sleep = sleep
        self.dims_path = Path(data_dir) / "dims"
        self.xrefs_path = Path(data_dir) / "xrefs"
        self.dim_path = self.dims_path / dim_npi_filename
        self.xref_path = self.xrefs_path / "xref_pg_member_npi.parquet"
        self.results = []
        self.lock = threading.Lock()
        self.processed = 0
        self.found = 0
        self.errors = 0
    
    def get_npis_to_fetch(self):
        """Get NPIs from xref file that haven't been fetched yet."""
        # Read xref file to get all NPIs
        xref_df = pl.read_parquet(str(self.xref_path))
        all_npis = set(xref_df.select("npi").to_series().to_list())
        
        print(f"📊 Found {len(all_npis)} unique NPIs in xref file")
        
        # Check if dim_npi file exists
        if not self.dim_path.exists():
            print("📝 No existing dim_npi file found - will fetch all NPIs")
            return list(all_npis)
        
        # Read existing dim_npi data
        existing_df = pl.read_parquet(str(self.dim_path))
        
        # Get NPIs that have already been fetched
        if "nppes_fetched" in existing_df.columns:
            fetched_npis = set(
                existing_df.filter(pl.col("nppes_fetched") == True)
                .select("npi").to_series().to_list()
            )
        else:
            # If no nppes_fetched column, assume all existing NPIs have been fetched
            fetched_npis = set(existing_df.select("npi").to_series().to_list())
        
        # Find NPIs that need fetching
        npis_to_fetch = all_npis - fetched_npis
        
        print(f"✅ Already fetched: {len(fetched_npis)} NPIs")
        print(f"🔄 Need to fetch: {len(npis_to_fetch)} NPIs")
        
        return list(npis_to_fetch)
        
    def fetch_single_npi(self, npi: str):
        """Fetch data for a single NPI."""
        try:
            # Fetch from NPPES
            rec = fetch_nppes_record(str(npi))
            if rec:
                # Normalize the data
                dim_df, addr_df = normalize_nppes_result(str(npi), rec, nppes_fetched=True)
                
                with self.lock:
                    self.results.append((dim_df, addr_df, npi))
                    self.found += 1
            else:
                with self.lock:
                    print(f"⚠️  NPI {npi} not found in NPPES")
            
            # Sleep to be respectful to API
            if self.sleep > 0:
                time.sleep(self.sleep)
                
        except Exception as e:
            with self.lock:
                self.errors += 1
                print(f"❌ Error fetching NPI {npi}: {e}")
    
    def update_dim_npi(self, results_batch):
        """Update dim_npi file with a batch of results."""
        if not results_batch:
            return
            
        try:
            # Ensure dims directory exists
            self.dims_path.mkdir(parents=True, exist_ok=True)
            
            # Read existing data if it exists
            if self.dim_path.exists():
                existing_df = pl.read_parquet(str(self.dim_path))
            else:
                # Create empty dataframe with the structure from the first result
                first_dim_df = results_batch[0][0]
                existing_df = first_dim_df.filter(pl.lit(False))  # Empty with same schema
            
            # Process each result
            for dim_df, addr_df, npi in results_batch:
                # Remove existing NPI if it exists
                existing_df = existing_df.filter(pl.col("npi") != str(npi))
                
                # Get all unique columns
                all_columns = list(set(existing_df.columns + dim_df.columns))
                
                # Add missing columns to existing_df
                for col in all_columns:
                    if col not in existing_df.columns:
                        existing_df = existing_df.with_columns(pl.lit(None).alias(col))
                
                # Add missing columns to new data
                for col in all_columns:
                    if col not in dim_df.columns:
                        dim_df = dim_df.with_columns(pl.lit(None).alias(col))
                
                # Reorder columns
                existing_df = existing_df.select(all_columns)
                dim_df = dim_df.select(all_columns)
                
                # Concatenate
                existing_df = pl.concat([existing_df, dim_df], how="vertical_relaxed")
            
            # Write updated data
            existing_df.write_parquet(str(self.dim_path))
            
            # Update addresses
            addr_path = self.dims_path / "dim_npi_address.parquet"
            for _, addr_df, _ in results_batch:
                if addr_df.height > 0:
                    upsert_dim_npi_address(addr_df, addr_path)
                    
        except Exception as e:
            print(f"❌ Error updating dim_npi: {e}")
    
    def fetch_npis_parallel(self, npis: list, batch_size: int = 50):
        """Fetch NPIs in parallel with batching."""
        total_npis = len(npis)
        print(f"🚀 Starting parallel fetch for {total_npis} NPIs")
        print(f"🧵 Using {self.threads} threads")
        print(f"⏱️  Sleep between requests: {self.sleep}s")
        print(f"📦 Batch size: {batch_size}")
        print()
        
        # Process in batches to avoid memory issues
        for i in range(0, total_npis, batch_size):
            batch_npis = npis[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_npis + batch_size - 1) // batch_size
            
            print(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch_npis)} NPIs)")
            
            # Clear results for this batch
            self.results = []
            
            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                futures = [executor.submit(self.fetch_single_npi, npi) for npi in batch_npis]
                
                # Wait for completion
                for future in as_completed(futures):
                    self.processed += 1
                    if self.processed % 100 == 0:
                        progress = (self.processed / total_npis) * 100
                        print(f"📊 Progress: {self.processed}/{total_npis} ({progress:.1f}%) - Found: {self.found}, Errors: {self.errors}")
            
            # Update dim_npi file with this batch
            if self.results:
                print(f"💾 Updating dim_npi with {len(self.results)} results...")
                self.update_dim_npi(self.results)
            
            print(f"✅ Batch {batch_num} complete")
            print()
        
        print("🎉 Parallel fetch complete!")
        print(f"   📈 Total processed: {self.processed}")
        print(f"   ✅ Found: {self.found}")
        print(f"   ❌ Errors: {self.errors}")

def main():
    parser = argparse.ArgumentParser(description="Fast parallel NPPES data fetcher")
    parser.add_argument("--data-dir", default="core/data", help="Path to data directory")
    parser.add_argument("--dim-npi-filename", default="dim_npi.parquet", help="dim_npi filename")
    parser.add_argument("--threads", type=int, default=5, help="Number of parallel threads")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between requests (seconds)")
    parser.add_argument("--limit", type=int, help="Limit number of NPIs to process")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    # Check if we're in the right directory
    if not Path("core/data").exists():
        print("❌ Error: core/data directory not found")
        print("   Please run this script from the prod_etl directory")
        return 1
    
    # Create fetcher to get NPIs that need fetching
    fetcher = NPPESFetcher(
        data_dir=args.data_dir,
        dim_npi_filename=args.dim_npi_filename,
        threads=args.threads,
        sleep=args.sleep
    )
    
    # Get NPIs that need fetching
    npis_to_fetch = fetcher.get_npis_to_fetch()
    
    if args.limit:
        npis_to_fetch = npis_to_fetch[:args.limit]
    
    if not npis_to_fetch:
        print("✅ No NPIs need fetching")
        return 0
    
    # Confirmation prompt
    if not args.yes:
        limit_text = f" (limited to {args.limit})" if args.limit else ""
        estimated_time = len(npis_to_fetch) * args.sleep / args.threads / 60
        
        print(f"⚠️  WARNING: This will fetch NPPES data for {len(npis_to_fetch)} NPIs{limit_text}")
        print(f"🧵 Using {args.threads} parallel threads")
        print(f"⏰ Estimated time: {estimated_time:.1f} minutes")
        print(f"🌐 This will make {len(npis_to_fetch)} API calls to NPPES")
        print()
        
        response = input("Do you want to continue? (y/N): ")
        if response.lower() != 'y':
            print("❌ Cancelled by user")
            return 0
    
    try:
        fetcher.fetch_npis_parallel(npis_to_fetch, args.batch_size)
        return 0
    except KeyboardInterrupt:
        print("\n⏹️  Interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
