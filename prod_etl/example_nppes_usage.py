#!/usr/bin/env python3
"""
Example usage of the updated utils_nppes.py

This script demonstrates how to use the NPPES utilities from within the prod_etl folder
with custom dim_npi filenames.
"""

import sys
from pathlib import Path

# Add utils to path so we can import
sys.path.append('utils')

from utils_nppes import (
    add_npi_to_dims,
    add_many_npis,
    get_npi_info,
    list_available_npis,
    batch_update_npis_from_fact
)

def main():
    # Configuration
    silver_dir = "core/data/silver"
    custom_dim_filename = "dim_npi_example.parquet"
    
    print("=== NPPES Utils Example ===")
    print(f"Silver directory: {silver_dir}")
    print(f"Custom dim filename: {custom_dim_filename}")
    print()
    
    # Example 1: Add a single NPI
    print("1. Adding single NPI...")
    npi = "1295795499"
    result = add_npi_to_dims(npi, silver_dir, dim_npi_filename=custom_dim_filename)
    print(f"   Added NPI {npi}: {result}")
    print()
    
    # Example 2: Add multiple NPIs
    print("2. Adding multiple NPIs...")
    npis = ["1639195834", "1477564367"]
    found, wrote = add_many_npis(
        npis, 
        silver_dir, 
        sleep=0.1,
        dim_npi_filename=custom_dim_filename
    )
    print(f"   Found: {found}, Wrote: {wrote}")
    print()
    
    # Example 3: List available NPIs
    print("3. Listing available NPIs...")
    available = list_available_npis(
        silver_dir=silver_dir,
        dim_npi_filename=custom_dim_filename,
        limit=5
    )
    print(f"   Found {available.height} NPIs:")
    print(available)
    print()
    
    # Example 4: Get detailed NPI info
    print("4. Getting detailed NPI info...")
    info = get_npi_info(npi, silver_dir, dim_npi_filename=custom_dim_filename)
    if info:
        print(f"   NPI {npi} info:")
        for key, value in info.items():
            if key in ['organization_name', 'first_name', 'last_name', 'primary_taxonomy_desc']:
                print(f"     {key}: {value}")
    else:
        print(f"   NPI {npi} not found")
    print()
    
    # Example 5: Show CLI usage
    print("5. CLI Usage Examples:")
    print("   # Add NPIs to custom file:")
    print(f"   python utils/utils_nppes.py --silver-dir {silver_dir} --add 1295795499 --dim-npi-filename {custom_dim_filename}")
    print()
    print("   # Batch update from fact table:")
    print(f"   python utils/utils_nppes.py --silver-dir {silver_dir} --file npis.txt --dim-npi-filename {custom_dim_filename} --sleep 0.1")
    print()
    
    print("=== Example Complete ===")

if __name__ == "__main__":
    main()
