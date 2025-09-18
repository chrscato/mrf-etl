#!/usr/bin/env python3
"""
Speed comparison for different NPPES fetch approaches
"""

import polars as pl

def calculate_times():
    """Calculate estimated times for different approaches."""
    
    # Read current status
    df = pl.read_parquet("core/data/silver/dim_npi/dim_npi.parquet")
    total_npis = df.height
    unfetched = df.filter(
        (pl.col("nppes_fetched") == False) | (pl.col("nppes_fetched").is_null())
    ).height
    
    print("🚀 NPPES Fetch Speed Comparison")
    print("=" * 50)
    print(f"📊 NPIs to fetch: {unfetched:,}")
    print()
    
    approaches = [
        ("Original (0.1s sleep)", 0.1, 1, "Safe, respectful to API"),
        ("Faster (0.05s sleep)", 0.05, 1, "2x faster, still safe"),
        ("Fast (0.02s sleep)", 0.02, 1, "5x faster, riskier"),
        ("Parallel 5 threads (0.1s)", 0.1, 5, "5x faster, safe"),
        ("Parallel 10 threads (0.1s)", 0.1, 10, "10x faster, safe"),
        ("Parallel 5 threads (0.05s)", 0.05, 5, "10x faster, still safe"),
        ("Parallel 10 threads (0.05s)", 0.05, 10, "20x faster, still safe"),
    ]
    
    print("Approach".ljust(30) + "Time".ljust(15) + "Description")
    print("-" * 70)
    
    for name, sleep, threads, desc in approaches:
        time_per_npi = sleep / threads
        total_seconds = unfetched * time_per_npi
        total_minutes = total_seconds / 60
        
        if total_minutes < 1:
            time_str = f"{total_seconds:.0f}s"
        elif total_minutes < 60:
            time_str = f"{total_minutes:.1f}m"
        else:
            hours = total_minutes / 60
            time_str = f"{hours:.1f}h"
        
        print(f"{name:<30} {time_str:<15} {desc}")
    
    print()
    print("🎯 Recommendations:")
    print("   • For testing: Parallel 5 threads (0.1s) - ~5 minutes for 1000 NPIs")
    print("   • For production: Parallel 10 threads (0.05s) - ~13 minutes total")
    print("   • For speed: Parallel 10 threads (0.02s) - ~5 minutes total (risky)")
    print()
    print("📝 Commands:")
    print("   # Test with 100 NPIs")
    print("   python fetch_npi_data_fast.py --silver-dir core/data/silver --threads 5 --limit 100")
    print()
    print("   # Fast production run")
    print("   python fetch_npi_data_fast.py --silver-dir core/data/silver --threads 10 --sleep 0.05")
    print()
    print("   # Original slow method")
    print("   python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1")

if __name__ == "__main__":
    calculate_times()
