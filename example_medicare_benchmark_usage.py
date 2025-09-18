#!/usr/bin/env python3
"""
Example Usage of Medicare Benchmark Tables

This script demonstrates how to use the Medicare benchmark dimension tables
for rate analysis and benchmarking against negotiated rates.

Usage:
    python example_medicare_benchmark_usage.py
"""

import pandas as pd
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_benchmark_tables():
    """Load Medicare benchmark tables"""
    benchmark_dir = Path("prod_etl/core/data/silver/benchmarks")
    
    if not benchmark_dir.exists():
        logger.error(f"Benchmark directory not found: {benchmark_dir}")
        logger.info("Run 'python build_medicare_benchmarks.py' first to create benchmark tables")
        return None, None, None, None
    
    try:
        # Load individual benchmark tables
        prof_bench = pd.read_parquet(benchmark_dir / "bench_medicare_professional.parquet")
        opps_bench = pd.read_parquet(benchmark_dir / "bench_medicare_opps.parquet")
        asc_bench = pd.read_parquet(benchmark_dir / "bench_medicare_asc.parquet")
        comprehensive = pd.read_parquet(benchmark_dir / "bench_medicare_comprehensive.parquet")
        
        logger.info(f"Loaded benchmark tables:")
        logger.info(f"  Professional: {len(prof_bench):,} rows")
        logger.info(f"  OPPS: {len(opps_bench):,} rows")
        logger.info(f"  ASC: {len(asc_bench):,} rows")
        logger.info(f"  Comprehensive: {len(comprehensive):,} rows")
        
        return prof_bench, opps_bench, asc_bench, comprehensive
        
    except FileNotFoundError as e:
        logger.error(f"Benchmark table not found: {e}")
        logger.info("Run 'python build_medicare_benchmarks.py' first to create benchmark tables")
        return None, None, None, None

def analyze_benchmark_coverage(comprehensive):
    """Analyze coverage of benchmark tables"""
    logger.info("\n" + "="*60)
    logger.info("BENCHMARK COVERAGE ANALYSIS")
    logger.info("="*60)
    
    # Coverage by benchmark type
    coverage_by_type = comprehensive.groupby('benchmark_type').agg({
        'state': 'nunique',
        'code': 'nunique',
        'medicare_prof_stateavg': lambda x: x.notna().sum(),
        'medicare_opps_stateavg': lambda x: x.notna().sum(),
        'medicare_asc_stateavg': lambda x: x.notna().sum()
    }).round(2)
    
    logger.info("Coverage by Benchmark Type:")
    print(coverage_by_type)
    
    # Top states by coverage
    state_coverage = comprehensive.groupby('state').agg({
        'code': 'nunique',
        'benchmark_type': 'nunique'
    }).sort_values('code', ascending=False)
    
    logger.info(f"\nTop 10 States by Code Coverage:")
    print(state_coverage.head(10))
    
    # Rate statistics
    logger.info(f"\nRate Statistics:")
    rate_stats = comprehensive.groupby('benchmark_type').agg({
        'medicare_prof_stateavg': ['mean', 'median', 'std'],
        'medicare_opps_stateavg': ['mean', 'median', 'std'],
        'medicare_asc_stateavg': ['mean', 'median', 'std']
    }).round(2)
    
    print(rate_stats)

def demonstrate_benchmark_joining():
    """Demonstrate how to join benchmarks with fact tables"""
    logger.info("\n" + "="*60)
    logger.info("BENCHMARK JOINING DEMONSTRATION")
    logger.info("="*60)
    
    # Check if fact table exists
    fact_path = Path("prod_etl/core/data/gold/fact_rate.parquet")
    if not fact_path.exists():
        logger.warning(f"Fact table not found: {fact_path}")
        logger.info("This example requires the fact table to demonstrate joining")
        return
    
    try:
        # Load fact table (sample)
        logger.info("Loading fact table sample...")
        fact = pd.read_parquet(fact_path)
        fact_sample = fact.head(1000)  # Sample for demonstration
        
        # Load comprehensive benchmarks
        benchmark_path = Path("prod_etl/core/data/silver/benchmarks/bench_medicare_comprehensive.parquet")
        benchmarks = pd.read_parquet(benchmark_path)
        
        logger.info(f"Fact table sample: {len(fact_sample):,} rows")
        logger.info(f"Benchmarks: {len(benchmarks):,} rows")
        
        # Join for professional benchmarking
        logger.info("\nJoining fact table with professional benchmarks...")
        fact_with_prof = fact_sample.merge(
            benchmarks[benchmarks['benchmark_type'] == 'professional'],
            on=['state', 'year_month', 'code_type', 'code'],
            how='left',
            suffixes=('', '_bench')
        )
        
        # Calculate percentage of Medicare
        fact_with_prof['pct_of_medicare'] = (
            fact_with_prof['negotiated_rate'] / 
            fact_with_prof['medicare_prof_stateavg']
        )
        
        # Show results
        matched = fact_with_prof['medicare_prof_stateavg'].notna().sum()
        logger.info(f"Matched {matched:,} out of {len(fact_sample):,} fact rows with benchmarks")
        
        if matched > 0:
            logger.info(f"\nSample benchmark analysis:")
            sample_results = fact_with_prof[
                fact_with_prof['medicare_prof_stateavg'].notna()
            ][['reporting_entity_name', 'code', 'negotiated_rate', 
               'medicare_prof_stateavg', 'pct_of_medicare']].head(10)
            
            print(sample_results.to_string(index=False))
            
            # Summary statistics
            logger.info(f"\nBenchmark Analysis Summary:")
            logger.info(f"  Average % of Medicare: {fact_with_prof['pct_of_medicare'].mean():.1f}%")
            logger.info(f"  Median % of Medicare: {fact_with_prof['pct_of_medicare'].median():.1f}%")
            logger.info(f"  Min % of Medicare: {fact_with_prof['pct_of_medicare'].min():.1f}%")
            logger.info(f"  Max % of Medicare: {fact_with_prof['pct_of_medicare'].max():.1f}%")
        
    except Exception as e:
        logger.error(f"Error in benchmark joining demonstration: {e}")

def demonstrate_state_analysis(comprehensive):
    """Demonstrate state-level analysis"""
    logger.info("\n" + "="*60)
    logger.info("STATE-LEVEL ANALYSIS DEMONSTRATION")
    logger.info("="*60)
    
    # State-level rate analysis
    state_analysis = comprehensive.groupby(['state', 'benchmark_type']).agg({
        'medicare_prof_stateavg': 'mean',
        'medicare_opps_stateavg': 'mean',
        'medicare_asc_stateavg': 'mean'
    }).reset_index()
    
    # Show states with highest professional rates
    prof_rates = state_analysis[state_analysis['benchmark_type'] == 'professional'].copy()
    prof_rates = prof_rates.sort_values('medicare_prof_stateavg', ascending=False)
    
    logger.info("Top 10 States by Average Professional Medicare Rates:")
    print(prof_rates[['state', 'medicare_prof_stateavg']].head(10).to_string(index=False))
    
    # Show states with highest OPPS rates
    opps_rates = state_analysis[state_analysis['benchmark_type'] == 'opps'].copy()
    opps_rates = opps_rates.sort_values('medicare_opps_stateavg', ascending=False)
    
    logger.info("\nTop 10 States by Average OPPS Medicare Rates:")
    print(opps_rates[['state', 'medicare_opps_stateavg']].head(10).to_string(index=False))

def demonstrate_code_analysis(comprehensive):
    """Demonstrate procedure code analysis"""
    logger.info("\n" + "="*60)
    logger.info("PROCEDURE CODE ANALYSIS DEMONSTRATION")
    logger.info("="*60)
    
    # Most expensive professional procedures
    prof_codes = comprehensive[comprehensive['benchmark_type'] == 'professional'].copy()
    prof_codes = prof_codes.groupby('code').agg({
        'medicare_prof_stateavg': 'mean',
        'state': 'nunique'
    }).reset_index()
    prof_codes = prof_codes.sort_values('medicare_prof_stateavg', ascending=False)
    
    logger.info("Top 10 Most Expensive Professional Procedures (by state average):")
    print(prof_codes.head(10).to_string(index=False))
    
    # Most expensive OPPS procedures
    opps_codes = comprehensive[comprehensive['benchmark_type'] == 'opps'].copy()
    opps_codes = opps_codes.groupby('code').agg({
        'medicare_opps_stateavg': 'mean',
        'state': 'nunique'
    }).reset_index()
    opps_codes = opps_codes.sort_values('medicare_opps_stateavg', ascending=False)
    
    logger.info("\nTop 10 Most Expensive OPPS Procedures (by state average):")
    print(opps_codes.head(10).to_string(index=False))

def main():
    """Main demonstration function"""
    logger.info("Medicare Benchmark Tables Usage Examples")
    logger.info("="*60)
    
    # Load benchmark tables
    prof_bench, opps_bench, asc_bench, comprehensive = load_benchmark_tables()
    
    if comprehensive is None:
        logger.error("Cannot proceed without benchmark tables")
        return
    
    # Run demonstrations
    analyze_benchmark_coverage(comprehensive)
    demonstrate_benchmark_joining()
    demonstrate_state_analysis(comprehensive)
    demonstrate_code_analysis(comprehensive)
    
    logger.info("\n" + "="*60)
    logger.info("DEMONSTRATION COMPLETE")
    logger.info("="*60)
    logger.info("Key takeaways:")
    logger.info("1. Benchmark tables provide comprehensive Medicare rate coverage")
    logger.info("2. Easy joining with fact tables using (state, year_month, code_type, code)")
    logger.info("3. Both national and state-averaged rates available")
    logger.info("4. Support for professional, OPPS, and ASC benchmarking")
    logger.info("5. Rich metadata for audit and analysis")

if __name__ == "__main__":
    main()
