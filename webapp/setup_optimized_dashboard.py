#!/usr/bin/env python3
"""
Setup Script for Optimized MRF Dashboard
Initializes materialized views and indexes for maximum performance
"""

import sys
import os
from pathlib import Path
import duckdb
import time

# Add the webapp directory to the path
webapp_dir = Path(__file__).parent
sys.path.append(str(webapp_dir))

from utils.optimized_queries import OptimizedMRFQueries

def setup_optimized_dashboard():
    """Initialize the optimized dashboard with materialized views and indexes"""
    
    print("🚀 Setting up Optimized MRF Dashboard...")
    print("=" * 50)
    
    # Check if data files exist
    data_root = webapp_dir.parent / "prod_etl/core/data"
    required_files = [
        "gold/fact_rate.parquet",
        "silver/dim_npi/dim_npi.parquet",
        "silver/dim_npi_address/dim_npi_address.parquet",
        "silver/dim_code/dim_code_cat.parquet",
        "xrefs/xref_pg_member_tin.parquet",
        "xrefs/xref_pg_member_npi.parquet"
    ]
    
    print("📁 Checking data files...")
    missing_files = []
    for file_path in required_files:
        full_path = data_root / file_path
        if not full_path.exists():
            missing_files.append(str(full_path))
        else:
            print(f"  ✅ {file_path}")
    
    if missing_files:
        print(f"\n❌ Missing required data files:")
        for file_path in missing_files:
            print(f"  - {file_path}")
        print("\nPlease ensure your ETL pipeline has generated these files.")
        return False
    
    print("\n🔧 Initializing optimized queries...")
    try:
        # Initialize the optimized queries (this creates materialized views)
        start_time = time.time()
        optimized_queries = OptimizedMRFQueries()
        init_time = time.time() - start_time
        
        print(f"  ✅ Materialized views created in {init_time:.2f} seconds")
        
        # Test the setup with a sample query
        print("\n🧪 Testing optimized queries...")
        
        # Get available data for testing
        try:
            # Test basic functionality using the same connection as optimized_queries
            conn = optimized_queries.conn
            
            # Test fact table access
            fact_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{data_root}/gold/fact_rate.parquet')").fetchone()[0]
            print(f"  ✅ Fact table accessible: {fact_count:,} records")
            
            # Test materialized views
            view_tests = [
                ("provider_search_index", "SELECT COUNT(*) FROM provider_search_index"),
                ("tin_provider_index", "SELECT COUNT(*) FROM tin_provider_index"),
                ("procedure_search_index", "SELECT COUNT(*) FROM procedure_search_index"),
                ("payer_search_index", "SELECT COUNT(*) FROM payer_search_index"),
                ("comprehensive_search_index", "SELECT COUNT(*) FROM comprehensive_search_index")
            ]
            
            for view_name, test_query in view_tests:
                try:
                    result = conn.execute(test_query).fetchone()[0]
                    print(f"  ✅ {view_name}: {result:,} records")
                except Exception as e:
                    print(f"  ❌ {view_name}: Error - {str(e)}")
            
        except Exception as e:
            print(f"  ❌ Error testing queries: {str(e)}")
            return False
        
        print("\n⚡ Performance test...")
        try:
            # Test search performance
            start_time = time.time()
            
            # Get a sample state and year_month for testing
            conn = duckdb.connect()
            sample_data = conn.execute(f"""
                SELECT state, year_month 
                FROM read_parquet('{data_root}/gold/fact_rate.parquet') 
                LIMIT 1
            """).fetchone()
            
            if sample_data:
                state, year_month = sample_data
                
                # Test multi-field search performance
                test_start = time.time()
                results = optimized_queries.multi_field_search(
                    state=state,
                    year_month=year_month,
                    limit=10
                )
                test_time = time.time() - test_start
                
                print(f"  ✅ Multi-field search: {len(results)} results in {test_time:.3f} seconds")
                
                # Test individual search types
                search_tests = [
                    ("TIN search", lambda: optimized_queries.search_by_organization("Medical", state, year_month, 5)),
                    ("Organization search", lambda: optimized_queries.search_by_organization("Medical", state, year_month, 5)),
                    ("Taxonomy search", lambda: optimized_queries.search_by_taxonomy("Medicine", state, year_month, 5)),
                ]
                
                for test_name, test_func in search_tests:
                    try:
                        test_start = time.time()
                        test_results = test_func()
                        test_time = time.time() - test_start
                        print(f"  ✅ {test_name}: {len(test_results)} results in {test_time:.3f} seconds")
                    except Exception as e:
                        print(f"  ⚠️  {test_name}: {str(e)}")
            
            conn.close()
            
        except Exception as e:
            print(f"  ⚠️  Performance test error: {str(e)}")
        
        print("\n🎉 Setup completed successfully!")
        print("\n📋 Next steps:")
        print("  1. Start the backend: python start_backend.py")
        print("  2. Start the frontend: python start_frontend.py")
        print("  3. Open http://localhost:8501 in your browser")
        print("  4. Try the '⚡ Fast Search' tab for optimized queries")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Setup failed: {str(e)}")
        print("\nTroubleshooting:")
        print("  1. Ensure all data files exist in prod_etl/core/data/")
        print("  2. Check that DuckDB is properly installed")
        print("  3. Verify file permissions")
        return False

def check_dependencies():
    """Check if all required dependencies are installed"""
    print("🔍 Checking dependencies...")
    
    required_packages = [
        'duckdb',
        'polars',
        'fastapi',
        'streamlit',
        'requests',
        'pandas',
        'plotly'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"  ❌ {package}")
    
    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("Install with: pip install " + " ".join(missing_packages))
        return False
    
    return True

def main():
    """Main setup function"""
    print("MRF Dashboard Optimization Setup")
    print("=" * 40)
    
    # Check dependencies
    if not check_dependencies():
        return 1
    
    # Setup optimized dashboard
    if setup_optimized_dashboard():
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
