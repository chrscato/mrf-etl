#!/usr/bin/env python3
"""
Consolidated MRF Dashboard - Single Process Solution
Combines FastAPI backend and HTML frontend in one high-performance application
"""

import asyncio
import threading
import time
import webbrowser
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn
import duckdb
import polars as pl
import json
import sys
from typing import List, Optional, Dict, Any
import re

# Add the webapp directory to Python path
webapp_dir = Path(__file__).parent
sys.path.insert(0, str(webapp_dir))

# Import simple queries to prevent crashes
from simple_queries import get_simple_queries

# Data paths
DATA_ROOT = webapp_dir.parent / "prod_etl/core/data"
FACT_TABLE = DATA_ROOT / "gold/fact_rate.parquet"
DIM_CODE = DATA_ROOT / "dims/dim_code.parquet"
DIM_CODE_CAT = DATA_ROOT / "dims/dim_code_cat.parquet"
DIM_PAYER = DATA_ROOT / "dims/dim_payer.parquet"
DIM_NPI = DATA_ROOT / "dims/dim_npi.parquet"
DIM_NPI_ADDRESS = DATA_ROOT / "dims/dim_npi_address.parquet"
DIM_PROVIDER_GROUP = DATA_ROOT / "dims/dim_provider_group.parquet"
DIM_POS_SET = DATA_ROOT / "dims/dim_pos_set.parquet"
XREF_GROUP_NPI = DATA_ROOT / "xrefs/xref_pg_member_npi.parquet"
XREF_GROUP_TIN = DATA_ROOT / "xrefs/xref_pg_member_tin.parquet"

# FastAPI app
app = FastAPI(
    title="MRF Consolidated Dashboard",
    description="High-performance MRF data dashboard with integrated backend and frontend",
    version="2.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory=webapp_dir / "frontend"), name="static")

def get_duckdb_connection():
    """Get DuckDB connection for complex queries"""
    conn = duckdb.connect()
    return conn

def slugify(s: str) -> str:
    """Convert string to URL-friendly slug"""
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_') if s else None

# =============================================================================
# API ENDPOINTS - Optimized for Performance
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard HTML"""
    return FileResponse(webapp_dir / "frontend" / "optimized_dashboard.html")

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        files_status = {
            "fact_table": FACT_TABLE.exists(),
            "dim_code": DIM_CODE.exists(),
            "dim_code_cat": DIM_CODE_CAT.exists(),
            "dim_payer": DIM_PAYER.exists(),
            "dim_npi": DIM_NPI.exists(),
            "dim_npi_address": DIM_NPI_ADDRESS.exists(),
            "dim_provider_group": DIM_PROVIDER_GROUP.exists(),
            "dim_pos_set": DIM_POS_SET.exists(),
            "xref_group_npi": XREF_GROUP_NPI.exists(),
            "xref_group_tin": XREF_GROUP_TIN.exists()
        }
        
        stats = {}
        if FACT_TABLE.exists():
            df = pl.read_parquet(FACT_TABLE)
            stats = {
                "total_records": len(df),
                "states": df["state"].unique().to_list(),
                "year_months": sorted(df["year_month"].unique().to_list()),
                "payers": df["reporting_entity_name"].unique().to_list()[:10],
                "code_types": df["code_type"].unique().to_list()
            }
        
        return {
            "status": "healthy",
            "files_available": files_status,
            "data_stats": stats
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/search/multi-field")
async def multi_field_search(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    primary_taxonomy_desc: Optional[str] = Query(None, description="Primary taxonomy description filter"),
    organization_name: Optional[str] = Query(None, description="Organization name filter"),
    npi: Optional[str] = Query(None, description="NPI filter"),
    enumeration_type: Optional[str] = Query(None, description="Enumeration type filter"),
    billing_class: Optional[str] = Query(None, description="Billing class filter"),
    proc_set: Optional[str] = Query(None, description="Procedure set filter"),
    proc_class: Optional[str] = Query(None, description="Procedure class filter"),
    proc_group: Optional[str] = Query(None, description="Procedure group filter"),
    billing_code: Optional[str] = Query(None, description="Billing code filter"),
    tin_value: Optional[str] = Query(None, description="TIN value filter"),
    payer: Optional[str] = Query(None, description="Payer name filter"),
    limit: int = Query(100, description="Number of results to return")
):
    """High-performance multi-field search using optimized queries"""
    try:
        simple_queries = get_simple_queries()
        
        # Parse comma-separated values for multi-select
        def parse_multi_value(value):
            if not value:
                return None
            values = [v.strip() for v in value.split(',') if v.strip()]
            return values if values else None

        results = simple_queries.multi_field_search(
            state=state,
            year_month=year_month,
            primary_taxonomy_desc=parse_multi_value(primary_taxonomy_desc),
            organization_name=parse_multi_value(organization_name),
            npi=parse_multi_value(npi),
            enumeration_type=parse_multi_value(enumeration_type),
            billing_class=parse_multi_value(billing_class),
            proc_set=parse_multi_value(proc_set),
            proc_class=parse_multi_value(proc_class),
            proc_group=parse_multi_value(proc_group),
            billing_code=parse_multi_value(billing_code),
            tin_value=parse_multi_value(tin_value),
            payer=parse_multi_value(payer),
            limit=limit
        )
        
        return {
            "search_type": "multi_field",
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/autocomplete/{field}")
async def get_autocomplete_suggestions(
    field: str,
    query: str = Query("", description="Search query"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(20, description="Number of suggestions to return")
):
    """Get autocomplete suggestions for various fields"""
    try:
        simple_queries = get_simple_queries()
        suggestions = simple_queries.get_autocomplete_suggestions(field, query, state, year_month, limit)
        
        return {
            "field": field,
            "query": query,
            "state": state,
            "year_month": year_month,
            "suggestions": suggestions
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/explore/data-availability")
async def explore_data_availability(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    category: str = Query(..., description="Category to explore"),
    limit: int = Query(25, description="Number of results to return"),
    offset: int = Query(0, description="Number of results to skip for pagination")
):
    """Explore data availability by category"""
    try:
        simple_queries = get_simple_queries()
        results = optimized_queries.explore_data_availability(state, year_month, category, limit, offset)
        
        return {
            "state": state,
            "year_month": year_month,
            "category": category,
            "limit": limit,
            "offset": offset,
            "result_count": len(results),
            "has_more": len(results) == limit,
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/explore/category-stats")
async def get_category_statistics(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format")
):
    """Get high-level statistics for each category"""
    try:
        simple_queries = get_simple_queries()
        stats = optimized_queries.get_category_statistics(state, year_month)
        
        return {
            "state": state,
            "year_month": year_month,
            "category_statistics": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/explore/drill-down")
async def drill_down_exploration(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    category: str = Query(..., description="Category to explore"),
    selected_value: str = Query(..., description="Selected value to drill down into"),
    drill_category: str = Query(..., description="Category to drill down to"),
    limit: int = Query(50, description="Number of results to return")
):
    """Drill down from one category to another"""
    try:
        simple_queries = get_simple_queries()
        results = optimized_queries.drill_down_exploration(
            state, year_month, category, selected_value, drill_category, limit
        )
        
        return {
            "state": state,
            "year_month": year_month,
            "source_category": category,
            "selected_value": selected_value,
            "drill_category": drill_category,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# MAIN APPLICATION
# =============================================================================

def open_browser_delayed(url: str, delay: float = 2.0):
    """Open browser after a delay"""
    def _open():
        time.sleep(delay)
        webbrowser.open(url)
    
    thread = threading.Thread(target=_open, daemon=True)
    thread.start()

def main():
    """Main function to start the consolidated dashboard"""
    print("🚀 Starting MRF Consolidated Dashboard...")
    print("=" * 60)
    print("📊 High-Performance Single-Process Dashboard")
    print("🔧 Backend: FastAPI with optimized queries")
    print("🎨 Frontend: HTML/JS with Bootstrap")
    print("⚡ Performance: Materialized views + indexing")
    print("=" * 60)
    
    # Try different ports if 8080 is busy
    ports_to_try = [8080, 8081, 8082, 8083, 8084]
    
    for port in ports_to_try:
        try:
            print(f"🌐 Starting server on port {port}...")
            
            # Open browser after a short delay
            open_browser_delayed(f"http://localhost:{port}", 3.0)
            
            # Start the server
            uvicorn.run(
                app,
                host="0.0.0.0",
                port=port,
                log_level="info",
                access_log=False  # Disable access logs for better performance
            )
            break
            
        except OSError as e:
            if "Address already in use" in str(e):
                print(f"❌ Port {port} is busy, trying next port...")
                continue
            else:
                print(f"❌ Error starting server: {e}")
                break
    else:
        print("❌ Could not find an available port. Please check if any servers are running.")
        print("💡 Try: netstat -ano | findstr :8080")

if __name__ == "__main__":
    main()
