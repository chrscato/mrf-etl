#!/usr/bin/env python3
"""
Staged MRF Dashboard - Step-by-step filtering process
Guides users through logical filtering steps to prevent over-filtering
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

# Import simple queries
from simple_queries import get_simple_queries

# Data paths
DATA_ROOT = webapp_dir.parent / "prod_etl/core/data"
FACT_TABLE = DATA_ROOT / "gold/fact_rate.parquet"

# FastAPI app
app = FastAPI(
    title="MRF Staged Dashboard",
    description="Step-by-step filtering process to prevent over-filtering",
    version="1.0.0"
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
    """Get DuckDB connection for queries"""
    conn = duckdb.connect()
    conn.execute("SET memory_limit='256MB'")
    conn.execute("SET max_memory='256MB'")
    return conn

# =============================================================================
# STAGED FILTERING API ENDPOINTS
# =============================================================================

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the staged dashboard HTML"""
    return FileResponse(webapp_dir / "frontend" / "staged_dashboard.html")

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    try:
        files_status = {
            "fact_table": FACT_TABLE.exists()
        }
        
        stats = {}
        if FACT_TABLE.exists():
            conn = get_duckdb_connection()
            result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{FACT_TABLE}')").fetchone()
            stats = {
                "total_records": result[0] if result else 0
            }
            conn.close()
        
        return {
            "status": "healthy",
            "files_available": files_status,
            "data_stats": stats
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stage1/billing-classes")
async def get_billing_classes(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format")
):
    """Stage 1: Get available billing classes"""
    try:
        conn = get_duckdb_connection()
        query = f"""
        SELECT DISTINCT billing_class, COUNT(*) as count
        FROM read_parquet('{FACT_TABLE}')
        WHERE state = ? AND year_month = ? AND billing_class IS NOT NULL
        GROUP BY billing_class
        ORDER BY count DESC
        """
        result = conn.execute(query, [state, year_month]).fetchall()
        conn.close()
        
        return {
            "stage": 1,
            "title": "Select Billing Class",
            "description": "What type of billing are you looking for?",
            "options": [
                {"value": row[0], "label": row[0], "count": row[1]}
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stage2/payers")
async def get_payers(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    billing_class: str = Query(..., description="Selected billing class")
):
    """Stage 2: Get available payers for selected billing class"""
    try:
        conn = get_duckdb_connection()
        query = f"""
        SELECT DISTINCT reporting_entity_name, COUNT(*) as count
        FROM read_parquet('{FACT_TABLE}')
        WHERE state = ? AND year_month = ? AND billing_class = ?
        GROUP BY reporting_entity_name
        ORDER BY count DESC
        LIMIT 20
        """
        result = conn.execute(query, [state, year_month, billing_class]).fetchall()
        conn.close()
        
        return {
            "stage": 2,
            "title": "Select Payer(s)",
            "description": f"Which payers have {billing_class} data?",
            "options": [
                {"value": row[0], "label": row[0], "count": row[1]}
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stage3/procedure-sets")
async def get_procedure_sets(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    billing_class: str = Query(..., description="Selected billing class"),
    payers: str = Query(..., description="Comma-separated list of selected payers")
):
    """Stage 3: Get available procedure sets"""
    try:
        payer_list = [p.strip() for p in payers.split(',') if p.strip()]
        placeholders = ','.join(['?' for _ in payer_list])
        
        conn = get_duckdb_connection()
        query = f"""
        SELECT DISTINCT 
            COALESCE(cc.proc_set, 'Unknown') as proc_set, 
            COUNT(*) as count
        FROM read_parquet('{FACT_TABLE}') f
        LEFT JOIN read_parquet('{DATA_ROOT / "dims/dim_code_cat.parquet"}') cc 
            ON f.code = cc.proc_cd
        WHERE f.state = ? 
            AND f.year_month = ? 
            AND f.billing_class = ?
            AND f.reporting_entity_name IN ({placeholders})
        GROUP BY COALESCE(cc.proc_set, 'Unknown')
        ORDER BY count DESC
        LIMIT 15
        """
        params = [state, year_month, billing_class] + payer_list
        result = conn.execute(query, params).fetchall()
        conn.close()
        
        return {
            "stage": 3,
            "title": "Select Procedure Set (Optional)",
            "description": "What type of procedures are you interested in?",
            "options": [
                {"value": row[0], "label": row[0], "count": row[1]}
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stage4/procedure-classes")
async def get_procedure_classes(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    billing_class: str = Query(..., description="Selected billing class"),
    payers: str = Query(..., description="Comma-separated list of selected payers"),
    proc_set: Optional[str] = Query(None, description="Selected procedure set")
):
    """Stage 4: Get available procedure classes"""
    try:
        payer_list = [p.strip() for p in payers.split(',') if p.strip()]
        placeholders = ','.join(['?' for _ in payer_list])
        
        where_conditions = [
            "f.state = ?", "f.year_month = ?", "f.billing_class = ?",
            f"f.reporting_entity_name IN ({placeholders})"
        ]
        params = [state, year_month, billing_class] + payer_list
        
        if proc_set and proc_set != "Unknown":
            where_conditions.append("cc.proc_set = ?")
            params.append(proc_set)
        
        where_clause = " AND ".join(where_conditions)
        
        conn = get_duckdb_connection()
        query = f"""
        SELECT DISTINCT 
            COALESCE(cc.proc_class, 'Unknown') as proc_class, 
            COUNT(*) as count
        FROM read_parquet('{FACT_TABLE}') f
        LEFT JOIN read_parquet('{DATA_ROOT / "dims/dim_code_cat.parquet"}') cc 
            ON f.code = cc.proc_cd
        WHERE {where_clause}
        GROUP BY COALESCE(cc.proc_class, 'Unknown')
        ORDER BY count DESC
        LIMIT 15
        """
        result = conn.execute(query, params).fetchall()
        conn.close()
        
        return {
            "stage": 4,
            "title": "Select Procedure Class (Optional)",
            "description": "What specific procedure category?",
            "options": [
                {"value": row[0], "label": row[0], "count": row[1]}
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stage5/taxonomies")
async def get_taxonomies(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    billing_class: str = Query(..., description="Selected billing class"),
    payers: str = Query(..., description="Comma-separated list of selected payers"),
    proc_set: Optional[str] = Query(None, description="Selected procedure set"),
    proc_class: Optional[str] = Query(None, description="Selected procedure class")
):
    """Stage 5: Get available taxonomy descriptions"""
    try:
        payer_list = [p.strip() for p in payers.split(',') if p.strip()]
        placeholders = ','.join(['?' for _ in payer_list])
        
        where_conditions = [
            "f.state = ?", "f.year_month = ?", "f.billing_class = ?",
            f"f.reporting_entity_name IN ({placeholders})"
        ]
        params = [state, year_month, billing_class] + payer_list
        
        if proc_set and proc_set != "Unknown":
            where_conditions.append("cc.proc_set = ?")
            params.append(proc_set)
        if proc_class and proc_class != "Unknown":
            where_conditions.append("cc.proc_class = ?")
            params.append(proc_class)
        
        where_clause = " AND ".join(where_conditions)
        
        conn = get_duckdb_connection()
        query = f"""
        SELECT DISTINCT 
            n.primary_taxonomy_desc, 
            COUNT(*) as count
        FROM read_parquet('{FACT_TABLE}') f
        LEFT JOIN read_parquet('{DATA_ROOT / "xrefs/xref_pg_member_npi.parquet"}') xn 
            ON f.pg_uid = xn.pg_uid
        LEFT JOIN read_parquet('{DATA_ROOT / "dims/dim_npi.parquet"}') n 
            ON xn.npi = n.npi
        LEFT JOIN read_parquet('{DATA_ROOT / "dims/dim_code_cat.parquet"}') cc 
            ON f.code = cc.proc_cd
        WHERE {where_clause}
            AND n.primary_taxonomy_desc IS NOT NULL
        GROUP BY n.primary_taxonomy_desc
        ORDER BY count DESC
        LIMIT 20
        """
        result = conn.execute(query, params).fetchall()
        conn.close()
        
        return {
            "stage": 5,
            "title": "Select Taxonomy (Optional)",
            "description": "What provider specialties are you interested in?",
            "options": [
                {"value": row[0], "label": row[0], "count": row[1]}
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/results")
async def get_filtered_results(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    billing_class: str = Query(..., description="Selected billing class"),
    payers: str = Query(..., description="Comma-separated list of selected payers"),
    proc_set: Optional[str] = Query(None, description="Selected procedure set"),
    proc_class: Optional[str] = Query(None, description="Selected procedure class"),
    taxonomies: Optional[str] = Query(None, description="Comma-separated list of selected taxonomies"),
    limit: int = Query(100, description="Number of results to return")
):
    """Get final filtered results"""
    try:
        payer_list = [p.strip() for p in payers.split(',') if p.strip()]
        payer_placeholders = ','.join(['?' for _ in payer_list])
        
        where_conditions = [
            "f.state = ?", "f.year_month = ?", "f.billing_class = ?",
            f"f.reporting_entity_name IN ({payer_placeholders})"
        ]
        params = [state, year_month, billing_class] + payer_list
        
        if proc_set and proc_set != "Unknown":
            where_conditions.append("cc.proc_set = ?")
            params.append(proc_set)
        if proc_class and proc_class != "Unknown":
            where_conditions.append("cc.proc_class = ?")
            params.append(proc_class)
        if taxonomies:
            taxonomy_list = [t.strip() for t in taxonomies.split(',') if t.strip()]
            taxonomy_placeholders = ','.join(['?' for _ in taxonomy_list])
            where_conditions.append(f"n.primary_taxonomy_desc IN ({taxonomy_placeholders})")
            params.extend(taxonomy_list)
        
        where_clause = " AND ".join(where_conditions)
        
        conn = get_duckdb_connection()
        query = f"""
        SELECT 
            f.fact_uid,
            f.code,
            f.code_type,
            f.negotiated_rate,
            f.billing_class,
            f.reporting_entity_name,
            n.npi,
            n.organization_name,
            n.primary_taxonomy_desc,
            cc.proc_set,
            cc.proc_class,
            cc.proc_group
        FROM read_parquet('{FACT_TABLE}') f
        LEFT JOIN read_parquet('{DATA_ROOT / "xrefs/xref_pg_member_npi.parquet"}') xn 
            ON f.pg_uid = xn.pg_uid
        LEFT JOIN read_parquet('{DATA_ROOT / "dims/dim_npi.parquet"}') n 
            ON xn.npi = n.npi
        LEFT JOIN read_parquet('{DATA_ROOT / "dims/dim_code_cat.parquet"}') cc 
            ON f.code = cc.proc_cd
        WHERE {where_clause}
        ORDER BY f.negotiated_rate DESC
        LIMIT {min(limit, 500)}
        """
        result = conn.execute(query, params).fetchall()
        conn.close()
        
        return {
            "total_results": len(result),
            "results": [
                {
                    "fact_uid": row[0],
                    "code": row[1],
                    "code_type": row[2],
                    "negotiated_rate": round(row[3], 2) if row[3] else 0,
                    "billing_class": row[4],
                    "payer": row[5],
                    "npi": row[6],
                    "organization_name": row[7],
                    "taxonomy": row[8],
                    "proc_set": row[9],
                    "proc_class": row[10],
                    "proc_group": row[11]
                }
                for row in result
            ]
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
    """Main function to start the staged dashboard"""
    print("🚀 Starting MRF Staged Dashboard...")
    print("=" * 60)
    print("📊 Step-by-Step Filtering Process")
    print("🔧 Prevents Over-filtering")
    print("🎨 User-Friendly Interface")
    print("⚡ Memory Efficient")
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
                access_log=False
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

if __name__ == "__main__":
    main()
