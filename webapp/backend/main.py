"""
FastAPI Backend for MRF Data Lookup Tool
Provides REST API endpoints for dashboard data access
"""

from fastapi import FastAPI, HTTPException, Query, Path as FastAPIPath
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import polars as pl
import duckdb
from pathlib import Path
import re
from datetime import datetime
import json
import sys
sys.path.append(str(Path(__file__).parent.parent))
from utils.optimized_queries import get_optimized_queries

app = FastAPI(
    title="MRF Data Lookup API",
    description="API for accessing MRF ETL data for dashboard visualization",
    version="1.0.0"
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data paths - go up one level from webapp directory
webapp_dir = Path(__file__).parent.parent
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

def slugify(s: str) -> str:
    """Convert string to URL-friendly slug"""
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_') if s else None

def get_duckdb_connection():
    """Get DuckDB connection for complex queries"""
    conn = duckdb.connect()
    return conn

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "MRF Data Lookup API is running"}

@app.get("/api/health")
async def health_check():
    """Detailed health check with data availability"""
    try:
        # Check if data files exist
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
        
        # Get basic stats if fact table exists
        stats = {}
        if FACT_TABLE.exists():
            df = pl.read_parquet(FACT_TABLE)
            stats = {
                "total_records": len(df),
                "states": df["state"].unique().to_list(),
                "year_months": sorted(df["year_month"].unique().to_list()),
                "payers": df["reporting_entity_name"].unique().to_list()[:10],  # First 10
                "code_types": df["code_type"].unique().to_list()
            }
        
        return {
            "status": "healthy",
            "files_available": files_status,
            "data_stats": stats
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/rates/summary")
async def get_rate_summary(
    state: str = Query(..., description="State code (e.g., GA)"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    payer: Optional[str] = Query(None, description="Filter by payer name"),
    code_type: Optional[str] = Query(None, description="Filter by code type (CPT, HCPCS, REV)"),
    code: Optional[str] = Query(None, description="Filter by specific procedure code"),
    billing_class: Optional[str] = Query(None, description="Filter by billing class (professional, institutional)"),
    tin_value: Optional[str] = Query(None, description="Filter by TIN value"),
    negotiated_type: Optional[str] = Query(None, description="Filter by negotiated type"),
    negotiation_arrangement: Optional[str] = Query(None, description="Filter by negotiation arrangement"),
    pos_set_id: Optional[str] = Query(None, description="Filter by place of service set ID")
):
    """Get rate summary statistics"""
    try:
        conn = get_duckdb_connection()
        
        # Build base query with optional TIN join
        base_from = f"FROM read_parquet('{FACT_TABLE}') f"
        if tin_value:
            base_from += f"""
            JOIN read_parquet('{XREF_GROUP_TIN}') x 
                ON x.year_month = f.year_month 
                AND x.payer_slug = regexp_replace(lower(f.reporting_entity_name), '[^a-z0-9]+', '_')
                AND x.pg_uid = f.pg_uid
            """
        
        query = f"""
        SELECT 
            COUNT(*) as total_rates,
            AVG(f.negotiated_rate) as avg_rate,
            MIN(f.negotiated_rate) as min_rate,
            MAX(f.negotiated_rate) as max_rate,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.negotiated_rate) as median_rate,
            COUNT(DISTINCT f.code) as unique_procedures,
            COUNT(DISTINCT f.reporting_entity_name) as unique_payers
        {base_from}
        WHERE f.state = '{state}' AND f.year_month = '{year_month}'
        """
        
        # Add filters
        if payer:
            query += f" AND f.reporting_entity_name ILIKE '%{payer}%'"
        if code_type:
            query += f" AND f.code_type = '{code_type}'"
        if code:
            query += f" AND f.code = '{code}'"
        if billing_class:
            query += f" AND f.billing_class = '{billing_class}'"
        if tin_value:
            query += f" AND x.tin_value = '{tin_value}'"
        if negotiated_type:
            query += f" AND f.negotiated_type = '{negotiated_type}'"
        if negotiation_arrangement:
            query += f" AND f.negotiation_arrangement = '{negotiation_arrangement}'"
        if pos_set_id:
            query += f" AND f.pos_set_id = '{pos_set_id}'"
        
        result = conn.execute(query).fetchone()
        
        return {
            "state": state,
            "year_month": year_month,
            "filters": {
                "payer": payer,
                "code_type": code_type,
                "code": code,
                "billing_class": billing_class,
                "tin_value": tin_value,
                "negotiated_type": negotiated_type,
                "negotiation_arrangement": negotiation_arrangement,
                "pos_set_id": pos_set_id
            },
            "summary": {
                "total_rates": result[0],
                "avg_rate": round(result[1], 2) if result[1] else 0,
                "min_rate": round(result[2], 2) if result[2] else 0,
                "max_rate": round(result[3], 2) if result[3] else 0,
                "median_rate": round(result[4], 2) if result[4] else 0,
                "unique_procedures": result[5],
                "unique_payers": result[6]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/rates/by-payer")
async def get_rates_by_payer(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(50, description="Number of results to return")
):
    """Get rate statistics grouped by payer"""
    try:
        conn = get_duckdb_connection()
        
        query = f"""
        SELECT 
            reporting_entity_name,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negotiated_rate) as median_rate,
            COUNT(DISTINCT code) as unique_procedures
        FROM read_parquet('{FACT_TABLE}')
        WHERE state = '{state}' AND year_month = '{year_month}'
        GROUP BY reporting_entity_name
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = conn.execute(query).fetchall()
        
        return {
            "state": state,
            "year_month": year_month,
            "payers": [
                {
                    "payer_name": row[0],
                    "rate_count": row[1],
                    "avg_rate": round(row[2], 2) if row[2] else 0,
                    "min_rate": round(row[3], 2) if row[3] else 0,
                    "max_rate": round(row[4], 2) if row[4] else 0,
                    "median_rate": round(row[5], 2) if row[5] else 0,
                    "unique_procedures": row[6]
                }
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/rates/by-procedure")
async def get_rates_by_procedure(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    code_type: Optional[str] = Query(None, description="Filter by code type"),
    billing_class: Optional[str] = Query(None, description="Filter by billing class"),
    tin_value: Optional[str] = Query(None, description="Filter by TIN value"),
    limit: int = Query(50, description="Number of results to return")
):
    """Get rate statistics grouped by procedure code"""
    try:
        conn = get_duckdb_connection()
        
        # Build query with optional filters
        base_from = f"FROM read_parquet('{FACT_TABLE}') f"
        if tin_value:
            base_from += f"""
            JOIN read_parquet('{XREF_GROUP_TIN}') x 
                ON x.year_month = f.year_month 
                AND x.payer_slug = regexp_replace(lower(f.reporting_entity_name), '[^a-z0-9]+', '_')
                AND x.pg_uid = f.pg_uid
            """
        
        where_conditions = [f"f.state = '{state}'", f"f.year_month = '{year_month}'"]
        if code_type:
            where_conditions.append(f"f.code_type = '{code_type}'")
        if billing_class:
            where_conditions.append(f"f.billing_class = '{billing_class}'")
        if tin_value:
            where_conditions.append(f"x.tin_value = '{tin_value}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        WITH rates_with_desc AS (
            SELECT 
                f.code_type,
                f.code,
                f.negotiated_rate,
                f.reporting_entity_name,
                COALESCE(d.code_desc, f.code) as code_desc
            {base_from}
            LEFT JOIN read_parquet('{DIM_CODE}') d 
                ON d.code_type = f.code_type AND d.code = f.code
            WHERE {where_clause}
        )
        SELECT 
            code_type,
            code,
            code_desc,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negotiated_rate) as median_rate,
            COUNT(DISTINCT reporting_entity_name) as unique_payers
        FROM rates_with_desc
        GROUP BY code_type, code, code_desc
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = conn.execute(query).fetchall()
        
        return {
            "state": state,
            "year_month": year_month,
            "filters": {
                "code_type": code_type,
                "billing_class": billing_class,
                "tin_value": tin_value
            },
            "procedures": [
                {
                    "code_type": row[0],
                    "code": row[1],
                    "code_desc": row[2],
                    "rate_count": row[3],
                    "avg_rate": round(row[4], 2) if row[4] else 0,
                    "min_rate": round(row[5], 2) if row[5] else 0,
                    "max_rate": round(row[6], 2) if row[6] else 0,
                    "median_rate": round(row[7], 2) if row[7] else 0,
                    "unique_payers": row[8]
                }
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/rates/detail")
async def get_rate_details(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    payer: Optional[str] = Query(None, description="Filter by payer name"),
    code: Optional[str] = Query(None, description="Filter by procedure code"),
    billing_class: Optional[str] = Query(None, description="Filter by billing class"),
    tin_value: Optional[str] = Query(None, description="Filter by TIN value"),
    limit: int = Query(100, description="Number of results to return")
):
    """Get detailed rate records with descriptions"""
    try:
        conn = get_duckdb_connection()
        
        # Build query with optional TIN join
        base_from = f"FROM read_parquet('{FACT_TABLE}') f"
        if tin_value:
            base_from += f"""
            JOIN read_parquet('{XREF_GROUP_TIN}') x 
                ON x.year_month = f.year_month 
                AND x.payer_slug = regexp_replace(lower(f.reporting_entity_name), '[^a-z0-9]+', '_')
                AND x.pg_uid = f.pg_uid
            """
        
        where_conditions = [f"f.state = '{state}'", f"f.year_month = '{year_month}'"]
        if payer:
            where_conditions.append(f"f.reporting_entity_name ILIKE '%{payer}%'")
        if code:
            where_conditions.append(f"f.code = '{code}'")
        if billing_class:
            where_conditions.append(f"f.billing_class = '{billing_class}'")
        if tin_value:
            where_conditions.append(f"x.tin_value = '{tin_value}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT 
            f.reporting_entity_name,
            f.code_type,
            f.code,
            COALESCE(d.code_desc, f.code) as code_desc,
            f.negotiated_rate,
            f.negotiated_type,
            f.negotiation_arrangement,
            f.expiration_date
        {base_from}
        LEFT JOIN read_parquet('{DIM_CODE}') d 
            ON d.code_type = f.code_type AND d.code = f.code
        WHERE {where_clause}
        ORDER BY f.reporting_entity_name, f.code, f.negotiated_rate
        LIMIT {limit}
        """
        
        result = conn.execute(query).fetchall()
        
        return {
            "state": state,
            "year_month": year_month,
            "filters": {
                "payer": payer, 
                "code": code,
                "billing_class": billing_class,
                "tin_value": tin_value
            },
            "records": [
                {
                    "payer_name": row[0],
                    "code_type": row[1],
                    "code": row[2],
                    "code_desc": row[3],
                    "negotiated_rate": round(row[4], 2) if row[4] else 0,
                    "negotiated_type": row[5],
                    "negotiation_arrangement": row[6],
                    "expiration_date": row[7]
                }
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/providers/search")
async def search_providers(
    q: str = Query(..., description="Search query for provider name"),
    limit: int = Query(20, description="Number of results to return")
):
    """Search providers by name"""
    try:
        conn = get_duckdb_connection()
        
        query = f"""
        SELECT 
            npi,
            organization_name,
            first_name,
            last_name,
            enumeration_type,
            primary_taxonomy_desc,
            status
        FROM read_parquet('{DIM_NPI}')
        WHERE organization_name ILIKE '%{q}%' 
           OR first_name ILIKE '%{q}%' 
           OR last_name ILIKE '%{q}%'
        ORDER BY organization_name, last_name, first_name
        LIMIT {limit}
        """
        
        result = conn.execute(query).fetchall()
        
        return {
            "query": q,
            "providers": [
                {
                    "npi": row[0],
                    "organization_name": row[1],
                    "first_name": row[2],
                    "last_name": row[3],
                    "enumeration_type": row[4],
                    "primary_taxonomy_desc": row[5],
                    "status": row[6]
                }
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/meta/available-data")
async def get_available_data():
    """Get available states, year_months, and payers"""
    try:
        conn = get_duckdb_connection()
        
        # Get available states and year_months
        query = f"""
        SELECT 
            state,
            year_month,
            COUNT(*) as record_count
        FROM read_parquet('{FACT_TABLE}')
        GROUP BY state, year_month
        ORDER BY state, year_month
        """
        
        result = conn.execute(query).fetchall()
        
        # Get unique payers
        payers_query = f"""
        SELECT DISTINCT reporting_entity_name
        FROM read_parquet('{FACT_TABLE}')
        ORDER BY reporting_entity_name
        """
        
        payers_result = conn.execute(payers_query).fetchall()
        
        return {
            "data_availability": [
                {
                    "state": row[0],
                    "year_month": row[1],
                    "record_count": row[2]
                }
                for row in result
            ],
            "available_payers": [row[0] for row in payers_result]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/meta/dimension-values")
async def get_dimension_values(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    dimension: str = Query(..., description="Dimension name (billing_class, code_type, negotiated_type, negotiation_arrangement, tin_value)")
):
    """Get available values for a specific dimension"""
    try:
        conn = get_duckdb_connection()
        
        if dimension == "billing_class":
            query = f"""
            SELECT DISTINCT billing_class, COUNT(*) as count
            FROM read_parquet('{FACT_TABLE}')
            WHERE state = '{state}' AND year_month = '{year_month}'
            GROUP BY billing_class
            ORDER BY count DESC
            """
        elif dimension == "code_type":
            query = f"""
            SELECT DISTINCT code_type, COUNT(*) as count
            FROM read_parquet('{FACT_TABLE}')
            WHERE state = '{state}' AND year_month = '{year_month}'
            GROUP BY code_type
            ORDER BY count DESC
            """
        elif dimension == "negotiated_type":
            query = f"""
            SELECT DISTINCT negotiated_type, COUNT(*) as count
            FROM read_parquet('{FACT_TABLE}')
            WHERE state = '{state}' AND year_month = '{year_month}'
            GROUP BY negotiated_type
            ORDER BY count DESC
            """
        elif dimension == "negotiation_arrangement":
            query = f"""
            SELECT DISTINCT negotiation_arrangement, COUNT(*) as count
            FROM read_parquet('{FACT_TABLE}')
            WHERE state = '{state}' AND year_month = '{year_month}'
            GROUP BY negotiation_arrangement
            ORDER BY count DESC
            """
        elif dimension == "tin_value":
            query = f"""
            SELECT DISTINCT x.tin_value, COUNT(*) as count
            FROM read_parquet('{FACT_TABLE}') f
            JOIN read_parquet('{XREF_GROUP_TIN}') x 
                ON x.year_month = f.year_month 
                AND x.payer_slug = regexp_replace(lower(f.reporting_entity_name), '[^a-z0-9]+', '_')
                AND x.pg_uid = f.pg_uid
            WHERE f.state = '{state}' AND f.year_month = '{year_month}'
            GROUP BY x.tin_value
            ORDER BY count DESC
            LIMIT 100
            """
        else:
            raise HTTPException(status_code=400, detail=f"Unknown dimension: {dimension}")
        
        result = conn.execute(query).fetchall()
        
        return {
            "dimension": dimension,
            "state": state,
            "year_month": year_month,
            "values": [
                {
                    "value": row[0],
                    "count": row[1]
                }
                for row in result
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# =============================================================================
# OPTIMIZED SEARCH ENDPOINTS - High Performance Indexed Queries
# =============================================================================

@app.get("/api/search/tin")
async def search_by_tin(
    tin_value: str = Query(..., description="TIN value to search for"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(100, description="Number of results to return")
):
    """Fast TIN-based search using materialized views"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.search_by_tin(tin_value, state, year_month, limit)
        
        return {
            "search_type": "tin",
            "search_value": tin_value,
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/organization")
async def search_by_organization(
    org_name: str = Query(..., description="Organization name to search for"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(100, description="Number of results to return")
):
    """Fast organization name search using materialized views"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.search_by_organization(org_name, state, year_month, limit)
        
        return {
            "search_type": "organization",
            "search_value": org_name,
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/taxonomy")
async def search_by_taxonomy(
    taxonomy_desc: str = Query(..., description="Taxonomy description to search for"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(100, description="Number of results to return")
):
    """Fast taxonomy description search"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.search_by_taxonomy(taxonomy_desc, state, year_month, limit)
        
        return {
            "search_type": "taxonomy",
            "search_value": taxonomy_desc,
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/procedure-category")
async def search_by_procedure_category(
    proc_class: str = Query(..., description="Procedure class to search for"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(100, description="Number of results to return")
):
    """Fast procedure category search"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.search_by_procedure_category(proc_class, state, year_month, limit)
        
        return {
            "search_type": "procedure_category",
            "search_value": proc_class,
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/billing-code")
async def search_by_billing_code(
    billing_code: str = Query(..., description="Billing code to search for"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(100, description="Number of results to return")
):
    """Fast billing code search"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.search_by_billing_code(billing_code, state, year_month, limit)
        
        return {
            "search_type": "billing_code",
            "search_value": billing_code,
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/payer")
async def search_by_payer(
    payer_name: str = Query(..., description="Payer name to search for"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(100, description="Number of results to return")
):
    """Fast payer search"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.search_by_payer(payer_name, state, year_month, limit)
        
        return {
            "search_type": "payer",
            "search_value": payer_name,
            "state": state,
            "year_month": year_month,
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/search/multi-field")
async def multi_field_search(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    primary_taxonomy_desc: Optional[str] = Query(None, description="Primary taxonomy description filter"),
    organization_name: Optional[str] = Query(None, description="Organization name filter (partial match)"),
    npi: Optional[str] = Query(None, description="NPI filter"),
    enumeration_type: Optional[str] = Query(None, description="Enumeration type filter (1=Individual, 2=Organization)"),
    billing_class: Optional[str] = Query(None, description="Billing class filter"),
    proc_set: Optional[str] = Query(None, description="Procedure set filter"),
    proc_class: Optional[str] = Query(None, description="Procedure class filter"),
    proc_group: Optional[str] = Query(None, description="Procedure group filter"),
    billing_code: Optional[str] = Query(None, description="Billing code filter"),
    tin_value: Optional[str] = Query(None, description="TIN value filter"),
    payer: Optional[str] = Query(None, description="Payer name filter"),
    limit: int = Query(100, description="Number of results to return")
):
    """Comprehensive multi-field search with all filters"""
    try:
        optimized_queries = get_optimized_queries()
        # Handle comma-separated values for multi-select
        def parse_multi_value(value):
            if not value:
                return None
            values = [v.strip() for v in value.split(',') if v.strip()]
            return values if values else None

        results = optimized_queries.multi_field_search(
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
            "filters": {
                "primary_taxonomy_desc": primary_taxonomy_desc,
                "organization_name": organization_name,
                "npi": npi,
                "enumeration_type": enumeration_type,
                "billing_class": billing_class,
                "proc_set": proc_set,
                "proc_class": proc_class,
                "proc_group": proc_group,
                "billing_code": billing_code,
                "tin_value": tin_value,
                "payer": payer
            },
            "result_count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/autocomplete/{field}")
async def get_autocomplete_suggestions(
    field: str = FastAPIPath(..., description="Field to get suggestions for (organization, taxonomy, procedure_category, payer, tin)"),
    query: str = Query(..., description="Search query"),
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    limit: int = Query(20, description="Number of suggestions to return")
):
    """Get autocomplete suggestions for various fields"""
    try:
        conn = get_duckdb_connection()
        
        # Simple field queries - just get distinct values
        field_queries = {
            "billing_class": f"""
                SELECT DISTINCT billing_class
                FROM read_parquet('{FACT_TABLE}')
                WHERE state = '{state}' AND year_month = '{year_month}'
                  AND billing_class IS NOT NULL
                  AND billing_class != ''
                ORDER BY billing_class
                LIMIT {limit}
            """,
            "payer": f"""
                SELECT DISTINCT reporting_entity_name
                FROM read_parquet('{FACT_TABLE}')
                WHERE state = '{state}' AND year_month = '{year_month}'
                  AND reporting_entity_name IS NOT NULL
                  AND reporting_entity_name != ''
                ORDER BY reporting_entity_name
                LIMIT {limit}
            """,
            "billing_code": f"""
                SELECT DISTINCT code
                FROM read_parquet('{FACT_TABLE}')
                WHERE state = '{state}' AND year_month = '{year_month}'
                  AND code IS NOT NULL
                  AND code != ''
                ORDER BY code
                LIMIT {limit}
            """,
            "proc_class": f"""
                SELECT DISTINCT proc_class
                FROM read_parquet('{DIM_CODE_CAT}')
                WHERE proc_class IS NOT NULL
                  AND proc_class != ''
                ORDER BY proc_class
                LIMIT {limit}
            """,
            "proc_set": f"""
                SELECT DISTINCT proc_set
                FROM read_parquet('{DIM_CODE_CAT}')
                WHERE proc_set IS NOT NULL
                  AND proc_set != ''
                ORDER BY proc_set
                LIMIT {limit}
            """,
            "proc_group": f"""
                SELECT DISTINCT proc_group
                FROM read_parquet('{DIM_CODE_CAT}')
                WHERE proc_group IS NOT NULL
                  AND proc_group != ''
                ORDER BY proc_group
                LIMIT {limit}
            """,
            "primary_taxonomy_desc": f"""
                SELECT DISTINCT primary_taxonomy_desc
                FROM read_parquet('{DIM_NPI}')
                WHERE primary_taxonomy_desc IS NOT NULL
                  AND primary_taxonomy_desc != ''
                ORDER BY primary_taxonomy_desc
                LIMIT {limit}
            """,
            "organization_name": f"""
                SELECT DISTINCT organization_name
                FROM read_parquet('{DIM_NPI}')
                WHERE organization_name IS NOT NULL
                  AND organization_name != ''
                ORDER BY organization_name
                LIMIT {limit}
            """,
            "npi": f"""
                SELECT DISTINCT npi
                FROM read_parquet('{DIM_NPI}')
                WHERE npi IS NOT NULL
                  AND npi != ''
                ORDER BY npi
                LIMIT {limit}
            """,
            "tin_value": f"""
                SELECT DISTINCT tin_value
                FROM read_parquet('{XREF_GROUP_TIN}')
                WHERE tin_value IS NOT NULL
                  AND tin_value != ''
                ORDER BY tin_value
                LIMIT {limit}
            """
        }
        
        if field not in field_queries:
            raise HTTPException(status_code=400, detail=f"Unknown field: {field}")
        
        result = conn.execute(field_queries[field]).fetchall()
        suggestions = [row[0] for row in result if row[0]]
        
        return {
            "field": field,
            "query": query,
            "state": state,
            "year_month": year_month,
            "suggestions": suggestions
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/search/statistics")
async def get_search_statistics(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format")
):
    """Get search statistics for the dashboard"""
    try:
        optimized_queries = get_optimized_queries()
        stats = optimized_queries.get_search_statistics(state, year_month)
        
        return {
            "state": state,
            "year_month": year_month,
            "statistics": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/explore/data-availability")
async def explore_data_availability(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format"),
    category: str = Query(..., description="Category to explore (payer, organization, taxonomy, procedure_set, procedure_class)"),
    limit: int = Query(25, description="Number of results to return"),
    offset: int = Query(0, description="Number of results to skip for pagination")
):
    """Explore data availability by category to help users understand what data exists"""
    try:
        optimized_queries = get_optimized_queries()
        results = optimized_queries.explore_data_availability(state, year_month, category, limit, offset)
        
        return {
            "state": state,
            "year_month": year_month,
            "category": category,
            "limit": limit,
            "offset": offset,
            "result_count": len(results),
            "has_more": len(results) == limit,  # Indicates if there are more results
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/explore/category-stats")
async def get_category_statistics(
    state: str = Query(..., description="State code"),
    year_month: str = Query(..., description="Year-month in YYYY-MM format")
):
    """Get high-level statistics for each category to show data availability"""
    try:
        optimized_queries = get_optimized_queries()
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
    """Drill down from one category to another to see related data"""
    try:
        optimized_queries = get_optimized_queries()
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
