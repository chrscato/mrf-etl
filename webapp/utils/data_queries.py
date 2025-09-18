"""
Data Query Utilities for MRF Webapp
Helper functions for common data access patterns
"""

import polars as pl
import duckdb
from pathlib import Path
from typing import List, Dict, Any, Optional
import re

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

class MRFDataQueries:
    """Main class for MRF data queries"""
    
    def __init__(self):
        self.conn = duckdb.connect()
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def get_available_data(self) -> Dict[str, Any]:
        """Get available states, year_months, and basic stats"""
        query = f"""
        SELECT 
            state,
            year_month,
            COUNT(*) as record_count,
            COUNT(DISTINCT reporting_entity_name) as unique_payers,
            COUNT(DISTINCT code) as unique_procedures
        FROM read_parquet('{FACT_TABLE}')
        GROUP BY state, year_month
        ORDER BY state, year_month
        """
        
        result = self.conn.execute(query).fetchall()
        
        # Get unique payers
        payers_query = f"""
        SELECT DISTINCT reporting_entity_name
        FROM read_parquet('{FACT_TABLE}')
        ORDER BY reporting_entity_name
        """
        
        payers_result = self.conn.execute(payers_query).fetchall()
        
        return {
            "data_availability": [
                {
                    "state": row[0],
                    "year_month": row[1],
                    "record_count": row[2],
                    "unique_payers": row[3],
                    "unique_procedures": row[4]
                }
                for row in result
            ],
            "available_payers": [row[0] for row in payers_result]
        }
    
    def get_rate_summary(self, state: str, year_month: str, 
                        payer: Optional[str] = None, 
                        code_type: Optional[str] = None,
                        code: Optional[str] = None) -> Dict[str, Any]:
        """Get rate summary statistics"""
        
        where_conditions = [f"state = '{state}'", f"year_month = '{year_month}'"]
        
        if payer:
            where_conditions.append(f"reporting_entity_name ILIKE '%{payer}%'")
        if code_type:
            where_conditions.append(f"code_type = '{code_type}'")
        if code:
            where_conditions.append(f"code = '{code}'")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT 
            COUNT(*) as total_rates,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negotiated_rate) as median_rate,
            COUNT(DISTINCT code) as unique_procedures,
            COUNT(DISTINCT reporting_entity_name) as unique_payers
        FROM read_parquet('{FACT_TABLE}')
        WHERE {where_clause}
        """
        
        result = self.conn.execute(query).fetchone()
        
        return {
            "state": state,
            "year_month": year_month,
            "filters": {
                "payer": payer,
                "code_type": code_type,
                "code": code
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
    
    def get_rates_by_payer(self, state: str, year_month: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get rate statistics grouped by payer"""
        
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
        
        result = self.conn.execute(query).fetchall()
        
        return [
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
    
    def get_rates_by_procedure(self, state: str, year_month: str, 
                              code_type: Optional[str] = None, 
                              limit: int = 50) -> List[Dict[str, Any]]:
        """Get rate statistics grouped by procedure code"""
        
        where_clause = f"WHERE state = '{state}' AND year_month = '{year_month}'"
        if code_type:
            where_clause += f" AND code_type = '{code_type}'"
        
        query = f"""
        WITH rates_with_desc AS (
            SELECT 
                f.code_type,
                f.code,
                f.negotiated_rate,
                f.reporting_entity_name,
                COALESCE(d.code_desc, f.code) as code_desc
            FROM read_parquet('{FACT_TABLE}') f
            LEFT JOIN read_parquet('{DIM_CODE}') d 
                ON d.code_type = f.code_type AND d.code = f.code
            {where_clause}
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
        
        result = self.conn.execute(query).fetchall()
        
        return [
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
    
    def get_rate_details(self, state: str, year_month: str,
                        payer: Optional[str] = None,
                        code: Optional[str] = None,
                        limit: int = 100) -> List[Dict[str, Any]]:
        """Get detailed rate records with descriptions"""
        
        where_conditions = [f"state = '{state}'", f"year_month = '{year_month}'"]
        
        if payer:
            where_conditions.append(f"reporting_entity_name ILIKE '%{payer}%'")
        if code:
            where_conditions.append(f"code = '{code}'")
        
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
        FROM read_parquet('{FACT_TABLE}') f
        LEFT JOIN read_parquet('{DIM_CODE}') d 
            ON d.code_type = f.code_type AND d.code = f.code
        WHERE {where_clause}
        ORDER BY f.reporting_entity_name, f.code, f.negotiated_rate
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
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
    
    def search_providers(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search providers by name"""
        
        search_query = f"""
        SELECT 
            npi,
            organization_name,
            first_name,
            last_name,
            enumeration_type,
            primary_taxonomy_desc,
            status
        FROM read_parquet('{DIM_NPI}')
        WHERE organization_name ILIKE '%{query}%' 
           OR first_name ILIKE '%{query}%' 
           OR last_name ILIKE '%{query}%'
        ORDER BY organization_name, last_name, first_name
        LIMIT {limit}
        """
        
        result = self.conn.execute(search_query).fetchall()
        
        return [
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
    
    def get_provider_rates(self, npi: str, state: str, year_month: str) -> List[Dict[str, Any]]:
        """Get rates for a specific provider"""
        
        query = f"""
        WITH provider_rates AS (
            SELECT 
                f.*,
                regexp_replace(lower(f.reporting_entity_name), '[^a-z0-9]+', '_') as payer_slug
            FROM read_parquet('{FACT_TABLE}') f
            WHERE f.state = '{state}' AND f.year_month = '{year_month}'
        )
        SELECT 
            pr.reporting_entity_name,
            pr.code_type,
            pr.code,
            COALESCE(d.code_desc, pr.code) as code_desc,
            pr.negotiated_rate,
            pr.negotiated_type,
            pr.negotiation_arrangement,
            pr.expiration_date
        FROM provider_rates pr
        LEFT JOIN read_parquet('{DIM_CODE}') d 
            ON d.code_type = pr.code_type AND d.code = pr.code
        JOIN read_parquet('{XREF_GROUP_NPI}') x 
            ON x.year_month = pr.year_month 
            AND x.payer_slug = pr.payer_slug 
            AND x.pg_uid = pr.pg_uid
        WHERE x.npi = '{npi}'
        ORDER BY pr.reporting_entity_name, pr.code
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
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

# Convenience function for quick access
def get_mrf_queries() -> MRFDataQueries:
    """Get a new MRFDataQueries instance"""
    return MRFDataQueries()
