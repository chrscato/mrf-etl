"""
Optimized Query Engine for MRF Dashboard
High-performance indexed queries using DuckDB materialized views
"""

import duckdb
import polars as pl
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import re
from datetime import datetime
import json

# Data paths
webapp_dir = Path(__file__).parent.parent
DATA_ROOT = webapp_dir.parent / "prod_etl/core/data"

class OptimizedMRFQueries:
    """High-performance MRF data queries with materialized views and indexing"""
    
    def __init__(self):
        self.conn = duckdb.connect()
        self._create_materialized_views()
        self._create_indexes()
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def _create_materialized_views(self):
        """Create materialized views for fast lookups"""
        
        # Provider search index with full-text search capabilities
        self.conn.execute("""
        CREATE OR REPLACE VIEW provider_search_index AS
        SELECT 
            n.npi,
            n.organization_name,
            n.first_name,
            n.last_name,
            n.primary_taxonomy_desc,
            n.status,
            n.enumeration_type,
            n.credential,
            na.city,
            na.state,
            na.postal_code,
            na.address_1,
            na.telephone_number,
            -- Pre-computed search fields for fast text search
            LOWER(CONCAT_WS(' ', 
                COALESCE(n.organization_name, ''), 
                COALESCE(n.first_name, ''), 
                COALESCE(n.last_name, ''),
                COALESCE(n.primary_taxonomy_desc, '')
            )) as search_text,
            -- Normalized organization name for exact matching
            LOWER(TRIM(COALESCE(n.organization_name, ''))) as org_name_normalized,
            -- Normalized taxonomy for exact matching
            LOWER(TRIM(COALESCE(n.primary_taxonomy_desc, ''))) as taxonomy_normalized
        FROM read_parquet('""" + str(DATA_ROOT / "dims/dim_npi.parquet") + """') n
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "dims/dim_npi_address.parquet") + """') na 
            ON n.npi = na.npi AND na.address_purpose = 'LOCATION'
        """)
        
        # TIN-based provider index for fast TIN lookups
        self.conn.execute("""
        CREATE OR REPLACE VIEW tin_provider_index AS
        SELECT 
            xt.tin_value,
            xt.tin_type,
            xt.pg_uid,
            n.npi,
            n.organization_name,
            n.first_name,
            n.last_name,
            n.primary_taxonomy_desc,
            f.state,
            f.year_month,
            f.payer_slug,
            f.reporting_entity_name,
            f.code,
            f.code_type,
            f.negotiated_rate,
            f.billing_class,
            f.negotiated_type,
            f.negotiation_arrangement
        FROM read_parquet('""" + str(DATA_ROOT / "xrefs/xref_pg_member_tin.parquet") + """') xt
        JOIN read_parquet('""" + str(DATA_ROOT / "xrefs/xref_pg_member_npi.parquet") + """') xn 
            ON xt.pg_uid = xn.pg_uid
        JOIN read_parquet('""" + str(DATA_ROOT / "dims/dim_npi.parquet") + """') n 
            ON xn.npi = n.npi
        JOIN read_parquet('""" + str(DATA_ROOT / "gold/fact_rate.parquet") + """') f 
            ON xt.pg_uid = f.pg_uid
        """)
        
        # Procedure code search index with categories
        self.conn.execute("""
        CREATE OR REPLACE VIEW procedure_search_index AS
        SELECT 
            f.code,
            f.code_type,
            f.negotiated_rate,
            f.payer_slug,
            f.reporting_entity_name,
            f.state,
            f.year_month,
            f.billing_class,
            f.negotiated_type,
            f.negotiation_arrangement,
            f.pg_uid,
            -- Procedure categorization
            cc.proc_set,
            cc.proc_class,
            cc.proc_group,
            -- Pre-computed search fields
            LOWER(CONCAT_WS(' ', 
                f.code, 
                COALESCE(cc.proc_set, ''),
                COALESCE(cc.proc_class, ''),
                COALESCE(cc.proc_group, '')
            )) as search_text,
            LOWER(TRIM(COALESCE(cc.proc_class, ''))) as proc_class_normalized,
            LOWER(TRIM(COALESCE(cc.proc_group, ''))) as proc_group_normalized
        FROM read_parquet('""" + str(DATA_ROOT / "gold/fact_rate.parquet") + """') f
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "dims/dim_code_cat.parquet") + """') cc 
            ON f.code = cc.proc_cd
        """)
        
        # Payer search index for fast payer lookups
        self.conn.execute("""
        CREATE OR REPLACE VIEW payer_search_index AS
        SELECT 
            f.payer_slug,
            f.reporting_entity_name,
            f.state,
            f.year_month,
            COUNT(*) as rate_count,
            AVG(f.negotiated_rate) as avg_rate,
            MIN(f.negotiated_rate) as min_rate,
            MAX(f.negotiated_rate) as max_rate,
            COUNT(DISTINCT f.code) as unique_procedures,
            COUNT(DISTINCT f.pg_uid) as unique_provider_groups,
            -- Pre-computed search fields
            LOWER(TRIM(f.reporting_entity_name)) as payer_name_normalized
        FROM read_parquet('""" + str(DATA_ROOT / "gold/fact_rate.parquet") + """') f
        GROUP BY f.payer_slug, f.reporting_entity_name, f.state, f.year_month
        """)
        
        # Comprehensive search index combining all dimensions
        self.conn.execute("""
        CREATE OR REPLACE VIEW comprehensive_search_index AS
        SELECT 
            f.fact_uid,
            f.state,
            f.year_month,
            f.payer_slug,
            f.reporting_entity_name,
            f.code,
            f.code_type,
            f.negotiated_rate,
            f.billing_class,
            f.negotiated_type,
            f.negotiation_arrangement,
            f.pg_uid,
            -- Provider information
            n.npi,
            n.organization_name,
            n.first_name,
            n.last_name,
            n.primary_taxonomy_desc,
            n.status,
            n.enumeration_type,
            -- TIN information
            xt.tin_value,
            xt.tin_type,
            -- Procedure categorization
            cc.proc_set,
            cc.proc_class,
            cc.proc_group,
            -- Address information
            na.city,
            na.state as provider_state,
            na.postal_code,
            -- Pre-computed search fields
            LOWER(CONCAT_WS(' ', 
                COALESCE(n.organization_name, ''),
                COALESCE(n.first_name, ''),
                COALESCE(n.last_name, ''),
                COALESCE(n.primary_taxonomy_desc, ''),
                COALESCE(f.code, ''),
                COALESCE(cc.proc_class, ''),
                COALESCE(cc.proc_group, ''),
                COALESCE(f.reporting_entity_name, ''),
                COALESCE(xt.tin_value, '')
            )) as full_search_text
        FROM read_parquet('""" + str(DATA_ROOT / "gold/fact_rate.parquet") + """') f
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "xrefs/xref_pg_member_npi.parquet") + """') xn 
            ON f.pg_uid = xn.pg_uid
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "dims/dim_npi.parquet") + """') n 
            ON xn.npi = n.npi
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "xrefs/xref_pg_member_tin.parquet") + """') xt 
            ON f.pg_uid = xt.pg_uid
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "dims/dim_code_cat.parquet") + """') cc 
            ON f.code = cc.proc_cd
        LEFT JOIN read_parquet('""" + str(DATA_ROOT / "dims/dim_npi_address.parquet") + """') na 
            ON n.npi = na.npi AND na.address_purpose = 'LOCATION'
        """)
    
    def _create_indexes(self):
        """Create indexes on materialized views for maximum performance"""
        
        # Indexes for provider search
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_search_text ON provider_search_index USING bm25(search_text)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_org_name ON provider_search_index(org_name_normalized)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_taxonomy ON provider_search_index(taxonomy_normalized)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_provider_npi ON provider_search_index(npi)")
        except:
            pass  # DuckDB may not support all index types
        
        # Indexes for TIN search
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tin_value ON tin_provider_index(tin_value)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tin_pg_uid ON tin_provider_index(pg_uid)")
        except:
            pass
        
        # Indexes for procedure search
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_procedure_code ON procedure_search_index(code)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_procedure_class ON procedure_search_index(proc_class_normalized)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_procedure_group ON procedure_search_index(proc_group_normalized)")
        except:
            pass
        
        # Indexes for payer search
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_payer_name ON payer_search_index(payer_name_normalized)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_payer_slug ON payer_search_index(payer_slug)")
        except:
            pass
    
    def search_by_tin(self, tin_value: str, state: str, year_month: str, 
                     limit: int = 100) -> List[Dict[str, Any]]:
        """Fast TIN-based search using materialized view"""
        
        query = f"""
        SELECT DISTINCT
            tin_value,
            tin_type,
            npi,
            organization_name,
            first_name,
            last_name,
            primary_taxonomy_desc,
            payer_slug,
            reporting_entity_name,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate
        FROM tin_provider_index
        WHERE tin_value = '{tin_value}'
          AND state = '{state}'
          AND year_month = '{year_month}'
        GROUP BY tin_value, tin_type, npi, organization_name, first_name, last_name, 
                 primary_taxonomy_desc, payer_slug, reporting_entity_name
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "tin_value": row[0],
                "tin_type": row[1],
                "npi": row[2],
                "organization_name": row[3],
                "first_name": row[4],
                "last_name": row[5],
                "primary_taxonomy_desc": row[6],
                "payer_slug": row[7],
                "reporting_entity_name": row[8],
                "rate_count": row[9],
                "avg_rate": round(row[10], 2) if row[10] else 0,
                "min_rate": round(row[11], 2) if row[11] else 0,
                "max_rate": round(row[12], 2) if row[12] else 0
            }
            for row in result
        ]
    
    def search_by_organization(self, org_name: str, state: str, year_month: str,
                              limit: int = 100) -> List[Dict[str, Any]]:
        """Fast organization name search using materialized view"""
        
        # Use ILIKE for case-insensitive partial matching
        query = f"""
        SELECT DISTINCT
            npi,
            organization_name,
            first_name,
            last_name,
            primary_taxonomy_desc,
            status,
            enumeration_type,
            city,
            provider_state,
            postal_code,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate
        FROM comprehensive_search_index
        WHERE organization_name ILIKE '%{org_name}%'
          AND state = '{state}'
          AND year_month = '{year_month}'
        GROUP BY npi, organization_name, first_name, last_name, primary_taxonomy_desc,
                 status, enumeration_type, city, provider_state, postal_code
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "npi": row[0],
                "organization_name": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "primary_taxonomy_desc": row[4],
                "status": row[5],
                "enumeration_type": row[6],
                "city": row[7],
                "provider_state": row[8],
                "postal_code": row[9],
                "rate_count": row[10],
                "avg_rate": round(row[11], 2) if row[11] else 0,
                "min_rate": round(row[12], 2) if row[12] else 0,
                "max_rate": round(row[13], 2) if row[13] else 0
            }
            for row in result
        ]
    
    def search_by_taxonomy(self, taxonomy_desc: str, state: str, year_month: str,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Fast taxonomy description search"""
        
        query = f"""
        SELECT DISTINCT
            npi,
            organization_name,
            first_name,
            last_name,
            primary_taxonomy_desc,
            city,
            provider_state,
            postal_code,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate
        FROM comprehensive_search_index
        WHERE primary_taxonomy_desc ILIKE '%{taxonomy_desc}%'
          AND state = '{state}'
          AND year_month = '{year_month}'
        GROUP BY npi, organization_name, first_name, last_name, primary_taxonomy_desc,
                 city, provider_state, postal_code
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "npi": row[0],
                "organization_name": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "primary_taxonomy_desc": row[4],
                "city": row[5],
                "provider_state": row[6],
                "postal_code": row[7],
                "rate_count": row[8],
                "avg_rate": round(row[9], 2) if row[9] else 0,
                "min_rate": round(row[10], 2) if row[10] else 0,
                "max_rate": round(row[11], 2) if row[11] else 0
            }
            for row in result
        ]
    
    def search_by_procedure_category(self, proc_class: str, state: str, year_month: str,
                                   limit: int = 100) -> List[Dict[str, Any]]:
        """Fast procedure category search"""
        
        query = f"""
        SELECT DISTINCT
            code,
            code_type,
            proc_set,
            proc_class,
            proc_group,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate,
            COUNT(DISTINCT payer_slug) as unique_payers
        FROM procedure_search_index
        WHERE proc_class ILIKE '%{proc_class}%'
          AND state = '{state}'
          AND year_month = '{year_month}'
        GROUP BY code, code_type, proc_set, proc_class, proc_group
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "code": row[0],
                "code_type": row[1],
                "proc_set": row[2],
                "proc_class": row[3],
                "proc_group": row[4],
                "rate_count": row[5],
                "avg_rate": round(row[6], 2) if row[6] else 0,
                "min_rate": round(row[7], 2) if row[7] else 0,
                "max_rate": round(row[8], 2) if row[8] else 0,
                "unique_payers": row[9]
            }
            for row in result
        ]
    
    def search_by_billing_code(self, billing_code: str, state: str, year_month: str,
                              limit: int = 100) -> List[Dict[str, Any]]:
        """Fast billing code search"""
        
        query = f"""
        SELECT DISTINCT
            code,
            code_type,
            proc_set,
            proc_class,
            proc_group,
            billing_class,
            COUNT(*) as rate_count,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate,
            COUNT(DISTINCT payer_slug) as unique_payers
        FROM procedure_search_index
        WHERE code = '{billing_code}'
          AND state = '{state}'
          AND year_month = '{year_month}'
        GROUP BY code, code_type, proc_set, proc_class, proc_group, billing_class
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "code": row[0],
                "code_type": row[1],
                "proc_set": row[2],
                "proc_class": row[3],
                "proc_group": row[4],
                "billing_class": row[5],
                "rate_count": row[6],
                "avg_rate": round(row[7], 2) if row[7] else 0,
                "min_rate": round(row[8], 2) if row[8] else 0,
                "max_rate": round(row[9], 2) if row[9] else 0,
                "unique_payers": row[10]
            }
            for row in result
        ]
    
    def search_by_payer(self, payer_name: str, state: str, year_month: str,
                       limit: int = 100) -> List[Dict[str, Any]]:
        """Fast payer search"""
        
        query = f"""
        SELECT 
            payer_slug,
            reporting_entity_name,
            rate_count,
            avg_rate,
            min_rate,
            max_rate,
            unique_procedures,
            unique_provider_groups
        FROM payer_search_index
        WHERE reporting_entity_name ILIKE '%{payer_name}%'
          AND state = '{state}'
          AND year_month = '{year_month}'
        ORDER BY rate_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "payer_slug": row[0],
                "reporting_entity_name": row[1],
                "rate_count": row[2],
                "avg_rate": round(row[3], 2) if row[3] else 0,
                "min_rate": round(row[4], 2) if row[4] else 0,
                "max_rate": round(row[5], 2) if row[5] else 0,
                "unique_procedures": row[6],
                "unique_provider_groups": row[7]
            }
            for row in result
        ]
    
    def multi_field_search(self, state: str, year_month: str,
                          primary_taxonomy_desc: Optional[List[str]] = None,
                          organization_name: Optional[List[str]] = None,
                          npi: Optional[List[str]] = None,
                          enumeration_type: Optional[List[str]] = None,
                          billing_class: Optional[List[str]] = None,
                          proc_set: Optional[List[str]] = None,
                          proc_class: Optional[List[str]] = None,
                          proc_group: Optional[List[str]] = None,
                          billing_code: Optional[List[str]] = None,
                          tin_value: Optional[List[str]] = None,
                          payer: Optional[List[str]] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Comprehensive multi-field search with all webapp filters"""
        
        where_conditions = [f"state = '{state}'", f"year_month = '{year_month}'"]
        
        # Helper function to build IN clause for lists
        def build_in_clause(field, values):
            if not values:
                return None
            if isinstance(values, list):
                quoted_values = [f"'{v}'" for v in values]
                return f"{field} IN ({', '.join(quoted_values)})"
            else:
                return f"{field} = '{values}'"
        
        # Helper function to build ILIKE clause for lists
        def build_ilike_clause(field, values):
            if not values:
                return None
            if isinstance(values, list):
                conditions = [f"{field} ILIKE '%{v}%'" for v in values]
                return f"({' OR '.join(conditions)})"
            else:
                return f"{field} ILIKE '%{values}%'"
        
        # Provider filters
        if primary_taxonomy_desc:
            condition = build_in_clause("primary_taxonomy_desc", primary_taxonomy_desc)
            if condition:
                where_conditions.append(condition)
        if organization_name:
            condition = build_ilike_clause("organization_name", organization_name)
            if condition:
                where_conditions.append(condition)
        if npi:
            condition = build_in_clause("npi", npi)
            if condition:
                where_conditions.append(condition)
        if enumeration_type:
            condition = build_in_clause("enumeration_type", enumeration_type)
            if condition:
                where_conditions.append(condition)
        
        # Procedure filters
        if billing_class:
            condition = build_in_clause("billing_class", billing_class)
            if condition:
                where_conditions.append(condition)
        if proc_set:
            condition = build_in_clause("proc_set", proc_set)
            if condition:
                where_conditions.append(condition)
        if proc_class:
            condition = build_in_clause("proc_class", proc_class)
            if condition:
                where_conditions.append(condition)
        if proc_group:
            condition = build_in_clause("proc_group", proc_group)
            if condition:
                where_conditions.append(condition)
        if billing_code:
            condition = build_in_clause("code", billing_code)
            if condition:
                where_conditions.append(condition)
        
        # TIN and payer filters
        if tin_value:
            condition = build_in_clause("tin_value", tin_value)
            if condition:
                where_conditions.append(condition)
        if payer:
            condition = build_ilike_clause("reporting_entity_name", payer)
            if condition:
                where_conditions.append(condition)
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT DISTINCT
            fact_uid,
            npi,
            organization_name,
            first_name,
            last_name,
            primary_taxonomy_desc,
            code,
            code_type,
            proc_class,
            proc_group,
            tin_value,
            tin_type,
            reporting_entity_name,
            negotiated_rate,
            billing_class,
            negotiated_type,
            negotiation_arrangement,
            city,
            provider_state,
            postal_code
        FROM comprehensive_search_index
        WHERE {where_clause}
        ORDER BY negotiated_rate DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "fact_uid": row[0],
                "npi": row[1],
                "organization_name": row[2],
                "first_name": row[3],
                "last_name": row[4],
                "primary_taxonomy_desc": row[5],
                "code": row[6],
                "code_type": row[7],
                "proc_class": row[8],
                "proc_group": row[9],
                "tin_value": row[10],
                "tin_type": row[11],
                "reporting_entity_name": row[12],
                "negotiated_rate": round(row[13], 2) if row[13] else 0,
                "billing_class": row[14],
                "negotiated_type": row[15],
                "negotiation_arrangement": row[16],
                "city": row[17],
                "provider_state": row[18],
                "postal_code": row[19]
            }
            for row in result
        ]
    
    def get_autocomplete_suggestions(self, field: str, query: str, state: str, year_month: str,
                                   limit: int = 20) -> List[str]:
        """Get autocomplete suggestions for various fields"""
        
        # Map field names to SQL fields and tables
        field_mapping = {
            "primary_taxonomy_desc": ("primary_taxonomy_desc", "comprehensive_search_index"),
            "organization_name": ("organization_name", "comprehensive_search_index"),
            "npi": ("npi", "comprehensive_search_index"),
            "billing_class": ("billing_class", "comprehensive_search_index"),
            "proc_set": ("proc_set", "comprehensive_search_index"),
            "proc_class": ("proc_class", "comprehensive_search_index"),
            "proc_group": ("proc_group", "comprehensive_search_index"),
            "billing_code": ("code", "comprehensive_search_index"),
            "tin_value": ("tin_value", "comprehensive_search_index"),
            "payer": ("reporting_entity_name", "comprehensive_search_index"),
            # Legacy field names for backward compatibility
            "organization": ("organization_name", "comprehensive_search_index"),
            "taxonomy": ("primary_taxonomy_desc", "comprehensive_search_index"),
            "procedure_category": ("proc_class", "comprehensive_search_index"),
            "tin": ("tin_value", "comprehensive_search_index")
        }
        
        if field not in field_mapping:
            return []
        
        sql_field, table = field_mapping[field]
        
        # Build the search query
        where_conditions = [
            f"state = '{state}'",
            f"year_month = '{year_month}'",
            f"{sql_field} IS NOT NULL",
            f"{sql_field} != ''"
        ]
        
        if query:
            where_conditions.append(f"{sql_field} ILIKE '%{query}%'")
        
        where_clause = " AND ".join(where_conditions)
        
        search_query = f"""
        SELECT DISTINCT {sql_field}
        FROM {table}
        WHERE {where_clause}
        ORDER BY {sql_field}
        LIMIT {limit}
        """
        
        try:
            result = self.conn.execute(search_query).fetchall()
            return [row[0] for row in result if row[0]]
        except Exception as e:
            print(f"Error in autocomplete for {field}: {e}")
            return []
    
    def get_search_statistics(self, state: str, year_month: str) -> Dict[str, Any]:
        """Get search statistics for the dashboard"""
        
        query = f"""
        SELECT 
            COUNT(DISTINCT npi) as unique_providers,
            COUNT(DISTINCT organization_name) as unique_organizations,
            COUNT(DISTINCT primary_taxonomy_desc) as unique_taxonomies,
            COUNT(DISTINCT code) as unique_procedures,
            COUNT(DISTINCT proc_class) as unique_procedure_classes,
            COUNT(DISTINCT reporting_entity_name) as unique_payers,
            COUNT(DISTINCT tin_value) as unique_tins,
            COUNT(*) as total_records
        FROM comprehensive_search_index
        WHERE state = '{state}' AND year_month = '{year_month}'
        """
        
        result = self.conn.execute(query).fetchone()
        
        return {
            "unique_providers": result[0],
            "unique_organizations": result[1],
            "unique_taxonomies": result[2],
            "unique_procedures": result[3],
            "unique_procedure_classes": result[4],
            "unique_payers": result[5],
            "unique_tins": result[6],
            "total_records": result[7]
        }
    
    def explore_data_availability(self, state: str, year_month: str, category: str, limit: int = 25, offset: int = 0) -> List[Dict[str, Any]]:
        """Explore data availability by category to help users understand what data exists"""
        
        # Map category names to SQL fields and additional info
        category_mapping = {
            "payer": {
                "field": "reporting_entity_name",
                "label": "Payer Name",
                "description": "Insurance companies and payers"
            },
            "organization": {
                "field": "organization_name", 
                "label": "Organization Name",
                "description": "Healthcare organizations and provider groups"
            },
            "taxonomy": {
                "field": "primary_taxonomy_desc",
                "label": "Taxonomy Description", 
                "description": "Provider specialties and taxonomy classifications"
            },
            "procedure_set": {
                "field": "proc_set",
                "label": "Procedure Set",
                "description": "High-level procedure categories"
            },
            "procedure_class": {
                "field": "proc_class",
                "label": "Procedure Class",
                "description": "Detailed procedure classifications"
            }
        }
        
        if category not in category_mapping:
            return []
        
        field = category_mapping[category]["field"]
        
        # Optimized query with pagination and reduced calculations
        query = f"""
        SELECT 
            {field} as value,
            COUNT(*) as record_count,
            COUNT(DISTINCT npi) as unique_providers,
            COUNT(DISTINCT code) as unique_procedures,
            ROUND(AVG(negotiated_rate), 2) as avg_rate,
            ROUND(MIN(negotiated_rate), 2) as min_rate,
            ROUND(MAX(negotiated_rate), 2) as max_rate
        FROM comprehensive_search_index
        WHERE state = '{state}' 
          AND year_month = '{year_month}'
          AND {field} IS NOT NULL 
          AND {field} != ''
        GROUP BY {field}
        ORDER BY record_count DESC
        LIMIT {limit} OFFSET {offset}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "value": row[0],
                "record_count": row[1],
                "unique_providers": row[2],
                "unique_procedures": row[3],
                "avg_rate": row[4] if row[4] else 0,
                "min_rate": row[5] if row[5] else 0,
                "max_rate": row[6] if row[6] else 0,
                "category_info": category_mapping[category]
            }
            for row in result
        ]
    
    def get_category_statistics(self, state: str, year_month: str) -> Dict[str, Any]:
        """Get high-level statistics for each category to show data availability"""
        
        # Get counts for each major category
        query = f"""
        SELECT 
            COUNT(DISTINCT reporting_entity_name) as unique_payers,
            COUNT(DISTINCT organization_name) as unique_organizations,
            COUNT(DISTINCT primary_taxonomy_desc) as unique_taxonomies,
            COUNT(DISTINCT proc_set) as unique_procedure_sets,
            COUNT(DISTINCT proc_class) as unique_procedure_classes,
            COUNT(DISTINCT code) as unique_procedures,
            COUNT(DISTINCT npi) as unique_providers,
            COUNT(DISTINCT tin_value) as unique_tins,
            COUNT(*) as total_records
        FROM comprehensive_search_index
        WHERE state = '{state}' AND year_month = '{year_month}'
        """
        
        result = self.conn.execute(query).fetchone()
        
        return {
            "payer": {
                "count": result[0],
                "label": "Payers",
                "description": "Insurance companies and payers"
            },
            "organization": {
                "count": result[1],
                "label": "Organizations", 
                "description": "Healthcare organizations and provider groups"
            },
            "taxonomy": {
                "count": result[2],
                "label": "Taxonomies",
                "description": "Provider specialties and classifications"
            },
            "procedure_set": {
                "count": result[3],
                "label": "Procedure Sets",
                "description": "High-level procedure categories"
            },
            "procedure_class": {
                "count": result[4],
                "label": "Procedure Classes",
                "description": "Detailed procedure classifications"
            },
            "procedure": {
                "count": result[5],
                "label": "Procedures",
                "description": "Individual procedure codes"
            },
            "provider": {
                "count": result[6],
                "label": "Providers",
                "description": "Individual healthcare providers"
            },
            "tin": {
                "count": result[7],
                "label": "TINs",
                "description": "Tax identification numbers"
            },
            "total_records": result[8]
        }
    
    def drill_down_exploration(self, state: str, year_month: str, category: str, 
                             selected_value: str, drill_category: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Drill down from one category to another to see related data"""
        
        # Map category names to SQL fields
        category_mapping = {
            "payer": "reporting_entity_name",
            "organization": "organization_name",
            "taxonomy": "primary_taxonomy_desc", 
            "procedure_set": "proc_set",
            "procedure_class": "proc_class",
            "procedure": "code",
            "provider": "npi",
            "tin": "tin_value"
        }
        
        if category not in category_mapping or drill_category not in category_mapping:
            return []
        
        source_field = category_mapping[category]
        drill_field = category_mapping[drill_category]
        
        query = f"""
        SELECT 
            {drill_field} as value,
            COUNT(*) as record_count,
            COUNT(DISTINCT npi) as unique_providers,
            COUNT(DISTINCT code) as unique_procedures,
            AVG(negotiated_rate) as avg_rate,
            MIN(negotiated_rate) as min_rate,
            MAX(negotiated_rate) as max_rate
        FROM comprehensive_search_index
        WHERE state = '{state}' 
          AND year_month = '{year_month}'
          AND {source_field} = '{selected_value}'
          AND {drill_field} IS NOT NULL 
          AND {drill_field} != ''
        GROUP BY {drill_field}
        ORDER BY record_count DESC
        LIMIT {limit}
        """
        
        result = self.conn.execute(query).fetchall()
        
        return [
            {
                "value": row[0],
                "record_count": row[1],
                "unique_providers": row[2],
                "unique_procedures": row[3],
                "avg_rate": round(row[4], 2) if row[4] else 0,
                "min_rate": round(row[5], 2) if row[5] else 0,
                "max_rate": round(row[6], 2) if row[6] else 0,
                "source_category": category,
                "source_value": selected_value,
                "drill_category": drill_category
            }
            for row in result
        ]

# Global instance for caching
_optimized_queries = None

def get_optimized_queries() -> OptimizedMRFQueries:
    """Get singleton instance of optimized queries"""
    global _optimized_queries
    if _optimized_queries is None:
        _optimized_queries = OptimizedMRFQueries()
    return _optimized_queries
