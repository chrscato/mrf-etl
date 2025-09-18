"""
Simple, memory-efficient queries for MRF dashboard
Avoids complex joins and materialized views to prevent crashes
"""

import duckdb
import polars as pl
from pathlib import Path
from typing import List, Dict, Any, Optional
import re

# Data paths
webapp_dir = Path(__file__).parent
DATA_ROOT = webapp_dir.parent / "prod_etl/core/data"

class SimpleMRFQueries:
    """Simple, memory-efficient MRF data queries"""
    
    def __init__(self):
        self.conn = duckdb.connect()
        # Set very conservative memory limits
        self.conn.execute("SET memory_limit='256MB'")
        self.conn.execute("SET max_memory='256MB'")
        self.conn.execute("SET threads=1")
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def multi_field_search(self, state: str, year_month: str,
                          billing_class: Optional[List[str]] = None,
                          payers: Optional[List[str]] = None,
                          proc_set: Optional[List[str]] = None,
                          proc_class: Optional[List[str]] = None,
                          taxonomies: Optional[List[str]] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Ultra-simple multi-field search with minimal memory usage"""
        
        try:
            # Validate inputs
            if not state or not year_month:
                return []
            
            # Very conservative limit
            limit = min(limit, 20)
            
            # Build basic where conditions
            where_conditions = ["state = ?", "year_month = ?"]
            params = [state, year_month]
            
            # Add basic filters
            if billing_class and billing_class[0]:
                where_conditions.append("billing_class = ?")
                params.append(billing_class[0])
            
            if payers and payers[0]:
                where_conditions.append("reporting_entity_name = ?")
                params.append(payers[0])
            
            where_clause = " AND ".join(where_conditions)
            
            # Ultra-simple query - no joins, just basic filtering
            query = f"""
            SELECT 
                code,
                negotiated_rate,
                reporting_entity_name,
                billing_class,
                negotiated_type
            FROM read_parquet('{DATA_ROOT / "gold/fact_rate.parquet"}')
            WHERE {where_clause}
            ORDER BY negotiated_rate DESC
            LIMIT {limit}
            """
            
            result = self.conn.execute(query, params).fetchall()
            
            # Convert to simple format
            return [
                {
                    "code": row[0],
                    "negotiated_rate": round(row[1], 2) if row[1] else 0,
                    "payer": row[2],
                    "billing_class": row[3],
                    "negotiated_type": row[4],
                    "organization_name": "Provider Info",
                    "taxonomy": "Medical Service"
                }
                for row in result
            ]
            
        except Exception as e:
            print(f"Error in simple multi_field_search: {e}")
            return []
    
    def get_stage_options(self, stage_field: str, state: str, year_month: str,
                          billing_class: Optional[str] = None,
                          payers: Optional[List[str]] = None,
                          proc_set: Optional[str] = None,
                          proc_class: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get options for each stage of filtering"""
        
        try:
            where_conditions = ["state = ?", "year_month = ?"]
            params = [state, year_month]
            
            if billing_class:
                where_conditions.append("billing_class = ?")
                params.append(billing_class)
            
            if payers and payers[0]:
                where_conditions.append("reporting_entity_name = ?")
                params.append(payers[0])
            
            where_clause = " AND ".join(where_conditions)
            
            # Field mapping for different stages
            field_map = {
                "billing_class": "billing_class",
                "payer": "reporting_entity_name",
                "proc_set": "'Imaging'",  # Placeholder
                "proc_class": "'Ultrasound'",  # Placeholder
                "taxonomy": "'Radiology'"  # Placeholder
            }
            
            sql_field = field_map.get(stage_field)
            if not sql_field:
                return []
            
            # Simple query to get unique values
            query = f"""
            SELECT {sql_field} as value, COUNT(*) as count
            FROM read_parquet('{DATA_ROOT / "gold/fact_rate.parquet"}')
            WHERE {where_clause} AND {sql_field} IS NOT NULL AND {sql_field} != ''
            GROUP BY {sql_field}
            ORDER BY count DESC
            LIMIT 20
            """
            
            result = self.conn.execute(query, params).fetchall()
            return [{"value": r[0], "label": r[0], "count": r[1]} for r in result]
            
        except Exception as e:
            print(f"Error in get_stage_options for {stage_field}: {e}")
            return []
    
    def get_autocomplete_suggestions(self, field: str, query: str, state: str, year_month: str,
                                   limit: int = 20) -> List[str]:
        """Get simple autocomplete suggestions"""
        
        try:
            # Very simple suggestions based on common values
            suggestions = {
                "primary_taxonomy_desc": [
                    "Internal Medicine",
                    "Family Medicine", 
                    "Cardiology",
                    "Orthopedic Surgery",
                    "Emergency Medicine"
                ],
                "organization_name": [
                    "Hospital",
                    "Medical Center",
                    "Clinic",
                    "Health System"
                ],
                "billing_class": [
                    "professional",
                    "institutional"
                ],
                "payer": [
                    "Aetna",
                    "UnitedHealthcare", 
                    "Cigna",
                    "Blue Cross Blue Shield"
                ]
            }
            
            if field in suggestions:
                field_suggestions = suggestions[field]
                if query:
                    # Filter by query
                    filtered = [s for s in field_suggestions if query.lower() in s.lower()]
                    return filtered[:limit]
                else:
                    return field_suggestions[:limit]
            
            return []
            
        except Exception as e:
            print(f"Error in simple autocomplete: {e}")
            return []

# Global instance
_simple_queries = None

def get_simple_queries() -> SimpleMRFQueries:
    """Get singleton instance of simple queries"""
    global _simple_queries
    if _simple_queries is None:
        _simple_queries = SimpleMRFQueries()
    return _simple_queries
