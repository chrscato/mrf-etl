#!/usr/bin/env python3
"""
Build Medicare Benchmark Dimension Tables

This script creates comprehensive Medicare benchmark dimension tables in parquet format
with both national and state-averaged rates for professional, OPPS, and ASC services.

Usage:
    python build_medicare_benchmarks.py

Output:
    - prod_etl/core/data/silver/benchmarks/bench_medicare_professional.parquet
    - prod_etl/core/data/silver/benchmarks/bench_medicare_opps.parquet  
    - prod_etl/core/data/silver/benchmarks/bench_medicare_asc.parquet
    - prod_etl/core/data/silver/benchmarks/bench_medicare_comprehensive.parquet
"""

import pandas as pd
import sqlite3
import logging
import numpy as np
from pathlib import Path
from typing import Dict, Any, List
import argparse
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MedicareBenchmarkBuilder:
    """Build comprehensive Medicare benchmark dimension tables"""
    
    def __init__(self, prof_db_path: str = "data/raw/compensation_rates.db", 
                 facility_db_path: str = "data/raw/medicare_test.db", year: int = 2025):
        self.prof_db_path = prof_db_path
        self.facility_db_path = facility_db_path
        self.year = year
        self.output_dir = Path("prod_etl/core/data/silver/benchmarks")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Constants for facility rates (CY2025)
        self.OPPS_CF = 89.169
        self.ASC_CF = 54.895
        self.OPPS_LABOR_SHARE = 0.60
        self.ASC_LABOR_SHARE = 0.50
        
        logger.info(f"Initialized MedicareBenchmarkBuilder for year {year}")
        logger.info(f"Professional database: {prof_db_path}")
        logger.info(f"Facility database: {facility_db_path}")
        logger.info(f"Output directory: {self.output_dir}")
    
    def build_all_benchmarks(self):
        """Build all Medicare benchmark dimension tables"""
        logger.info("=" * 60)
        logger.info("Building comprehensive Medicare benchmark tables...")
        logger.info("=" * 60)
        
        # Load all reference data
        logger.info("Loading reference data from database...")
        ref_data = self._load_all_reference_data()
        
        # Build each benchmark table
        self._build_medicare_professional_benchmarks(ref_data)
        self._build_medicare_opps_benchmarks(ref_data)
        self._build_medicare_asc_benchmarks(ref_data)
        self._build_medicare_comprehensive_benchmarks()
        
        logger.info("=" * 60)
        logger.info("All Medicare benchmark tables built successfully!")
        logger.info("=" * 60)
    
    def _load_all_reference_data(self) -> Dict[str, Any]:
        """Load all Medicare reference tables from SQLite databases"""
        ref_data = {}
        
        # Load professional rate tables from compensation_rates.db
        logger.info(f"Loading professional rate tables from: {self.prof_db_path}")
        if not Path(self.prof_db_path).exists():
            raise FileNotFoundError(f"Professional database file not found: {self.prof_db_path}")
        
        prof_conn = sqlite3.connect(self.prof_db_path)
        try:
            ref_data['mloc'] = pd.read_sql_query("SELECT * FROM medicare_locality_map", prof_conn)
            ref_data['meta'] = pd.read_sql_query("SELECT * FROM medicare_locality_meta", prof_conn)
            ref_data['gpci'] = pd.read_sql_query("SELECT * FROM cms_gpci WHERE year = ?", prof_conn, params=(self.year,))
            ref_data['rvu'] = pd.read_sql_query("SELECT * FROM cms_rvu WHERE year = ? AND (modifier IS NULL OR modifier = '')", prof_conn, params=(self.year,))
            ref_data['cf'] = pd.read_sql_query("SELECT * FROM cms_conversion_factor WHERE year = ?", prof_conn, params=(self.year,))
            
            # Clean GPCI data - remove header row if present
            if len(ref_data['gpci']) > 0 and ref_data['gpci'].iloc[0]['locality_code'] == 'locality_code':
                ref_data['gpci'] = ref_data['gpci'].iloc[1:].reset_index(drop=True)
            
            logger.info("Professional rate tables loaded:")
            for table_name in ['mloc', 'meta', 'gpci', 'rvu', 'cf']:
                if table_name in ref_data:
                    logger.info(f"  {table_name}: {len(ref_data[table_name]):,} rows")
        finally:
            prof_conn.close()
        
        # Load facility rate tables from medicare_test.db
        logger.info(f"Loading facility rate tables from: {self.facility_db_path}")
        if not Path(self.facility_db_path).exists():
            raise FileNotFoundError(f"Facility database file not found: {self.facility_db_path}")
        
        facility_conn = sqlite3.connect(self.facility_db_path)
        try:
            ref_data['df_aa'] = pd.read_sql("SELECT * FROM asc_addendum_aa", facility_conn)
            ref_data['df_b'] = pd.read_sql("SELECT * FROM opps_addendum_b", facility_conn)
            ref_data['df_wi'] = pd.read_sql("SELECT * FROM cbsa_wage_index", facility_conn)
            ref_data['df_zip_cbsa'] = pd.read_sql("SELECT * FROM zip_cbsa", facility_conn)
            
            logger.info("Facility rate tables loaded:")
            for table_name in ['df_aa', 'df_b', 'df_wi', 'df_zip_cbsa']:
                if table_name in ref_data:
                    logger.info(f"  {table_name}: {len(ref_data[table_name]):,} rows")
        finally:
            facility_conn.close()
        
        return ref_data
    
    def _build_medicare_professional_benchmarks(self, ref_data: Dict[str, Any]):
        """Build comprehensive Medicare professional benchmark table"""
        logger.info("\n" + "-" * 50)
        logger.info("Building Medicare Professional Benchmarks")
        logger.info("-" * 50)
        
        # Get all unique procedure codes
        procedure_codes = ref_data['rvu']['procedure_code'].unique()
        logger.info(f"Found {len(procedure_codes):,} unique procedure codes")
        
        # Get all states from locality data
        states = self._extract_states_from_locality_data(ref_data)
        logger.info(f"Found {len(states)} states: {', '.join(states[:5])}{'...' if len(states) > 5 else ''}")
        
        # Create comprehensive state/code combinations
        combinations = []
        for state in states:
            for code in procedure_codes:
                combinations.append({
                    'state': state,
                    'year_month': f"{self.year}-01",  # Default to January
                    'code_type': 'CPT',
                    'code': code
                })
        
        combinations_df = pd.DataFrame(combinations)
        logger.info(f"Created {len(combinations_df):,} state/code combinations")
        
        # Calculate rates for each combination
        benchmarks = self._calculate_professional_benchmarks(combinations_df, ref_data)
        
        # Add metadata
        benchmarks['benchmark_type'] = 'professional'
        benchmarks['created_date'] = datetime.now()
        benchmarks['data_year'] = self.year
        
        # Save to parquet
        output_path = self.output_dir / "bench_medicare_professional.parquet"
        benchmarks.to_parquet(output_path, index=False)
        logger.info(f"✓ Saved Medicare professional benchmarks to {output_path}")
        logger.info(f"  Rows: {len(benchmarks):,}, Columns: {len(benchmarks.columns)}")
    
    def _build_medicare_opps_benchmarks(self, ref_data: Dict[str, Any]):
        """Build comprehensive Medicare OPPS benchmark table"""
        logger.info("\n" + "-" * 50)
        logger.info("Building Medicare OPPS Benchmarks")
        logger.info("-" * 50)
        
        # Get all HCPCS codes from OPPS data
        opps_codes = ref_data['df_b']['hcpcs'].unique()
        logger.info(f"Found {len(opps_codes):,} unique HCPCS codes for OPPS")
        
        # Get all states from wage index
        states = self._extract_states_from_wage_index(ref_data)
        logger.info(f"Found {len(states)} states from wage index")
        
        # Create comprehensive combinations
        combinations = []
        for state in states:
            for code in opps_codes:
                combinations.append({
                    'state': state,
                    'year_month': f"{self.year}-01",
                    'code_type': 'HCPCS',
                    'code': code
                })
        
        combinations_df = pd.DataFrame(combinations)
        logger.info(f"Created {len(combinations_df):,} state/code combinations")
        
        # Calculate OPPS benchmarks
        benchmarks = self._calculate_opps_benchmarks(combinations_df, ref_data)
        
        # Add metadata
        benchmarks['benchmark_type'] = 'opps'
        benchmarks['created_date'] = datetime.now()
        benchmarks['data_year'] = self.year
        
        # Save to parquet
        output_path = self.output_dir / "bench_medicare_opps.parquet"
        benchmarks.to_parquet(output_path, index=False)
        logger.info(f"✓ Saved Medicare OPPS benchmarks to {output_path}")
        logger.info(f"  Rows: {len(benchmarks):,}, Columns: {len(benchmarks.columns)}")
    
    def _build_medicare_asc_benchmarks(self, ref_data: Dict[str, Any]):
        """Build comprehensive Medicare ASC benchmark table"""
        logger.info("\n" + "-" * 50)
        logger.info("Building Medicare ASC Benchmarks")
        logger.info("-" * 50)
        
        # Get all HCPCS codes from ASC data
        asc_codes = ref_data['df_aa']['hcpcs'].unique()
        logger.info(f"Found {len(asc_codes):,} unique HCPCS codes for ASC")
        
        # Get all states from wage index
        states = self._extract_states_from_wage_index(ref_data)
        
        # Create comprehensive combinations
        combinations = []
        for state in states:
            for code in asc_codes:
                combinations.append({
                    'state': state,
                    'year_month': f"{self.year}-01",
                    'code_type': 'CPT',
                    'code': code
                })
        
        combinations_df = pd.DataFrame(combinations)
        logger.info(f"Created {len(combinations_df):,} state/code combinations")
        
        # Calculate ASC benchmarks
        benchmarks = self._calculate_asc_benchmarks(combinations_df, ref_data)
        
        # Add metadata
        benchmarks['benchmark_type'] = 'asc'
        benchmarks['created_date'] = datetime.now()
        benchmarks['data_year'] = self.year
        
        # Save to parquet
        output_path = self.output_dir / "bench_medicare_asc.parquet"
        benchmarks.to_parquet(output_path, index=False)
        logger.info(f"✓ Saved Medicare ASC benchmarks to {output_path}")
        logger.info(f"  Rows: {len(benchmarks):,}, Columns: {len(benchmarks.columns)}")
    
    def _build_medicare_comprehensive_benchmarks(self):
        """Build a single comprehensive Medicare benchmark table"""
        logger.info("\n" + "-" * 50)
        logger.info("Building Comprehensive Medicare Benchmark Table")
        logger.info("-" * 50)
        
        # Load individual benchmark tables
        prof_path = self.output_dir / "bench_medicare_professional.parquet"
        opps_path = self.output_dir / "bench_medicare_opps.parquet"
        asc_path = self.output_dir / "bench_medicare_asc.parquet"
        
        if not all(p.exists() for p in [prof_path, opps_path, asc_path]):
            logger.error("Individual benchmark tables not found. Run build_all_benchmarks() first.")
            return
        
        prof_bench = pd.read_parquet(prof_path)
        opps_bench = pd.read_parquet(opps_path)
        asc_bench = pd.read_parquet(asc_path)
        
        # Combine all benchmarks
        comprehensive = pd.concat([prof_bench, opps_bench, asc_bench], ignore_index=True)
        
        # Add comprehensive metadata
        comprehensive['table_version'] = '1.0'
        comprehensive['last_updated'] = datetime.now()
        
        # Save comprehensive table
        output_path = self.output_dir / "bench_medicare_comprehensive.parquet"
        comprehensive.to_parquet(output_path, index=False)
        logger.info(f"✓ Saved comprehensive Medicare benchmarks to {output_path}")
        logger.info(f"  Total rows: {len(comprehensive):,}, Columns: {len(comprehensive.columns)}")
        logger.info(f"  Breakdown by type:")
        logger.info(f"    Professional: {len(prof_bench):,} rows")
        logger.info(f"    OPPS: {len(opps_bench):,} rows")
        logger.info(f"    ASC: {len(asc_bench):,} rows")
    
    def _extract_states_from_locality_data(self, ref_data: Dict[str, Any]) -> List[str]:
        """Extract unique states from locality data"""
        # Extract state from state_name column (format like "ALABAMA ")
        states = ref_data['meta']['state_name'].str.strip().str.extract(r'^([A-Z]{2})')[0].dropna().unique()
        return sorted(states.tolist())
    
    def _extract_states_from_wage_index(self, ref_data: Dict[str, Any]) -> List[str]:
        """Extract unique states from wage index data"""
        states = ref_data['df_wi']['state'].str.upper().unique()
        return sorted([s for s in states if pd.notna(s)])
    
    def _calculate_professional_benchmarks(self, combinations_df: pd.DataFrame, ref_data: Dict[str, Any]) -> pd.DataFrame:
        """Calculate professional benchmarks for all state/code combinations"""
        logger.info("Calculating professional benchmarks...")
        
        # Get reference tables
        mloc = ref_data['mloc']
        meta = ref_data['meta']
        gpci = ref_data['gpci']
        rvu = ref_data['rvu']
        cf = ref_data['cf']
        
        # Build state-averaged GPCI values
        state_gpci_avg = self._calculate_state_gpci_averages(gpci, mloc, meta)
        logger.info(f"Calculated state-averaged GPCI for {len(state_gpci_avg)} states")
        
        # Merge combinations with RVU data
        benchmarks = combinations_df.merge(rvu, left_on='code', right_on='procedure_code', how='left')
        
        # Merge with state-averaged GPCI
        benchmarks = benchmarks.merge(state_gpci_avg, on='state', how='left')
        
        # Calculate national professional rate (using national average GPCI)
        national_gpci = gpci[['work_gpci', 'pe_gpci', 'mp_gpci']].mean()
        benchmarks['medicare_prof_national'] = self._calc_professional_rate(
            benchmarks, national_gpci, cf
        )
        
        # Calculate state-averaged professional rate
        benchmarks['medicare_prof_stateavg'] = self._calc_professional_rate(
            benchmarks, 
            benchmarks[['work_gpci', 'pe_gpci', 'mp_gpci']], 
            cf
        )
        
        # Add RVU components for reference
        benchmarks['work_rvu'] = benchmarks['work_rvu']
        benchmarks['practice_expense_rvu'] = benchmarks['practice_expense_rvu']
        benchmarks['malpractice_rvu'] = benchmarks['malpractice_rvu']
        benchmarks['conversion_factor'] = cf['conversion_factor'].iloc[0]
        
        # Clean up
        benchmarks = benchmarks.drop(columns=['procedure_code'], errors='ignore')
        
        return benchmarks
    
    def _calculate_opps_benchmarks(self, combinations_df: pd.DataFrame, ref_data: Dict[str, Any]) -> pd.DataFrame:
        """Calculate OPPS benchmarks for all state/code combinations"""
        logger.info("Calculating OPPS benchmarks...")
        
        # Get reference tables
        df_b = ref_data['df_b']
        df_wi = ref_data['df_wi']
        
        # Build state-averaged wage index
        state_wi_avg = self._calculate_state_wage_index_averages(df_wi)
        logger.info(f"Calculated state-averaged wage index for {len(state_wi_avg)} states")
        
        # Normalize HCPCS codes
        df_b['hcpcs_norm'] = df_b['hcpcs'].astype(str).str.strip().str.upper()
        
        # Build OPPS lookup table
        opps_lut = df_b.rename(columns={
            'rel_wt': 'opps_weight',
            'si': 'opps_si',
            'short_desc': 'opps_short_desc'
        })[['hcpcs_norm', 'opps_weight', 'opps_si', 'opps_short_desc']].drop_duplicates('hcpcs_norm')
        
        # Merge combinations with OPPS data
        benchmarks = combinations_df.copy()
        benchmarks['code_norm'] = benchmarks['code'].astype(str).str.strip().str.upper()
        benchmarks = benchmarks.merge(opps_lut, left_on='code_norm', right_on='hcpcs_norm', how='left')
        
        # Merge with state wage index
        benchmarks = benchmarks.merge(state_wi_avg, on='state', how='left')
        
        # Calculate national OPPS rate
        benchmarks['medicare_opps_national'] = pd.to_numeric(benchmarks['opps_weight'], errors='coerce') * self.OPPS_CF
        
        # Calculate state-averaged OPPS rate
        benchmarks['opps_adj_factor_stateavg'] = (
            self.OPPS_LABOR_SHARE * benchmarks['state_wage_index_avg'] + 
            (1 - self.OPPS_LABOR_SHARE)
        )
        benchmarks['medicare_opps_stateavg'] = (
            benchmarks['medicare_opps_national'] * benchmarks['opps_adj_factor_stateavg']
        )
        
        # Clean up
        benchmarks = benchmarks.drop(columns=['code_norm', 'hcpcs_norm'], errors='ignore')
        
        return benchmarks
    
    def _calculate_asc_benchmarks(self, combinations_df: pd.DataFrame, ref_data: Dict[str, Any]) -> pd.DataFrame:
        """Calculate ASC benchmarks for all state/code combinations"""
        logger.info("Calculating ASC benchmarks...")
        
        # Get reference tables
        df_aa = ref_data['df_aa']
        df_wi = ref_data['df_wi']
        
        # Build state-averaged wage index
        state_wi_avg = self._calculate_state_wage_index_averages(df_wi)
        
        # Normalize HCPCS codes
        df_aa['hcpcs_norm'] = df_aa['hcpcs'].astype(str).str.strip().str.upper()
        
        # Build ASC lookup table
        asc_lut = df_aa.rename(columns={
            'asc_ind': 'asc_pi',
            'nat_rate': 'asc_nat_rate',
            'short_desc': 'asc_short_desc'
        })[['hcpcs_norm', 'asc_pi', 'asc_nat_rate', 'asc_short_desc']].drop_duplicates('hcpcs_norm')
        
        # Merge combinations with ASC data
        benchmarks = combinations_df.copy()
        benchmarks['code_norm'] = benchmarks['code'].astype(str).str.strip().str.upper()
        benchmarks = benchmarks.merge(asc_lut, left_on='code_norm', right_on='hcpcs_norm', how='left')
        
        # Merge with state wage index
        benchmarks = benchmarks.merge(state_wi_avg, on='state', how='left')
        
        # Calculate national ASC rate
        benchmarks['medicare_asc_national'] = pd.to_numeric(benchmarks['asc_nat_rate'], errors='coerce')
        
        # Calculate state-averaged ASC rate
        benchmarks['asc_adj_factor_stateavg'] = (
            self.ASC_LABOR_SHARE * benchmarks['state_wage_index_avg'] + 
            (1 - self.ASC_LABOR_SHARE)
        )
        benchmarks['medicare_asc_stateavg'] = (
            benchmarks['medicare_asc_national'] * benchmarks['asc_adj_factor_stateavg']
        )
        
        # Clean up
        benchmarks = benchmarks.drop(columns=['code_norm', 'hcpcs_norm'], errors='ignore')
        
        return benchmarks
    
    def _calculate_state_gpci_averages(self, gpci: pd.DataFrame, mloc: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
        """Calculate state-averaged GPCI values"""
        # Join GPCI with locality metadata to get states
        gpci_with_state = gpci.merge(
            meta[['state_name', 'locality_code']], 
            left_on=['locality_code'],
            right_on=['locality_code'],
            how='left'
        )
        
        # Extract state from state_name (format like "ALABAMA ")
        gpci_with_state['state'] = gpci_with_state['state_name'].str.strip().str.extract(r'^([A-Z]{2})')
        
        # Calculate state averages for each GPCI component
        state_avg_cols = ['work_gpci', 'pe_gpci', 'mp_gpci']
        state_gpci_avg = gpci_with_state.groupby('state')[state_avg_cols].mean().reset_index()
        
        return state_gpci_avg
    
    def _calculate_state_wage_index_averages(self, df_wi: pd.DataFrame) -> pd.DataFrame:
        """Calculate state-averaged wage index values"""
        # Build state-averaged wage index
        df_wi2 = df_wi.copy()
        df_wi2['cbsa_5'] = df_wi2['cbsa'].astype(str).str.extract(r'(\d{5})')[0]
        df_wi2['state_up'] = df_wi2['state'].astype(str).str.upper()
        df_wi2 = df_wi2[pd.notna(df_wi2['cbsa_5'])]
        
        # Keep non-rural CBSA rows only (if the column exists)
        if 'is_state_rural' in df_wi2.columns:
            df_wi2 = df_wi2[(df_wi2['is_state_rural'].isna()) | (df_wi2['is_state_rural'] == 0)]
        
        # Find wage index column
        wi_col = None
        for candidate in ['wi_pre', 'wage_index', 'wi', 'wageindex']:
            if candidate in df_wi2.columns:
                wi_col = candidate
                break
        
        if wi_col is not None:
            state_wi_avg = df_wi2.groupby('state_up')[wi_col].mean(numeric_only=True).reset_index()
            state_wi_avg.columns = ['state', 'state_wage_index_avg']
        else:
            # Fallback if no wage index column found
            state_wi_avg = pd.DataFrame({
                'state': df_wi2['state_up'].unique(),
                'state_wage_index_avg': 1.0  # Default to 1.0
            })
        
        return state_wi_avg
    
    def _calc_professional_rate(self, df: pd.DataFrame, gpci_values: pd.Series, cf: pd.Series) -> pd.Series:
        """Calculate professional rate using GPCI values and conversion factor"""
        try:
            work_rvu = pd.to_numeric(df['work_rvu'], errors='coerce').fillna(0)
            pe_rvu = pd.to_numeric(df['practice_expense_rvu'], errors='coerce').fillna(0)
            mp_rvu = pd.to_numeric(df['malpractice_rvu'], errors='coerce').fillna(0)
            
            if isinstance(gpci_values, pd.Series):
                # National average GPCI
                work_gpci = gpci_values.get('work_gpci', 1.0)
                pe_gpci = gpci_values.get('pe_gpci', 1.0)
                mp_gpci = gpci_values.get('mp_gpci', 1.0)
            else:
                # State-specific GPCI
                work_gpci = pd.to_numeric(gpci_values['work_gpci'], errors='coerce').fillna(1.0)
                pe_gpci = pd.to_numeric(gpci_values['pe_gpci'], errors='coerce').fillna(1.0)
                mp_gpci = pd.to_numeric(gpci_values['mp_gpci'], errors='coerce').fillna(1.0)
            
            conversion_factor = float(cf['conversion_factor'])
            
            return (work_rvu * work_gpci + pe_rvu * pe_gpci + mp_rvu * mp_gpci) * conversion_factor
            
        except Exception as e:
            logger.warning(f"Error calculating professional rate: {e}")
            return pd.Series([np.nan] * len(df), index=df.index)

def main():
    """Main function to run the Medicare benchmark builder"""
    parser = argparse.ArgumentParser(description='Build Medicare benchmark dimension tables')
    parser.add_argument('--prof-db-path', default='data/raw/compensation_rates.db', 
                       help='Path to professional rates SQLite database')
    parser.add_argument('--facility-db-path', default='data/raw/medicare_test.db', 
                       help='Path to facility rates SQLite database')
    parser.add_argument('--year', type=int, default=2025, 
                       help='Year for Medicare data')
    parser.add_argument('--output-dir', default='prod_etl/core/data/silver/benchmarks',
                       help='Output directory for benchmark tables')
    
    args = parser.parse_args()
    
    try:
        # Initialize builder
        builder = MedicareBenchmarkBuilder(
            prof_db_path=args.prof_db_path,
            facility_db_path=args.facility_db_path,
            year=args.year
        )
        
        # Build all benchmarks
        builder.build_all_benchmarks()
        
        logger.info("\n🎉 Medicare benchmark tables built successfully!")
        logger.info(f"📁 Output directory: {builder.output_dir}")
        logger.info("\nGenerated files:")
        for file_path in builder.output_dir.glob("*.parquet"):
            logger.info(f"  📄 {file_path.name}")
        
    except Exception as e:
        logger.error(f"❌ Error building Medicare benchmarks: {e}")
        raise

if __name__ == "__main__":
    main()
