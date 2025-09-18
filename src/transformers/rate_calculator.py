"""
Calculate Medicare and other reference rates
"""
import pandas as pd
import sqlite3
import logging
from typing import Iterator
from config.settings import FilePaths, DatabaseConfig
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class RateCalculator:
    """Calculate Medicare professional and facility rates"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
        self.year = DatabaseConfig.DEFAULT_YEAR
        
        # Load reference tables once
        self._load_reference_tables()
    
    def _load_reference_tables(self):
        """Load Medicare reference tables from database"""
        logger.info("Loading Medicare reference tables...")
        
        try:
            # Professional rates reference tables
            with sqlite3.connect(FilePaths.COMPENSATION_DB) as conn:
                self.medicare_locality_map = pd.read_sql_query("SELECT * FROM medicare_locality_map", conn)
                self.medicare_locality_meta = pd.read_sql_query("SELECT * FROM medicare_locality_meta", conn)
                self.cms_gpci = pd.read_sql_query("SELECT * FROM cms_gpci WHERE year = ?", conn, params=(self.year,))
                self.cms_rvu = pd.read_sql_query("SELECT * FROM cms_rvu WHERE year = ? AND (modifier IS NULL OR modifier = '')", conn, params=(self.year,))
                self.cms_conversion_factor = pd.read_sql_query("SELECT * FROM cms_conversion_factor WHERE year = ?", conn, params=(self.year,))
            
            # Facility rates reference tables  
            with sqlite3.connect(FilePaths.MEDICARE_DB) as conn:
                self.asc_addendum_aa = pd.read_sql("SELECT * FROM asc_addendum_aa", conn)
                self.opps_addendum_b = pd.read_sql("SELECT * FROM opps_addendum_b", conn)
                self.cbsa_wage_index = pd.read_sql("SELECT * FROM cbsa_wage_index", conn)
                self.zip_cbsa = pd.read_sql("SELECT * FROM zip_cbsa", conn)
            
            # Prepare reference data
            self._prepare_reference_data()
            
            logger.info("Successfully loaded all reference tables")
            
        except Exception as e:
            logger.error(f"Failed to load reference tables: {e}")
            raise
    
    def _prepare_reference_data(self):
        """Prepare and normalize reference data"""
        # Normalize locality map
        self.medicare_locality_map["zip_code"] = self.medicare_locality_map["zip_code"].astype(str).str.zfill(5)
        
        # Prepare meta data
        self.medicare_locality_meta["mac_code"] = self.medicare_locality_meta["mac_code"].astype(str)
        self.medicare_locality_meta["locality_code"] = self.medicare_locality_meta["locality_code"].astype(str)
        
        # Prepare GPCI data
        self.cms_gpci["locality_name"] = self.cms_gpci["locality_name"].astype(str).str.strip()
        self.cms_gpci["locality_code"] = self.cms_gpci["locality_code"].astype(str)
        
        # Prepare RVU data
        self.cms_rvu["procedure_code"] = self.cms_rvu["procedure_code"].astype(str).str.strip()
        
        # Get conversion factor (single value)
        self.conversion_factor = self.cms_conversion_factor.iloc[0]["conversion_factor"]
        
        # Prepare facility lookup tables
        self._prepare_facility_lookups()
    
    def _prepare_facility_lookups(self):
        """Prepare OPPS and ASC lookup tables"""
        # OPPS lookup
        self.opps_lut = (
            self.opps_addendum_b.assign(hcpcs_norm=self.opps_addendum_b['hcpcs'].astype(str).str.strip().str.upper())
            .rename(columns={'rel_wt': 'opps_weight', 'si': 'opps_si', 'short_desc': 'opps_short_desc'})
            [['hcpcs_norm', 'opps_weight', 'opps_si', 'opps_short_desc']]
            .drop_duplicates('hcpcs_norm')
        )
        
        # ASC lookup
        self.asc_lut = (
            self.asc_addendum_aa.assign(hcpcs_norm=self.asc_addendum_aa['hcpcs'].astype(str).str.strip().str.upper())
            .rename(columns={'asc_ind': 'asc_pi', 'nat_rate': 'asc_nat_rate', 'short_desc': 'asc_short_desc'})
            [['hcpcs_norm', 'asc_pi', 'asc_nat_rate', 'asc_short_desc']]
            .drop_duplicates('hcpcs_norm')
        )
        
        # State-averaged wage index
        self._calculate_state_wage_index()
    
    def _calculate_state_wage_index(self):
        """Calculate state-averaged wage index"""
        wi_df = self.cbsa_wage_index.copy()
        wi_df['cbsa_5'] = wi_df['cbsa'].astype(str).str.extract(r'(\d{5})')[0]
        wi_df['state_up'] = wi_df['state'].astype(str).str.upper()
        wi_df = wi_df[pd.notna(wi_df['cbsa_5'])]
        
        # Filter non-rural CBSA rows
        if 'is_state_rural' in wi_df.columns:
            wi_df = wi_df[(wi_df['is_state_rural'].isna()) | (wi_df['is_state_rural'] == 0)]
        
        # Find wage index column
        wi_col = None
        for candidate in ['wi_pre', 'wage_index', 'wi', 'wageindex']:
            if candidate in wi_df.columns:
                wi_col = candidate
                break
        
        if wi_col:
            self.state_wi_avg = wi_df.groupby('state_up')[wi_col].mean(numeric_only=True)
        else:
            logger.warning("No wage index column found")
            self.state_wi_avg = pd.Series(dtype=float)
    
    def calculate_medicare_professional(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Calculate Medicare professional rates for chunks"""
        logger.info("Calculating Medicare professional rates...")
        
        def calc_prof_rates(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns or 'postal_code' not in chunk.columns:
                chunk['medicare_prof'] = pd.NA
                return chunk
            
            # Normalize inputs
            chunk = chunk.copy()
            chunk["_zip5"] = chunk["postal_code"].apply(self._normalize_zip)
            chunk["_code"] = chunk["billing_code"].astype(str).str.strip()
            
            # Get unique pairs for lookup
            pairs = chunk.loc[
                chunk["_zip5"].notna() & chunk["_code"].notna(), 
                ["_zip5", "_code"]
            ].drop_duplicates()
            
            if pairs.empty:
                chunk["medicare_prof"] = pd.NA
                return chunk.drop(columns=["_zip5", "_code"])
            
            # Perform Medicare calculation
            pairs = pairs.rename(columns={"_zip5": "zip_code", "_code": "procedure_code"})
            pairs = self._calculate_medicare_rates(pairs)
            
            # Merge back to chunk
            chunk = chunk.merge(
                pairs[["zip_code", "procedure_code", "medicare_prof"]],
                left_on=["_zip5", "_code"],
                right_on=["zip_code", "procedure_code"],
                how="left"
            )
            
            # Clean up
            chunk = chunk.drop(columns=["_zip5", "_code", "zip_code", "procedure_code"], errors="ignore")
            chunk["medicare_prof"] = pd.to_numeric(chunk["medicare_prof"], errors="coerce")
            
            return chunk
        
        result = self.chunk_processor.filter_chunks(chunks, calc_prof_rates)
        logger.info(f"Rate calculator professional result type: {type(result)}")
        if result is None:
            logger.error("Rate calculator professional returned None - returning empty iterator")
            return iter([])
        return result
    
    def _normalize_zip(self, zip_code) -> str:
        """Normalize ZIP code to 5 digits"""
        s = str(zip_code)
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits[:5].zfill(5) if digits else None
    
    def _calculate_medicare_rates(self, pairs: pd.DataFrame) -> pd.DataFrame:
        """Calculate Medicare rates for unique zip/code pairs"""
        # Merge with locality map
        pairs = pairs.merge(
            self.medicare_locality_map[["zip_code", "carrier_code", "locality_code"]], 
            on="zip_code", how="left"
        )
        
        # Merge with meta to get fee schedule area
        pairs = pairs.merge(
            self.medicare_locality_meta[["mac_code", "locality_code", "fee_schedule_area"]], 
            left_on=["carrier_code", "locality_code"], 
            right_on=["mac_code", "locality_code"], 
            how="left"
        )
        
        # Merge with GPCI
        pairs = pairs.merge(
            self.cms_gpci, 
            left_on=["fee_schedule_area", "locality_code"], 
            right_on=["locality_name", "locality_code"], 
            how="left", 
            suffixes=('', '_gpci')
        )
        
        # Merge with RVU
        pairs = pairs.merge(self.cms_rvu, on="procedure_code", how="left")
        
        # Calculate Medicare professional rate
        pairs["medicare_prof"] = pairs.apply(self._calc_professional_rate, axis=1)
        
        return pairs
    
    def _calc_professional_rate(self, row) -> float:
        """Calculate professional rate for a single row"""
        try:
            work_rvu = float(row.get("work_rvu", 0) or 0)
            pe_rvu = float(row.get("practice_expense_rvu", 0) or 0)
            mp_rvu = float(row.get("malpractice_rvu", 0) or 0)
            work_gpci = float(row.get("work_gpci", 0) or 0)
            pe_gpci = float(row.get("pe_gpci", 0) or 0)
            mp_gpci = float(row.get("mp_gpci", 0) or 0)
            
            return (work_rvu * work_gpci + pe_rvu * pe_gpci + mp_rvu * mp_gpci) * self.conversion_factor
            
        except Exception:
            return pd.NA
    
    def calculate_medicare_facility(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Calculate Medicare OPPS and ASC facility rates"""
        logger.info("Calculating Medicare facility rates...")
        
        def calc_facility_rates(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns:
                return chunk
            
            chunk = chunk.copy()
            chunk['billing_code_norm'] = chunk['billing_code'].astype(str).str.strip().str.upper()
            
            # Add state wage index if state column exists
            if 'state' in chunk.columns:
                chunk['state_up'] = chunk['state'].astype(str).str.upper()
                chunk['state_wage_index_avg'] = chunk['state_up'].map(self.state_wi_avg)
            else:
                chunk['state_wage_index_avg'] = pd.NA
            
            # Merge OPPS rates
            chunk = chunk.merge(self.opps_lut, left_on='billing_code_norm', right_on='hcpcs_norm', how='left')
            chunk = chunk.drop(columns=['hcpcs_norm'], errors='ignore')
            
            # Merge ASC rates
            chunk = chunk.merge(self.asc_lut, left_on='billing_code_norm', right_on='hcpcs_norm', how='left', suffixes=('', '_asc'))
            chunk = chunk.drop(columns=['hcpcs_norm'], errors='ignore')
            
            # Calculate national rates
            chunk['medicare_opps_mar_national'] = pd.to_numeric(chunk['opps_weight'], errors='coerce') * DatabaseConfig.OPPS_CF
            chunk['medicare_asc_mar_national'] = pd.to_numeric(chunk['asc_nat_rate'], errors='coerce')
            
            # Calculate state-averaged localized rates
            chunk['opps_adj_factor_stateavg'] = (
                DatabaseConfig.OPPS_LABOR_SHARE * chunk['state_wage_index_avg'] + 
                (1 - DatabaseConfig.OPPS_LABOR_SHARE)
            )
            chunk['asc_adj_factor_stateavg'] = (
                DatabaseConfig.ASC_LABOR_SHARE * chunk['state_wage_index_avg'] + 
                (1 - DatabaseConfig.ASC_LABOR_SHARE)
            )
            
            chunk['medicare_opps_mar_stateavg'] = chunk['medicare_opps_mar_national'] * chunk['opps_adj_factor_stateavg']
            chunk['medicare_asc_mar_stateavg'] = chunk['medicare_asc_mar_national'] * chunk['asc_adj_factor_stateavg']
            
            # Convert to numeric and downcast for memory efficiency
            numeric_cols = [
                'state_wage_index_avg', 'opps_weight', 'medicare_opps_mar_national', 
                'medicare_asc_mar_national', 'opps_adj_factor_stateavg', 
                'asc_adj_factor_stateavg', 'medicare_opps_mar_stateavg', 'medicare_asc_mar_stateavg'
            ]
            
            for col in numeric_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce', downcast='float')
            
            return chunk
        
        result = self.chunk_processor.filter_chunks(chunks, calc_facility_rates)
        logger.info(f"Rate calculator facility result type: {type(result)}")
        if result is None:
            logger.error("Rate calculator facility returned None - returning empty iterator")
            return iter([])
        return result