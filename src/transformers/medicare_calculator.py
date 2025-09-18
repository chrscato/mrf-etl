"""
Medicare rate calculations for professional and facility services
"""
import pandas as pd
import logging
import sqlite3
import numpy as np
from typing import Iterator, Dict, Any
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class MedicareCalculator:
    """Calculate Medicare professional and facility rates"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
        self.prof_ref_tables = None
        self.facility_ref_tables = None
    
    def calculate_medicare_professional_rates(self, chunks: Iterator[pd.DataFrame], 
                                            db_path: str = "data/raw/medicare_test.db", 
                                            year: int = 2025) -> Iterator[pd.DataFrame]:
        """Calculate Medicare professional rates using SQLite DB (chunked processing)"""
        logger.info("Calculating Medicare professional rates...")
        
        # Load reference tables once for all chunks
        logger.info("Loading Medicare professional reference tables...")
        try:
            self.prof_ref_tables = self._load_medicare_prof_reference_tables(db_path, year)
        except Exception as e:
            logger.error(f"Failed to load Medicare reference tables: {e}")
            # Return chunks unchanged if reference tables fail to load
            return chunks
        
        def calc_medicare_prof_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns or 'postal_code' not in chunk.columns:
                logger.warning("Missing required columns for Medicare professional calculation")
                return chunk
            
            try:
                return self._attach_medicare_prof_chunked(chunk, self.prof_ref_tables)
            except Exception as e:
                logger.error(f"Medicare professional calculation failed: {e}")
                return chunk
        
        return self.chunk_processor.filter_chunks(chunks, calc_medicare_prof_chunk)
    
    def calculate_medicare_facility_rates(self, chunks: Iterator[pd.DataFrame], 
                                        db_path: str = "data/raw/medicare_test.db") -> Iterator[pd.DataFrame]:
        """Calculate Medicare facility rates (OPPS/ASC) (chunked processing)"""
        logger.info("Calculating Medicare facility rates...")
        
        # Load reference tables once for all chunks
        logger.info("Loading Medicare facility reference tables...")
        try:
            self.facility_ref_tables = self._load_medicare_facility_reference_tables(db_path)
        except Exception as e:
            logger.error(f"Failed to load Medicare facility reference tables: {e}")
            # Return chunks unchanged if reference tables fail to load
            return chunks
        
        def calc_medicare_facility_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            try:
                return self._attach_medicare_facility_rates_chunked(chunk, self.facility_ref_tables)
            except Exception as e:
                logger.error(f"Medicare facility calculation failed: {e}")
                return chunk
        
        return self.chunk_processor.filter_chunks(chunks, calc_medicare_facility_chunk)
    
    def _load_medicare_prof_reference_tables(self, db_path: str, year: int) -> Dict[str, Any]:
        """Load Medicare professional reference tables once for all chunks"""
        try:
            con = sqlite3.connect(db_path)
            # Load tables as DataFrames
            mloc = pd.read_sql_query("SELECT * FROM medicare_locality_map", con)
            meta = pd.read_sql_query("SELECT * FROM medicare_locality_meta", con)
            gpci = pd.read_sql_query("SELECT * FROM cms_gpci WHERE year = ?", con, params=(year,))
            rvu  = pd.read_sql_query("SELECT * FROM cms_rvu WHERE year = ? AND (modifier IS NULL OR modifier = '')", con, params=(year,))
            cf   = pd.read_sql_query("SELECT * FROM cms_conversion_factor WHERE year = ?", con, params=(year,))
            con.close()
            
            # Prepare reference tables
            mloc["zip_code"] = mloc["zip_code"].astype(str).str.zfill(5)
            meta["mac_code"] = meta["mac_code"].astype(str)
            meta["locality_code"] = meta["locality_code"].astype(str)
            gpci["locality_name"] = gpci["locality_name"].astype(str).str.strip()
            gpci["locality_code"] = gpci["locality_code"].astype(str)
            rvu["procedure_code"] = rvu["procedure_code"].astype(str).str.strip()
            cf = cf.iloc[0]  # Only one row for the year
            
            return {
                'mloc': mloc,
                'meta': meta,
                'gpci': gpci,
                'rvu': rvu,
                'cf': cf
            }
        except Exception as e:
            logger.error(f"Failed to load Medicare professional reference tables: {e}")
            raise
    
    def _load_medicare_facility_reference_tables(self, db_path: str) -> Dict[str, Any]:
        """Load Medicare facility reference tables once for all chunks"""
        try:
            conn = sqlite3.connect(db_path)
            
            # Load lookup tables from DB
            df_aa = pd.read_sql("SELECT * FROM asc_addendum_aa", conn)
            df_b = pd.read_sql("SELECT * FROM opps_addendum_b", conn)
            df_wi = pd.read_sql("SELECT * FROM cbsa_wage_index", conn)
            df_zip_cbsa = pd.read_sql("SELECT * FROM zip_cbsa", conn)
            
            conn.close()
            
            return {
                'df_aa': df_aa,
                'df_b': df_b,
                'df_wi': df_wi,
                'df_zip_cbsa': df_zip_cbsa
            }
        except Exception as e:
            logger.error(f"Failed to load facility rate reference tables: {e}")
            raise
    
    def _attach_medicare_prof_chunked(self, df: pd.DataFrame, ref_tables: Dict[str, Any]) -> pd.DataFrame:
        """Enrich DataFrame with Medicare professional rates using pre-loaded reference tables"""
        # Check for required columns
        required_cols = {"billing_code", "postal_code"}
        missing = required_cols - set(df.columns)
        if missing:
            logger.warning(f"Missing required columns for Medicare calculation: {missing}")
            return df

        df = df.copy()

        # Normalize zip and code
        def norm_zip(z):
            s = str(z)
            digits = "".join(ch for ch in s if ch.isdigit())
            return digits[:5].zfill(5) if digits else None

        df["_zip5"] = df["postal_code"].apply(norm_zip)
        df["_code"] = df["billing_code"].astype(str).str.strip()

        # Only unique, non-null (zip, code) pairs are looked up
        pairs = (
            df.loc[df["_zip5"].notna() & df["_code"].notna(), ["_zip5", "_code"]]
              .drop_duplicates()
        )

        if pairs.empty:
            df["medicare_prof"] = pd.NA
            return df.drop(columns=["_zip5", "_code"])

        # Use pre-loaded reference tables
        mloc = ref_tables['mloc']
        meta = ref_tables['meta']
        gpci = ref_tables['gpci']
        rvu = ref_tables['rvu']
        cf = ref_tables['cf']

        # Merge pairs with mloc to get carrier_code and locality_code
        pairs = pairs.rename(columns={"_zip5": "zip_code", "_code": "procedure_code"})
        pairs = pairs.merge(mloc[["zip_code", "carrier_code", "locality_code"]], on="zip_code", how="left")

        # Merge with meta to get fee_schedule_area
        pairs = pairs.merge(meta[["mac_code", "locality_code", "fee_schedule_area"]], 
                           left_on=["carrier_code", "locality_code"], 
                           right_on=["mac_code", "locality_code"], how="left")

        # Merge with gpci to get GPCI values
        pairs = pairs.merge(gpci, left_on=["fee_schedule_area", "locality_code"], 
                           right_on=["locality_name", "locality_code"], how="left", suffixes=('', '_gpci'))

        # Merge with rvu to get RVU values
        pairs = pairs.merge(rvu, on="procedure_code", how="left", suffixes=('', '_rvu'))

        # Calculate medicare_prof
        def calc_prof(row):
            try:
                work_rvu = float(row.get("work_rvu", 0) or 0)
                pe_rvu = float(row.get("practice_expense_rvu", 0) or 0)
                mp_rvu = float(row.get("malpractice_rvu", 0) or 0)
                work_gpci = float(row.get("work_gpci", 0) or 0)
                pe_gpci = float(row.get("pe_gpci", 0) or 0)
                mp_gpci = float(row.get("mp_gpci", 0) or 0)
                conversion_factor = float(cf["conversion_factor"])
                return (
                    (work_rvu * work_gpci + pe_rvu * pe_gpci + mp_rvu * mp_gpci) * conversion_factor
                )
            except Exception:
                return pd.NA

        pairs["medicare_prof"] = pairs.apply(calc_prof, axis=1)

        # Now merge back to the original df
        out = df.merge(
            pairs[["zip_code", "procedure_code", "medicare_prof"]],
            how="left",
            left_on=["_zip5", "_code"],
            right_on=["zip_code", "procedure_code"]
        )

        # Clean up
        out = out.drop(columns=["_zip5", "_code", "zip_code", "procedure_code"], errors="ignore")

        # Ensure numeric
        out["medicare_prof"] = pd.to_numeric(out["medicare_prof"], errors="coerce")

        return out
    
    def _attach_medicare_facility_rates_chunked(self, df: pd.DataFrame, ref_tables: Dict[str, Any]) -> pd.DataFrame:
        """Calculate Medicare facility rates (OPPS/ASC) using pre-loaded reference tables"""
        # Constants (CY2025)
        OPPS_CF = 89.169
        ASC_CF  = 54.895
        OPPS_LABOR_SHARE = 0.60
        ASC_LABOR_SHARE  = 0.50

        def _norm_code(s: pd.Series) -> pd.Series:
            return s.astype(str).str.strip().str.upper()

        # Use pre-loaded reference tables
        df_aa = ref_tables['df_aa']
        df_b = ref_tables['df_b']
        df_wi = ref_tables['df_wi']
        df_zip_cbsa = ref_tables['df_zip_cbsa']

        # Build state-averaged wage index
        df_wi2 = df_wi.copy()
        df_wi2['cbsa_5']   = df_wi2['cbsa'].astype(str).str.extract(r'(\d{5})')[0]
        df_wi2['state_up'] = df_wi2['state'].astype(str).str.upper()
        df_wi2 = df_wi2[pd.notna(df_wi2['cbsa_5'])]

        # Keep non-rural CBSA rows only (if the column exists)
        if 'is_state_rural' in df_wi2.columns:
            df_wi2 = df_wi2[(df_wi2['is_state_rural'].isna()) | (df_wi2['is_state_rural'] == 0)]

        # Simple mean wage index per state
        wi_col = None
        for candidate in ['wi_pre', 'wage_index', 'wi', 'wageindex']:
            if candidate in df_wi2.columns:
                wi_col = candidate
                break

        if wi_col is not None:
            state_wi_avg = df_wi2.groupby('state_up')[wi_col].mean(numeric_only=True)
        else:
            state_wi_avg = pd.Series(np.nan, index=df['state'].astype(str).str.upper().unique() if 'state' in df.columns else [])

        # Build OPPS/ASC lookup tables
        opps_lut = (df_b.assign(hcpcs_norm=_norm_code(df_b['hcpcs']))
                      .rename(columns={'rel_wt':'opps_weight',
                                       'si':'opps_si',
                                       'short_desc':'opps_short_desc'})
                      [['hcpcs_norm','opps_weight','opps_si','opps_short_desc']]
                      .drop_duplicates('hcpcs_norm'))

        asc_lut = (df_aa.assign(hcpcs_norm=_norm_code(df_aa['hcpcs']))
                     .rename(columns={
                         'asc_ind': 'asc_pi',
                         'nat_rate': 'asc_nat_rate',
                         'short_desc': 'asc_short_desc'
                     })
                     [['hcpcs_norm','asc_pi','asc_nat_rate','asc_short_desc']]
                     .drop_duplicates('hcpcs_norm'))

        # Enrich df with national rates
        if 'state' in df.columns:
            df['state_up'] = df['state'].astype(str).str.upper()
        else:
            df['state_up'] = pd.NA

        df['billing_code_norm'] = _norm_code(df['billing_code']) if 'billing_code' in df.columns else ''

        # Map state-avg wage index
        df['state_wage_index_avg'] = df['state_up'].map(state_wi_avg)

        # Merge once for OPPS & ASC
        df = df.merge(opps_lut, left_on='billing_code_norm', right_on='hcpcs_norm', how='left')
        df.drop(columns=['hcpcs_norm'], inplace=True)
        df = df.merge(asc_lut, left_on='billing_code_norm', right_on='hcpcs_norm', how='left', suffixes=('','_asc'))
        if 'hcpcs_norm' in df.columns:
            df.drop(columns=['hcpcs_norm'], inplace=True)

        # National rates (no geography)
        df['medicare_opps_mar_national'] = pd.to_numeric(df['opps_weight'], errors='coerce') * OPPS_CF

        # ASC national rate
        if 'asc_nat_rate' in df.columns:
            df['medicare_asc_mar_national']  = pd.to_numeric(df['asc_nat_rate'], errors='coerce')
        else:
            df['medicare_asc_mar_national'] = np.nan

        # State-avg "localized" rates
        df['opps_adj_factor_stateavg'] = (OPPS_LABOR_SHARE * df['state_wage_index_avg']
                                         + (1 - OPPS_LABOR_SHARE))
        df['asc_adj_factor_stateavg']  = (ASC_LABOR_SHARE  * df['state_wage_index_avg']
                                         + (1 - ASC_LABOR_SHARE))

        df['medicare_opps_mar_stateavg'] = df['medicare_opps_mar_national'] * df['opps_adj_factor_stateavg']
        df['medicare_asc_mar_stateavg']  = df['medicare_asc_mar_national']  * df['asc_adj_factor_stateavg']

        # Memory optimization
        for c in ['state_wage_index_avg','opps_weight',
                  'medicare_opps_mar_national','medicare_asc_mar_national',
                  'opps_adj_factor_stateavg','asc_adj_factor_stateavg',
                  'medicare_opps_mar_stateavg','medicare_asc_mar_stateavg']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce', downcast='float')

        return df
