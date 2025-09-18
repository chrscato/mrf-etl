"""
Main ETL Pipeline for Healthcare Rate Processing
"""
import logging
import sys
from pathlib import Path
from typing import Iterator
import pandas as pd
from tqdm import tqdm

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from config.settings import FilePaths, CHUNK_SIZE
from extractors.parquet_extractor import ParquetExtractor
from extractors.excel_extractor import ExcelExtractor
from transformers.data_cleaner import DataCleaner
from transformers.rate_calculator import RateCalculator
from transformers.categorizer import ProcedureCategorizer
from transformers.benchmark_joiner import BenchmarkJoiner
# from transformers.taxonomy_filter import TaxonomyFilter  # DISABLED - not using taxonomy filtering
from loaders.parquet_loader import ParquetLoader
from utils.chunk_processor import ChunkProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class HealthcareRateETL:
    """Main ETL pipeline orchestrator"""
    
    def __init__(self, chunk_size: int = CHUNK_SIZE):
        self.chunk_size = chunk_size
        
        # Initialize components
        self.parquet_extractor = ParquetExtractor(chunk_size)
        self.excel_extractor = ExcelExtractor()
        self.data_cleaner = DataCleaner(chunk_size)
        self.rate_calculator = RateCalculator(chunk_size)
        self.categorizer = ProcedureCategorizer(chunk_size)
        self.benchmark_joiner = BenchmarkJoiner(chunk_size)
        # self.taxonomy_filter = TaxonomyFilter(chunk_size)  # DISABLED - not using taxonomy filtering
        self.loader = ParquetLoader()
        
        logger.info(f"Initialized ETL pipeline with chunk size: {chunk_size}")
    
    def run_full_pipeline(self, skip_nppes: bool = False) -> None:
        """Execute the complete ETL pipeline in the correct order"""
        logger.info("Starting full ETL pipeline...")
        
        # Define pipeline phases for progress tracking
        pipeline_phases = [
            "Extraction & Merge",
            "Billing Codes Filter", 
            "Column Rename",
            "Column Drop",
            "GA WC Initialize",
            "NPPES Enrichment",
            "State Filtering",
            "Geocoding",
            "NPPES Cleanup",
            "GA WC Merge",
            "Procedure Categorization",
            "Benchmark Joining",
            "Final Loading"
        ]
        
        if skip_nppes:
            pipeline_phases.remove("NPPES Enrichment")
            pipeline_phases.remove("NPPES Cleanup")
        
        # Create progress bar for overall pipeline
        phase_progress = tqdm(
            pipeline_phases, 
            desc="ETL Pipeline", 
            unit="phase",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} phases [{elapsed}<{remaining}]"
        )
        
        try:
            # Step 1: Extract and merge base data (rates + providers)
            logger.info("=== EXTRACTION PHASE ===")
            merged_chunks = self._extract_and_merge_data()
            phase_progress.update(1)
            
            # Step 2: Filter by billing codes from cpt_codes.txt
            logger.info("=== BILLING CODES FILTERING PHASE ===")
            filtered_chunks = self._filter_by_billing_codes(merged_chunks)
            phase_progress.update(1)
            
            # Step 3: Rename columns
            logger.info("=== COLUMN RENAMING PHASE ===")
            renamed_chunks = self._rename_columns(filtered_chunks)
            phase_progress.update(1)
            
            # Step 4: Drop unwanted columns
            logger.info("=== COLUMN DROPPING PHASE ===")
            cleaned_chunks = self._drop_columns(renamed_chunks)
            phase_progress.update(1)
            
            # Step 5: Add GA WC rate columns (initialize)
            logger.info("=== GA WC COLUMNS INITIALIZATION PHASE ===")
            ga_initialized_chunks = self._initialize_ga_wc_columns(cleaned_chunks)
            phase_progress.update(1)
            
            # Step 6: NPPES lookup and enrichment
            if not skip_nppes:
                logger.info("=== NPPES ENRICHMENT PHASE ===")
                nppes_enriched_chunks = self._enrich_with_nppes_data(ga_initialized_chunks)
                phase_progress.update(1)
                
                # Step 6.5: Filter by target state
                logger.info("=== STATE FILTERING PHASE ===")
                state_filtered_chunks = self._filter_by_state(nppes_enriched_chunks)
                phase_progress.update(1)
                
                # Step 6.6: Census geocoding using NPPES address data
                logger.info("=== CENSUS GEOCODING PHASE ===")
                geocoded_chunks = self._enrich_with_census_geocoding(state_filtered_chunks)
                phase_progress.update(1)
                
                # Step 7: Drop NPPES columns
                logger.info("=== NPPES COLUMNS CLEANUP PHASE ===")
                nppes_cleaned_chunks = self._drop_nppes_columns(geocoded_chunks)
                phase_progress.update(1)
            else:
                logger.info("=== SKIPPING NPPES PHASES ===")
                nppes_cleaned_chunks = ga_initialized_chunks
                phase_progress.update(4)  # Skip 4 phases: NPPES, State Filter, Geocoding, NPPES Cleanup
            
            # Step 8: Merge with GA WC data
            logger.info("=== GA WC MERGE PHASE ===")
            ga_merged_chunks = self._merge_ga_wc_data(nppes_cleaned_chunks)
            phase_progress.update(1)
            
            # Step 9: Categorize procedures
            logger.info("=== PROCEDURE CATEGORIZATION PHASE ===")
            categorized_chunks = self._categorize_procedures(ga_merged_chunks)
            phase_progress.update(1)
            
            # Step 10: Join Medicare benchmarks based on billing_class
            logger.info("=== MEDICARE BENCHMARK JOINING PHASE ===")
            benchmarked_chunks = self.benchmark_joiner.join_benchmarks_with_filtered_data(categorized_chunks)
            phase_progress.update(1)
            
            # Step 11: Load final results
            logger.info("=== LOADING PHASE ===")
            self._load_final_results(benchmarked_chunks)
            phase_progress.update(1)
            
            phase_progress.close()
            logger.info("ETL pipeline completed successfully!")
            
        except Exception as e:
            phase_progress.close()
            logger.error(f"ETL pipeline failed: {e}")
            raise
    
    def _extract_and_merge_data(self) -> Iterator[pd.DataFrame]:
        """Extract rates and providers data, then merge using correct join columns"""
        logger.info("Extracting rates data...")
        rates_chunks = self.parquet_extractor.extract_rates_data()
        
        logger.info("Extracting providers data...")
        providers_df = self.parquet_extractor.extract_providers_data()
        
        if providers_df.empty:
            raise ValueError("Providers data is empty - cannot proceed")
        
        logger.info("Merging rates and providers using provider_reference_id and provider_group_id...")
        # Use the correct join columns as specified
        merged_chunks = self.parquet_extractor.merge_rates_providers(
            rates_chunks, 
            providers_df, 
            left_on='provider_reference_id', 
            right_on='provider_group_id', 
            how='left'
        )
        
        return self.parquet_extractor.apply_column_transformations(merged_chunks)
    
    def _filter_by_billing_codes(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Filter data by billing codes from cpt_codes.txt file"""
        logger.info("Filtering by billing codes from cpt_codes.txt...")
        
        # Read billing codes from txt file
        billing_codes_txt_path = Path("data/raw/cpt_codes.txt")
        if not billing_codes_txt_path.exists():
            logger.warning(f"Billing codes file not found: {billing_codes_txt_path}")
            return chunks
        
        with open(billing_codes_txt_path, "r") as f:
            billing_codes_list = [line.strip() for line in f if line.strip()]
        billing_codes_set = set(billing_codes_list)
        
        logger.info(f"Loaded {len(billing_codes_set)} billing codes from file")
        
        def filter_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns:
                logger.warning("No billing_code column found for filtering")
                return chunk
            
            # Filter to only include billing codes in our set
            chunk['billing_code_str'] = chunk['billing_code'].astype(str)
            filtered = chunk[chunk['billing_code_str'].isin(billing_codes_set)]
            filtered = filtered.drop(columns=['billing_code_str'], errors='ignore')
            
            logger.info(f"Filtered chunk: {len(chunk)} -> {len(filtered)} rows")
            return filtered
        
        return self.data_cleaner.clean_chunks(chunks, filter_chunk)
    
    def _rename_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Rename columns according to the specified mapping"""
        logger.info("Renaming columns...")
        
        # Define the rename dictionary as specified
        rename_dict = {
            'negotiated_rate': 'rate',
            'last_updated_on_x': 'rate_updated_on',
            'reporting_entity_name_x': 'payer',
            'reporting_entity_type_x': 'payer_type',
            'npi': 'prov_npi',
            'description': 'code_desc'
        }
        
        def rename_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            # Only rename columns that exist
            existing_renames = {old: new for old, new in rename_dict.items() if old in chunk.columns}
            if existing_renames:
                chunk = chunk.rename(columns=existing_renames)
                logger.info(f"Renamed columns: {existing_renames}")
            return chunk
        
        return self.data_cleaner.clean_chunks(chunks, rename_chunk)
    
    def _drop_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Drop unwanted columns"""
        logger.info("Dropping unwanted columns...")
        
        # Define columns to drop as specified
        drop_cols = [
            'provider_reference_id', 'version_x', 'provider_group_id', 'reporting_entity_name_y',
            'reporting_entity_type_y', 'last_updated_on_y', 'version_y', 'expiration_date'
        ]
        
        def drop_chunk_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Only drop columns that exist
            existing_drops = [col for col in drop_cols if col in chunk.columns]
            if existing_drops:
                chunk = chunk.drop(columns=existing_drops, errors='ignore')
                logger.info(f"Dropped columns: {existing_drops}")
            return chunk
        
        return self.data_cleaner.clean_chunks(chunks, drop_chunk_columns)
    
    def _initialize_ga_wc_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Initialize GA WC rate columns with None values"""
        logger.info("Initializing GA WC rate columns...")
        
        def init_ga_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Add GA WC rate columns and initialize with None
            chunk['GA_PROF_MAR'] = None
            chunk['GA_OP_MAR'] = None
            chunk['GA_ASC_MAR'] = None
            return chunk
        
        return self.data_cleaner.clean_chunks(chunks, init_ga_columns)
    
    def _enrich_with_nppes_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Enrich with NPPES data using the specified NPPES lookup logic"""
        logger.info("Enriching with NPPES data...")
        
        # Import here to avoid circular imports
        from transformers.geocoder import GeocodingTransformer
        geocoder = GeocodingTransformer(self.chunk_size)
        return geocoder.enrich_with_nppes_data(chunks)
    
    def _filter_by_state(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Filter records to only include the target state"""
        from config.settings import TARGET_STATE
        
        logger.info(f"Filtering records to only include state: {TARGET_STATE}")
        
        for chunk in chunks:
            if 'state' in chunk.columns:
                initial_count = len(chunk)
                # Filter to only include records where state matches TARGET_STATE
                chunk = chunk[chunk['state'] == TARGET_STATE]
                filtered_count = len(chunk)
                logger.info(f"State filter: {initial_count} -> {filtered_count} rows (removed {initial_count - filtered_count} non-{TARGET_STATE} records)")
            else:
                logger.warning("No 'state' column found in chunk, skipping state filtering")
            
            yield chunk
    
    def _enrich_with_census_geocoding(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Enrich with census geocoding data using address components from NPPES"""
        logger.info("Enriching with census geocoding data...")
        
        # Import here to avoid circular imports
        from transformers.geocoder import GeocodingTransformer
        geocoder = GeocodingTransformer(self.chunk_size)
        return geocoder.enrich_with_census_geocoding(chunks)
    
    def _drop_nppes_columns(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Drop NPPES-specific columns after enrichment"""
        logger.info("Dropping NPPES columns...")
        
        # Define NPPES columns to drop as specified
        nppes_drop_cols = ['npi', 'error']
        
        def drop_nppes_columns(chunk: pd.DataFrame) -> pd.DataFrame:
            # Only drop columns that exist
            existing_drops = [col for col in nppes_drop_cols if col in chunk.columns]
            if existing_drops:
                chunk = chunk.drop(columns=existing_drops, errors='ignore')
                logger.info(f"Dropped NPPES columns: {existing_drops}")
            return chunk
        
        return self.data_cleaner.clean_chunks(chunks, drop_nppes_columns)
    
    def _merge_ga_wc_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Merge with Georgia WC data"""
        logger.info("Merging with Georgia WC data...")
        
        # Load GA WC data
        ga_wc_data = self.excel_extractor.extract_georgia_wc_data()
        
        if ga_wc_data.empty:
            logger.warning("Georgia WC data not available - skipping merge")
            return chunks
        
        def merge_ga_wc_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns:
                logger.warning("No billing_code column found for GA WC merge")
                return chunk
            
            # Ensure both columns are string type for join
            chunk['billing_code'] = chunk['billing_code'].astype(str)
            ga_wc_data['CODE'] = ga_wc_data['CODE'].astype(str)
            
            # Merge with GA WC data
            merged = pd.merge(
                chunk,
                ga_wc_data,
                left_on='billing_code',
                right_on='CODE',
                how='left'
            )
            
            # Move values from source columns to target columns (only overwrite where src_col is not null)
            rate_mapping = [
                ('PROF MAR', 'GA_PROF_MAR'),
                ('OP MAR', 'GA_OP_MAR'), 
                ('ASC MAR', 'GA_ASC_MAR')
            ]
            
            for src_col, dest_col in rate_mapping:
                if src_col in merged.columns and dest_col in merged.columns:
                    # Only overwrite where src_col is not null
                    merged[dest_col] = merged[src_col].combine_first(merged[dest_col])
            
            # Clean up temporary columns
            cols_to_drop = ['CODE'] + [col for col, _ in rate_mapping if col in merged.columns]
            cols_to_drop.extend(['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 
                               'MOD', 'DESCRIPTION', 'FUD', 'APC', 'SI', 'PI'])
            
            return merged.drop(columns=cols_to_drop, errors='ignore')
        
        return self.data_cleaner.clean_chunks(chunks, merge_ga_wc_chunk)
    
    
    def _calculate_medicare_professional_rates(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Calculate Medicare professional rates using SQLite DB (chunked processing)"""
        logger.info("Calculating Medicare professional rates...")
        
        # Load reference tables once for all chunks
        logger.info("Loading Medicare professional reference tables...")
        try:
            ref_tables = self._load_medicare_prof_reference_tables()
        except Exception as e:
            logger.error(f"Failed to load Medicare reference tables: {e}")
            # Return chunks unchanged if reference tables fail to load
            return chunks
        
        def calc_medicare_prof_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns or 'postal_code' not in chunk.columns:
                logger.warning("Missing required columns for Medicare professional calculation")
                return chunk
            
            try:
                return self._attach_medicare_prof_chunked(chunk, ref_tables)
            except Exception as e:
                logger.error(f"Medicare professional calculation failed: {e}")
                return chunk
        
        return self.data_cleaner.clean_chunks(chunks, calc_medicare_prof_chunk)
    
    def _load_medicare_prof_reference_tables(self, db_path: str = "data/raw/compensation_rates.db", year: int = 2025) -> dict:
        """Load Medicare professional reference tables once for all chunks"""
        import sqlite3
        
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
    
    def _attach_medicare_prof_chunked(self, df: pd.DataFrame, ref_tables: dict) -> pd.DataFrame:
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
    
    def _attach_medicare_prof(self, df: pd.DataFrame, db_path: str = "data/raw/medicare_test.db", year: int = 2025) -> pd.DataFrame:
        """Enrich DataFrame with Medicare professional rates from SQLite DB"""
        import sqlite3
        
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

        # Load all reference tables needed for the calculation
        try:
            con = sqlite3.connect(db_path)
            # Load tables as DataFrames
            mloc = pd.read_sql_query("SELECT * FROM medicare_locality_map", con)
            meta = pd.read_sql_query("SELECT * FROM medicare_locality_meta", con)
            gpci = pd.read_sql_query("SELECT * FROM cms_gpci WHERE year = ?", con, params=(year,))
            rvu  = pd.read_sql_query("SELECT * FROM cms_rvu WHERE year = ? AND (modifier IS NULL OR modifier = '')", con, params=(year,))
            cf   = pd.read_sql_query("SELECT * FROM cms_conversion_factor WHERE year = ?", con, params=(year,))
        except Exception as e:
            logger.error(f"Failed to load Medicare reference tables: {e}")
            df["medicare_prof"] = pd.NA
            return df.drop(columns=["_zip5", "_code"])
        finally:
            con.close()

        # Prepare reference tables
        mloc["zip_code"] = mloc["zip_code"].astype(str).str.zfill(5)
        meta["mac_code"] = meta["mac_code"].astype(str)
        meta["locality_code"] = meta["locality_code"].astype(str)
        gpci["locality_name"] = gpci["locality_name"].astype(str).str.strip()
        gpci["locality_code"] = gpci["locality_code"].astype(str)
        rvu["procedure_code"] = rvu["procedure_code"].astype(str).str.strip()
        cf = cf.iloc[0]  # Only one row for the year

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
    
    def _calculate_medicare_facility_rates(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Calculate Medicare facility rates (OPPS/ASC) (chunked processing)"""
        logger.info("Calculating Medicare facility rates...")
        
        # Load reference tables once for all chunks
        logger.info("Loading Medicare facility reference tables...")
        try:
            ref_tables = self._load_medicare_facility_reference_tables()
        except Exception as e:
            logger.error(f"Failed to load Medicare facility reference tables: {e}")
            # Return chunks unchanged if reference tables fail to load
            return chunks
        
        def calc_medicare_facility_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            try:
                return self._attach_medicare_facility_rates_chunked(chunk, ref_tables)
            except Exception as e:
                logger.error(f"Medicare facility calculation failed: {e}")
                return chunk
        
        return self.data_cleaner.clean_chunks(chunks, calc_medicare_facility_chunk)
    
    def _load_medicare_facility_reference_tables(self, db_path: str = "data/raw/medicare_test.db") -> dict:
        """Load Medicare facility reference tables once for all chunks"""
        import sqlite3
        
        try:
            con = sqlite3.connect(db_path)
            # Load tables as DataFrames
            asc_addendum_aa = pd.read_sql_query("SELECT * FROM asc_addendum_aa", con)
            opps_addendum_b = pd.read_sql_query("SELECT * FROM opps_addendum_b", con)
            cbsa_wage_index = pd.read_sql_query("SELECT * FROM cbsa_wage_index", con)
            zip_cbsa = pd.read_sql_query("SELECT * FROM zip_cbsa", con)
            con.close()
            
            return {
                'asc_addendum_aa': asc_addendum_aa,
                'opps_addendum_b': opps_addendum_b,
                'cbsa_wage_index': cbsa_wage_index,
                'zip_cbsa': zip_cbsa
            }
        except Exception as e:
            logger.error(f"Failed to load Medicare facility reference tables: {e}")
            raise
    
    def _attach_medicare_facility_rates_chunked(self, df: pd.DataFrame, ref_tables: dict) -> pd.DataFrame:
        """Calculate Medicare facility rates using pre-loaded reference tables"""
        import numpy as np
        
        # Constants (CY2025)
        OPPS_CF = 89.169
        ASC_CF  = 54.895
        OPPS_LABOR_SHARE = 0.60
        ASC_LABOR_SHARE  = 0.50

        def _norm_code(s: pd.Series) -> pd.Series:
            return s.astype(str).str.strip().str.upper()

        # Use pre-loaded reference tables
        df_aa = ref_tables['asc_addendum_aa']
        df_b = ref_tables['opps_addendum_b']
        df_wi = ref_tables['cbsa_wage_index']
        df_zip_cbsa = ref_tables['zip_cbsa']

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
    
    def _attach_medicare_facility_rates(self, df: pd.DataFrame, db_path: str = "data/raw/medicare_test.db") -> pd.DataFrame:
        """Calculate Medicare facility rates (OPPS/ASC) using SQLite DB"""
        import sqlite3
        import numpy as np
        
        # Constants (CY2025)
        OPPS_CF = 89.169
        ASC_CF  = 54.895
        OPPS_LABOR_SHARE = 0.60
        ASC_LABOR_SHARE  = 0.50

        def _norm_code(s: pd.Series) -> pd.Series:
            return s.astype(str).str.strip().str.upper()

        # Connect to the database
        try:
            conn = sqlite3.connect(db_path)
            
            # Load lookup tables from DB
            df_aa = pd.read_sql("SELECT * FROM asc_addendum_aa", conn)
            df_b = pd.read_sql("SELECT * FROM opps_addendum_b", conn)
            df_wi = pd.read_sql("SELECT * FROM cbsa_wage_index", conn)
            df_zip_cbsa = pd.read_sql("SELECT * FROM zip_cbsa", conn)
            
        except Exception as e:
            logger.error(f"Failed to load facility rate reference tables: {e}")
            return df
        finally:
            conn.close()

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
    
    def _calculate_medicare_rates(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Calculate Medicare professional and facility rates"""
        logger.info("Calculating Medicare rates...")
        prof_enhanced = self.rate_calculator.calculate_medicare_professional(chunks)
        return self.rate_calculator.calculate_medicare_facility(prof_enhanced)
    
    def _clean_and_transform_data(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Apply data cleaning and basic transformations"""
        logger.info("Cleaning and transforming data...")
        
        def clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            # Apply data quality validation
            chunk = self.parquet_extractor.validate_data_quality(chunk)
            
            # Additional cleaning can be added here
            return chunk
        
        return self.data_cleaner.clean_chunks(chunks, clean_chunk)
    
    
    def _filter_by_taxonomy(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Filter data based on primary_taxonomy_desc whitelist
        
        This filtering happens after NPPES API enrichment to ensure we only
        process data for providers with taxonomy descriptions that match
        the whitelist in data/raw/primary_taxonomy_desc_whitelist.txt
        """
        logger.info("Filtering by taxonomy whitelist...")
        return self.taxonomy_filter.filter_by_taxonomy(chunks)
    
    def _calculate_rates(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Calculate Medicare professional and facility rates"""
        logger.info("Calculating Medicare professional rates...")
        prof_enhanced = self.rate_calculator.calculate_medicare_professional(chunks)
        
        logger.info("Calculating Medicare facility rates...")
        return self.rate_calculator.calculate_medicare_facility(prof_enhanced)
    
    def _add_georgia_wc_rates(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Add Georgia Workers' Compensation rates"""
        logger.info("Loading Georgia WC reference data...")
        logger.info(f"Georgia WC rates input chunks type: {type(chunks)}")
        
        if chunks is None:
            logger.error("Georgia WC rates received None chunks - returning empty iterator")
            return iter([])
        
        ga_wc_data = self.excel_extractor.extract_georgia_wc_data()
        
        if ga_wc_data.empty:
            logger.warning("Georgia WC data not available - skipping")
            return chunks
        
        def merge_ga_rates(chunk: pd.DataFrame) -> pd.DataFrame:
            logger.info(f"merge_ga_rates: Processing chunk with {len(chunk)} rows")
            logger.info(f"merge_ga_rates: Chunk billing codes: {chunk['billing_code'].head().tolist() if 'billing_code' in chunk.columns else 'NO BILLING_CODE COLUMN'}")
            logger.info(f"merge_ga_rates: GA WC data shape: {ga_wc_data.shape}")
            logger.info(f"merge_ga_rates: GA WC sample codes: {ga_wc_data['CODE'].head().tolist() if 'CODE' in ga_wc_data.columns else 'NO CODE COLUMN'}")
            
            # Initialize GA rate columns
            for col in ['GA_PROF_MAR', 'GA_OP_MAR', 'GA_ASC_MAR']:
                if col not in chunk.columns:
                    chunk[col] = None
            
            if 'billing_code' not in chunk.columns:
                logger.warning("merge_ga_rates: No billing_code column found")
                return chunk
            
            # Merge with GA WC data
            chunk['billing_code_str'] = chunk['billing_code'].astype(str)
            merged = pd.merge(
                chunk,
                ga_wc_data,
                left_on='billing_code_str',
                right_on='CODE',
                how='left'
            )
            
            logger.info(f"merge_ga_rates: After merge: {len(merged)} rows")
            logger.info(f"merge_ga_rates: Matches found: {merged['CODE'].notna().sum()}")
            
            # Move values from source columns to target columns
            rate_mapping = [
                ('PROF MAR', 'GA_PROF_MAR'),
                ('OP MAR', 'GA_OP_MAR'), 
                ('ASC MAR', 'GA_ASC_MAR')
            ]
            
            for src_col, dest_col in rate_mapping:
                if src_col in merged.columns:
                    merged[dest_col] = merged[src_col].combine_first(merged[dest_col])
            
            # Clean up temporary columns
            cols_to_drop = ['CODE', 'billing_code_str'] + [col for col, _ in rate_mapping if col in merged.columns]
            cols_to_drop.extend(['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 
                               'MOD', 'DESCRIPTION', 'FUD', 'APC', 'SI', 'PI'])
            
            result = merged.drop(columns=cols_to_drop, errors='ignore')
            logger.info(f"merge_ga_rates: Final result: {len(result)} rows")
            return result
        
        result = self.data_cleaner.clean_chunks(chunks, merge_ga_rates)
        if result is None:
            logger.error("Data cleaner returned None - returning empty iterator")
            return iter([])
        return result
    
    def _categorize_procedures(self, chunks: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """Apply procedure categorization using the specified CPT code hierarchy"""
        logger.info("Categorizing procedures...")
        
        def categorize_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            if 'billing_code' not in chunk.columns:
                logger.warning("No billing_code column found for categorization")
                return chunk
            
            return self._categorize_cpt_codes(chunk, 'billing_code')
        
        return self.data_cleaner.clean_chunks(chunks, categorize_chunk)
    
    def _categorize_cpt_codes(self, df: pd.DataFrame, cpt_column: str = 'billing_code') -> pd.DataFrame:
        """
        Categorizes CPT codes into three hierarchical levels:
        - Procedure Set (highest level - 6 main categories)
        - Procedure Class (mid level - anatomical/specialty based)
        - Procedure Group (lowest level - specific procedure types)
        """
        
        def get_procedure_categories(cpt_code):
            """Map CPT code to three-level hierarchy"""
            try:
                # Convert to string and clean
                code_str = str(cpt_code).strip()
                
                # Handle special codes first
                if code_str.startswith('G'):
                    return 'HCPCS', 'Medicare Specific', 'G-Codes'
                elif code_str.startswith('IME'):
                    return 'HCPCS', 'Facility Specific', 'IME Codes'
                
                # Convert to integer for numeric comparison
                try:
                    code = int(code_str)
                except ValueError:
                    return 'Unknown', 'Unknown', 'Unknown'
                
                # RADIOLOGY (70000-79999)
                if 70000 <= code <= 79999:
                    procedure_set = 'Radiology'
                    
                    if 70000 <= code <= 70999:
                        if 70000 <= code <= 70559:
                            return procedure_set, 'Head and Neck Imaging', 'Skull and Brain'
                        else:
                            return procedure_set, 'Head and Neck Imaging', 'Spine and Neck'
                    elif 71000 <= code <= 71999:
                        return procedure_set, 'Chest Imaging', 'Thoracic Studies'
                    elif 72000 <= code <= 72999:
                        if 72000 <= code <= 72159:
                            return procedure_set, 'Spine Imaging', 'Spinal Radiography'
                        else:
                            return procedure_set, 'Spine Imaging', 'Spinal CT/MRI'
                    elif 73000 <= code <= 73999:
                        if 73000 <= code <= 73225:
                            return procedure_set, 'Extremity Imaging', 'Upper Extremity'
                        else:
                            return procedure_set, 'Extremity Imaging', 'Lower Extremity'
                    elif 74000 <= code <= 74999:
                        return procedure_set, 'Abdomen/GI Imaging', 'Abdominal Studies'
                    elif 75000 <= code <= 75999:
                        return procedure_set, 'Vascular Imaging', 'Angiography'
                    elif 76000 <= code <= 76999:
                        if 76000 <= code <= 76499:
                            return procedure_set, 'Other Imaging', 'Fluoroscopy'
                        else:
                            return procedure_set, 'Other Imaging', 'Ultrasound'
                    elif 77000 <= code <= 77999:
                        if 77000 <= code <= 77099:
                            return procedure_set, 'Radiation Oncology', 'Treatment Planning'
                        else:
                            return procedure_set, 'Radiation Oncology', 'Treatment Delivery'
                    elif 78000 <= code <= 78999:
                        return procedure_set, 'Nuclear Medicine', 'Nuclear Studies'
                    else:
                        return procedure_set, 'Radiopharmaceutical', 'Therapeutic Nuclear'
                
                # SURGERY (10000-69999)
                elif 10000 <= code <= 69999:
                    procedure_set = 'Surgery'
                    
                    if 10000 <= code <= 19999:
                        if 10000 <= code <= 17999:
                            return procedure_set, 'Integumentary System', 'Skin Procedures'
                        else:
                            return procedure_set, 'Integumentary System', 'Breast Procedures'
                    elif 20000 <= code <= 29999:
                        if 20000 <= code <= 25999:
                            return procedure_set, 'Musculoskeletal System', 'General Orthopedic'
                        else:
                            return procedure_set, 'Musculoskeletal System', 'Hand/Wrist Surgery'
                    elif 30000 <= code <= 32999:
                        return procedure_set, 'Respiratory System', 'Pulmonary Procedures'
                    elif 33000 <= code <= 37999:
                        return procedure_set, 'Cardiovascular System', 'Cardiac/Vascular Surgery'
                    elif 38000 <= code <= 41999:
                        return procedure_set, 'Hemic/Lymphatic System', 'Blood/Lymph Procedures'
                    elif 42000 <= code <= 49999:
                        if 42000 <= code <= 43999:
                            return procedure_set, 'Digestive System', 'Upper GI Surgery'
                        else:
                            return procedure_set, 'Digestive System', 'Lower GI Surgery'
                    elif 50000 <= code <= 54999:
                        return procedure_set, 'Urinary System', 'Urological Surgery'
                    elif 55000 <= code <= 59999:
                        return procedure_set, 'Genital System', 'Reproductive Surgery'
                    elif 60000 <= code <= 64999:
                        return procedure_set, 'Endocrine/Nervous System', 'Specialized Surgery'
                    else:
                        return procedure_set, 'Eye/Ear System', 'Sensory Surgery'
                
                # EVALUATION & MANAGEMENT (99000-99999)
                elif 99000 <= code <= 99999:
                    procedure_set = 'Evaluation & Management'
                    
                    if 99091 <= code <= 99091:
                        return procedure_set, 'Remote Monitoring', 'Digital Health'
                    elif 99202 <= code <= 99215:
                        return procedure_set, 'Office Visits', 'Outpatient E&M'
                    elif 99221 <= code <= 99239:
                        return procedure_set, 'Hospital Care', 'Inpatient E&M'
                    elif 99242 <= code <= 99255:
                        return procedure_set, 'Consultations', 'Consultation E&M'
                    elif 99281 <= code <= 99288:
                        return procedure_set, 'Emergency Care', 'Emergency E&M'
                    elif 99291 <= code <= 99292:
                        return procedure_set, 'Critical Care', 'Intensive Care E&M'
                    elif 99304 <= code <= 99318:
                        return procedure_set, 'Nursing Facility', 'Long-term Care E&M'
                    elif 99341 <= code <= 99350:
                        return procedure_set, 'Home Visits', 'Home Care E&M'
                    elif 99358 <= code <= 99499:
                        return procedure_set, 'Special Services', 'Miscellaneous E&M'
                    else:
                        return procedure_set, 'Other E&M', 'General E&M'
                
                # Default for unrecognized codes
                else:
                    return 'Other', 'Miscellaneous', 'Unspecified'
                    
            except Exception as e:
                return 'Error', 'Error', f'Processing Error: {str(e)}'
        
        # Apply categorization to each CPT code
        categories = df[cpt_column].apply(get_procedure_categories)
        
        # Split the results into separate columns
        df = df.copy()  # Create a copy to avoid modifying original
        df[['procedure_set', 'procedure_class', 'procedure_group']] = pd.DataFrame(
            categories.tolist(), index=df.index
        )
        
        return df
    
    def _load_final_results(self, chunks: Iterator[pd.DataFrame]) -> None:
        """Load final results to output file"""
        logger.info("Loading final results...")
        
        if chunks is None:
            logger.error("Chunks is None - cannot process")
            return
        
        # Process all chunks and combine
        chunk_processor = ChunkProcessor(self.chunk_size)
        final_df = chunk_processor.process_chunks(
            chunks,
            transform_func=lambda x: x,  # No transformation, just combine
            output_path=FilePaths.COMMERCIAL_RATES_OUTPUT
        )
        
        logger.info(f"Final dataset: {len(final_df)} rows, {len(final_df.columns)} columns")
        
        # Optional: Append to existing data if needed
        if self._should_append_to_existing():
            self._append_to_existing_data(final_df)
    
    def _should_append_to_existing(self) -> bool:
        """Check if we should append to existing data"""
        existing_file = Path("data/output/commercial_rates_combined.parquet")
        return existing_file.exists()
    
    def _append_to_existing_data(self, new_df: pd.DataFrame) -> None:
        """Memory-efficient append new data to existing dataset using chunked processing"""
        existing_file = Path("data/output/commercial_rates_combined.parquet")
        
        try:
            # Check existing file size to determine memory-efficient approach
            existing_size_mb = existing_file.stat().st_size / (1024 * 1024)
            new_size_mb = new_df.memory_usage(deep=True).sum() / (1024 * 1024)
            
            logger.info(f"Existing file: {existing_size_mb:.1f} MB, New data: {new_size_mb:.1f} MB")
            
            # If existing file is large (>500MB), use streaming append
            if existing_size_mb > 500:
                logger.info("Using memory-efficient streaming append for large dataset")
                self._streaming_append_data(new_df, existing_file)
            else:
                logger.info("Using standard append for small dataset")
                self._standard_append_data(new_df, existing_file)
            
        except Exception as e:
            logger.error(f"Failed to append to existing data: {e}")
            logger.info("Saving as separate file instead")
    
    def _standard_append_data(self, new_df: pd.DataFrame, existing_file: Path) -> None:
        """Standard append for small datasets"""
        logger.info("Loading existing data for append...")
        existing_df = pd.read_parquet(existing_file)
        
        logger.info(f"Combining {len(existing_df)} existing + {len(new_df)} new rows")
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Save combined dataset
        output_path = Path("data/output/commercial_rates_combined.parquet")
        combined_df.to_parquet(output_path, index=False)
        
        logger.info(f"Combined dataset saved: {len(combined_df)} total rows")
    
    def _streaming_append_data(self, new_df: pd.DataFrame, existing_file: Path) -> None:
        """Memory-efficient streaming append for large datasets"""
        import tempfile
        import shutil
        
        logger.info("Using streaming append to avoid memory issues...")
        
        # Create temporary file for streaming
        temp_file = Path("data/output/commercial_rates_combined_temp.parquet")
        
        try:
            # Process existing data in chunks
            existing_chunks = pd.read_parquet(existing_file, chunksize=self.chunk_size)
            
            # Write existing data to temp file in chunks
            first_chunk = True
            for chunk in existing_chunks:
                if first_chunk:
                    chunk.to_parquet(temp_file, index=False)
                    first_chunk = False
                else:
                    # Append chunk to temp file
                    chunk.to_parquet(temp_file, index=False, append=True)
            
            # Append new data to temp file
            new_df.to_parquet(temp_file, index=False, append=True)
            
            # Replace original file with temp file
            shutil.move(str(temp_file), str(existing_file))
            
            logger.info(f"Streaming append completed successfully")
            
        except Exception as e:
            logger.error(f"Streaming append failed: {e}")
            # Clean up temp file
            if temp_file.exists():
                temp_file.unlink()
            raise


def main():
    """Main entry point"""
    try:
        # Initialize and run pipeline
        pipeline = HealthcareRateETL(chunk_size=CHUNK_SIZE)
        pipeline.run_full_pipeline()
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()