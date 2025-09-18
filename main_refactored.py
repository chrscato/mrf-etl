"""
Main ETL Pipeline for Healthcare Rate Processing - Refactored
"""
import logging
import sys
from pathlib import Path
from typing import Iterator
import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / 'src'))

from config.settings import FilePaths, CHUNK_SIZE
from extractors.parquet_extractor import ParquetExtractor
from transformers.data_cleaner import DataCleaner
from transformers.geocoder import GeocodingTransformer
from transformers.medicare_calculator import MedicareCalculator
from transformers.ga_wc_processor import GAWCProcessor
from transformers.geocoding_processor import GeocodingProcessor
from transformers.categorizer import ProcedureCategorizer
from transformers.benchmark_joiner import BenchmarkJoiner
from loaders.parquet_loader import ParquetLoader
from src.utils.chunk_processor import ChunkProcessor

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
    """Main ETL pipeline orchestrator - Refactored"""
    
    def __init__(self, chunk_size: int = CHUNK_SIZE):
        self.chunk_size = chunk_size
        
        # Initialize components
        self.parquet_extractor = ParquetExtractor(chunk_size)
        self.data_cleaner = DataCleaner(chunk_size)
        self.geocoder = GeocodingTransformer(chunk_size)
        self.medicare_calculator = MedicareCalculator(chunk_size)
        self.ga_wc_processor = GAWCProcessor(chunk_size)
        self.geocoding_processor = GeocodingProcessor(chunk_size)
        self.categorizer = ProcedureCategorizer(chunk_size)
        self.benchmark_joiner = BenchmarkJoiner(chunk_size)
        self.loader = ParquetLoader()
        
        logger.info(f"Initialized ETL pipeline with chunk size: {chunk_size}")
    
    def run_full_pipeline(self) -> None:
        """Execute the complete ETL pipeline in the correct order"""
        logger.info("Starting full ETL pipeline...")
        
        try:
            # Step 1: Extract and merge base data (rates + providers)
            logger.info("=== EXTRACTION PHASE ===")
            merged_chunks = self._extract_and_merge_data()
            
            # Step 2: Filter by billing codes from cpt_codes.txt
            logger.info("=== BILLING CODES FILTERING PHASE ===")
            filtered_chunks = self.data_cleaner.filter_by_billing_codes(merged_chunks)
            
            # Step 3: Rename columns
            logger.info("=== COLUMN RENAMING PHASE ===")
            renamed_chunks = self.data_cleaner.rename_columns(filtered_chunks)
            
            # Step 4: Drop unwanted columns
            logger.info("=== COLUMN DROPPING PHASE ===")
            cleaned_chunks = self.data_cleaner.drop_columns(renamed_chunks)
            
            # Step 5: Add GA WC rate columns (initialize)
            logger.info("=== GA WC COLUMNS INITIALIZATION PHASE ===")
            ga_initialized_chunks = self.ga_wc_processor.initialize_ga_wc_columns(cleaned_chunks)
            
            # Step 6: NPPES lookup and enrichment
            logger.info("=== NPPES ENRICHMENT PHASE ===")
            nppes_enriched_chunks = self.geocoder.enrich_with_nppes_data(ga_initialized_chunks)
            
            # Step 7: Drop NPPES columns
            logger.info("=== NPPES COLUMNS CLEANUP PHASE ===")
            nppes_cleaned_chunks = self.data_cleaner.drop_nppes_columns(nppes_enriched_chunks)
            
            # Step 8: Merge with GA WC data
            logger.info("=== GA WC MERGE PHASE ===")
            ga_merged_chunks = self.ga_wc_processor.merge_ga_wc_data(nppes_cleaned_chunks)
            
            # Step 9: Add geocoding data (lat/lng/CBSA)
            logger.info("=== GEOCODING PHASE ===")
            geocoded_chunks = self.geocoding_processor.add_geocoding_data(ga_merged_chunks)
            
            # Step 10: Categorize procedures
            logger.info("=== PROCEDURE CATEGORIZATION PHASE ===")
            categorized_chunks = self._categorize_procedures(geocoded_chunks)
            
            # Step 11: Join Medicare benchmarks based on billing_class
            logger.info("=== MEDICARE BENCHMARK JOINING PHASE ===")
            benchmarked_chunks = self.benchmark_joiner.join_benchmarks_with_filtered_data(categorized_chunks)
            
            # Step 12: Load final results
            logger.info("=== LOADING PHASE ===")
            self._load_final_results(benchmarked_chunks)
            
            logger.info("ETL pipeline completed successfully!")
            
        except Exception as e:
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
