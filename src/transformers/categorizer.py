"""
Categorize CPT codes into hierarchical groups
"""
import pandas as pd
import logging
from typing import Iterator, Tuple
from src.utils.chunk_processor import ChunkProcessor

logger = logging.getLogger(__name__)

class ProcedureCategorizer:
    """Categorize CPT codes into three-level hierarchy"""
    
    def __init__(self, chunk_size: int = 10000):
        self.chunk_processor = ChunkProcessor(chunk_size)
    
    def categorize_procedures(self, chunks: Iterator[pd.DataFrame], code_column: str = 'billing_code') -> Iterator[pd.DataFrame]:
        """Apply procedure categorization to chunks"""
        logger.info("Categorizing procedures...")
        
        def apply_categorization(chunk: pd.DataFrame) -> pd.DataFrame:
            if chunk is None or chunk.empty:
                logger.warning("Empty chunk received in categorizer")
                return chunk
                
            if code_column not in chunk.columns:
                logger.warning(f"Column {code_column} not found in chunk")
                chunk['procedure_set'] = 'Unknown'
                chunk['procedure_class'] = 'Unknown'
                chunk['procedure_group'] = 'Unknown'
                return chunk
            
            # Apply categorization
            categories = chunk[code_column].apply(self._get_procedure_categories)
            
            # Split results into separate columns
            chunk[['procedure_set', 'procedure_class', 'procedure_group']] = pd.DataFrame(
                categories.tolist(), index=chunk.index
            )
            
            return chunk
        
        try:
            logger.info(f"Categorizer calling chunk_processor.filter_chunks with chunks type: {type(chunks)}")
            result = self.chunk_processor.filter_chunks(chunks, apply_categorization)
            logger.info(f"Categorizer received result type: {type(result)}")
            
            if result is None:
                logger.error("ChunkProcessor.filter_chunks returned None - returning empty iterator")
                return iter([])
            
            # Yield all chunks from the result
            chunk_count = 0
            for chunk in result:
                chunk_count += 1
                logger.info(f"Categorizer yielding chunk {chunk_count} with {len(chunk)} rows")
                yield chunk
            
            if chunk_count == 0:
                logger.info("No chunks to categorize - generator is empty")
                
        except Exception as e:
            logger.error(f"Error in categorize_procedures: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return empty iterator if there's an error
            return iter([])
    
    def _get_procedure_categories(self, cpt_code) -> Tuple[str, str, str]:
        """Map CPT code to three-level hierarchy"""
        try:
            code_str = str(cpt_code).strip()
            
            # Handle special codes
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
                return self._categorize_radiology(code)
            
            # SURGERY (10000-69999)
            elif 10000 <= code <= 69999:
                return self._categorize_surgery(code)
            
            # EVALUATION & MANAGEMENT (99000-99999)
            elif 99000 <= code <= 99999:
                return self._categorize_evaluation_management(code)
            
            # PATHOLOGY & LABORATORY (80000-89999)
            elif 80000 <= code <= 89999:
                return self._categorize_pathology_lab(code)
            
            # MEDICINE (90000-99999, excluding E&M)
            elif 90000 <= code <= 98999:
                return self._categorize_medicine(code)
            
            # Default for unrecognized codes
            else:
                return 'Other', 'Miscellaneous', 'Unspecified'
                
        except Exception as e:
            logger.warning(f"Error categorizing code {cpt_code}: {e}")
            return 'Error', 'Error', f'Processing Error: {str(e)}'
    
    def _categorize_radiology(self, code: int) -> Tuple[str, str, str]:
        """Categorize radiology codes"""
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
    
    def _categorize_surgery(self, code: int) -> Tuple[str, str, str]:
        """Categorize surgery codes"""
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
    
    def _categorize_evaluation_management(self, code: int) -> Tuple[str, str, str]:
        """Categorize E&M codes"""
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
    
    def _categorize_pathology_lab(self, code: int) -> Tuple[str, str, str]:
        """Categorize pathology and laboratory codes"""
        procedure_set = 'Pathology & Laboratory'
        
        if 80000 <= code <= 84999:
            return procedure_set, 'Chemistry', 'Clinical Chemistry'
        elif 85000 <= code <= 85999:
            return procedure_set, 'Hematology', 'Blood Studies'
        elif 86000 <= code <= 86999:
            return procedure_set, 'Immunology', 'Immune System Tests'
        elif 87000 <= code <= 87999:
            return procedure_set, 'Microbiology', 'Infectious Disease'
        elif 88000 <= code <= 88999:
            return procedure_set, 'Anatomic Pathology', 'Tissue Studies'
        else:
            return procedure_set, 'Other Laboratory', 'Miscellaneous Lab'
    
    def _categorize_medicine(self, code: int) -> Tuple[str, str, str]:
        """Categorize medicine codes"""
        procedure_set = 'Medicine'
        
        if 90000 <= code <= 90999:
            return procedure_set, 'Immunizations', 'Vaccines and Injections'
        elif 91000 <= code <= 91999:
            return procedure_set, 'Gastroenterology', 'GI Procedures'
        elif 92000 <= code <= 92999:
            if 92000 <= code <= 92499:
                return procedure_set, 'Ophthalmology', 'Eye Procedures'
            else:
                return procedure_set, 'Otolaryngology', 'ENT Procedures'
        elif 93000 <= code <= 93999:
            return procedure_set, 'Cardiovascular', 'Cardiac Procedures'
        elif 94000 <= code <= 94999:
            return procedure_set, 'Pulmonary', 'Lung Function Tests'
        elif 95000 <= code <= 95999:
            return procedure_set, 'Neurology', 'Neurological Tests'
        elif 96000 <= code <= 96999:
            return procedure_set, 'Psychiatry', 'Mental Health Services'
        elif 97000 <= code <= 97999:
            return procedure_set, 'Physical Medicine', 'Rehabilitation'
        else:
            return procedure_set, 'Other Medicine', 'Miscellaneous Medicine'