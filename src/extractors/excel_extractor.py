"""
Extract data from Excel files (Georgia Workers' Comp rates)
"""
import pandas as pd
import logging
from pathlib import Path
from config.settings import FilePaths

logger = logging.getLogger(__name__)

class ExcelExtractor:
    """Extract Georgia Workers' Compensation rate data from Excel"""
    
    def extract_georgia_wc_data(self) -> pd.DataFrame:
        """Extract and combine all Georgia WC rate sheets"""
        logger.info("Extracting Georgia Workers' Comp data...")
        
        try:
            # Read different sheets
            ga_wc_surg = pd.read_excel(FilePaths.GA_WC_EXCEL, sheet_name="Surgery")
            ga_wc_em = pd.read_excel(FilePaths.GA_WC_EXCEL, sheet_name="Evaluation and Management")
            ga_wc_rad = pd.read_excel(FilePaths.GA_WC_EXCEL, sheet_name="Radiology")
            
            # Filter surgery data (only J1 records)
            ga_wc_surg = ga_wc_surg[ga_wc_surg['SI'] == 'J1'] if 'SI' in ga_wc_surg.columns else ga_wc_surg
            
            # Combine all sheets
            ga_wc_combined = pd.concat([ga_wc_surg, ga_wc_em, ga_wc_rad], ignore_index=True)
            
            logger.info(f"Georgia WC data loaded: {len(ga_wc_combined)} rows")
            logger.info(f"Columns: {list(ga_wc_combined.columns)}")
            
            # Ensure CODE column is string type for joining
            if 'CODE' in ga_wc_combined.columns:
                ga_wc_combined['CODE'] = ga_wc_combined['CODE'].astype(str)
            
            return ga_wc_combined
            
        except FileNotFoundError:
            logger.warning(f"Georgia WC Excel file not found: {FilePaths.GA_WC_EXCEL}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to extract Georgia WC data: {e}")
            return pd.DataFrame()