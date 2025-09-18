"""
Configuration settings for the ETL pipeline
"""
import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DATA_DIR = DATA_DIR / "output"

# Create directories if they don't exist
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Processing settings
CHUNK_SIZE = 500000  # Number of rows to process at once
MAX_WORKERS = 6     # For threading operations

# State filtering
TARGET_STATE = "GA"  # State code to filter for (e.g., "GA" for Georgia)

# File paths
class FilePaths:
    # Input files
    RATES_PARQUET = RAW_DATA_DIR / "aetna_ga_rates.parquet"
    PROVIDERS_PARQUET = RAW_DATA_DIR / "aetna_ga_prov.parquet"
    CPT_CODES_TXT = RAW_DATA_DIR / "cpt_codes.txt"
    GA_WC_EXCEL = RAW_DATA_DIR / "GeorgiaWC_Medical_2024_rev3 (1).xlsx"
    COMPENSATION_DB = RAW_DATA_DIR / "compensation_rates.db"
    MEDICARE_DB = RAW_DATA_DIR / "medicare_test.db"
    # TAXONOMY_WHITELIST = RAW_DATA_DIR / "primary_taxonomy_desc_whitelist.txt"  # DISABLED - not using taxonomy filtering
    
    # Output files
    COMMERCIAL_RATES_OUTPUT = OUTPUT_DATA_DIR / "commercial_rates.parquet"
    TEMP_PROCESSED = PROCESSED_DATA_DIR / "temp_processed.parquet"

# API settings
class APIConfig:
    NPPES_BASE_URL = "https://npiregistry.cms.hhs.gov/api/"
    CENSUS_GEOCODE_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    HUD_URL = "https://www.huduser.gov/hudapi/public/usps"
    
    # API headers
    NPPES_HEADERS = {"User-Agent": "ETL-Pipeline/1.0 (+your_email@example.com)"}
    HUD_TOKEN = os.getenv("HUD_TOKEN", "")  # Set as environment variable
    
    # Rate limiting
    REQUEST_TIMEOUT = 10  # Reduced for faster failures
    RETRY_ATTEMPTS = 2  # Reduced retries for faster processing
    RATE_LIMIT_DELAY = 1.0  # Reduced delay
    
    # NPPES API specific settings
    NPPES_MAX_WORKERS = 10  # Increased for better performance
    NPPES_BATCH_SIZE = 100  # Process NPIs in batches

# Database settings
class DatabaseConfig:
    # Medicare calculation constants
    OPPS_CF = 89.169
    ASC_CF = 54.895
    OPPS_LABOR_SHARE = 0.60
    ASC_LABOR_SHARE = 0.50
    
    # Default year for calculations
    DEFAULT_YEAR = 2025

# Data validation settings
class ValidationConfig:
    REQUIRED_COLUMNS = {
        'rates': ['provider_reference_id', 'negotiated_rate', 'billing_code'],
        'providers': ['provider_group_id', 'npi'],
    }
    
    # Data quality thresholds
    MIN_VALID_RATE = 0.01
    MAX_VALID_RATE = 50000.00
    
# Column mapping configuration
class ColumnMapping:
    RENAME_DICT = {
        'negotiated_rate': 'rate',
        'last_updated_on_x': 'rate_updated_on',
        'reporting_entity_name_x': 'payer',
        'reporting_entity_type_x': 'payer_type',
        'npi': 'prov_npi',
        'description': 'code_desc'
    }
    
    DROP_COLUMNS = [
        'provider_reference_id', 'version_x', 'provider_group_id', 
        'reporting_entity_name_y', 'reporting_entity_type_y', 
        'last_updated_on_y', 'version_y', 'expiration_date'
    ]