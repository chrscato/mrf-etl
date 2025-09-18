# Healthcare Rate ETL Pipeline

A chunked ETL pipeline for processing large healthcare rate datasets, enriching with Medicare rates, provider data, and geographic information.

## Features

- **Chunked Processing**: Handles large parquet files by processing in configurable chunks
- **Rate Calculations**: Calculates Medicare professional and facility rates
- **Provider Enrichment**: Adds NPPES provider taxonomy and organizational data
- **Geographic Enhancement**: Geocoding with lat/lng and CBSA information
- **Procedure Categorization**: Hierarchical categorization of CPT codes
- **Georgia Workers' Comp**: Integration of Georgia WC rate schedules
- **Error Handling**: Robust error handling with data salvaging capabilities

## Project Structure

```
etl_pipeline/
├── config/
│   └── settings.py          # Configuration and file paths
├── src/
│   ├── extractors/
│   │   ├── parquet_extractor.py    # Extract from parquet files
│   │   └── excel_extractor.py      # Extract Georgia WC data
│   ├── transformers/
│   │   ├── data_cleaner.py         # Data cleaning utilities
│   │   ├── rate_calculator.py      # Medicare rate calculations
│   │   ├── geocoder.py             # Geographic enrichment
│   │   └── categorizer.py          # Procedure categorization
│   ├── loaders/
│   │   └── parquet_loader.py       # Save processed data
│   └── utils/
│       ├── chunk_processor.py      # Chunked processing utilities
│       └── api_clients.py          # External API clients
├── data/
│   ├── raw/                 # Input data files
│   ├── processed/           # Intermediate processing files
│   └── output/              # Final output files
├── main.py                  # Main pipeline orchestrator
├── run_etl.py              # Simple runner script
└── requirements.txt         # Python dependencies
```

## Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set Environment Variables**
   ```bash
   export HUD_TOKEN="your_hud_api_token"  # Optional for CBSA names
   ```

3. **Prepare Input Data**
   Place the following files in `data/raw/`:
   - `rates.parquet` - Healthcare rates data
   - `providers.parquet` - Provider information
   - `cpt_codes.txt` - List of CPT codes to filter (one per line)
   - `GeorgiaWC_Medical_2024_rev3 (1).xlsx` - Georgia WC rates
   - `compensation_rates.db` - Medicare professional rate database
   - `medicare_test.db` - Medicare facility rate database

## Usage

### Basic Usage
```bash
python run_etl.py
```

### Advanced Options
```bash
python run_etl.py --chunk-size 5000 --log-level DEBUG
```

### Direct Python Import
```python
from main import HealthcareRateETL

pipeline = HealthcareRateETL(chunk_size=10000)
pipeline.run_full_pipeline()
```

## Configuration

Edit `config/settings.py` to customize:

- **File paths**: Update `FilePaths` class with your data locations
- **Chunk size**: Adjust `CHUNK_SIZE` for memory management
- **API settings**: Configure timeouts and retry limits
- **Database settings**: Medicare calculation constants
- **Column mappings**: Customize data transformations

## Pipeline Stages

### 1. Extraction
- Reads rates and providers parquet files in chunks
- Filters by CPT codes from text file
- Merges rates with provider data
- Applies column transformations and cleanup

### 2. Enrichment
- **NPPES Lookup**: Adds provider taxonomy and organizational data
- **Geocoding**: Converts postal codes to lat/lng coordinates
- **CBSA Mapping**: Adds metropolitan statistical area information

### 3. Rate Calculations
- **Medicare Professional**: Calculates physician fee schedule rates using RVU/GPCI
- **Medicare Facility**: Calculates OPPS and ASC rates with wage adjustments
- **State Averaging**: Applies state-level wage index averaging

### 4. Additional Data
- **Georgia Workers' Comp**: Merges GA WC rate schedules
- **Procedure Categorization**: Three-level hierarchical grouping of CPT codes

### 5. Loading
- Saves final dataset to parquet format
- Optional appending to existing datasets
- Robust error handling with CSV fallback

## Performance Considerations

- **Memory Usage**: Chunked processing keeps memory footprint low
- **API Rate Limiting**: Built-in delays and retry logic for external APIs
- **Database Connections**: Efficient bulk loading of reference tables
- **Parallel Processing**: Threaded API calls for NPPES lookups

## Error Handling

- **Corrupted Parquet Files**: Attempts row-group salvaging
- **API Failures**: Graceful degradation with retries
- **Missing Data**: Continues processing with warnings
- **Data Quality**: Validates rates and removes outliers

## Output Schema

The final dataset includes:

**Core Fields:**
- `billing_code` - CPT/HCPCS procedure code
- `rate` - Negotiated commercial rate
- `payer` - Insurance company name
- `prov_npi` - Provider NPI number

**Geographic Fields:**
- `prov_lat`, `prov_lng` - Provider coordinates
- `postal_code` - ZIP code
- `state` - State abbreviation
- `cbsa`, `cbsaname` - Metropolitan area codes/names

**Rate Fields:**
- `medicare_prof` - Medicare physician fee schedule rate
- `medicare_opps_mar_national` - Medicare outpatient facility rate
- `medicare_asc_mar_national` - Medicare ASC rate
- `GA_PROF_MAR`, `GA_OP_MAR`, `GA_ASC_MAR` - Georgia WC rates

**Provider Fields:**
- `org_name` - Organization name
- `primary_taxonomy_code` - Provider specialty taxonomy
- `status` - NPI status
- `enumeration_type` - NPI type (NPI-1 for individuals, NPI-2 for organizations)

**Categorization Fields:**
- `procedure_set` - High-level category (Surgery, Radiology, etc.)
- `procedure_class` - Mid-level category (anatomical system)
- `procedure_group` - Specific procedure type

## Troubleshooting

**Common Issues:**

1. **Out of Memory**: Reduce chunk size in settings
2. **API Timeouts**: Check internet connection and API tokens
3. **Missing Files**: Verify all input files are in `data/raw/`
4. **Parquet Errors**: Check for mixed data types in source files

**Logs:**
- Pipeline logs are written to `etl_pipeline.log`
- Use `--log-level DEBUG` for detailed troubleshooting

## Development

To extend the pipeline:

1. **Add New Extractors**: Inherit from base extractor classes
2. **Custom Transformers**: Implement chunk-based processing
3. **New APIs**: Add clients to `utils/api_clients.py`
4. **Additional Rates**: Extend `RateCalculator` class

## Example Usage

```python
# Custom chunk processing
from utils.chunk_processor import ChunkProcessor

processor = ChunkProcessor(chunk_size=5000)
chunks = processor.read_parquet_chunks('data.parquet')

def my_transform(chunk):
    # Your transformation logic
    return chunk

result = processor.process_chunks(chunks, my_transform)
```

## License

[Add your license information here]

## Support

For issues or questions:
1. Check the logs in `etl_pipeline.log`
2. Review the troubleshooting section
3. Verify input data format and completeness