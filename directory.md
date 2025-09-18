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