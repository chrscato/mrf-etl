# Medicare Benchmark Dimension Tables

This system builds comprehensive Medicare benchmark dimension tables with both national and state-averaged rates for professional, OPPS, and ASC services.

## Quick Start

1. **Build the benchmark tables**:
   ```bash
   python build_medicare_benchmarks.py
   ```

2. **View usage examples**:
   ```bash
   python example_medicare_benchmark_usage.py
   ```

## Files Generated

The script creates the following parquet files in `prod_etl/core/data/silver/benchmarks/`:

- `bench_medicare_professional.parquet` - Professional service benchmarks (CPT codes)
- `bench_medicare_opps.parquet` - OPPS benchmarks (HCPCS codes)  
- `bench_medicare_asc.parquet` - ASC benchmarks (HCPCS codes)
- `bench_medicare_comprehensive.parquet` - Combined all benchmarks

## Key Features

### State Averages for Professional Rates
Unlike the current Medicare calculator which only provides zip-specific rates, these tables include **state-averaged professional rates** calculated using state-averaged GPCI values.

### Comprehensive Coverage
- **All 50 states** + DC
- **All procedure codes** from Medicare reference tables
- **Multiple rate types**: National and state-averaged rates
- **Rich metadata**: Creation dates, data lineage, RVU components

### Easy Integration
Consistent key structure `(state, year_month, code_type, code)` for easy joining with fact tables.

## Usage Examples

### Join with Fact Table
```python
import pandas as pd

# Load data
fact = pd.read_parquet('prod_etl/core/data/gold/fact_rate.parquet')
benchmarks = pd.read_parquet('prod_etl/core/data/silver/benchmarks/bench_medicare_comprehensive.parquet')

# Join for benchmarking
fact_with_benchmarks = fact.merge(
    benchmarks[benchmarks['benchmark_type'] == 'professional'],
    on=['state', 'year_month', 'code_type', 'code'],
    how='left'
)

# Calculate percentage of Medicare
fact_with_benchmarks['pct_of_medicare'] = (
    fact_with_benchmarks['negotiated_rate'] / 
    fact_with_benchmarks['medicare_prof_stateavg']
)
```

### DuckDB Query
```sql
-- Compare negotiated rates to Medicare benchmarks
WITH fact AS (
  SELECT *
  FROM read_parquet('prod_etl/core/data/gold/fact_rate.parquet')
  WHERE state = 'GA' AND year_month = '2025-01'
),
benchmarks AS (
  SELECT *
  FROM read_parquet('prod_etl/core/data/silver/benchmarks/bench_medicare_comprehensive.parquet')
  WHERE state = 'GA' AND year_month = '2025-01' AND benchmark_type = 'professional'
)
SELECT 
  f.reporting_entity_name,
  f.code,
  f.negotiated_rate,
  b.medicare_prof_stateavg,
  f.negotiated_rate / NULLIF(b.medicare_prof_stateavg, 0) AS pct_of_medicare
FROM fact f
LEFT JOIN benchmarks b ON (
  b.state = f.state AND 
  b.year_month = f.year_month AND 
  b.code_type = f.code_type AND 
  b.code = f.code
)
WHERE b.medicare_prof_stateavg IS NOT NULL
ORDER BY pct_of_medicare DESC;
```

## Rate Types Available

### Professional Rates
- `medicare_prof_national` - National average professional rate
- `medicare_prof_stateavg` - State-averaged professional rate

### OPPS Rates  
- `medicare_opps_national` - National OPPS rate
- `medicare_opps_stateavg` - State-averaged OPPS rate

### ASC Rates
- `medicare_asc_national` - National ASC rate  
- `medicare_asc_stateavg` - State-averaged ASC rate

## Configuration

The script can be configured via command line arguments:

```bash
python build_medicare_benchmarks.py --help
```

Options:
- `--db-path`: Path to Medicare SQLite database (default: `data/raw/medicare_test.db`)
- `--year`: Year for Medicare data (default: 2025)
- `--output-dir`: Output directory (default: `prod_etl/core/data/silver/benchmarks`)

## Data Sources

The script reads from the following SQLite database tables:
- `medicare_locality_map` - ZIP to locality mapping
- `medicare_locality_meta` - Locality metadata
- `cms_gpci` - Geographic Practice Cost Index
- `cms_rvu` - Relative Value Units
- `cms_conversion_factor` - Medicare conversion factor
- `asc_addendum_aa` - ASC rates
- `opps_addendum_b` - OPPS rates
- `cbsa_wage_index` - Wage index data
- `zip_cbsa` - ZIP to CBSA mapping

## Maintenance

- **Annual Updates**: Rebuild when new Medicare data is released
- **Validation**: Compare against official CMS published rates
- **Performance**: Optimize parquet files for query performance

## Schema Documentation

See `MEDICARE_BENCHMARK_SCHEMA.md` for detailed schema documentation.

## Troubleshooting

### Database Not Found
```
FileNotFoundError: Database file not found: data/raw/medicare_test.db
```
**Solution**: Ensure the Medicare SQLite database exists at the specified path.

### No Benchmark Tables Found
```
Benchmark table not found: ...
```
**Solution**: Run `python build_medicare_benchmarks.py` first to create the benchmark tables.

### Memory Issues
For large datasets, consider processing in chunks or increasing available memory.

## Performance Notes

- **Build Time**: ~5-10 minutes for full benchmark tables
- **File Sizes**: 
  - Professional: ~50-100MB
  - OPPS: ~20-50MB  
  - ASC: ~10-30MB
  - Comprehensive: ~80-180MB
- **Query Performance**: Optimized parquet format for fast analytical queries
