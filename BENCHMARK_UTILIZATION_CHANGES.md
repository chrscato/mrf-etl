# Medicare Benchmark Utilization Changes

## Overview

This document describes the changes made to implement the new Medicare benchmark utilization approach where benchmarks are applied **after** the dataset is filtered down, based on the `billing_class` field.

## Key Changes

### 1. New Benchmark Joiner (`src/transformers/benchmark_joiner.py`)

Created a new transformer that handles Medicare benchmark joining based on billing class:

- **Professional Services** (`billing_class = 'professional'`): Joins with Medicare professional benchmarks
- **Institutional Services** (`billing_class = 'institutional'`): Joins with Medicare OPPS and ASC benchmarks

#### Key Features:
- Loads benchmark tables once for efficiency
- Joins benchmarks based on `(state, year_month, code_type, code)` keys
- Calculates percentage of Medicare rates
- Provides coverage statistics
- Handles both professional and institutional billing classes

### 2. Updated ETL Pipeline Flow

#### Before (Old Approach):
```
1. Extract & Merge Data
2. Filter by Billing Codes
3. Column Transformations
4. NPPES Enrichment
5. Geocoding
6. GA WC Processing
7. **Medicare Professional Rate Calculation** ← Benchmarks calculated here
8. **Medicare Facility Rate Calculation** ← Benchmarks calculated here
9. Procedure Categorization
10. Final Loading
```

#### After (New Approach):
```
1. Extract & Merge Data
2. Filter by Billing Codes
3. Column Transformations
4. NPPES Enrichment
5. Geocoding
6. GA WC Processing
7. Procedure Categorization
8. **Medicare Benchmark Joining** ← Benchmarks applied here based on billing_class
9. Final Loading
```

### 3. Modified Files

#### `main_refactored.py`:
- Added `BenchmarkJoiner` import and initialization
- Removed Medicare calculation phases
- Added benchmark joining phase after procedure categorization
- Updated pipeline flow to apply benchmarks after filtering

#### `main.py`:
- Added `BenchmarkJoiner` import and initialization
- Removed Medicare calculation phases from pipeline phases list
- Added benchmark joining phase after procedure categorization
- Updated pipeline flow to apply benchmarks after filtering

### 4. Benchmark Application Logic

#### Professional Services (`billing_class = 'professional'`):
- Joins with `bench_medicare_professional.parquet`
- Adds columns: `medicare_prof_national`, `medicare_prof_stateavg`, `pct_of_medicare`
- Uses state-averaged professional rates for benchmarking

#### Institutional Services (`billing_class = 'institutional'`):
- Joins with `bench_medicare_opps.parquet` and `bench_medicare_asc.parquet`
- Adds columns: `medicare_opps_national`, `medicare_opps_stateavg`, `medicare_asc_national`, `medicare_asc_stateavg`
- Calculates: `pct_of_medicare_opps`, `pct_of_medicare_asc`
- Uses both OPPS and ASC benchmarks for comprehensive institutional benchmarking

## Benefits of New Approach

1. **Efficiency**: Benchmarks are pre-calculated and stored in dimension tables
2. **Consistency**: Uses standardized benchmark tables across all processing
3. **Flexibility**: Easy to update benchmarks without recalculating rates
4. **Separation of Concerns**: Rate calculation and benchmark joining are separate processes
5. **Better Performance**: No need to calculate Medicare rates on-the-fly during ETL

## Usage

### Running the Updated Pipeline

The pipeline now uses the new benchmark approach automatically:

```bash
# Run the refactored pipeline
python main_refactored.py

# Or run the original pipeline (also updated)
python main.py
```

### Testing the Implementation

A test script is provided to verify the new approach:

```bash
python test_benchmark_approach.py
```

### Prerequisites

Ensure benchmark tables are built before running the pipeline:

```bash
python build_medicare_benchmarks.py
```

This creates the required benchmark tables in `prod_etl/core/data/silver/benchmarks/`:
- `bench_medicare_professional.parquet`
- `bench_medicare_opps.parquet`
- `bench_medicare_asc.parquet`

## Data Flow

1. **Dataset Filtering**: Data is filtered down to relevant records
2. **Billing Class Identification**: Each record has a `billing_class` field
3. **Benchmark Selection**: 
   - `professional` → Medicare professional benchmarks
   - `institutional` → Medicare OPPS and ASC benchmarks
4. **Benchmark Joining**: Benchmarks are joined based on state, year_month, code_type, and code
5. **Rate Comparison**: Percentage of Medicare rates are calculated

## Output Columns

The new approach adds the following columns to the final dataset:

### Professional Services:
- `medicare_prof_national`: National Medicare professional rate
- `medicare_prof_stateavg`: State-averaged Medicare professional rate
- `pct_of_medicare`: Negotiated rate as percentage of Medicare
- `work_rvu`, `practice_expense_rvu`, `malpractice_rvu`: RVU components
- `conversion_factor`: Medicare conversion factor

### Institutional Services:
- `medicare_opps_national`: National OPPS rate
- `medicare_opps_stateavg`: State-averaged OPPS rate
- `medicare_asc_national`: National ASC rate
- `medicare_asc_stateavg`: State-averaged ASC rate
- `pct_of_medicare_opps`: Negotiated rate as percentage of OPPS Medicare
- `pct_of_medicare_asc`: Negotiated rate as percentage of ASC Medicare
- `opps_weight`, `asc_pi`: OPPS/ASC specific fields

## Migration Notes

- The `MedicareCalculator` class is no longer used in the main pipeline flow
- Benchmark tables must be built before running the ETL pipeline
- The new approach requires the dataset to have proper `billing_class` values
- All existing data processing steps remain the same until the benchmark joining phase




