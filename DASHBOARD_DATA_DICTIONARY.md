# Dashboard Data Dictionary - MRF ETL Pipeline

## Overview

This document provides a comprehensive guide for presenting MRF (Machine Readable Files) data in dashboard-friendly formats. The data model follows a star schema with a skinny fact table and supporting dimension tables optimized for analytical queries.

## Data Architecture

### Current Structure
- **Gold Layer**: `fact_rate.parquet` - Single skinny fact table with all negotiated rates
- **Silver Layer**: Dimension tables and cross-reference tables for lookups
- **Partitioning**: Currently using single file approach, with partitioned structure available for future scaling

## Core Data Tables

### 1. Fact Table: `fact_rate.parquet`

**Purpose**: Central table containing all negotiated rate transactions

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `fact_uid` | string | Unique hash identifier for deduplication | Primary key, not displayed |
| `state` | string | State code (e.g., 'GA') | Filter dimension |
| `year_month` | string | YYYY-MM format (e.g., '2025-08') | Time dimension |
| `reporting_entity_name` | string | Raw payer name from MRF | Payer dimension |
| `code_type` | string | Code classification (CPT, HCPCS, REV) | Code type filter |
| `code` | string | Procedure/service code (e.g., '10121') | Primary analysis dimension |
| `pg_uid` | string | Provider group unique identifier | Provider grouping |
| `pos_set_id` | string | Place of service set identifier | POS filtering |
| `negotiated_type` | string | Rate type (e.g., 'negotiated') | Rate type filter |
| `negotiation_arrangement` | string | Arrangement type (e.g., 'ffs') | Arrangement filter |
| `negotiated_rate` | float | The actual rate amount | Primary metric |
| `expiration_date` | string/null | Rate expiration (null if 9999-12-31) | Validity filter |
| `provider_group_id_raw` | string/int | Raw provider group ID from source | Lineage tracking |

**Dashboard Applications**:
- Rate analysis by procedure code
- Payer comparison views
- Time series analysis
- Geographic analysis by state

### 2. Dimension Tables (Silver Layer)

#### `dim_code.parquet`
**Purpose**: Human-readable descriptions for procedure codes

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `code_type` | string | Code classification | Join key |
| `code` | string | Procedure code | Join key |
| `code_desc` | string | Short description | Display label |
| `name` | string | Full procedure name | Detailed description |

**Join Pattern**: `(code_type, code)` → fact table
**Dashboard Usage**: Procedure name lookups, search functionality

#### `dim_pos_set.parquet`
**Purpose**: Place of service code sets

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `pos_set_id` | string | Unique identifier for POS set | Join key |
| `service_codes` | list[string] | Array of POS codes | POS filtering |

**Join Pattern**: `pos_set_id` → fact table
**Dashboard Usage**: Filter by specific places of service

#### `xref_pos.parquet`
**Purpose**: Exploded POS set for single POS filtering

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `pos_set_id` | string | POS set identifier | Join key |
| `pos` | string | Individual POS code | Filter dimension |

**Join Pattern**: `pos_set_id` → fact table
**Dashboard Usage**: Filter by specific POS (e.g., office=11, hospital=21)

#### `dim_reporting_entity.parquet`
**Purpose**: Payer information and metadata

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `payer_slug` | string | URL-friendly payer identifier | Join key |
| `reporting_entity_name` | string | Full payer name | Display label |
| `reporting_entity_type` | string | Payer type (e.g., 'Insurer') | Payer categorization |
| `version` | string | Data version | Data lineage |

**Join Pattern**: `slugify(reporting_entity_name)` → `payer_slug`
**Dashboard Usage**: Payer selection, payer type filtering

#### `dim_npi.parquet`
**Purpose**: Provider information from NPPES registry

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `npi` | string | National Provider Identifier | Join key |
| `organization_name` | string | Provider organization name | Display label |
| `first_name` | string | Individual provider first name | Individual provider info |
| `last_name` | string | Individual provider last name | Individual provider info |
| `enumeration_type` | string | Provider type (Individual/Organization) | Provider categorization |
| `primary_taxonomy_code` | string | Primary specialty code | Specialty filtering |
| `primary_taxonomy_desc` | string | Specialty description | Specialty display |
| `status` | string | Provider status | Active/inactive filtering |
| `nppes_fetch_date` | string | Last NPPES update | Data freshness |

**Join Pattern**: Via `xref_group_npi` table
**Dashboard Usage**: Provider search, specialty filtering, provider details

#### `dim_tin.parquet`
**Purpose**: Tax Identification Number information

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `tin_type` | string | TIN type (EIN, SSN, etc.) | TIN categorization |
| `tin_value` | string | TIN value | Join key |

**Join Pattern**: Via `xref_group_tin` table
**Dashboard Usage**: Organization-level analysis

### 3. Cross-Reference Tables

#### `xref_group_npi.parquet`
**Purpose**: Links provider groups to individual NPIs

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `year_month` | string | Time period | Join key |
| `payer_slug` | string | Payer identifier | Join key |
| `pg_uid` | string | Provider group identifier | Join key |
| `npi` | string | Individual NPI | Join to dim_npi |

**Join Pattern**: `(year_month, payer_slug, pg_uid)` → fact table
**Dashboard Usage**: Provider-level rate analysis

#### `xref_group_tin.parquet`
**Purpose**: Links provider groups to TINs

| Column | Type | Description | Dashboard Usage |
|--------|------|-------------|-----------------|
| `year_month` | string | Time period | Join key |
| `payer_slug` | string | Payer identifier | Join key |
| `pg_uid` | string | Provider group identifier | Join key |
| `tin_type` | string | TIN type | Join key |
| `tin_value` | string | TIN value | Join to dim_tin |

**Join Pattern**: `(year_month, payer_slug, pg_uid)` → fact table
**Dashboard Usage**: Organization-level rate analysis

## Dashboard Query Patterns

### 1. Basic Rate Analysis
```sql
-- Get rates with procedure descriptions
SELECT 
    f.reporting_entity_name,
    f.code_type,
    f.code,
    d.code_desc,
    f.negotiated_rate,
    f.year_month
FROM fact_rate f
LEFT JOIN dim_code d ON d.code_type = f.code_type AND d.code = f.code
WHERE f.state = 'GA' AND f.year_month = '2025-08'
```

### 2. Payer Comparison
```sql
-- Compare rates across payers for specific procedure
SELECT 
    f.reporting_entity_name,
    f.negotiated_rate,
    COUNT(*) as rate_count,
    AVG(f.negotiated_rate) as avg_rate,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.negotiated_rate) as median_rate
FROM fact_rate f
WHERE f.state = 'GA' 
    AND f.year_month = '2025-08'
    AND f.code = '10121'
    AND f.code_type = 'CPT'
GROUP BY f.reporting_entity_name
```

### 3. Provider-Level Analysis
```sql
-- Get provider details for rates
SELECT 
    f.reporting_entity_name,
    f.code,
    d.code_desc,
    f.negotiated_rate,
    n.organization_name,
    n.primary_taxonomy_desc
FROM fact_rate f
LEFT JOIN dim_code d ON d.code_type = f.code_type AND d.code = f.code
LEFT JOIN xref_group_npi x ON x.year_month = f.year_month 
    AND x.payer_slug = slugify(f.reporting_entity_name)
    AND x.pg_uid = f.pg_uid
LEFT JOIN dim_npi n ON n.npi = x.npi
WHERE f.state = 'GA' AND f.year_month = '2025-08'
```

### 4. Place of Service Filtering
```sql
-- Filter by specific POS (e.g., office visits)
SELECT 
    f.reporting_entity_name,
    f.code,
    f.negotiated_rate
FROM fact_rate f
JOIN xref_pos p ON p.pos_set_id = f.pos_set_id
WHERE f.state = 'GA' 
    AND f.year_month = '2025-08'
    AND p.pos = '11'  -- Office
```

## Dashboard-Friendly Data Views

### 1. Rate Summary View
**Purpose**: High-level rate statistics for dashboard KPIs

| Metric | Calculation | Use Case |
|--------|-------------|----------|
| Total Rates | COUNT(*) | Volume indicator |
| Average Rate | AVG(negotiated_rate) | Market overview |
| Median Rate | PERCENTILE_CONT(0.5) | Typical rate |
| Rate Range | MIN/MAX(negotiated_rate) | Market spread |
| Unique Procedures | COUNT(DISTINCT code) | Procedure diversity |
| Unique Payers | COUNT(DISTINCT reporting_entity_name) | Market coverage |

### 2. Payer Performance View
**Purpose**: Payer-specific metrics and comparisons

| Dimension | Metric | Description |
|-----------|--------|-------------|
| Payer Name | Rate Count | Number of negotiated rates |
| Payer Name | Avg Rate | Average negotiated rate |
| Payer Name | Rate Coverage | % of procedures covered |
| Payer Name | Rate Trend | Month-over-month change |

### 3. Procedure Analysis View
**Purpose**: Procedure-specific insights

| Dimension | Metric | Description |
|-----------|--------|-------------|
| Procedure Code | Rate Distribution | Min/Max/Avg/Median rates |
| Procedure Code | Payer Count | Number of payers with rates |
| Procedure Code | Geographic Coverage | States with rates |
| Procedure Code | Rate Volatility | Standard deviation of rates |

### 4. Provider Network View
**Purpose**: Provider and organization insights

| Dimension | Metric | Description |
|-----------|--------|-------------|
| Provider Group | Rate Count | Number of negotiated rates |
| Provider Group | Avg Rate | Average negotiated rate |
| Provider Group | Specialty Mix | Distribution of specialties |
| Provider Group | Geographic Coverage | States served |

## Performance Optimization Recommendations

### 1. Indexing Strategy
- **Primary Indexes**: `(state, year_month)` for time-based filtering
- **Secondary Indexes**: `(code_type, code)` for procedure filtering
- **Tertiary Indexes**: `(reporting_entity_name)` for payer filtering

### 2. Materialized Views
Consider creating materialized views for common dashboard queries:
- `mv_rate_summary_by_payer` - Payer-level aggregations
- `mv_rate_summary_by_procedure` - Procedure-level aggregations
- `mv_provider_rate_summary` - Provider-level aggregations

### 3. Partitioning Strategy
For future scaling, consider:
- **Monthly partitions**: `year_month=2025-08/`
- **State partitions**: `state=GA/`
- **Combined**: `state=GA/year_month=2025-08/`

### 4. Caching Strategy
- Cache dimension tables (they change infrequently)
- Cache aggregated views for common time periods
- Use Redis/Memcached for frequently accessed payer/procedure combinations

## Dashboard Implementation Guidelines

### 1. Data Loading
- Load dimension tables first (small, stable)
- Load fact table with appropriate filters
- Use lazy loading for large datasets

### 2. Filtering Strategy
- Always filter by `state` and `year_month` first
- Use dimension table lookups for user-friendly filters
- Implement progressive disclosure (start broad, narrow down)

### 3. Aggregation Levels
- **Level 1**: Payer + Procedure (most common)
- **Level 2**: Payer + Procedure + Provider Group
- **Level 3**: Individual Provider + Procedure
- **Level 4**: Individual Rate (drill-down)

### 4. User Experience
- Provide clear procedure descriptions
- Show payer names in user-friendly format
- Include data freshness indicators
- Implement search and autocomplete for codes/procedures

## Data Quality Considerations

### 1. Validation Rules
- All rates must have valid procedure codes
- All rates must have valid payer information
- All rates must have valid provider group references
- Expiration dates should be validated

### 2. Data Freshness
- Track `last_updated_on` from source MRF files
- Monitor NPPES fetch dates for provider data
- Implement data staleness alerts

### 3. Completeness Metrics
- Track coverage by payer
- Track coverage by procedure
- Track coverage by provider
- Monitor missing dimension lookups

## API Endpoints for Dashboard

### 1. Rate Analysis
- `GET /api/rates/summary?state=GA&year_month=2025-08`
- `GET /api/rates/by-payer?state=GA&year_month=2025-08&payer=UnitedHealthcare`
- `GET /api/rates/by-procedure?state=GA&year_month=2025-08&code=10121`

### 2. Provider Analysis
- `GET /api/providers/search?q=organization_name`
- `GET /api/providers/rates?npi=1234567890&state=GA`
- `GET /api/providers/by-specialty?specialty=Internal Medicine`

### 3. Benchmarking
- `GET /api/benchmarks/medicare?state=GA&year_month=2025-08&code=10121`
- `GET /api/benchmarks/comparison?state=GA&year_month=2025-08&code=10121`

This data dictionary provides the foundation for building comprehensive, performant dashboards that can effectively present MRF data to end users while maintaining data integrity and query performance.
