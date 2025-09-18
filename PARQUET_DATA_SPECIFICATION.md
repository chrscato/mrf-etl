# Commercial Rates Parquet Data Specification

This document outlines the expected structure, columns, and formatting requirements for the commercial rates parquet data files used in the WorkComp Rates application.

## File Naming Convention

- **State-specific files**: `commercial_rates_{STATE_CODE}.parquet` (e.g., `commercial_rates_GA.parquet`)
- **Default file**: `commercial_rates.parquet`
- **Location**: `core/data/` directory

## Required Columns

The parquet file must contain the following columns with the specified data types and formats:

### Core Rate Information
| Column Name | Data Type | Description | Required | Example |
|-------------|-----------|-------------|----------|---------|
| `rate` | Float64 | Commercial rate amount | Yes | 125.50 |
| `negotiated_type` | String | Type of negotiated rate | Yes | "negotiated" |
| `billing_class` | String | Classification of billing | Yes | "professional" or "institutional" |
| `service_codes` | String | Service code identifiers | No | "99213,99214" |
| `billing_code` | String | CPT/HCPCS billing code | Yes | "99213" |
| `billing_code_type` | String | Type of billing code | No | "CPT" |
| `code_desc` | String | Description of the billing code | Yes | "Office visit" |
| `name` | String | Provider or service name | No | "Dr. Smith" |
| `negotiation_arrangement` | String | Type of negotiation | No | "fee-for-service" |
| `payer` | String | Insurance company name | Yes | "Blue Cross Blue Shield" |
| `payer_type` | String | Type of payer | No | "commercial" |
| `rate_updated_on` | String | Date when rate was updated | No | "2024-01-15" |

### Provider Information
| Column Name | Data Type | Description | Required | Example |
|-------------|-----------|-------------|----------|---------|
| `prov_npi` | String | National Provider Identifier | No | "1234567890" |
| `tin_type` | String | Tax ID Number type | No | "EIN" |
| `tin_value` | String | Tax ID Number value | Yes | "123456789" |
| `org_name` | String | Organization name | Yes | "General Hospital" |
| `status` | String | Provider status | No | "active" |
| `primary_taxonomy_code` | String | Primary taxonomy code | Yes | "207RC0000X" |
| `primary_taxonomy_desc` | String | Primary taxonomy description | Yes | "Cardiology" |
| `city` | String | Provider city | No | "Atlanta" |
| `state` | String | Provider state | No | "GA" |
| `postal_code` | String | Provider ZIP code | No | "30309" |
| `prov_lat` | Float64 | Provider latitude | No | 33.7490 |
| `prov_lng` | Float64 | Provider longitude | No | -84.3880 |
| `cbsa` | String | Core Based Statistical Area | No | "12060" |

### Medicare Reference Data
| Column Name | Data Type | Description | Required | Example |
|-------------|-----------|-------------|----------|---------|
| `medicare_prof` | Float64 | Medicare professional rate | No | 85.25 |
| `state_up` | String | State update indicator | No | "Y" |
| `billing_code_norm` | String | Normalized billing code | No | "99213" |
| `state_wage_index_avg` | Float64 | State wage index average | No | 0.95 |
| `opps_weight` | Float64 | OPPS weight | No | 1.25 |
| `opps_si` | String | OPPS status indicator | No | "1" |
| `opps_short_desc` | String | OPPS short description | No | "Office visit" |
| `asc_pi` | String | ASC payment indicator | No | "1" |
| `asc_nat_rate` | Float64 | ASC national rate | No | 150.00 |
| `asc_short_desc` | String | ASC short description | No | "Surgery" |

### Georgia-Specific MAR Data
| Column Name | Data Type | Description | Required | Example |
|-------------|-----------|-------------|----------|---------|
| `GA_PROF_MAR` | Float64 | Georgia Professional MAR | No | 95.50 |
| `GA_OP_MAR` | Float64 | Georgia Outpatient MAR | No | 450.00 |
| `GA_ASC_MAR` | Float64 | Georgia ASC MAR | No | 350.00 |
| `medicare_opps_mar_national` | Float64 | Medicare OPPS MAR National | No | 400.00 |
| `medicare_asc_mar_national` | Float64 | Medicare ASC MAR National | No | 300.00 |
| `opps_adj_factor_stateavg` | Float64 | OPPS adjustment factor state avg | No | 0.95 |
| `asc_adj_factor_stateavg` | Float64 | ASC adjustment factor state avg | No | 0.90 |
| `medicare_opps_mar_stateavg` | Float64 | Medicare OPPS MAR state average | No | 380.00 |
| `medicare_asc_mar_stateavg` | Float64 | Medicare ASC MAR state average | No | 270.00 |

### Procedure Classification
| Column Name | Data Type | Description | Required | Example |
|-------------|-----------|-------------|----------|---------|
| `procedure_set` | String | Procedure set category | Yes | "Cardiology" |
| `procedure_class` | String | Procedure class | Yes | "Professional" or "Facility" |
| `procedure_group` | String | Procedure group | Yes | "Cardiac" |

## Data Quality Requirements

### Required Values
- `rate`: Must be numeric (can be NULL for missing rates)
- `billing_class`: Must be either "professional" or "institutional"
- `payer`: Cannot be NULL or empty
- `org_name`: Cannot be NULL or empty
- `tin_value`: Cannot be NULL or empty
- `primary_taxonomy_code`: Cannot be NULL or empty
- `primary_taxonomy_desc`: Cannot be NULL or empty
- `procedure_set`: Cannot be NULL or empty
- `procedure_class`: Cannot be NULL or empty
- `procedure_group`: Cannot be NULL or empty

### Data Validation Rules
1. **Rate Values**: Should be positive numbers when present
2. **Billing Class**: Must match exactly "professional" or "institutional"
3. **Procedure Class**: Must match exactly "Professional" or "Facility"
4. **Taxonomy Descriptions**: Should not contain blacklisted terms (see taxonomy_blacklist.txt)
5. **Geographic Data**: Latitude/longitude should be valid coordinates if provided
6. **CBSA Codes**: Should be valid 5-digit CBSA codes if provided

### Special Handling
- **Hospital Classification**: Records with `primary_taxonomy_desc` containing "Hospital" are treated as hospital facilities
- **Rate Filtering**: The application filters out rates below certain thresholds for quality analysis
- **Geographic Analysis**: CBSA codes are used for metropolitan vs. rural comparisons

## File Format Requirements

### Parquet Format
- **Format**: Apache Parquet
- **Compression**: Recommended to use snappy or gzip compression
- **Schema**: All columns should be properly typed (no mixed types)
- **Encoding**: UTF-8 for string columns

### Performance Considerations
- **File Size**: Large files (>1GB) are supported with chunked processing
- **Memory Usage**: Application uses DuckDB for efficient querying
- **Indexing**: Consider partitioning by state or billing_class for large datasets

## Sample Data Structure

```python
# Example of expected data structure
{
    'rate': 125.50,
    'billing_class': 'professional',
    'payer': 'Blue Cross Blue Shield',
    'org_name': 'General Hospital',
    'tin_value': '123456789',
    'primary_taxonomy_code': '207RC0000X',
    'primary_taxonomy_desc': 'Cardiology',
    'procedure_set': 'Cardiology',
    'procedure_class': 'Professional',
    'procedure_group': 'Cardiac',
    'billing_code': '99213',
    'code_desc': 'Office visit',
    'cbsa': '12060',
    'GA_PROF_MAR': 95.50,
    'medicare_prof': 85.25
}
```

## ETL Process Guidelines

### Data Transformation
1. **Standardize Text Fields**: Convert to uppercase/lowercase as needed
2. **Handle Missing Values**: Use NULL for missing data, not empty strings
3. **Validate Required Fields**: Ensure all required columns are present
4. **Type Conversion**: Ensure numeric fields are properly typed
5. **Deduplication**: Remove duplicate records if present

### Quality Checks
1. **Rate Validation**: Check for reasonable rate ranges
2. **Taxonomy Validation**: Verify taxonomy codes and descriptions
3. **Geographic Validation**: Validate state codes and coordinates
4. **Reference Data**: Ensure Medicare reference data is current

### File Generation
1. **Naming**: Use correct state-specific naming convention
2. **Location**: Place in `core/data/` directory
3. **Backup**: Keep backup of previous versions
4. **Testing**: Validate with application before deployment

## Troubleshooting

### Common Issues
1. **Missing Columns**: Application will log warnings for missing required columns
2. **Type Mismatches**: Ensure numeric columns are properly typed
3. **Encoding Issues**: Use UTF-8 encoding for text fields
4. **File Size**: Large files may require chunked processing

### Validation Tools
- Use the notebook `edit_GA.ipynb` for data exploration
- Check application logs for data loading errors
- Validate with sample queries before full deployment

## Support

For questions about data format or ETL process issues, refer to:
- Application logs in `logs/django.log`
- Sample data generation in `core/utils/parquet_utils.py`
- Data exploration notebooks in `notebooks/` directory
