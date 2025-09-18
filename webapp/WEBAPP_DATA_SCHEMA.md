# Webapp Data Schema — Simplified MRF Dashboard Schema

This document defines the simplified data schema for the MRF webapp dashboard, focusing on the specific filter fields and search capabilities.

---

## Schema Overview

The webapp uses a simplified view of the MRF data model, focusing on the most commonly used filter fields for rate analysis and provider lookup.

**Core Architecture:**
- **Central Fact Table**: `fact_rate` with negotiated rates and basic identifiers
- **Provider Dimensions**: NPI details, organization info, and taxonomy data
- **Procedure Dimensions**: Code categorizations and billing classifications
- **Payer Dimensions**: Insurance company information
- **Cross-References**: TIN and NPI mappings to provider groups

---

## Filter Fields Schema

### Primary Filter Fields

| Field | Source Table | Type | Description | Filter Type |
|-------|-------------|------|-------------|-------------|
| `primary_taxonomy_desc` | `dim_npi` | String | Provider specialty/taxonomy | Exact match |
| `organization_name` | `dim_npi` | String | Provider organization name | **Partial lookup** |
| `npi` | `dim_npi` | String | National Provider Identifier | Exact match |
| `enumeration_type` | `dim_npi` | String | Individual (1) or Organization (2) | Exact match |
| `billing_class` | `fact_rate` | String | Professional, facility, etc. | Exact match |
| `proc_set` | `dim_code_cat` | String | Procedure set/group | Exact match |
| `proc_class` | `dim_code_cat` | String | Procedure class/category | Exact match |
| `proc_group` | `dim_code_cat` | String | High-level procedure group | Exact match |
| `billing_code` | `fact_rate` | String | Procedure code (CPT, HCPCS) | Exact match |
| `tin_value` | `xref_pg_member_tin` | String | Tax Identification Number | Exact match |
| `payer` | `fact_rate` | String | Payer name (reporting_entity_name) | Exact match |
| `state` | `fact_rate` | String | State abbreviation | Exact match |

---

## Core Tables for Webapp

### 1. `fact_rate` (Central Fact Table)

**Location:** `prod_etl/core/data/gold/fact_rate.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `fact_uid` | String | Unique fact identifier | "2c6a6a035d7ab8b5558f9b422ace9a32" |
| `state` | String | State abbreviation | "GA" |
| `year_month` | String | Year-month (YYYY-MM) | "2025-08" |
| `payer_slug` | String | Normalized payer identifier | "unitedhealthcare-of-georgia-inc" |
| `billing_class` | String | Billing class | "professional" |
| `code_type` | String | Code system (CPT, HCPCS, etc.) | "CPT" |
| `code` | String | Procedure code | "33216" |
| `pg_uid` | String | Provider group unique identifier | "049049fa50d881db5db61293fa01cb5e" |
| `pos_set_id` | String | Place of service set identifier | "d41d8cd98f00b204e9800998ecf8427e" |
| `negotiated_type` | String | Rate type (negotiated, fee schedule, etc.) | "negotiated" |
| `negotiation_arrangement` | String | Contract arrangement type | "ffs" |
| `negotiated_rate` | Float64 | **The negotiated rate amount** | 752.23 |
| `expiration_date` | String | Rate expiration date | "9999-12-31" |
| `provider_group_id_raw` | String | Original provider group ID | "222" |
| `reporting_entity_name` | String | Full payer name | "UnitedHealthcare of Georgia Inc." |

**Primary Key:** `fact_uid`

---

### 2. `dim_npi` (Provider Information)

**Location:** `prod_etl/core/data/dims/dim_npi.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `npi` | String | National Provider Identifier | "1003070913" |
| `first_name` | String | Provider first name (23.48% null) | "STEPHANIE" |
| `last_name` | String | Provider last name (23.48% null) | "LEONI" |
| `organization_name` | String | Organization name (76.52% null) | "HEADWAY COLORADO BEHAVIORAL HEALTH SERVICES, INC." |
| `enumeration_type` | String | NPI type (NPI-1 individual, NPI-2 organization) | "NPI-1" |
| `status` | String | Provider status | "A" |
| `primary_taxonomy_code` | String | Primary specialty code | "101YP2500X" |
| `primary_taxonomy_desc` | String | Primary specialty description | "Counselor, Professional" |
| `primary_taxonomy_state` | String | License state (19.36% null) | "GA" |
| `primary_taxonomy_license` | String | License number (20.35% null) | "LPC005043" |
| `credential` | String | Professional credential (31.47% null) | "LPC" |
| `sole_proprietor` | String | Sole proprietor status (23.48% null) | "YES" |
| `enumeration_date` | String | NPI enumeration date | "2008-07-10" |
| `last_updated` | String | Last update date | "2021-09-03" |
| `nppes_fetched` | Boolean | Whether NPPES data was fetched | True |
| `nppes_fetch_date` | String | NPPES fetch date | "2021-09-03" |
| `replacement_npi` | String | Replacement NPI (100% null) | null |

**Primary Key:** `npi`

---

### 3. `dim_code_cat` (Procedure Categorization)

**Location:** `prod_etl/core/data/dims/dim_code_cat.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `proc_cd` | String | Procedure code | "99201" |
| `proc_set` | String | High-level procedure category | "Evaluation and Management" |
| `proc_class` | String | Procedure class | "Office/ outpatient services" |
| `proc_group` | String | Specific procedure group | "New office visits" |

**Primary Key:** `proc_cd`

**Join to fact_rate:** `dim_code_cat.proc_cd = fact_rate.code`

---

### 4. `xref_pg_member_tin` (TIN Cross-Reference)

**Location:** `prod_etl/core/data/xrefs/xref_pg_member_tin.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `pg_uid` | String | Provider group unique identifier | "11ce3cbdcf491bc5ea76386e84a55b4d" |
| `tin_type` | String | TIN type | "ein" |
| `tin_value` | String | Tax ID number | "881009565" |

**Primary Key:** (`pg_uid`, `tin_value`)

**Join to fact_rate:** `xref_pg_member_tin.pg_uid = fact_rate.pg_uid`

---

### 5. `xref_pg_member_npi` (NPI Cross-Reference)

**Location:** `prod_etl/core/data/xrefs/xref_pg_member_npi.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `pg_uid` | String | Provider group unique identifier | "33c5ebf41b7fe9461b8ccf3202cb6604" |
| `npi` | String | National Provider Identifier | "1780875781" |

**Primary Key:** (`pg_uid`, `npi`)

**Join to fact_rate:** `xref_pg_member_npi.pg_uid = fact_rate.pg_uid`

---

### 6. `dim_code` (Procedure Codes)

**Location:** `prod_etl/core/data/dims/dim_code.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `code_type` | String | Code system type | "CPT" |
| `code` | String | Procedure code | "31614" |
| `code_description` | String | Full procedure description | "Tracheostoma revision; complex, with flap rotation" |
| `code_name` | String | Shortened procedure name | "TRACHEOSTOMA REVJ CPLX W/FLAP ROTATION" |

**Primary Key:** (`code_type`, `code`)

**Join to fact_rate:** `dim_code.code_type = fact_rate.code_type AND dim_code.code = fact_rate.code`

---

### 7. `dim_payer` (Insurance Payer Information)

**Location:** `prod_etl/core/data/dims/dim_payer.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `payer_slug` | String | Normalized payer identifier | "unitedhealthcare-of-georgia-inc" |
| `reporting_entity_name` | String | Full payer name | "UnitedHealthcare of Georgia Inc." |
| `version` | String | Data version | "1.0.0" |

**Primary Key:** `payer_slug`

**Join to fact_rate:** `dim_payer.payer_slug = fact_rate.payer_slug`

---

### 8. `dim_provider_group` (Provider Group Information)

**Location:** `prod_etl/core/data/dims/dim_provider_group.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `pg_uid` | String | Provider group unique identifier | "b8b29688e92394ea7cc3d736446337d0" |
| `payer_slug` | String | Associated payer | "unitedhealthcare-of-georgia-inc" |
| `provider_group_id_raw` | Int64 | Original provider group ID | 772 |
| `version` | String | Data version | "1.0.0" |

**Primary Key:** `pg_uid`

**Join to fact_rate:** `dim_provider_group.pg_uid = fact_rate.pg_uid`

---

### 9. `dim_npi_address` (Provider Address Information)

**Location:** `prod_etl/core/data/dims/dim_npi_address.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `npi` | String | National Provider Identifier | "1235233776" |
| `address_purpose` | String | Address type (MAILING/LOCATION) | "MAILING" |
| `address_type` | String | Address format | "DOM" |
| `address_1` | String | Primary address line | "582 MOUNT GERIZIM RD SE" |
| `address_2` | String | Secondary address line (78.42% null) | "BLDG 400, STE 102" |
| `city` | String | City | "MABLETON" |
| `state` | String | State code | "GA" |
| `postal_code` | String | ZIP code | "301266410" |
| `country_code` | String | Country code | "US" |
| `telephone_number` | String | Phone number (5.19% null) | "4047301650" |
| `fax_number` | String | Fax number (45.35% null) | "7062868442" |
| `last_updated` | String | Last update date | "2007-07-08" |
| `address_hash` | String | Unique address identifier | "cd0237207cae95b80fa11879df9fb182" |

**Primary Key:** (`npi`, `address_hash`)

**Join to dim_npi:** `dim_npi_address.npi = dim_npi.npi`

---

### 10. `dim_pos_set` (Place of Service Sets)

**Location:** `prod_etl/core/data/dims/dim_pos_set.parquet`

**Key Columns for Webapp:**
| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `pos_set_id` | String | Place of service set identifier | "17b00c58b3dcdb9c20cb2a70b52a4cc1" |
| `pos_members` | List[String] | List of place of service codes | ["02", "05", "06", "07", "08", "19", "21", "22", "23", "24", "26", "31", "34", "41", "42", "51", "52", "53", "56", "61"] |

**Primary Key:** `pos_set_id`

**Join to fact_rate:** `dim_pos_set.pos_set_id = fact_rate.pos_set_id`

---

## Webapp Data Relationships

### Simplified Join Pattern

```
fact_rate (central)
├── JOIN xref_pg_member_npi ON pg_uid
│   └── JOIN dim_npi ON npi
├── JOIN xref_pg_member_tin ON pg_uid
└── JOIN dim_code_cat ON code = proc_cd
```

### Key Relationships for Filters

1. **Provider Filters**: `fact_rate` → `xref_pg_member_npi` → `dim_npi`
   - `npi`, `organization_name`, `primary_taxonomy_desc`, `enumeration_type`

2. **Procedure Filters**: `fact_rate` → `dim_code_cat`
   - `billing_code` (from fact_rate.code), `proc_set`, `proc_class`, `proc_group`

3. **TIN Filters**: `fact_rate` → `xref_pg_member_tin`
   - `tin_value`

4. **Direct Filters**: From `fact_rate` directly
   - `state`, `billing_class`, `payer` (reporting_entity_name)

---

## Webapp Query Patterns

### 1. Multi-Filter Search (Primary Use Case)

```sql
SELECT 
    fr.fact_uid,
    fr.state,
    fr.year_month,
    fr.billing_class,
    fr.code as billing_code,
    fr.negotiated_rate,
    fr.reporting_entity_name as payer,
    -- Provider info
    n.npi,
    n.organization_name,
    n.first_name,
    n.last_name,
    n.primary_taxonomy_desc,
    n.enumeration_type,
    -- Procedure categorization
    cc.proc_set,
    cc.proc_class,
    cc.proc_group,
    -- TIN info
    xt.tin_value,
    xt.tin_type
FROM fact_rate fr
LEFT JOIN xref_pg_member_npi xn ON fr.pg_uid = xn.pg_uid
LEFT JOIN dim_npi n ON xn.npi = n.npi
LEFT JOIN xref_pg_member_tin xt ON fr.pg_uid = xt.pg_uid
LEFT JOIN dim_code_cat cc ON fr.code = cc.proc_cd
WHERE 1=1
  AND fr.state = 'GA'  -- Required filter
  AND fr.year_month = '2024-01'  -- Required filter
  -- Optional filters
  AND (n.primary_taxonomy_desc = 'Internal Medicine' OR n.primary_taxonomy_desc IS NULL)
  AND (n.organization_name ILIKE '%Medical%' OR n.organization_name IS NULL)
  AND (n.npi = '1234567890' OR n.npi IS NULL)
  AND (n.enumeration_type = '1' OR n.enumeration_type IS NULL)
  AND (fr.billing_class = 'professional' OR fr.billing_class IS NULL)
  AND (cc.proc_set = 'OFFICE_VISITS' OR cc.proc_set IS NULL)
  AND (cc.proc_class = 'EVALUATION_MANAGEMENT' OR cc.proc_class IS NULL)
  AND (cc.proc_group = 'PRIMARY_CARE' OR cc.proc_group IS NULL)
  AND (fr.code = '99213' OR fr.code IS NULL)
  AND (xt.tin_value = '123456789' OR xt.tin_value IS NULL)
  AND (fr.reporting_entity_name = 'Aetna Life Insurance Company' OR fr.reporting_entity_name IS NULL)
ORDER BY fr.negotiated_rate DESC
LIMIT 100;
```

### 2. Filter Value Lookups (For Dropdowns)

```sql
-- Get unique taxonomy descriptions
SELECT DISTINCT primary_taxonomy_desc 
FROM dim_npi 
WHERE primary_taxonomy_desc IS NOT NULL 
ORDER BY primary_taxonomy_desc;

-- Get unique organization names (for autocomplete)
SELECT DISTINCT organization_name 
FROM dim_npi 
WHERE organization_name IS NOT NULL 
  AND organization_name ILIKE '%search_term%'
ORDER BY organization_name;

-- Get unique procedure sets
SELECT DISTINCT proc_set 
FROM dim_code_cat 
WHERE proc_set IS NOT NULL 
ORDER BY proc_set;

-- Get unique payers
SELECT DISTINCT reporting_entity_name as payer
FROM fact_rate 
WHERE reporting_entity_name IS NOT NULL 
ORDER BY reporting_entity_name;
```

---

## Webapp Performance Optimizations

### Materialized Views for Fast Filtering

1. **Provider Search Index**: Pre-joined provider data
2. **Procedure Search Index**: Pre-joined procedure categorization
3. **TIN Search Index**: Pre-joined TIN data
4. **Comprehensive Search Index**: All filters in one view

### Indexing Strategy

- **Primary Keys**: Natural clustering on all dimension tables
- **Foreign Keys**: Efficient joins via pg_uid relationships
- **Filter Columns**: Indexed for fast WHERE clause filtering
- **Text Search**: ILIKE optimization for organization_name partial matching

---

## Filter Implementation Notes

### Special Filter Behaviors

1. **organization_name**: Uses ILIKE for partial matching
   - Example: "Medical" matches "Atlanta Medical Group"
   - Case-insensitive search

2. **Required Filters**: `state` and `year_month` are always required
   - Ensures reasonable result sets
   - Provides temporal and geographic scope

3. **Optional Filters**: All other filters are optional
   - Use OR conditions to allow null values
   - Enables progressive filtering

4. **Multi-Value Filters**: Can be extended to support multiple values
   - Use IN clauses instead of equality
   - Example: `npi IN ('1234567890', '9876543210')`

---

## Data Quality for Webapp

### Expected Data Ranges

- **States**: 2-character abbreviations (GA, CA, NY, etc.)
- **Year-Month**: YYYY-MM format (2024-01, 2024-02, etc.)
- **NPIs**: 10-digit strings
- **Enumeration Types**: "1" (Individual) or "2" (Organization)
- **Billing Classes**: "professional", "facility", etc.
- **TIN Types**: "EIN", "NPI"

### Null Handling

- **organization_name**: Can be null for individual providers
- **Cross-reference data**: LEFT JOINs handle missing relationships
- **Procedure categorization**: Not all codes may have categorization

---

*This schema is optimized for the webapp's specific filtering and search requirements. Last updated: [Current Date]*
