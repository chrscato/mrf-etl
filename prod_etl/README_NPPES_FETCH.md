# NPPES Data Fetching Guide

This guide explains how to fetch real provider data from the NPPES (National Plan and Provider Enumeration System) registry for your existing NPIs in `dim_npi.parquet`.

## 🎯 Overview

The system allows you to:
- Fetch real provider data from NPPES for existing NPIs
- Run the process iteratively (in batches) to avoid overwhelming the API
- Track which NPIs have been fetched using the `nppes_fetched` flag
- Resume interrupted processes
- Monitor progress and handle errors gracefully

## 📁 Files

- `fetch_npi_data.py` - Main script to fetch NPPES data
- `utils/utils_nppes.py` - Core NPPES utilities
- `migrate_dim_npi.py` - Migration script (if needed)
- `core/data/silver/dim_npi/dim_npi.parquet` - Your NPI data file

## 🚀 Quick Start

### 1. Test with Small Batch
```bash
# Test with 10 NPIs first
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --limit 10
```

### 2. Run Full Fetch
```bash
# Fetch all NPIs (takes ~26 minutes for 15,885 NPIs)
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1
```

## 🔄 Iterative Processing

### Method 1: Batch Processing
Process NPIs in smaller batches to avoid long-running processes:

```bash
# Process 1000 NPIs at a time
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --limit 1000

# Wait a bit, then process next 1000
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --limit 1000

# Continue until all are processed
```

### Method 2: Resume from Interruption
The script is idempotent - you can safely re-run it:

```bash
# If interrupted, just run again - it will skip already processed NPIs
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1
```

### Method 3: Check Progress
Monitor how many NPIs still need processing:

```bash
# Check current status
python -c "
import polars as pl
df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet')
total = df.height
fetched = df.filter(pl.col('nppes_fetched') == True).height
unfetched = total - fetched
print(f'Total NPIs: {total}')
print(f'Fetched: {fetched}')
print(f'Remaining: {unfetched}')
"
```

## ⚙️ Command Line Options

```bash
python fetch_npi_data.py [OPTIONS]

Required:
  --silver-dir PATH     Path to silver directory (e.g., core/data/silver)

Optional:
  --dim-npi-filename    Custom dim_npi filename (default: dim_npi.parquet)
  --sleep SECONDS       Sleep between API calls (default: 0.1)
  --limit NUMBER        Limit number of NPIs to process (for testing)
  --yes                 Skip confirmation prompt
```

## 📊 Examples

### Basic Usage
```bash
# Fetch all NPIs with default settings
python fetch_npi_data.py --silver-dir core/data/silver

# Fetch with custom sleep time
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.2

# Test with 50 NPIs
python fetch_npi_data.py --silver-dir core/data/silver --limit 50
```

### Production Usage
```bash
# Conservative approach (slower but safer)
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.2

# Faster approach (be careful with API limits)
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.05

# Automated (no prompts)
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --yes
```

### Batch Processing Script
Create a batch processing script:

```bash
# batch_fetch.sh (Linux/Mac)
#!/bin/bash
for i in {1..16}; do
    echo "Processing batch $i..."
    python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --limit 1000 --yes
    echo "Batch $i complete. Waiting 30 seconds..."
    sleep 30
done
```

```batch
REM batch_fetch.bat (Windows)
@echo off
for /L %%i in (1,1,16) do (
    echo Processing batch %%i...
    python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1 --limit 1000 --yes
    echo Batch %%i complete. Waiting 30 seconds...
    timeout /t 30
)
```

## 📈 Monitoring Progress

### Check Status
```bash
# Quick status check
python -c "
import polars as pl
df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet')
total = df.height
fetched = df.filter(pl.col('nppes_fetched') == True).height
print(f'Progress: {fetched}/{total} ({fetched/total*100:.1f}%)')
"
```

### View Sample Data
```bash
# See what data was fetched
python -c "
import polars as pl
df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet')
print('Columns:', df.columns)
print('Sample data:')
print(df.filter(pl.col('organization_name').is_not_null()).head(3))
"
```

## 🛠️ Troubleshooting

### Common Issues

1. **"dim_npi file not found"**
   ```bash
   # Check if file exists
   ls core/data/silver/dim_npi/dim_npi.parquet
   ```

2. **"Schema errors"**
   ```bash
   # Check current schema
   python -c "import polars as pl; df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet'); print(df.columns)"
   ```

3. **"API rate limiting"**
   ```bash
   # Increase sleep time
   python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.5
   ```

4. **"Interrupted process"**
   ```bash
   # Just run again - it will resume from where it left off
   python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1
   ```

### Error Recovery

If the process fails partway through:

1. **Check what was processed:**
   ```bash
   python -c "
   import polars as pl
   df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet')
   fetched = df.filter(pl.col('nppes_fetched') == True).height
   print(f'NPIs with data: {fetched}')
   "
   ```

2. **Resume processing:**
   ```bash
   # The script will skip already processed NPIs
   python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1
   ```

## 📋 Data Schema

After fetching, your `dim_npi.parquet` will have these columns:

| Column | Description |
|--------|-------------|
| `npi` | National Provider Identifier |
| `enumeration_type` | NPI-1 (Individual) or NPI-2 (Organization) |
| `status` | Active (A) or Inactive (I) |
| `organization_name` | Organization name (for NPI-2) |
| `first_name` | First name (for NPI-1) |
| `last_name` | Last name (for NPI-1) |
| `credential` | Provider credentials |
| `sole_proprietor` | Sole proprietor status |
| `enumeration_date` | Date NPI was assigned |
| `last_updated` | Last update date from NPPES |
| `replacement_npi` | Replacement NPI if applicable |
| `primary_taxonomy_code` | Primary taxonomy code |
| `primary_taxonomy_desc` | Primary taxonomy description |
| `primary_taxonomy_state` | State for primary taxonomy |
| `primary_taxonomy_license` | License number |
| `nppes_fetched` | Flag indicating data was fetched |
| `nppes_fetch_date` | Date data was fetched |

## 🎯 Best Practices

### 1. Start Small
```bash
# Always test with a small batch first
python fetch_npi_data.py --silver-dir core/data/silver --limit 10
```

### 2. Be Respectful to NPPES API
```bash
# Use appropriate sleep times
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.1  # Good
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.01 # Too fast
```

### 3. Monitor Progress
```bash
# Check progress regularly
python -c "import polars as pl; df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet'); print(f'Progress: {df.filter(pl.col(\"nppes_fetched\") == True).height}/{df.height}')"
```

### 4. Use Batch Processing for Large Datasets
```bash
# Process in chunks to avoid long-running processes
python fetch_npi_data.py --silver-dir core/data/silver --limit 1000
```

### 5. Keep Backups
```bash
# Create backup before major runs
cp core/data/silver/dim_npi/dim_npi.parquet core/data/silver/dim_npi/dim_npi.parquet.backup
```

## 🔧 Advanced Usage

### Custom Sleep Patterns
```bash
# Vary sleep time based on time of day
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.2  # Peak hours
python fetch_npi_data.py --silver-dir core/data/silver --sleep 0.05 # Off hours
```

### Parallel Processing (Advanced)
For very large datasets, you could split the NPIs and run multiple instances:

```bash
# Split NPIs into chunks and process in parallel
python -c "
import polars as pl
df = pl.read_parquet('core/data/silver/dim_npi/dim_npi.parquet')
chunk_size = 1000
for i in range(0, df.height, chunk_size):
    chunk = df.slice(i, chunk_size)
    chunk.write_parquet(f'chunk_{i//chunk_size}.parquet')
"
```

## 📞 Support

If you encounter issues:

1. Check the error messages carefully
2. Verify file paths and permissions
3. Ensure you're in the correct directory (`prod_etl`)
4. Check NPPES API status if getting many failures
5. Review the troubleshooting section above

## 🎉 Success!

Once complete, you'll have a rich `dim_npi.parquet` file with real provider data that you can use for:

- Provider analysis and reporting
- Joining with your fact tables for enriched rate data
- Provider network analysis
- Taxonomy-based filtering and grouping
- Provider status monitoring

Your NPI data will now be much more valuable for analytics and reporting! 🚀
