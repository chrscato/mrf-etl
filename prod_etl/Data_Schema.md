# Single-File vs Partitioned Parquet — Quick Guide for Your Stack

Great question! Here’s the quick take on single-file vs partitioned for your use case (indie Django app on a DO droplet, Polars/DuckDB, state-by-state MRF loads).

## TL;DR

- **Choose a single skinny Parquet** when you want maximum simplicity and your total size is still manageable. It’s easy to reason about, easy to back up, and avoids partition-key headaches.
- **Choose partitioned files** when you need fast selective reads, incremental corrections, or you’re moving to S3/Athena/Trino/Spark-style analytics where partition pruning really matters.
- **A great middle path:** one file per month (or per state+month). You keep things simple but avoid a single huge file rewrite.

## Why you’d go single file

- **Simplicity:** One path to read/write; no slug/hash partition keys to maintain; fewer chances for path bugs (you already ran into Windows rename quirks).
- **No “small files” problem:** Partitioning often explodes into many tiny files that you eventually need to compact.
- **Lower operational overhead:** Appending and dedup are straightforward; backups and moves are a single copy.
- **Local VM sweet spot:** On a single DO droplet with DuckDB/Polars, the I/O overhead of listing and opening lots of files often outweighs partition benefits.

## Why you’d not go single file

- **Selective reads are slower:** If you mostly query a subset (e.g., one month, one payer), a single big file forces more scanning (row-group pruning helps, but directory-level pruning is better).
- **Edits/corrections are heavier:** Fixing one month means rewriting/merging the whole file (your current append-unique logic does a union + replace).
- **Concurrency:** Two writers contend for the single file; partitioned lets multiple independent writers touch different folders.
- **Cloud analytics:** Engines like Athena/Trino/Spark/Snowflake-on-iceberg rely on partitions for speed and cost control.

## Heuristics (rules of thumb)

**Stick with single file if:**
- Total fact size ≲ 5–10 GB and append windows are OK.
- You’re mostly querying broad slices (e.g., statewide/market views).
- You’re not yet on S3 / serverless SQL.

**Switch to sharded single files** (e.g., `fact_rate_YYYY-MM.parquet` or `fact_rate_GA_YYYY-MM.parquet`) **if:**
- File grows past 8–20 GB or >100M rows and append/union gets sluggish.
- You frequently query by month/state and want lighter reads without full-blown partitions.

**Go partitioned directories** (`state=.../year_month=.../...`) **if:**
- You move to S3 or multi-engine analytics.
- You need incremental reprocessing (rewrite just `year_month=2025-08`), or concurrent writers.

## What I recommend for you right now

Given your current stack and the desire for a single skinny fact, you’re fine staying single-file or (**better yet**) monthly sharded files:

- **Single-file (what I gave you):** dead simple; good while you’re iterating.
- **Monthly shards** (lightweight “partitioning” by filename): same schema, no directory partitions, just files like:
  - `core/data/gold/fact_rate_2025-08.parquet`
  - `core/data/gold/fact_rate_2025-09.parquet`

Your ETL appends to the current month’s file; historical months never rewrite. Queries for a month read exactly one file; queries across months read a small list of files. It’s a sweet spot.

> If you want, I can hand you the exact patches to convert your current single-file code to monthly shards with the same append-unique behavior.

---

# BeaconPoint Health — Rate Data Model & Joining Guide

**Purpose:** This guide shows exactly how to use the single skinny fact table together with your dimension/xref tables to power lookups, analytics, and your Django UI. It includes schemas, key conventions, and copy-paste examples in DuckDB SQL and Polars.

## 0) Files & Layout

Recommended repo layout (adjust paths as you like):

```
core/
  data/
    gold/
      fact_rate.parquet                      # ← ONE skinny Parquet (append-unique by fact_uid)
    silver/
      dim_code/dim_code.parquet              # code_type+code → code_desc, name
      dim_pos_set/dim_pos_set.parquet        # pos_set_id → service_codes (list[str])
      xref_pos/xref_pos.parquet              # (pos_set_id, pos)
      dim_reporting_entity/dim_reporting_entity.parquet  # payer_slug → reporting_entity_name, type, version
      dim_npi/dim_npi.parquet                # canonical NPI list
      dim_tin/dim_tin.parquet                # canonical TIN list  (tin_type, tin_value)
      xref_group_npi/xref_group_npi.parquet  # (year_month, payer_slug, pg_uid, npi)
      xref_group_tin/xref_group_tin.parquet  # (year_month, payer_slug, pg_uid, tin_type, tin_value)

      # (Optional) Benchmarks — store however you prefer; examples below assume:
      bench_medicare_prof.parquet
      bench_opps.parquet
      bench_asc.parquet
      bench_wcfs.parquet
```

## 1) Data Model (Skinny Fact + Dims)

### 1.1 Skinny Fact — `gold/fact_rate.parquet`

One row per negotiated rate at the grain:
`(state, year_month, reporting_entity_name, code_type, code, pg_uid, pos_set_id, negotiated_type, negotiation_arrangement, expiration_date, negotiated_rate, provider_group_id_raw)`

**Columns**

- `fact_uid` (str) — stable hash id for the row (used for dedup/idempotency).
- `state` (str) — state where you wish to analyze/report the rate.
- `year_month` (str, YYYY-MM) — from last_updated_on in the MRF batch.
- `reporting_entity_name` (str) — raw payer name as published (unchanged).
- `code_type` (str) — CPT | HCPCS | REV (etc.).
- `code` (str) — e.g., 10121.
- `pg_uid` (str) — provider-group unique id for this batch; `md5(batch_id | provider_reference_id)`.
- `pos_set_id` (str) — hash id of the list of POS codes that the rate applies to.
- `negotiated_type` (str) — e.g., negotiated.
- `negotiation_arrangement` (str) — e.g., ffs.
- `negotiated_rate` (float) — rate value.
- `expiration_date` (str or null) — null if 9999-12-31 in source.
- `provider_group_id_raw` (str/int) — raw group id from the MRF “provider reference” section (lineage).

> **No slugging in fact:** you keep the raw payer name. Where a slug is required for joins, compute it on the fly (examples below) or use a small bridge table.

### 1.2 Dimensions & Xrefs (Silver)

**`dim_code`**  
Keys: `(code_type, code)`  
Columns: `code_desc`, `name` (short and long human-readable descriptors).

**`dim_pos_set`**  
Keys: `(pos_set_id)`  
Columns: `service_codes` (list of POS codes as strings).

**`xref_pos`**  
Keys: `(pos_set_id, pos)`  
“Exploded” view of dim_pos_set to filter by a single POS quickly.

**`dim_reporting_entity`**  
Keys: `(payer_slug)`  
Columns: `reporting_entity_name`, `reporting_entity_type`, `version`.  
Use `slugify(reporting_entity_name)` to bridge from fact → dim when needed.

**`dim_npi`, `dim_tin`**  
Canonical lists; typically used indirectly.

**`xref_group_npi`**  
Keys: `(year_month, payer_slug, pg_uid, npi)`  
Joins a provider group id (`pg_uid`) to its NPIs for a given payer/month.

**`xref_group_tin`**  
Keys: `(year_month, payer_slug, pg_uid, tin_type, tin_value)`  
Joins a provider group id (`pg_uid`) to its TINs for a given payer/month.

> **Why `pg_uid`?** It’s stable within a batch and avoids carrying heavy provider arrays in the fact. You resolve to NPIs/TINs by joining the xref tables when you need them.

### 1.3 Optional Benchmarks

Keep Medicare/OPPS/ASC/WCFS in small “bench” tables with predictable keys:

- `bench_medicare_prof`: `(state, year_month, code_type='CPT', code) → medicare_prof_rate`
- `bench_opps`: `(state, year_month, code_type='HCPCS', code) → opps_weight, opps_si, opps_mar_stateavg, etc.`
- `bench_asc`: `(state, year_month, code_type='CPT', code) → asc_pi, asc_mar_stateavg, etc.`
- `bench_wcfs`: `(state, year_month, code_type, code) → state schedule amounts (WCFS)`

Normalize your bench keys so they match the fact: `state, year_month, code_type, code`.

## 2) Join Keys — Quick Reference

- **Fact → Code text:** `(code_type, code)` → `dim_code`
- **Fact → POS list:** `(pos_set_id)` → `dim_pos_set` or `xref_pos`
- **Fact → NPI/TIN:** `(year_month, payer_slug, pg_uid)` → `xref_group_npi` / `xref_group_tin`  
  (Compute `payer_slug` from `reporting_entity_name` at query time.)
- **Fact → Benchmarks:** `(state, year_month, code_type, code)` → bench tables

**On-the-fly slugify** (only for joins; do not store in fact):

```python
import re
def slugify(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_') if s else None
```

## 3) DuckDB SQL — Common Queries

All examples assume your files are in the paths shown in §0. If they’re elsewhere, adjust the `read_parquet(...)` paths.

### 3.1 Show the human description for codes (join dim_code)

```sql
WITH fact AS (
  SELECT *
  FROM read_parquet('core/data/gold/fact_rate.parquet')
  WHERE state = 'GA' AND year_month = '2025-08'
)
SELECT f.year_month, f.reporting_entity_name, f.code_type, f.code,
       d.code_desc, d.name AS code_name,
       f.negotiated_rate
FROM fact f
LEFT JOIN read_parquet('core/data/silver/dim_code/dim_code.parquet') d
  ON d.code_type = f.code_type AND d.code = f.code
LIMIT 25;
```

### 3.2 Filter by a single POS (e.g., office 11) via xref_pos

```sql
WITH fact AS (
  SELECT *
  FROM read_parquet('core/data/gold/fact_rate.parquet')
  WHERE state = 'GA' AND year_month = '2025-08'
),
pos AS (
  SELECT pos_set_id
  FROM read_parquet('core/data/silver/xref_pos/xref_pos.parquet')
  WHERE pos = '11'
)
SELECT f.reporting_entity_name, f.code_type, f.code, f.negotiated_rate
FROM fact f
JOIN pos USING (pos_set_id)
ORDER BY f.reporting_entity_name, f.code;
```

### 3.3 Resolve provider NPIs for a fact row

`xref_group_npi` keys include `payer_slug`, so compute it on the fly:

```sql
-- Example with computed slug inline (string ops in SQL)
WITH fact AS (
  SELECT *,
         regexp_replace(lower(reporting_entity_name), '[^a-z0-9]+', '_') AS payer_slug
  FROM read_parquet('core/data/gold/fact_rate.parquet')
  WHERE state = 'GA' AND year_month = '2025-08'
)
SELECT f.reporting_entity_name, f.code, x.npi, f.negotiated_rate
FROM fact f
JOIN read_parquet('core/data/silver/xref_group_npi/xref_group_npi.parquet') x
  ON x.year_month = f.year_month
 AND x.payer_slug = f.payer_slug
 AND x.pg_uid     = f.pg_uid
LIMIT 50;
```

### 3.4 Resolve TINs and aggregate by organization

```sql
WITH fact AS (
  SELECT *,
         regexp_replace(lower(reporting_entity_name), '[^a-z0-9]+', '_') AS payer_slug
  FROM read_parquet('core/data/gold/fact_rate.parquet')
  WHERE state = 'GA' AND year_month = '2025-08'
)
SELECT f.code_type, f.code, t.tin_value,
       count(*) AS n,
       avg(f.negotiated_rate) AS mean_rate,
       quantile_cont(f.negotiated_rate, 0.5) AS p50
FROM fact f
JOIN read_parquet('core/data/silver/xref_group_tin/xref_group_tin.parquet') t
  ON t.year_month = f.year_month
 AND t.payer_slug = f.payer_slug
 AND t.pg_uid     = f.pg_uid
GROUP BY 1,2,3
ORDER BY 1,2,3;
```

### 3.5 Compare to Medicare (professional) benchmark

```sql
WITH fact AS (
  SELECT *
  FROM read_parquet('core/data/gold/fact_rate.parquet')
  WHERE state = 'GA' AND year_month = '2025-08' AND code_type = 'CPT'
)
SELECT f.reporting_entity_name, f.code, d.code_desc,
       f.negotiated_rate,
       b.medicare_prof_rate,
       f.negotiated_rate / NULLIF(b.medicare_prof_rate, 0) AS pct_of_medicare
FROM fact f
LEFT JOIN read_parquet('core/data/silver/dim_code/dim_code.parquet') d
  ON d.code_type = f.code_type AND d.code = f.code
LEFT JOIN read_parquet('core/data/silver/bench_medicare_prof.parquet') b
  ON b.state = f.state AND b.year_month = f.year_month
 AND b.code_type = f.code_type AND b.code = f.code
ORDER BY f.reporting_entity_name, f.code
LIMIT 100;
```

## 4) Polars — Common Joins

```python
import polars as pl, re

# Helper slugify for joins to xref tables (only when needed)
def slugify(s: str) -> str:
    return re.sub(r'[^a-z0-9]+','_', s.lower()).strip('_') if s else None

fact = pl.read_parquet("core/data/gold/fact_rate.parquet")

# Filter scope
fact = fact.filter(
    (pl.col("state")=="GA") & (pl.col("year_month")=="2025-08")
)

# 4.1 Join code text
dim_code = pl.read_parquet("core/data/silver/dim_code/dim_code.parquet")
fact_with_text = fact.join(dim_code, on=["code_type","code"], how="left")

# 4.2 Filter by one POS (11 = office)
xref_pos = pl.read_parquet("core/data/silver/xref_pos/xref_pos.parquet")
pos11 = xref_pos.filter(pl.col("pos")=="11")
fact_pos11 = fact.join(pos11, on="pos_set_id", how="inner")

# 4.3 Resolve NPIs for a row (compute slug on the fly for join)
xref_npi = pl.read_parquet("core/data/silver/xref_group_npi/xref_group_npi.parquet")
fact_npi = (
    fact.with_columns(
        pl.col("reporting_entity_name").map_elements(slugify).alias("payer_slug")
    )
    .join(xref_npi, on=["year_month","payer_slug","pg_uid"], how="inner")
)

# 4.4 Resolve TINs and aggregate by org
xref_tin = pl.read_parquet("core/data/silver/xref_group_tin/xref_group_tin.parquet")
agg_by_tin = (
    fact.with_columns(pl.col("reporting_entity_name").map_elements(slugify).alias("payer_slug"))
        .join(xref_tin, on=["year_month","payer_slug","pg_uid"], how="inner")
        .group_by(["code_type","code","tin_value"])
        .agg([
            pl.len().alias("n"),
            pl.col("negotiated_rate").mean().alias("mean_rate"),
            pl.col("negotiated_rate").quantile(0.5, "nearest").alias("p50"),
        ])
        .sort(["code_type","code","tin_value"])
)

# 4.5 Benchmark compare (Medicare prof)
bench = pl.read_parquet("core/data/silver/bench_medicare_prof.parquet")
fact_bench = (
    fact.join(bench, on=["state","year_month","code_type","code"], how="left")
        .with_columns(
            (pl.col("negotiated_rate") / pl.col("medicare_prof_rate")).alias("pct_of_medicare")
        )
)
```

## 5) API/Query Patterns (map to Django endpoints)

Here are typical request patterns and what to query:

**“Give me rates for CPT 10121 in GA (Aug 2025) with code text”**
- Filter fact by `(state='GA', year_month='2025-08', code_type='CPT', code='10121')`
- Left join `dim_code` for descriptions.
- Optional POS filter with `xref_pos`.

**“Show me NPI-level rates for CPT 10121 (payer X)”**
- Filter fact to CPT 10121 and payer by raw name or precomputed slug in query.
- Join `xref_group_npi` on `(year_month, payer_slug, pg_uid)`.

**“Compare contracted to Medicare”**
- Join `bench_medicare_prof` on `(state, year_month, code_type, code)` and compute `% of Medicare`.

**“Organization (TIN) market stats”**
- Join `xref_group_tin`, then group by `(code_type, code, tin_value)` and compute `p50/p90`.

**“POS-specific view (e.g., office only)”**
- Join `xref_pos` with `pos='11'` to filter the fact.

Each of these patterns maps cleanly to a view function or async query in Django.

## 6) Idempotency, Updates, and Corrections

- **Idempotent appends:** you write `fact_rate.parquet` using `fact_uid` as the dedup key. Re-running the same batch adds nothing.
- **Corrections:**
  - If you find bad rows, filter them out and rewrite the file (DuckDB “SELECT … WHERE …” to a new `.next.parquet`, then `os.replace`).
  - If you shard by month later, you can rewrite just that month’s file quickly.

## 7) Performance Tips

- **Single file** works great up to several GB. When it grows:
  - Switch to **monthly shards** (e.g., `fact_rate_YYYY-MM.parquet`). Same schema, same joins. Keep one file per month to avoid huge rewrites.
- **Polars:**
  - Select only the columns you need (`columns=[...]`) to limit I/O.
  - Use `lazy()` for complex pipelines if you chain multiple joins.
- **DuckDB:**
  - Put hot joins in a temporary persistent DB (`gold.duckdb`) if you want materialized aggregates.
  - Use `quantile_cont` for p10/p50/p90; it’s vectorized and fast.

## 8) Data Quality Checks (copy/paste)

### 8.1 Foreign key checks

```sql
-- Codes present in fact but missing in dim_code:
SELECT f.code_type, f.code, COUNT(*) AS n_missing
FROM read_parquet('core/data/gold/fact_rate.parquet') f
LEFT JOIN read_parquet('core/data/silver/dim_code/dim_code.parquet') d
  ON d.code_type=f.code_type AND d.code=f.code
WHERE d.code IS NULL
GROUP BY 1,2
ORDER BY n_missing DESC
LIMIT 50;
```

### 8.2 POS integrity

```sql
-- Any pos_set_id in fact not defined in dim_pos_set?
SELECT f.pos_set_id, COUNT(*) AS n
FROM read_parquet('core/data/gold/fact_rate.parquet') f
LEFT JOIN read_parquet('core/data/silver/dim_pos_set/dim_pos_set.parquet') p
  ON p.pos_set_id=f.pos_set_id
WHERE p.pos_set_id IS NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 25;
```

### 8.3 Provider xref coverage

```sql
-- % of fact rows by month that can resolve to at least one NPI
WITH fact AS (
  SELECT *,
         regexp_replace(lower(reporting_entity_name), '[^a-z0-9]+', '_') AS payer_slug
  FROM read_parquet('core/data/gold/fact_rate.parquet')
)
SELECT year_month,
       100.0 * SUM(has_npi)::DOUBLE / COUNT(*) AS pct_with_npi
FROM (
  SELECT f.year_month, (x.npi IS NOT NULL) AS has_npi
  FROM fact f
  LEFT JOIN read_parquet('core/data/silver/xref_group_npi/xref_group_npi.parquet') x
    ON x.year_month=f.year_month AND x.payer_slug=f.payer_slug AND x.pg_uid=f.pg_uid
)
GROUP BY year_month
ORDER BY year_month;
```

## 9) Minimal Schema Reference

**Fact (`fact_rate.parquet`)**

- `fact_uid` (str, hash)  
- `state` (str)  
- `year_month` (str, YYYY-MM)  
- `reporting_entity_name` (str, raw)  
- `code_type` (str)  
- `code` (str)  
- `pg_uid` (str)  
- `pos_set_id` (str)  
- `negotiated_type` (str)  
- `negotiation_arrangement` (str)  
- `negotiated_rate` (float)  
- `expiration_date` (str|null)  
- `provider_group_id_raw` (str|int)

**dim_code**  
`code_type` (str), `code` (str), `code_desc` (str), `name` (str)

**dim_pos_set**  
`pos_set_id` (str), `service_codes` (list[str])

**xref_pos**  
`pos_set_id` (str), `pos` (str)

**dim_reporting_entity**  
`payer_slug` (str), `reporting_entity_name` (str), `reporting_entity_type` (str), `version` (str)

**xref_group_npi**  
`year_month` (str), `payer_slug` (str), `pg_uid` (str), `npi` (str)

**xref_group_tin**  
`year_month` (str), `payer_slug` (str), `pg_uid` (str), `tin_type` (str), `tin_value` (str)

**bench_ (example)**

- `bench_medicare_prof`: `state`, `year_month`, `code_type`, `code`, `medicare_prof_rate`  
- `bench_opps`: `state`, `year_month`, `code_type`, `code`, `opps_weight`, `opps_si`, `opps_mar_stateavg`, ...  
- `bench_asc`: `state`, `year_month`, `code_type`, `code`, `asc_pi`, `asc_mar_stateavg`, ...  
- `bench_wcfs`: `state`, `year_month`, `code_type`, `code`, `wcfs_amount`, ...

## 10) Troubleshooting

- **Windows path rename errors:** If you ever switch back to partitioned writes, write final files directly instead of temp+rename. For single-file updates, use DuckDB to build `.next.parquet` and `os.replace` it.
- **Polars version differences:** Avoid `.str.json_*` and `.str.strip*` in ETL; use `map_elements` with Python parsing for `service_codes`.
- **Odd service_codes:** Normalize `['n','u','l'] → []`. Always hash `pos_set_id` from the sorted list of POS codes.

## 11) Suggested Next Steps

- Keep running the single-file approach while iterating.
- When size grows, switch to **monthly shards**: `core/data/gold/fact_rate_YYYY-MM.parquet`.
- Add a tiny bridge table if you want to avoid computing `payer_slug` at query time:  
  `silver/dim_payer_bridge.parquet`: `(reporting_entity_name_raw, payer_slug)`

---

*Prepared for BeaconPoint Health (BPH).*
