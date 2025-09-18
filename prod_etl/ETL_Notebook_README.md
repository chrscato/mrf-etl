
# ETL Notebook — Cell‑by‑Cell README (Aetna/UHC MRF → Dims/Xrefs → Gold Fact)

This README explains **what each cell does**, the **data model**, and **how to run safely on very large files**. It corresponds to the notebook cells I provided (Cells 1–9).

---

## Quick Overview

**Goal:** Normalize payer “negotiated rates” (MRF-derived parquet files) and provider rosters into a small set of **dimension/xref** tables and a skinny **fact_rate** parquet with **idempotent upsert** (insert-if-new).

**Key properties**
- **Append-only facts**: re-running with a new payer/state appends without duplicating prior facts (controlled by a deterministic `fact_uid`).
- **Deduped dims/xrefs**: idempotent appends on primary keys (only new rows are written).
- **Memory-light writers**: no full “old+new” concatenation in RAM.
- **Robust column handling**: parquet columns differ by publisher; missing columns are tolerated and back-filled as nulls.

**Inputs**
- `RATES_PARQ` — payer “rates” parquet (e.g., Aetna or UHC).
- `PROV_PARQ` — payer “providers” parquet (roster with NPI/TIN/etc.).
- `STATE` — logical state tag for this run (e.g., `GA`, `VA`).

**Outputs (under `core/data/`)**
- `dims/dim_code.parquet` (`code_type`, `code`, `code_description`, `code_name`)
- `dims/dim_payer.parquet` (`payer_slug`, `reporting_entity_name`, `version`)
- `dims/dim_provider_group.parquet` (`pg_uid`, `payer_slug`, `provider_group_id_raw`, `version`)
- `dims/dim_pos_set.parquet` (`pos_set_id`, `pos_members`)
- `dims/dim_npi.parquet` (NPI provider details - built separately)
- `dims/dim_npi_address.parquet` (NPI address details - built separately)
- `dims/dim_code_cat.parquet` (procedure code categorization - built separately)
- `xrefs/xref_pg_member_npi.parquet` (`pg_uid`, `npi`)
- `xrefs/xref_pg_member_tin.parquet` (`pg_uid`, `tin_type`, `tin_value`)
- `gold/fact_rate.parquet` (skinny fact, keys below)

**Primary keys / IDs**
- `pg_uid` = md5 of `(payer_slug | version | provider_group_id | provider_reference_id)`
- `pos_set_id` = md5 of `sorted(service_codes)`
- `fact_uid` = md5 of
  ```
  state | year_month | payer_slug | billing_class | code_type | code |
  pg_uid | pos_set_id | negotiated_type | negotiation_arrangement |
  expiration_date | round(negotiated_rate, 4) | provider_group_id_raw
  ```

`state` **is included** in `fact_uid` so GA/VA facts never collide.

---

## Cell‑by‑Cell

### **Cell 1 — Config & Paths**
- Sets `STATE`, `RATES_PARQ`, `PROV_PARQ`, and the output root `core/data/`.
- Optionally override `PAYER_SLUG_OVERRIDE` to force a payer slug.
- Declares the minimal **columns to read** from input parquets (to avoid loading everything).

**Why it matters:** Tuning paths/columns here limits memory and I/O from the start.

---

### **Cell 2 — Imports & Versions**
- Imports Polars and DuckDB **without** installing in-notebook (to avoid kernel crashes from ABI swaps).
- Prints versions for reproducibility.

**Why it matters:** Keeping installs out of the notebook avoids segfault-style kernel restarts.

---

### **Cell 3 — Helpers (slug, md5, parsing, IDs)**
- `md5`, `slugify`, and `_co` (coalesce to string) utilities.
- `payer_slug_from_name`: derives a slug from payer name unless you set `PAYER_SLUG_OVERRIDE`.
- `normalize_yymm`: turns `last_updated_on` into `YYYY-MM`.
- `normalize_service_codes`: normalizes `service_codes` to **sorted unique strings**.
- `pos_set_id_from_members`: deterministic ID for a POS set.
- `pg_uid_from_parts`: deterministic provider group ID.
- `fact_uid_from_struct`: deterministic fact PK (includes `state`). Rounds rate to 4 decimals to avoid float drift collisions.
- `prj_cols`: safe projector that back-fills missing columns with nulls.

**Why it matters:** Deterministic IDs make upsert idempotent and repeatable.

---

### **Cell 4 — Read Inputs (Robust)**
- Uses a **lazy scan** of parquet schemas (`pl.scan_parquet`), **intersects** desired columns with actually present columns, and **back-fills missing columns as nulls**.
- Returns **eager DataFrames** with only the needed columns.

**Why it matters:** MRF schemas differ across payers; this prevents `ColumnNotFoundError` and saves memory by projecting only required columns.

---

### **Cell 5 — Normalize & Build Keys**
- Derives `payer_slug` and `year_month`.
- Parses `service_codes` → `pos_members` → `pos_set_id`.
- Builds `pg_uid` from `(payer_slug, version, provider_group_id, provider_reference_id)`.
- Creates **candidate rows** for dims/xrefs:
  - `dim_code_new`, `dim_payer_new`, `dim_pg_new`, `dim_pos_new`
  - `xref_pg_npi_new`, `xref_pg_tin_new` from `PROV_PARQ`

**Why it matters:** Consolidates messy columns into normalized, dedup-able keys before writing.

---

### **Cell 6 — Memory‑Lighter Append‑Unique Writers**
- `append_unique_parquet(df_new, path, keys)`:
  - Reads **only key columns** from existing parquet.
  - Anti-joins to find new rows.
  - Appends using DuckDB `COPY` **without** loading old+new into Python memory.
- Writes each **dim/xref** with its natural primary key to prevent duplicates.

**Why it matters:** Ensures idempotent, memory-efficient dim/xref maintenance as volumes grow.

---

### **Cell 7 — Build Fact Batch**
- Constructs `fact_new` with **minimal columns**, injects `state`, and mints `fact_uid` with the full keyset (incl. `state`).
- Ensures types (e.g., `negotiated_rate` as `Float64`) and uniqueness at the end of the cell.

**Why it matters:** Keeps the fact skinny and consistent across re-runs/payers/states.

---

### **Cell 8 — DuckDB Insert‑If‑New Upsert (Gold Fact)**
- Writes a temporary stage parquet from `fact_new`.
- If `gold/fact_rate.parquet` **doesn’t exist**, creates it.
- Else:
  - Loads existing fact into DuckDB virtual table.
  - **Inserts only rows whose `fact_uid` is not already present** (left join `IS NULL` pattern).
  - Rewrites `fact_rate.parquet` atomically.

**Why it matters:** Avoids loading the entire fact into Python and scales better than in‑memory anti‑joins.

---

### **Cell 9 — Counters & Sanity Checks**
- DuckDB `COUNT(*)` of each parquet (dims/xrefs/fact) so you can see growth after each run.
- Helps validate that Aetna → UHC runs **append** rather than overwrite.

**Why it matters:** Quick instrumentation that proves the pipeline is behaving.

---

## Data Model (Columns)

### `dims/dim_code.parquet`
- `code_type` (e.g., CPT, HCPCS)
- `code`
- `code_description`
- `code_name`

**PK:** (`code_type`, `code`)

---

### `dims/dim_payer.parquet`
- `payer_slug`
- `reporting_entity_name`
- `version`

**PK:** (`payer_slug`) — single row per payer slug per run; keep latest or all (you can expand PK if you want per-version rows).

---

### `dims/dim_provider_group.parquet`
- `pg_uid` (deterministic group ID)
- `payer_slug`
- `provider_group_id_raw` (from group/reference id)
- `version`

**PK:** (`pg_uid`)

---

### `dims/dim_pos_set.parquet`
- `pos_set_id`
- `pos_members` (list of POS strings)

**PK:** (`pos_set_id`)

---

### `dims/dim_npi.parquet` *(built separately)*
- `enumeration_date`
- `sole_proprietor`
- `primary_taxonomy_state`
- `last_name`
- `credential`
- `organization_name`
- `status`
- `npi`
- `primary_taxonomy_desc`
- `replacement_npi`
- `primary_taxonomy_code`
- `first_name`
- `enumeration_type`
- `last_updated`
- `primary_taxonomy_license`
- `nppes_fetch_date`
- `nppes_fetched`

**PK:** (`npi`)

---

### `dims/dim_npi_address.parquet` *(built separately)*
- `npi`
- `address_purpose`
- `address_type`
- `address_1`
- `address_2`
- `city`
- `state`
- `postal_code`
- `country_code`
- `telephone_number`
- `fax_number`
- `last_updated`
- `address_hash`

**PK:** (`npi`, `address_hash`)

---

### `dims/dim_code_cat.parquet` *(built separately)*
- `proc_cd`
- `proc_set`
- `proc_class`
- `proc_group`

**PK:** (`proc_cd`)

---

### `xrefs/xref_pg_member_npi.parquet`
- `pg_uid`
- `npi`

**PK:** (`pg_uid`, `npi`)

---

### `xrefs/xref_pg_member_tin.parquet`
- `pg_uid`
- `tin_type`
- `tin_value`

**PK:** (`pg_uid`, `tin_value`)

---

### `gold/fact_rate.parquet` (skinny fact)
- `fact_uid` **(PK)**
- `state`
- `year_month`
- `payer_slug`
- `billing_class`
- `code_type`
- `code`
- `pg_uid`
- `pos_set_id`
- `negotiated_type`
- `negotiation_arrangement`
- `negotiated_rate` (Float64)
- `expiration_date`
- `provider_group_id_raw`
- `reporting_entity_name`

**PK:** (`fact_uid`)

---

## How to Run Aetna → UHC (Append‑Only)

1. Set `STATE = "GA"` and point `RATES_PARQ`/`PROV_PARQ` to **Aetna** files. Run all cells.
2. Change only the input paths to **UHC** files (and change `STATE` if appropriate). Run all cells again.

- **Dims/Xrefs**: only new rows get appended (keys-based anti-join).
- **Fact**: only **new** `fact_uid`s are inserted into `gold/fact_rate.parquet`.
- Previous Aetna rows remain intact; UHC rows append.

---

## Running on **Huge Files** (Best Practices)

The notebook already includes several protections (column projection, robust schema handling, memory‑lighter writers, DuckDB upsert). For **very large** inputs, consider the following:

### 1) Pre‑project to a **stage parquet** with DuckDB
Project only the columns you truly need **before** Polars touches them:
```sql
-- Example (duckdb CLI or Python)
COPY (
  SELECT
    last_updated_on, reporting_entity_name, version,
    billing_class, billing_code_type, billing_code,
    service_codes, negotiated_type, negotiation_arrangement,
    negotiated_rate, expiration_date, description, name,
    provider_reference_id, provider_group_id, provider_group_id_raw
  FROM read_parquet('aetna_ga_rates.parquet')
) TO 'aetna_ga_rates_stage.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```
Then point `RATES_PARQ` at the `*_stage.parquet`. Same idea for providers.

**Why:** DuckDB can stream over large parquet efficiently and write a compact, column-projected stage file.

### 2) Use **lazy** Polars until select points
Cells 4–7 already push projection to the scan where possible. If your transforms grow, keep operations **lazy** and only `.collect()` at natural boundaries.

### 3) Process by **payer-month** or **chunks**
If the raw parquet includes multiple months or very large row-groups:
- Use DuckDB filters to split by `year_month` (or payer slug) into multiple stage files.
- Run the notebook per file; the idempotent upsert will accrete safely.

### 4) Keep **dims/xrefs** lean
- The writers only read **key columns** from existing parquets; that keeps memory flat.
- If xrefs get extremely large, consider **partitioning** xrefs by `payer_slug` or hashing `pg_uid` for on-disk sharding (optional).

### 5) Version pinning
Use a fixed environment and avoid in-notebook `pip install`:
```
python==3.11.* (or your 3.13 build with matching wheels)
polars==0.20.*
duckdb==1.1.*
pyarrow==16.*
```
ABI mixups between Arrow/Polars/DuckDB can cause kernel crashes.

### 6) File system & compression
- Use **ZSTD** compression for outputs (already configured).
- On Windows, prefer paths without spaces or escape them carefully; for large data, NTFS plus local SSD is preferable.
- For cloud/S3, use DuckDB’s s3 integration or `fsspec` to read/write directly without local disk bloat.

### 7) Monitoring sizes & counts
Run **Cell 9** often. If `fact_rate.parquet` grows very large, consider:
- Partitioning `gold/fact_rate` by `year_month` or `payer_slug` **only when needed**.
- Or maintaining a monthly “rollup” parquet alongside the skinny fact for query speed.

---

## Troubleshooting

- **`ColumnNotFoundError`** (Polars)  
  → The robust reader in Cell 4 already mitigates this. If a truly critical column is missing in **both** rates and providers, decide on a fallback (e.g., derive `pg_uid` from whatever IDs are available).

- **Kernel crashes / restarts**  
  → Remove in-notebook `pip install`. Pin versions and restart kernel **after** environment updates.

- **High RAM during dims/xrefs**  
  → The writer reads only key columns and appends via DuckDB; if a specific file still explodes, ensure keys are minimal and consider partitioning long-tail xrefs by `payer_slug` on disk.

- **No growth after re-run**  
  → Facts de-duplicate by `fact_uid`. If you changed only `STATE`, but all other fields are identical, including `provider_group_id_raw`, it **should** still insert because `state` is part of the key. Validate by printing a few `fact_uid`s from each run.

---

## Validation Tips

- After each run, Cell 9 prints counts. Expect **monotonic growth** in `gold/fact_rate.parquet` when switching payers/states.
- Spot-check a few rows:
  ```python
  import duckdb
  con = duckdb.connect()
  print(con.execute("SELECT payer_slug, state, COUNT(*) FROM read_parquet('core/data/gold/fact_rate.parquet') GROUP BY 1,2 ORDER BY 1,2").fetchdf())
  con.close()
  ```

---

## FAQ

**Q: Can I include `state` in dims?**  
A: You can, but it’s usually a **fact attribute**. Dims here are mostly payer/code/pos/provider-group. If you truly need state-specific dim behavior, create a `dim_state` and attach FK in the fact.

**Q: How do I rebuild from scratch?**  
A: Delete `core/data/` and re-run. Because IDs are deterministic, results will be consistent across runs with the same inputs.

**Q: Should I partition `fact_rate` now?**  
A: Not until it becomes a problem. Keep it simple; partition by `year_month` or `payer_slug` later if queries slow down or file size gets huge.

**Q: How are `dim_npi`, `dim_npi_address`, and `dim_code_cat` built?**  
A: These dimensions are built and maintained by separate ETL scripts outside of this notebook. They contain NPI provider details from NPPES, address information, and procedure code categorizations respectively. The main ETL notebook focuses on the core MRF rate processing.

---

## Next Steps (Optional Enhancements)

- **Add indexes**: Maintain a small DuckDB db file with virtual views over the parquets for lightning-fast queries.
- **Quality checks**: Add a “nulls & ranges” cell to flag out-of-bounds rates or empty key fields before upsert.
- **Metadata log**: Write a `runs.jsonl` with `STATE`, `payer_slug`, counts inserted, and durations per run.

---

**You can safely run this on large files** with the practices above—particularly **pre-projecting** with DuckDB (or partitioning by month), sticking to **column projection**, and using the **insert-if-new** upsert.
