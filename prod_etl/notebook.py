# %% [markdown]
# ### Stage 1 ETL

# %% [markdown]
# - takes prov & rates parquet files from mrf engine repo

# %%
# !pip install polars duckdb pyarrow --quiet

import os, re, hashlib, shutil, uuid
from pathlib import Path
import polars as pl
import duckdb
from datetime import datetime

# ---------- CONFIG ----------
STATE        = "GA"  # <- change per run
RATES_PARQ   = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bph\mrf-etl\data\raw\rates_uhc.parquet"      # input (this batch)
PROV_PARQ    = r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bph\mrf-etl\prod_etl\data\input\prov_uhc.parquet"       # input (this batch)

# Output "dataset root" (state-scoped will be inside)
DS_ROOT      = Path("core/data")                     # dataset root
SILVER_DIR   = DS_ROOT / "silver"                    # small dims/xrefs
GOLD_FACT    = DS_ROOT / "gold" / "fact_rate"        # skinny fact (partitioned)
DUCKDB_DIR   = Path("/srv/data")                     # local serving DB folder (feel free to change)
DUCKDB_NEXT  = DUCKDB_DIR / "gold_next.duckdb"
DUCKDB_LIVE  = DUCKDB_DIR / "gold.duckdb"

# performance knobs
ROWGROUP_MB  = 256  # target parquet row-group size (if/when using pyarrow writer params)


# %% [markdown]
# Cell 2 — Helpers & keys

# %%
# CELL 2 — HELPERS (no .str.*; safe on older Polars)

import polars as pl
import re, json, ast, hashlib

def slugify(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_') if s else None

def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()

def _parse_pos_str_py(s):
    """Parse service_codes from messy strings like "['02','11']" or '[]' or ['n','u','l'] → list[str]."""
    if s is None:
        return []
    s2 = str(s).strip()
    if not s2 or s2 in ("[]", "['n','u','l']", "['n', 'u', 'l']"):
        return []
    try:  # prefer JSON
        v = json.loads(s2.replace("'", '"'))
        return v if isinstance(v, list) else []
    except Exception:
        try:
            v = ast.literal_eval(s2)
            return v if isinstance(v, list) else []
        except Exception:
            return []

def ensure_pos_list(df: pl.DataFrame, col: str = "service_codes") -> pl.DataFrame:
    """
    Ensure df[col] is List(Utf8). Uses map_elements Python parser (works on old Polars).
    - If missing → create empty List[Utf8].
    - If List[...] → cast inner to Utf8.
    - If Utf8 → parse to list[str].
    - Else → empty List[Utf8].
    """
    if col not in df.columns:
        return df.with_columns(pl.lit([]).cast(pl.List(pl.Utf8)).alias(col))

    dt = df[col].dtype
    if isinstance(dt, pl.List):
        return df.with_columns(pl.col(col).cast(pl.List(pl.Utf8)))
    if dt == pl.Utf8:
        return df.with_columns(pl.col(col).map_elements(_parse_pos_str_py).alias(col))
    return df.with_columns(pl.lit([]).cast(pl.List(pl.Utf8)).alias(col))

def _safe_to_list(obj):
    """Convert obj (list/tuple/set/Series/scalar/None) → Python list, never rely on truthiness."""
    if obj is None:
        return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, (tuple, set)):
        return list(obj)
    # Polars Series case (sometimes seen inside map_elements)
    if isinstance(obj, pl.Series):
        try:
            if len(obj) == 1:
                v = obj.item()
                return v if isinstance(v, list) else ([] if v is None else [v])
            return obj.to_list()
        except Exception:
            return []
    try:
        # Last resort: attempt to coerce iterables to list
        return list(obj)
    except Exception:
        return []

def pos_set_id_from_obj(obj) -> str:
    """Deterministic ID for a POS set; sorts, drops empties, then md5 of joined string."""
    arr = _safe_to_list(obj)
    vals = [str(x) for x in arr if x is not None and str(x).strip() != "" and str(x).lower() not in ("n","u","l")]
    vals.sort()
    return md5(",".join(vals))


# %% [markdown]
# Cell 3 — Ingest & normalize this batch (mint batch_id & pg_uid)

# %%
# CELL 3 — INGEST & NORMALIZE

import polars as pl

# Load inputs
rates = pl.read_parquet(RATES_PARQ)
prov  = pl.read_parquet(PROV_PARQ)

# service_codes → List[Utf8]
rates = ensure_pos_list(rates, "service_codes")

# last_updated_on → Date then "YYYY-MM"
if rates["last_updated_on"].dtype == pl.Utf8:
    rates = rates.with_columns(pl.col("last_updated_on").str.strptime(pl.Date, strict=False))

rates = rates.with_columns([
    pl.col("last_updated_on").dt.strftime("%Y-%m").alias("year_month"),
    pl.col("reporting_entity_name").map_elements(slugify).alias("payer_slug"),
    pl.col("billing_code_type").alias("code_type"),
    pl.col("billing_code").alias("code"),
    pl.when(pl.col("expiration_date") == "9999-12-31").then(pl.lit(None)).otherwise(pl.col("expiration_date")).alias("expiration_date"),
])

# default version if missing/blank
rates = rates.with_columns(
    pl.when(pl.col("version").is_null() | (pl.col("version") == ""))
      .then(pl.lit("1.0.0"))
      .otherwise(pl.col("version"))
      .alias("version")
)

# batch_id + pg_uid
rates = rates.with_columns(
    (pl.col("payer_slug") + "|" + pl.col("year_month") + "|" + pl.col("version")).alias("batch_id")
).with_columns(
    pl.struct(["batch_id","provider_reference_id"])
      .map_elements(lambda s: md5(f"{s['batch_id']}|{s['provider_reference_id']}"))
      .alias("pg_uid")
)

# ---- POS dims for this batch (no truthiness inside the function) ----
dim_pos_set_new = (
    rates.select("service_codes")
         .drop_nulls()
         .unique()
         .with_columns(pl.col("service_codes").map_elements(pos_set_id_from_obj).alias("pos_set_id"))
         .select("pos_set_id","service_codes")
         .unique()
)
xref_pos_new = dim_pos_set_new.explode("service_codes").rename({"service_codes":"pos"})

# ---- Code & payer dims ----
dim_code_new = (
    rates.select("code_type","code","description","name")
         .unique()
         .rename({"description":"code_desc"})
)
dim_reporting_entity_new = (
    rates.select("payer_slug","reporting_entity_name","reporting_entity_type","version")
         .unique()
)

# ---- Skinny fact (STATE added at upsert) ----
fact_new = (
    rates.select([
        "year_month","payer_slug","billing_class","code_type","code",
        pl.col("provider_reference_id").alias("provider_group_id_raw"),
        "negotiated_type","negotiation_arrangement","service_codes",
        "negotiated_rate","expiration_date","pg_uid"
    ])
    .with_columns(pl.col("service_codes").map_elements(pos_set_id_from_obj).alias("pos_set_id"))
    .select("year_month","payer_slug","billing_class","code_type","code","pg_uid",
            "negotiated_type","negotiation_arrangement","pos_set_id",
            "negotiated_rate","expiration_date","provider_group_id_raw")
)

# ---- Provider-group membership (PATCH) ----
if prov["last_updated_on"].dtype == pl.Utf8:
    prov = prov.with_columns(pl.col("last_updated_on").str.strptime(pl.Date, strict=False))

# 1) create base columns
prov = prov.with_columns([
    pl.col("last_updated_on").dt.strftime("%Y-%m").alias("year_month"),
    pl.col("reporting_entity_name").map_elements(slugify).alias("payer_slug"),
    pl.when(pl.col("version").is_null() | (pl.col("version") == ""))
      .then(pl.lit("1.0.0"))
      .otherwise(pl.col("version"))
      .alias("version"),
])

# 2) now that payer_slug/year_month/version exist, build batch_id
prov = prov.with_columns(
    (pl.col("payer_slug") + "|" + pl.col("year_month") + "|" + pl.col("version")).alias("batch_id")
)

# 3) mint pg_uid from batch_id + provider_group_id
prov = prov.with_columns(
    pl.struct(["batch_id","provider_group_id"])
      .map_elements(lambda s: md5(f"{s['batch_id']}|{s['provider_group_id']}"))
      .alias("pg_uid")
)

# 4) build xrefs/dims
xref_group_npi_new = prov.select("year_month","payer_slug","pg_uid","npi").drop_nulls(["npi"]).unique()
xref_group_tin_new = prov.select("year_month","payer_slug","pg_uid","tin_type","tin_value").drop_nulls(["tin_value"]).unique()
dim_npi_new        = prov.select("npi").drop_nulls().unique()
dim_tin_new        = prov.select("tin_type","tin_value").drop_nulls(["tin_value"]).unique()

print("Cell 3 complete:",
      f"\n  fact_new rows: {fact_new.height}",
      f"\n  dim_code_new: {dim_code_new.height}",
      f"\n  dim_pos_set_new: {dim_pos_set_new.height}",
      f"\n  xref_group_npi_new: {xref_group_npi_new.height}",
      f"\n  xref_group_tin_new: {xref_group_tin_new.height}")


# %% [markdown]
# Cell 4 — Small “append-unique” writers for dims/xrefs

# %%
# CELL 4 — APPEND-UNIQUE WRITERS FOR DIMS/XREFS

from pathlib import Path

def append_unique_parquet(df_new: pl.DataFrame, path: Path, keys: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        df_old = pl.read_parquet(path)
        df_all = pl.concat([df_old, df_new], how="vertical_relaxed").unique(subset=keys, keep="last")
    else:
        df_all = df_new.unique(subset=keys, keep="last")
    df_all.write_parquet(path, compression="zstd")

# write small dims/xrefs
append_unique_parquet(dim_code_new,               SILVER_DIR/"dim_code"/"dim_code.parquet",               ["code_type","code"])
append_unique_parquet(dim_reporting_entity_new,   SILVER_DIR/"dim_reporting_entity"/"dim_reporting_entity.parquet", ["payer_slug"])
append_unique_parquet(dim_pos_set_new,            SILVER_DIR/"dim_pos_set"/"dim_pos_set.parquet",         ["pos_set_id"])
append_unique_parquet(xref_pos_new,               SILVER_DIR/"xref_pos"/"xref_pos.parquet",               ["pos_set_id","pos"])
append_unique_parquet(dim_npi_new,                SILVER_DIR/"dim_npi"/"dim_npi.parquet",                 ["npi"])
append_unique_parquet(dim_tin_new,                SILVER_DIR/"dim_tin"/"dim_tin.parquet",                 ["tin_type","tin_value"])
append_unique_parquet(xref_group_npi_new,         SILVER_DIR/"xref_group_npi"/"xref_group_npi.parquet",   ["year_month","payer_slug","pg_uid","npi"])
append_unique_parquet(xref_group_tin_new,         SILVER_DIR/"xref_group_tin"/"xref_group_tin.parquet",   ["year_month","payer_slug","pg_uid","tin_type","tin_value"])

print("Cell 4 complete: dims/xrefs appended (deduped).")


# %% [markdown]
# Cell 5 — Upsert the FACT into a state-scoped Parquet dataset (dedup partitions)

# %%
# CELL 5 — UPSERT FACT PARTITIONS (STATE-SCOPED, WINDOWS-SAFE)

from pathlib import Path
from functools import reduce
import operator, uuid
import polars as pl

def upsert_fact_state(fact_batch: pl.DataFrame, state: str):
    """
    Writes/updates Parquet partitions under:
      GOLD_FACT/state=<STATE>/year_month=YYYY-MM/payer_slug=.../billing_class=.../code_type=...
    Idempotent via dedup keys; safe if a partition resolves to 0 rows (no write/rename).
    """
    if fact_batch.is_empty():
        print("No fact rows in batch.")
        return

    # ensure 'state' exists and partition cols are strings (path safety)
    fact_batch = fact_batch.with_columns(pl.lit(state).alias("state")).with_columns([
        pl.col("state").cast(pl.Utf8),
        pl.col("year_month").cast(pl.Utf8),
        pl.col("payer_slug").cast(pl.Utf8),
        pl.col("billing_class").cast(pl.Utf8),
        pl.col("code_type").cast(pl.Utf8),
    ])

    part_cols = ["state","year_month","payer_slug","billing_class","code_type"]
    partitions = fact_batch.select(part_cols).unique().to_dicts()

    dedup_keys = [
        "state","year_month","payer_slug","billing_class","code_type","code","pg_uid",
        "pos_set_id","negotiated_type","negotiation_arrangement",
        "expiration_date","negotiated_rate","provider_group_id_raw"
    ]

    wrote = 0
    cleaned = 0

    for p in partitions:
        # boolean mask (a & b & c & ...)
        exprs = [(pl.col(k) == pl.lit(v)) for k, v in p.items()]
        mask  = reduce(operator.and_, exprs)
        new_part = fact_batch.filter(mask)

        tgt = GOLD_FACT / f"state={p['state']}" / f"year_month={p['year_month']}" / \
              f"payer_slug={p['payer_slug']}" / f"billing_class={p['billing_class']}" / \
              f"code_type={p['code_type']}"
        tgt.mkdir(parents=True, exist_ok=True)

        existing_files = list(tgt.glob("*.parquet"))
        if existing_files:
            old = pl.read_parquet([str(f) for f in existing_files])
            combined = pl.concat([old, new_part], how="vertical_relaxed")
        else:
            combined = new_part

        # Deduplicate
        combined = combined.unique(subset=dedup_keys, keep="last")

        nrows = combined.height
        # If no rows remain, remove any stale files and skip write
        if nrows == 0:
            for f in existing_files:
                f.unlink(missing_ok=True)
                cleaned += 1
            # optional: print a hint
            print(f"[skip-empty] {p} → 0 rows after dedup; removed {len(existing_files)} old file(s).")
            continue

        # Write atomically: tmp then replace
        tmp = tgt / f"_tmp_{uuid.uuid4().hex}.parquet"
        combined.write_parquet(tmp, compression="zstd")

        for f in existing_files:
            f.unlink(missing_ok=True)
        final = tgt / f"part-{uuid.uuid4().hex}.parquet"
        tmp.replace(final)  # use Path.replace on Windows to avoid some rename quirks
        wrote += 1
        print(f"[upsert] {p} → {nrows} rows")

    print(f"Upserted partitions: wrote={wrote}, cleaned_empty={cleaned}, total_seen={len(partitions)}")

# run upsert for this batch
upsert_fact_state(fact_new, STATE)


# %% [markdown]
# Cell 6 — (Optional) Refresh local gold.duckdb for fast UI

# %%
DUCKDB_DIR.mkdir(parents=True, exist_ok=True)

def build_or_refresh_duckdb(state: str, year_months: list[str]):
    con = duckdb.connect(str(DUCKDB_NEXT))
    con.execute("PRAGMA threads=%d" % os.cpu_count())

    # Create dims if not exist
    con.execute("""
    CREATE TABLE IF NOT EXISTS dim_code AS SELECT * FROM read_parquet($1) WHERE 1=0;
    """, [str(SILVER_DIR/"dim_code"/"dim_code.parquet")])
    con.execute("DELETE FROM dim_code;")
    con.execute("INSERT INTO dim_code SELECT * FROM read_parquet($1);", [str(SILVER_DIR/"dim_code"/"dim_code.parquet")])

    # Create/refresh agg_market_rates for the specified year_months (for state only)
    for ym in sorted(set(year_months)):
        # drop & rebuild month slice
        con.execute("""
        CREATE TABLE IF NOT EXISTS agg_market_rates (
            year_month VARCHAR, state VARCHAR, payer_slug VARCHAR, billing_class VARCHAR,
            code_type VARCHAR, code VARCHAR,
            npi_count BIGINT, ein_count BIGINT,
            p10 DOUBLE, p25 DOUBLE, p50 DOUBLE, p75 DOUBLE, p90 DOUBLE,
            mean DOUBLE, min DOUBLE, max DOUBLE
        );
        DELETE FROM agg_market_rates WHERE year_month=? AND state=?;
        """, [ym, state])

        # Join to xrefs for counts
        con.execute(f"""
        INSERT INTO agg_market_rates
        SELECT
          f.year_month, f.state, f.payer_slug, f.billing_class, f.code_type, f.code,
          COUNT(DISTINCT npi.npi)  AS npi_count,
          COUNT(DISTINCT tin.tin_value) AS ein_count,
          quantile_cont(f.negotiated_rate, 0.10) AS p10,
          quantile_cont(f.negotiated_rate, 0.25) AS p25,
          quantile_cont(f.negotiated_rate, 0.50) AS p50,
          quantile_cont(f.negotiated_rate, 0.75) AS p75,
          quantile_cont(f.negotiated_rate, 0.90) AS p90,
          AVG(f.negotiated_rate) AS mean,
          MIN(f.negotiated_rate) AS min,
          MAX(f.negotiated_rate) AS max
        FROM read_parquet('{GOLD_FACT}/state={state}/year_month={ym}/*/*/*/*.parquet') f
        LEFT JOIN read_parquet('{SILVER_DIR}/xref_group_npi/xref_group_npi.parquet') npi
          ON npi.year_month=f.year_month AND npi.payer_slug=f.payer_slug AND npi.pg_uid=f.pg_uid
        LEFT JOIN read_parquet('{SILVER_DIR}/xref_group_tin/xref_group_tin.parquet') tin
          ON tin.year_month=f.year_month AND tin.payer_slug=f.payer_slug AND tin.pg_uid=f.pg_uid
        GROUP BY 1,2,3,4,5,6;
        """)

    con.close()

    # atomic swap
    DUCKDB_LIVE.parent.mkdir(parents=True, exist_ok=True)
    if DUCKDB_LIVE.exists():
        shutil.copy2(DUCKDB_LIVE, DUCKDB_LIVE.with_suffix(".bak"))
    shutil.move(DUCKDB_NEXT, DUCKDB_LIVE)
    print(f"Refreshed {DUCKDB_LIVE} for {state}, months={year_months}")

# refresh only months present in this batch
months_touched = rates.select("year_month").unique().to_series().to_list()
build_or_refresh_duckdb(STATE, months_touched)


# %% [markdown]
# Cell 7 — Quick sanity checks

# %%
# Count fact rows for this state/month after upsert
ym = months_touched[0]
cnt = duckdb.sql(f"""
SELECT count(*) AS rows
FROM read_parquet('{GOLD_FACT}/state={STATE}/year_month={ym}/*/*/*/*.parquet')
""").fetchdf()
cnt



