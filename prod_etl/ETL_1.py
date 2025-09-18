# %% [markdown]
# ### Stage 1 ETL

# %% [markdown]
# - takes prov & rates parquet files from mrf engine repo

# %%
# ---------- CONFIG ----------
from pathlib import Path

# Change these for each run
STATE = "GA"  # "VA", etc.
RATES_PARQ = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bph\mrf-etl\data\raw\rates_uhc.parquet")    # switch to UHC file on rerun
PROV_PARQ  = Path(r"C:\Users\ChristopherCato\OneDrive - clarity-dx.com\code\bph\mrf-etl\prod_etl\data\input\prov_uhc.parquet")  # switch to UHC file on rerun

# Optional hard override (otherwise computed from reporting_entity_name)
PAYER_SLUG_OVERRIDE = None  # e.g., "aetna", "uhc"

# Data store root (dims/xrefs/gold will live under here)
DS_ROOT = Path("core/data")
DS_ROOT.mkdir(parents=True, exist_ok=True)

# Outputs
DIM_DIR   = DS_ROOT / "dims"
XREF_DIR  = DS_ROOT / "xrefs"
GOLD_DIR  = DS_ROOT / "gold"
DIM_DIR.mkdir(parents=True, exist_ok=True)
XREF_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

# Parquet file targets
DIM_CODE_FILE   = DIM_DIR  / "dim_code.parquet"
DIM_PAYER_FILE  = DIM_DIR  / "dim_payer.parquet"
DIM_PG_FILE     = DIM_DIR  / "dim_provider_group.parquet"
DIM_POS_FILE    = DIM_DIR  / "dim_pos_set.parquet"

XREF_PG_NPI     = XREF_DIR / "xref_pg_member_npi.parquet"
XREF_PG_TIN     = XREF_DIR / "xref_pg_member_tin.parquet"

GOLD_FACT_FILE  = GOLD_DIR / "fact_rate.parquet"

# Columns we’ll actually read (memory saver)
RATES_COLS = [
    "last_updated_on","reporting_entity_name","version",
    "billing_class","billing_code_type","billing_code",
    "service_codes","negotiated_type","negotiation_arrangement",
    "negotiated_rate","expiration_date","description","name",
    "provider_reference_id","provider_group_id","provider_group_id_raw"
]

# Some MRFs don’t include both provider_reference_id & provider_group_id; we handle that later.
PROV_COLS = [
    "last_updated_on","reporting_entity_name","version",
    "provider_group_id","provider_reference_id",
    "npi","tin_type","tin_value"
]


# %% [markdown]
# Cell 2 — Helpers & keys

# %%
import os, sys, math, json, re, hashlib, duckdb
import polars as pl
from datetime import datetime

print("Python:", sys.version)
print("Polars:", pl.__version__)
print("DuckDB:", duckdb.__version__)


# %% [markdown]
# Cell 3 — Ingest & normalize this batch (mint batch_id & pg_uid)

# %%
def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def slugify(s: str) -> str:
    if s is None:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    s = re.sub(r"-+", "-", s)
    return s

def _co(x):
    return "" if x is None else str(x)

def payer_slug_from_name(name: str) -> str:
    if PAYER_SLUG_OVERRIDE:
        return PAYER_SLUG_OVERRIDE
    return slugify(name or "")

def normalize_yymm(date_str: str | None) -> str:
    if not date_str:
        return ""
    # try common formats
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m", "%Y/%m", "%Y%m%d", "%Y%m"):
        try:
            dt = datetime.strptime(date_str[:len(fmt.replace("%","").replace("-","").replace("/",""))], fmt)
            return dt.strftime("%Y-%m")
        except Exception:
            continue
    # fallback: extract yyyy-mm
    m = re.search(r"(20\d{2})[-/](0[1-9]|1[0-2])", date_str)
    return f"{m.group(1)}-{m.group(2)}" if m else ""

def normalize_service_codes(svc) -> list[str]:
    """
    Normalize service_codes into a sorted unique list of strings.
    Handles None, list/tuple, JSON-like strings, and CSV-ish strings.
    Never uses bare truthiness checks that could hit Series ambiguity.
    """
    if svc is None:
        return []

    # If it's already a list/tuple, stringify elements
    if isinstance(svc, (list, tuple)):
        vals = ["" if v is None else str(v) for v in svc]
    else:
        s = str(svc)

        # Try to parse JSON-like list strings (e.g., '["11","22"]')
        if s.startswith("[") and s.endswith("]"):
            try:
                import json
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    vals = ["" if v is None else str(v) for v in parsed]
                else:
                    vals = re.split(r"[;,|\s]+", s)
            except Exception:
                vals = re.split(r"[;,|\s]+", s)
        else:
            vals = re.split(r"[;,|\s]+", s)

    cleaned: list[str] = []
    for v in vals:
        sv = str(v).strip()
        if len(sv) > 0:
            cleaned.append(sv)

    # dedupe + sorted
    return sorted(set(cleaned))


def pos_set_id_from_members(members) -> str:
    """
    Make a stable id from a list of POS members.
    Avoid bare truthiness; handle non-list defensive cases.
    """
    if members is None:
        return md5("none")
    try:
        n = len(members)
    except Exception:
        # not iterable; coerce to single-element list
        members = [str(members)]
        n = 1
    if n == 0:
        return md5("none")
    # stringify defensively
    parts = ["" if m is None else str(m) for m in members]
    return md5("|".join(parts))


def pg_uid_from_parts(payer_slug: str, version: str | None, pgid: str | None, pref: str | None) -> str:
    # Prefer group_id, fall back to provider_reference_id
    key = f"{_co(payer_slug)}|{_co(version)}|{_co(pgid)}|{_co(pref)}"
    return md5(key)

def fact_uid_from_struct(s: dict) -> str:
    """
    Deterministic ID for one fact row (idempotent upsert).
    Includes STATE so GA vs VA don't collide.
    Rounds negotiated_rate to 4 decimals to avoid float drift.
    """
    rate_val = s.get("negotiated_rate")
    try:
        rate_str = f"{float(rate_val):.4f}" if rate_val is not None else ""
    except Exception:
        rate_str = ""
    parts = [
        _co(s.get("state")),
        _co(s.get("year_month")),
        _co(s.get("payer_slug")),
        _co(s.get("billing_class")),
        _co(s.get("code_type")),
        _co(s.get("code")),
        _co(s.get("pg_uid")),
        _co(s.get("pos_set_id")),
        _co(s.get("negotiated_type")),
        _co(s.get("negotiation_arrangement")),
        _co(s.get("expiration_date")),
        rate_str,
        _co(s.get("provider_group_id_raw")),
    ]
    return md5("|".join(parts))

def prj_cols(df: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        # add missing as nulls so downstream code doesn't crash
        df = df.with_columns([pl.lit(None).alias(c) for c in missing])
    return df.select(cols)


# %% [markdown]
# Cell 4 — Small “append-unique” writers for dims/xrefs

# %%
# Robust read that tolerates missing columns by intersecting with the file schema.
# Then, we add any still-missing columns as nulls so later code can rely on them.

def read_parquet_safely(path: Path, desired_cols: list[str]) -> pl.DataFrame:
    lf = pl.scan_parquet(str(path))  # lazy, no data loaded yet
    avail = set(lf.columns)
    use_cols = [c for c in desired_cols if c in avail]
    df = lf.select(use_cols).collect()  # load only what's present

    # back-fill missing columns as nulls to keep downstream selects happy
    missing = [c for c in desired_cols if c not in df.columns]
    if missing:
        df = df.with_columns([pl.lit(None).alias(c) for c in missing])
    return df

if not RATES_PARQ.exists():
    raise FileNotFoundError(f"Missing RATES_PARQ: {RATES_PARQ}")
if not PROV_PARQ.exists():
    raise FileNotFoundError(f"Missing PROV_PARQ:  {PROV_PARQ}")

# TIP: if your RATES_COLS still includes super-optional fields,
# it's fine because read_parquet_safely will just back-fill them.
rates = read_parquet_safely(RATES_PARQ, RATES_COLS)
prov  = read_parquet_safely(PROV_PARQ,  PROV_COLS)

print("rates rows:", rates.height, "cols:", len(rates.columns))
print("prov  rows:", prov.height, "cols:", len(prov.columns))

# Optional quick peek if you’re debugging schemas:
print("RATES present cols:", [c for c in rates.columns if rates.select(pl.col(c).is_not_null().any()).item()])
print("PROV  present cols:", [c for c in prov.columns  if prov.select(pl.col(c).is_not_null().any()).item()])


# %% [markdown]
# Cell 5 — Upsert the FACT into a state-scoped Parquet dataset (dedup partitions)

# %%
# payer + year_month
base = (
    rates
    .with_columns([
        pl.col("reporting_entity_name").fill_null("").alias("ren"),
        pl.col("version").fill_null("").alias("ver"),
        pl.col("last_updated_on").fill_null("").alias("luo"),
    ])
    .with_columns([
        pl.col("ren").map_elements(payer_slug_from_name, return_dtype=pl.Utf8).alias("payer_slug"),
        pl.col("luo").map_elements(normalize_yymm, return_dtype=pl.Utf8).alias("year_month"),
    ])
)

# POS set
base = base.with_columns(
    pl.col("service_codes")
      .map_elements(normalize_service_codes, return_dtype=pl.List(pl.Utf8))
      .alias("pos_members")
).with_columns(
    pl.col("pos_members").map_elements(pos_set_id_from_members, return_dtype=pl.Utf8).alias("pos_set_id")
)

# Provider group UID
base = base.with_columns(
    pl.struct(["payer_slug","version","provider_group_id","provider_reference_id"])
      .map_elements(lambda s: pg_uid_from_parts(s["payer_slug"], s["version"], s["provider_group_id"], s["provider_reference_id"]),
                    return_dtype=pl.Utf8)
      .alias("pg_uid")
)

# Minimal projections for dims/xrefs
dim_code_new = (
    base
      .select([
          pl.col("billing_code_type").alias("code_type"),
          pl.col("billing_code").cast(pl.Utf8).alias("code"),
          pl.col("description").alias("code_description"),
          pl.col("name").alias("code_name"),
      ])
      .drop_nulls(subset=["code_type","code"])
      .unique()
)

dim_payer_new = (
    base
      .select([
          pl.col("payer_slug"),
          pl.col("reporting_entity_name").alias("reporting_entity_name"),
          pl.col("version").alias("version"),
      ])
      .drop_nulls(subset=["payer_slug"])
      .unique()
)

dim_pg_new = (
    base
      .select([
          pl.col("pg_uid"),
          pl.col("payer_slug"),
          pl.coalesce([pl.col("provider_group_id"), pl.col("provider_reference_id")]).alias("provider_group_id_raw"),
          pl.col("version"),
      ])
      .drop_nulls(subset=["pg_uid"])
      .unique()
)

dim_pos_new = (
    base
      .select(["pos_set_id","pos_members"])
      .drop_nulls(subset=["pos_set_id"])
      .unique()
)

# XREFs from provider file (map NPI/TIN to pg_uid)
prov_aug = (
    prov
      .with_columns([
          pl.col("reporting_entity_name").fill_null("").alias("ren"),
          pl.col("version").fill_null("").alias("ver"),
      ])
      .with_columns(pl.col("ren").map_elements(payer_slug_from_name, return_dtype=pl.Utf8).alias("payer_slug"))
      .with_columns(
          pl.struct(["payer_slug","version","provider_group_id","provider_reference_id"])
            .map_elements(lambda s: pg_uid_from_parts(s["payer_slug"], s["version"], s["provider_group_id"], s["provider_reference_id"]),
                          return_dtype=pl.Utf8)
            .alias("pg_uid")
      )
)

xref_pg_npi_new = (
    prov_aug
      .select(["pg_uid","npi"])
      .drop_nulls(subset=["pg_uid","npi"])
      .unique()
)

xref_pg_tin_new = (
    prov_aug
      .select(["pg_uid","tin_type","tin_value"])
      .drop_nulls(subset=["pg_uid","tin_value"])
      .unique()
)


# %% [markdown]
# Cell 6

# %%
def append_unique_parquet(df_new: pl.DataFrame, path: Path, keys: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        old_keys = pl.read_parquet(path, columns=keys).unique()
        to_add = df_new.join(old_keys, on=keys, how="anti")
    else:
        to_add = df_new
    if to_add.is_empty():
        return

    tmp_new = path.with_suffix(".new.parquet")
    tmp_out = path.with_suffix(".next.parquet")

    to_add.write_parquet(tmp_new, compression="zstd")
    con = duckdb.connect()
    if path.exists():
        con.execute(f"""
          COPY (
            SELECT * FROM read_parquet('{path}')
            UNION ALL
            SELECT * FROM read_parquet('{tmp_new}')
          ) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
    else:
        con.execute(f"""
          COPY (SELECT * FROM read_parquet('{tmp_new}'))
          TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
    con.close()
    os.replace(tmp_out, path)
    os.remove(tmp_new)

# Write dims/xrefs
append_unique_parquet(dim_code_new, DIM_CODE_FILE, keys=["code_type","code"])
append_unique_parquet(dim_payer_new, DIM_PAYER_FILE, keys=["payer_slug"])
append_unique_parquet(dim_pg_new,   DIM_PG_FILE,    keys=["pg_uid"])
append_unique_parquet(dim_pos_new,  DIM_POS_FILE,   keys=["pos_set_id"])

append_unique_parquet(xref_pg_npi_new, XREF_PG_NPI, keys=["pg_uid","npi"])
append_unique_parquet(xref_pg_tin_new, XREF_PG_TIN, keys=["pg_uid","tin_value"])

print("Dims/Xrefs up to date.")


# %% [markdown]
# Cell 7

# %%
# Build fact_new with minimal columns + deterministic fact_uid
fact_new = (
    base
      .with_columns(pl.lit(STATE).alias("state"))
      .select(
          "state",
          pl.col("year_month"),
          pl.col("payer_slug"),
          pl.col("billing_class"),
          pl.col("billing_code_type").alias("code_type"),
          pl.col("billing_code").cast(pl.Utf8).alias("code"),
          pl.col("pg_uid"),
          pl.col("pos_set_id"),
          pl.col("negotiated_type"),
          pl.col("negotiation_arrangement"),
          pl.col("negotiated_rate").cast(pl.Float64).alias("negotiated_rate"),
          pl.col("expiration_date"),
          pl.coalesce([pl.col("provider_group_id"), pl.col("provider_reference_id")]).alias("provider_group_id_raw"),
          pl.col("reporting_entity_name"),
      )
      .with_columns(
          pl.struct([
              "state","year_month","payer_slug","billing_class","code_type","code",
              "pg_uid","pos_set_id","negotiated_type","negotiation_arrangement",
              "expiration_date","negotiated_rate","provider_group_id_raw"
          ]).map_elements(fact_uid_from_struct, return_dtype=pl.Utf8).alias("fact_uid")
      )
      .select(
          "fact_uid","state","year_month","payer_slug","billing_class","code_type","code",
          "pg_uid","pos_set_id","negotiated_type","negotiation_arrangement",
          "negotiated_rate","expiration_date","provider_group_id_raw","reporting_entity_name"
      )
      .unique()
)

print("fact_new rows:", fact_new.height)
fact_new.head(3)


# %% [markdown]
# cell 8

# %%
def upsert_fact_single(fact_batch: pl.DataFrame):
    if fact_batch.is_empty():
        print("No fact rows in batch.")
        return

    need_cols = {
        "fact_uid","state","year_month","payer_slug","billing_class","code_type","code",
        "pg_uid","pos_set_id","negotiated_type","negotiation_arrangement",
        "negotiated_rate","expiration_date","provider_group_id_raw","reporting_entity_name"
    }
    missing = need_cols - set(fact_batch.columns)
    if missing:
        raise ValueError(f"fact_batch missing columns: {sorted(missing)}")

    GOLD_FACT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_new = str(GOLD_FACT_FILE.with_suffix(".stage.parquet"))
    tmp_out = str(GOLD_FACT_FILE.with_suffix(".next.parquet"))

    fact_batch.select(sorted(list(need_cols))).write_parquet(tmp_new, compression="zstd")

    con = duckdb.connect()
    if not GOLD_FACT_FILE.exists():
        con.execute(f"""
          COPY (SELECT * FROM read_parquet('{tmp_new}'))
          TO '{GOLD_FACT_FILE}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        con.close()
        os.remove(tmp_new)
        print(f"Created {GOLD_FACT_FILE} with {fact_batch.height} rows.")
        return

    con.execute(f"""
      CREATE OR REPLACE TABLE _all   AS SELECT * FROM read_parquet('{GOLD_FACT_FILE}');
      CREATE OR REPLACE TABLE _stage AS SELECT * FROM read_parquet('{tmp_new}');

      INSERT INTO _all
      SELECT s.* FROM _stage s
      LEFT JOIN _all a ON a.fact_uid = s.fact_uid
      WHERE a.fact_uid IS NULL;

      COPY (SELECT * FROM _all) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    con.close()

    os.replace(tmp_out, GOLD_FACT_FILE)
    os.remove(tmp_new)
    print(f"Upsert complete into {GOLD_FACT_FILE}.")

upsert_fact_single(fact_new)


# %% [markdown]
# Cell 9 - Sanity Check

# %%
def count_parquet_rows(path: Path) -> int:
    if not path.exists():
        return 0
    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{str(path)}')").fetchone()[0]
    con.close()
    return int(n)

print("Row counts:")
print("  dim_code         :", count_parquet_rows(DIM_CODE_FILE))
print("  dim_payer        :", count_parquet_rows(DIM_PAYER_FILE))
print("  dim_provider_grp :", count_parquet_rows(DIM_PG_FILE))
print("  dim_pos_set      :", count_parquet_rows(DIM_POS_FILE))
print("  xref_pg_npi      :", count_parquet_rows(XREF_PG_NPI))
print("  xref_pg_tin      :", count_parquet_rows(XREF_PG_TIN))
print("  fact_rate (gold) :", count_parquet_rows(GOLD_FACT_FILE))



