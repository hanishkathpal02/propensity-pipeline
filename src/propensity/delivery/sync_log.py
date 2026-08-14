"""
Persist the Klaviyo sync payload to Redshift (propensity.klaviyo_sync_log_v4) for audit/debug.

Runs as the last DAG step (after sync_klaviyo). Appends one row per profile built this run:
identity/metadata as explicit columns, and EVERY prop_* attribute field serialized into a single
SUPER `payload` column. That keeps the table future-proof — adding a new dimension changes only the
registry (config/dimensions.py); the log table never needs a schema migration.

Write path = CSV -> S3 -> COPY (the same mechanism verify.py uses). Redshift's COPY parses the
JSON string in the CSV directly into the SUPER column, so `payload.prop_1_metal` is queryable.
"""
import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2

from propensity.config.settings import REDSHIFT_CONFIG
from propensity.delivery.profiles import payload_columns
from propensity.io import s3
from propensity.util import logger

TABLE = "propensity.klaviyo_sync_log_v4"

# Explicit table columns = the identity/metadata you filter & join on. Everything else — every
# prop_* attribute field (legacy + per-dimension) — goes into the SUPER `payload` column.
META_COLS = ["country", "run_date", "synced_at", "dry_run", "email",
             "prop_model_version", "prop_scored_at", "test_bucket", "valid_email"]
_META_SET = set(META_COLS)
PAYLOAD_FIELDS = [c for c in payload_columns() if c not in _META_SET]   # legacy + per-dim prop_*
LOG_COLS = META_COLS + ["payload"]                                      # CSV order == DDL order

_COPY = """
COPY {table} ({cols})
FROM '{path}'
CREDENTIALS '{cred}'
IGNOREHEADER 1 CSV EMPTYASNULL BLANKSASNULL ACCEPTINVCHARS AS '?'
TRUNCATECOLUMNS MAXERROR 100
"""


def _row_payload(row) -> str:
    """Serialize one profile's prop_* fields to a compact JSON string (absent fields omitted).

    Keys are LOWERCASED: Redshift folds unquoted SUPER paths to lowercase, so a mixed-case key
    like `prop_1_Category` would be unreachable via `payload.prop_1_category` otherwise. Lowercasing
    keeps every field navigable without quoting or session flags (values are untouched).
    """
    d = {}
    for k, v in row.items():
        if pd.isna(v):
            continue                                    # skip absent attributes -> smaller JSON
        key = k.lower()
        if isinstance(v, (np.floating, float)):
            d[key] = round(float(v), 4)
        elif isinstance(v, (np.integer,)):
            d[key] = int(v)
        else:
            d[key] = str(v)
    return json.dumps(d, separators=(",", ":"))


def log_sync_to_redshift(df_profiles, country: str, run_date: str, dry_run: bool) -> dict:
    """Append this run's profiles + send metadata to propensity.klaviyo_sync_log_v4."""
    df = df_profiles.copy()
    df["country"]   = country
    df["run_date"]  = run_date
    df["synced_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["dry_run"]   = int(bool(dry_run))                       # 1/0 -> BOOLEAN on COPY
    if "valid_email" in df.columns:
        df["valid_email"] = df["valid_email"].fillna(False).astype(bool).astype(int)

    for c in PAYLOAD_FIELDS:                                   # tolerate a missing attribute column
        if c not in df.columns:
            df[c] = None
    df["payload"] = df[PAYLOAD_FIELDS].apply(_row_payload, axis=1)

    for c in LOG_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[LOG_COLS]

    key = s3.upload_df(df, f"klaviyo_sync_log_v4_{country}_{run_date}")
    try:
        conn = psycopg2.connect(**REDSHIFT_CONFIG)
        try:
            cur = conn.cursor()
            cur.execute(_COPY.format(table=TABLE, cols=",".join(c.lower() for c in LOG_COLS),
                                     path=s3.s3_uri(key), cred=s3.credentials_clause()))
            conn.commit()
            cur.close()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    finally:
        s3.delete(key)

    result = {"table": TABLE, "rows": int(len(df)), "country": country,
              "run_date": run_date, "dry_run": bool(dry_run)}
    logger.info(f"Logged sync to Redshift: {result}")
    return result
