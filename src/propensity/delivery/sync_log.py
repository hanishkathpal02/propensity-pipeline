"""
Persist the Klaviyo sync payload to Redshift (propensity.klaviyo_sync_log) for audit/debug.

Runs as the last DAG step (after sync_klaviyo). Appends one row per profile built this run,
with the send metadata (country, run_date, synced_at, dry_run) alongside the exact payload
(top-3 categories, scores/bands, model version, scored-at date, test bucket, valid flag).
Append-only, so every run's send is preserved and comparable over time.

Write path = CSV -> S3 -> COPY (the same mechanism verify.py uses); no new infra.
"""
from datetime import datetime, timezone

import psycopg2

from propensity.config.settings import REDSHIFT_CONFIG
from propensity.io import s3
from propensity.util import logger

TABLE = "propensity.klaviyo_sync_log"

_DIMS = ["score", "band", "stone", "Jewelry", "Category"]
_PROFILE_COLS = (["email"]
                 + [f"prop_{r}_{d}" for r in (1, 2, 3) for d in _DIMS]
                 + ["prop_model_version", "prop_scored_at", "test_bucket", "valid_email"])
# metadata first, then the profile payload — MUST match the column order in the table DDL
LOG_COLS = ["country", "run_date", "synced_at", "dry_run"] + _PROFILE_COLS

_COPY = """
COPY {table} ({cols})
FROM '{path}'
CREDENTIALS '{cred}'
IGNOREHEADER 1 CSV EMPTYASNULL BLANKSASNULL ACCEPTINVCHARS AS '?'
TRUNCATECOLUMNS MAXERROR 100
"""


def log_sync_to_redshift(df_profiles, country: str, run_date: str, dry_run: bool) -> dict:
    """Append this run's profiles + send metadata to propensity.klaviyo_sync_log."""
    df = df_profiles.copy()
    df["country"]   = country
    df["run_date"]  = run_date
    df["synced_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    df["dry_run"]   = int(bool(dry_run))                       # 1/0 -> BOOLEAN on COPY
    if "valid_email" in df.columns:
        df["valid_email"] = df["valid_email"].fillna(False).astype(bool).astype(int)  # 1/0 -> BOOLEAN

    for c in LOG_COLS:                                         # tolerate a missing column (shouldn't happen)
        if c not in df.columns:
            df[c] = None
    df = df[LOG_COLS]

    key = s3.upload_df(df, f"klaviyo_sync_log_{country}_{run_date}")
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
