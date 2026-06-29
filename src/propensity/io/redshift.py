"""Redshift access for the single warehouse cluster (redshift-cluster-angara / dev).

Replaces propensity_model2/modules/redhsift.py. There is now only ONE cluster
(the prod/CDP connection is gone — Klaviyo is read in-DB via the datashare), so
the old `dev=True/False` branching is removed.
"""
import psycopg2
import pandas as pd

from propensity.config.settings import REDSHIFT_CONFIG
from propensity.util import logger

_KEEPALIVE = dict(keepalives=1, keepalives_idle=60, keepalives_interval=10, keepalives_count=5)


def fetch_df(query: str) -> pd.DataFrame:
    """Run a SELECT and return a DataFrame."""
    conn = psycopg2.connect(**REDSHIFT_CONFIG, **_KEEPALIVE)
    try:
        df = pd.read_sql(query, conn)
    finally:
        conn.close()
    logger.info(f"Fetched {df.shape[0]:,} rows x {df.shape[1]} cols")
    return df


def execute(sql: str, params: tuple = None) -> None:
    """Run a statement (CALL / DML / DDL) in one committed transaction."""
    conn = psycopg2.connect(**REDSHIFT_CONFIG, **_KEEPALIVE)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
