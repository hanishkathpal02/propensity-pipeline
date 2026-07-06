"""
Verification: did the people we scored actually buy the category we predicted?

Ported from propensity_model2/modules/data_transformation.create_verification_dataframe
(+ the temp-table flow from modules/redhsift.fetch_data_temp_table). Uses the io.s3
helpers and the single warehouse connection.

Flow: top score per email -> CSV to S3 -> COPY into a session temp table ->
join to actual purchases (all_transaction_customer / ace_transactions / dim_product)
within 30 days of the snapshot -> compare predicted vs purchased category.
"""
import psycopg2
import pandas as pd

from propensity.config.params import SCORE_BANDS
from propensity.config.settings import REDSHIFT_CONFIG
from propensity.io import s3
from propensity.util import logger

_MED, _HIGH = SCORE_BANDS["medium"], SCORE_BANDS["high"]   # keep buckets consistent with delivery bands

_TEMP = "temp_propensity_verify"

_TEMP_DDL = """
DROP TABLE IF EXISTS {temp};
CREATE TEMP TABLE {temp} (
    email               VARCHAR(1000),
    propensity_category VARCHAR(1000),
    snapshot_date       date,
    propensity_score    float,
    propensity_bucket   VARCHAR(1000)
);
"""

_COPY = """
COPY {temp}
FROM '{path}'
CREDENTIALS '{cred}'
IGNOREHEADER 1 CSV NULL AS 'nan' BLANKSASNULL ACCEPTINVCHARS AS '?'
TRUNCATECOLUMNS MAXERROR 10
"""

_VERIFICATION_QUERY = """
SELECT DISTINCT at.customer_email, dp.product_category
FROM all_transaction_customer at
LEFT JOIN ace_transactions act ON at.order_id = act.order_id
LEFT JOIN reporting.dim_product dp
       ON lower(act.sku) = lower(dp.sku)
      AND dp.country = '{country}'
      AND dp.is_valid_product > 0
WHERE EXISTS (
    SELECT 1 FROM {temp} t
    WHERE lower(at.customer_email) = lower(t.email)
      AND at.purchase_date >= t.snapshot_date
      AND at.purchase_date <= t.snapshot_date + INTERVAL '{label_window} days'
)
AND at.customer_email NOT ILIKE '%angara.com%'
AND dp.product_category IS NOT NULL
"""


def _actual_purchases(object_key: str, country: str, label_window: int) -> pd.DataFrame:
    """Temp table + COPY + verification query, all in one session (temp tables are session-scoped)."""
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    try:
        cur = conn.cursor()
        cur.execute(_TEMP_DDL.format(temp=_TEMP))
        cur.execute(_COPY.format(temp=_TEMP, path=s3.s3_uri(object_key), cred=s3.credentials_clause()))
        cur.execute(_VERIFICATION_QUERY.format(temp=_TEMP, country=country, label_window=label_window))
        cols = [d[0] for d in cur.description]
        data = cur.fetchall()
        cur.close()
        return pd.DataFrame(data, columns=cols)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def build_verification(df_features: pd.DataFrame, country: str, label_window: int) -> pd.DataFrame:
    """One row per email (top score), with whether they actually bought that category."""
    df = df_features.copy()
    # buckets aligned to config.SCORE_BANDS (same low/medium/high used for Klaviyo delivery)
    df["propensity_bucket"] = pd.cut(
        df["propensity_score"], bins=[0, _MED, _HIGH, 1.01],
        labels=["low", "medium", "high"], right=False, include_lowest=True,
    )
    df["email"] = df["email"].str.lower()

    df = (df.loc[df.groupby("email")["propensity_score"].idxmax()]
            [["email", "propensity_category", "snapshot_date", "propensity_score", "propensity_bucket"]]
            .sort_values(["email", "propensity_score"], ascending=[True, False]))
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    object_key = s3.upload_df(df, "propensity_verify")
    try:
        actual = _actual_purchases(object_key, country, label_window)
    finally:
        s3.delete(object_key)

    actual["customer_email"] = actual["customer_email"].str.lower()
    df = df.merge(actual, left_on="email", right_on="customer_email", how="left")
    df["match_flag"] = df["propensity_category"] == df["product_category"]

    total = df["email"].nunique()
    matched = df[df["match_flag"]]["email"].nunique()
    logger.info(f"Verification: {matched:,}/{total:,} bought the predicted category")
    return df
