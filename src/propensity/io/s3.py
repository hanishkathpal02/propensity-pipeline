"""S3 helpers for the verification COPY round-trip (DataFrame -> S3 -> Redshift temp table).

Trimmed/cleaned from propensity_model2/modules/s3.py.
"""
from io import StringIO

import boto3

from propensity.config.settings import S3_CONFIG
from propensity.util import logger, get_current_datetime

STAGING_PREFIX = "temp-folder-rwd"


def _client():
    return boto3.client(
        "s3",
        aws_access_key_id=S3_CONFIG["aws_access_key_id"],
        aws_secret_access_key=S3_CONFIG["aws_secret_access_key"],
        region_name=S3_CONFIG["region_name"],
    )


def upload_df(df, name: str) -> str:
    """Write a DataFrame to S3 as CSV; return the object key."""
    key = f"{STAGING_PREFIX}/{name}_{get_current_datetime()}.csv"
    buf = StringIO()
    df.to_csv(buf, index=False)
    _client().put_object(Bucket=S3_CONFIG["bucket"], Key=key, Body=buf.getvalue())
    logger.info(f"Uploaded to {s3_uri(key)}")
    return key


def delete(key: str) -> None:
    _client().delete_object(Bucket=S3_CONFIG["bucket"], Key=key)
    logger.info(f"Deleted {s3_uri(key)}")


def s3_uri(key: str) -> str:
    return f"s3://{S3_CONFIG['bucket']}/{key}"


def credentials_clause() -> str:
    """CREDENTIALS string for a Redshift COPY."""
    return (f"aws_access_key_id={S3_CONFIG['aws_access_key_id']};"
            f"aws_secret_access_key={S3_CONFIG['aws_secret_access_key']}")
