"""
Connection / secret configuration for the propensity pipeline.

Loaded from a local .env for now (AWS Secrets Manager is the intended next step —
swap the os.getenv calls for a secrets-manager fetch when available).

v2 cleanup vs propensity_model2/config/settings.py:
  * ONE Redshift connection. Everything the model reads (propensity.*, profiles.*,
    reporting.*) lives on the warehouse cluster (redshift-cluster-angara / db `dev`),
    and Klaviyo is read in-DB via the `shared_klaviyo` datashare — so the old
    production-cluster (CDP) connection is no longer needed.
  * Dropped unused SFTP (id_rsa keys) and email config — the pipeline never used them.
"""
import os
from dotenv import load_dotenv

load_dotenv()

ENV = os.getenv("ENV", "dev")

# Redshift — the warehouse / analytics cluster (redshift-cluster-angara, db `dev`):
# hosts the propensity, profiles and reporting schemas, plus the shared_klaviyo datashare.
REDSHIFT_CONFIG = {
    "host":     os.getenv("REDSHIFT_HOST_DEV"),
    "port":     os.getenv("REDSHIFT_PORT_DEV"),
    "dbname":   os.getenv("REDSHIFT_DB_DEV"),
    "user":     os.getenv("REDSHIFT_USER_DEV"),
    "password": os.getenv("REDSHIFT_PASS_DEV"),
}

# S3 — staging bucket used for the verification COPY round-trip.
S3_CONFIG = {
    "aws_access_key_id":     os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "region_name":           os.getenv("REGION_NAME"),
    "bucket":                os.getenv("BUCKET_NAME"),
}

# Klaviyo Profile API — a SEPARATE production account/key per geo; one shared sandbox for dev.
#   dev  -> KLAVIYO_SANDBOX_API_KEY        (single sandbox; dev runs are dry-run anyway)
#   prod -> KLAVIYO_API_KEY_<COUNTRY>      (one Klaviyo account per geo, e.g. KLAVIYO_API_KEY_US)
def klaviyo_api_key(country: str) -> str:
    """Klaviyo key for a geo: sandbox in dev, the per-country production account in prod."""
    if ENV == "dev":
        return os.getenv("KLAVIYO_SANDBOX_API_KEY")
    return os.getenv(f"KLAVIYO_API_KEY_{country.upper()}")
