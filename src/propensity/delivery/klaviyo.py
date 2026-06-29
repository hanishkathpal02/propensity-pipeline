"""
Push the top-3 propensity profiles to Klaviyo via the profile-import API (upsert by email).

Ported from propensity_model2/modules/klaviyo_profile_sync.sync_profiles_to_klaviyo.
dry_run defaults to True — builds payloads, logs samples, sends nothing — so v2 runs
don't touch Klaviyo until the output has been reviewed.
"""
import json
import time

import numpy as np
import requests

from propensity.config.params import MODEL_VERSION
from propensity.util import logger

KLAVIYO_PROFILE_IMPORT_URL = "https://a.klaviyo.com/api/profile-import/"
KLAVIYO_API_VERSION = "2024-02-15"
_DIMS = ["score", "band", "stone", "Jewelry", "Category"]


def _build_payload(row: dict) -> dict:
    """One profile-import payload. Missing ranks are sent as null to clear stale values."""
    props = {}
    for rk in (1, 2, 3):
        for dim in _DIMS:
            val = row.get(f"prop_{rk}_{dim}")
            missing = val is None or (isinstance(val, float) and np.isnan(val))
            props[f"prop_{rk}_{dim}"] = None if missing else val

    props["prop_model_version"] = row.get("prop_model_version", MODEL_VERSION)
    props["prop_scored_at"]     = row.get("prop_scored_at")
    props["test_bucket"]        = row.get("test_bucket")

    return {"data": {"type": "profile",
                     "attributes": {"email": row["email"], "properties": props}}}


def sync_profiles_to_klaviyo(df_profiles, api_key: str,
                             dry_run: bool = True, rate_limit_sleep: float = 0.1) -> dict:
    """Upsert profiles to Klaviyo. dry_run=True logs samples and sends nothing."""
    headers = {
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "revision": KLAVIYO_API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    total = len(df_profiles)
    mode = "[DRY RUN]" if dry_run else "[LIVE]"
    logger.info(f"{mode} Klaviyo profile sync — {total:,} customers")

    success = failed = 0
    errors = []
    for i, row in enumerate(df_profiles.to_dict(orient="records"), 1):
        payload = _build_payload(row)

        if dry_run:
            if i <= 3:
                logger.info(f"{mode} sample #{i}:\n{json.dumps(payload, indent=2)}")
            continue

        try:
            resp = requests.post(KLAVIYO_PROFILE_IMPORT_URL, headers=headers, json=payload, timeout=10)
            if resp.status_code in (200, 201, 202):
                success += 1
            else:
                failed += 1
                errors.append({"email": row["email"], "status": resp.status_code, "body": resp.text[:200]})
                logger.warning(f"Failed [{resp.status_code}] {row['email']}: {resp.text[:200]}")
        except Exception as e:
            failed += 1
            errors.append({"email": row["email"], "error": str(e)})
            logger.error(f"Exception syncing {row['email']}: {e}")

        if i % 100 == 0:
            logger.info(f"  {i:,}/{total:,} | success={success} failed={failed}")
        time.sleep(rate_limit_sleep)

    if dry_run:
        logger.info(f"{mode} Done — {total:,} payloads built, nothing sent.")
        return {"total": total, "success": 0, "failed": 0, "skipped_dry_run": total}

    logger.info(f"{mode} Sync complete — total={total} success={success} failed={failed}")
    if errors:
        logger.warning(f"First 5 errors: {errors[:5]}")
    return {"total": total, "success": success, "failed": failed, "errors": errors}
