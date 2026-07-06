"""
Push the top-3 propensity profiles to Klaviyo.

Uses the BULK profile-import jobs endpoint (up to 10,000 profiles per request), so a
full-base sync is a handful of requests instead of one-per-profile. It is an UPSERT
(create-or-update by email) and does NOT trigger flows. dry_run=True builds the
payloads, logs a sample, and sends nothing — so runs don't touch Klaviyo until reviewed.
"""
import json
import time
from collections import Counter

import numpy as np
import requests

from propensity.config.params import MODEL_VERSION
from propensity.util import logger

KLAVIYO_BULK_IMPORT_URL = "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
KLAVIYO_API_VERSION = "2024-02-15"
BULK_MAX = 10_000                 # Klaviyo hard limit: profiles per bulk-import job
_DIMS = ["score", "band", "stone", "Jewelry", "Category"]


def _profile_properties(row: dict) -> dict:
    """Custom `prop_*` properties for one profile. Missing ranks -> null (clears stale values)."""
    props = {}
    for rk in (1, 2, 3):
        for dim in _DIMS:
            val = row.get(f"prop_{rk}_{dim}")
            missing = val is None or (isinstance(val, float) and np.isnan(val))
            props[f"prop_{rk}_{dim}"] = None if missing else val
    props["prop_model_version"] = row.get("prop_model_version", MODEL_VERSION)
    props["prop_scored_at"]     = row.get("prop_scored_at")
    props["test_bucket"]        = row.get("test_bucket")
    props["propensity_source"]  = True   # flag so Klaviyo can identify propensity-pipeline profiles
    return props


def _profile_obj(row: dict) -> dict:
    """A single profile resource (used inside a bulk job's profiles list)."""
    return {"type": "profile",
            "attributes": {"email": row["email"], "properties": _profile_properties(row)}}


def _build_payload(row: dict) -> dict:
    """Single-profile profile-import body (kept for ad-hoc / one-off posting & tests)."""
    return {"data": _profile_obj(row)}


def _bulk_body(rows) -> dict:
    """A profile-bulk-import-job body wrapping up to BULK_MAX profiles."""
    return {"data": {"type": "profile-bulk-import-job",
                     "attributes": {"profiles": {"data": [_profile_obj(r) for r in rows]}}}}


def _poll_jobs(job_ids, headers, max_wait_s: int = 600, interval_s: int = 10) -> dict:
    """Poll bulk-import jobs until complete/cancelled (or timeout). Returns status counts."""
    pending, status, waited = set(job_ids), {}, 0
    while pending and waited < max_wait_s:
        for jid in list(pending):
            try:
                r = requests.get(f"{KLAVIYO_BULK_IMPORT_URL}{jid}/", headers=headers, timeout=30)
                st = r.json().get("data", {}).get("attributes", {}).get("status") if r.status_code == 200 else f"http_{r.status_code}"
            except Exception as e:
                st = f"error:{e}"
            status[jid] = st
            if st in ("complete", "cancelled") or str(st).startswith(("http_", "error")):
                pending.discard(jid)
        if pending:
            time.sleep(interval_s); waited += interval_s
    counts = dict(Counter(status.values()))
    logger.info(f"bulk job statuses: {counts}")
    return counts


def sync_profiles_to_klaviyo(df_profiles, api_key: str, dry_run: bool = True,
                             batch_size: int = 5_000, poll: bool = True,
                             rate_limit_sleep: float = 0.3) -> dict:
    """Bulk-upsert profiles to Klaviyo. dry_run=True builds payloads + logs a sample, sends nothing."""
    headers = {
        "Authorization": f"Klaviyo-API-Key {api_key}",
        "revision": KLAVIYO_API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    records = df_profiles.to_dict(orient="records")
    total = len(records)
    batch_size = min(batch_size, BULK_MAX)
    chunks = [records[i:i + batch_size] for i in range(0, total, batch_size)]
    mode = "[DRY RUN]" if dry_run else "[LIVE]"
    logger.info(f"{mode} Klaviyo bulk sync — {total:,} profiles in {len(chunks)} job(s) of up to {batch_size:,}")

    if dry_run:
        if records:
            logger.info(f"{mode} sample profile:\n{json.dumps(_profile_obj(records[0]), indent=2)}")
        logger.info(f"{mode} Done — {len(chunks)} bulk job(s) prepared, nothing sent.")
        return {"total": total, "jobs": len(chunks), "skipped_dry_run": total}

    job_ids, failed = [], 0
    for ci, chunk in enumerate(chunks, 1):
        try:
            resp = requests.post(KLAVIYO_BULK_IMPORT_URL, headers=headers, json=_bulk_body(chunk), timeout=60)
            if resp.status_code in (200, 201, 202):
                jid = resp.json().get("data", {}).get("id")
                job_ids.append(jid)
                logger.info(f"job {ci}/{len(chunks)} accepted: {jid} ({len(chunk):,} profiles)")
            else:
                failed += 1
                logger.warning(f"job {ci}/{len(chunks)} failed [{resp.status_code}]: {resp.text[:250]}")
        except Exception as e:
            failed += 1
            logger.error(f"job {ci}/{len(chunks)} exception: {e}")
        time.sleep(rate_limit_sleep)

    result = {"total": total, "jobs": len(job_ids), "failed_jobs": failed, "job_ids": job_ids}
    if poll and job_ids:
        result["job_status"] = _poll_jobs(job_ids, headers)
    logger.info(f"{mode} Sync complete — {result}")
    return result
