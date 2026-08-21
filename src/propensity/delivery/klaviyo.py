"""
Push the top-3 propensity profiles to Klaviyo.

Uses the BULK profile-import jobs endpoint (up to 10,000 profiles per request), so a
full-base sync is a handful of requests instead of one-per-profile. It is an UPSERT
(create-or-update by email) and does NOT trigger flows. dry_run=True builds the
payloads, logs a sample, and sends nothing — so runs don't touch Klaviyo until reviewed.
"""
import json
import re
import time
from collections import Counter

import numpy as np
import requests

from propensity.config.params import MODEL_VERSION
from propensity.delivery.profiles import payload_columns
from propensity.util import logger

KLAVIYO_BULK_IMPORT_URL = "https://a.klaviyo.com/api/profile-bulk-import-jobs/"
KLAVIYO_API_VERSION = "2024-02-15"
# Klaviyo 400s a whole bulk job if ANY email is invalid, pointing at the offending index:
#   /data/attributes/profiles/data/<idx>/attributes/email
_INVALID_EMAIL_PTR = re.compile(r"/data/attributes/profiles/data/(\d+)/attributes/email")
BULK_MAX = 10_000                 # Klaviyo hard limit: profiles per bulk-import job
MAX_BULK_BYTES = 4_500_000        # keep each job body under Klaviyo's 5MB request limit (413 otherwise)

# Properties to send = every v4 payload field except the identifier / internal flag. Registry-driven
# (via payload_columns) so new dimensions/grammar fields are sent automatically — no drift.
_PROP_KEYS = [c for c in payload_columns() if c not in ("email", "valid_email")]
_DATE_KEYS = {"prop_scored_at"}


def _as_klaviyo_date(v):
    """Klaviyo detects a property as a DATE only for a full ISO-8601 datetime string; a bare
    'YYYY-MM-DD' comes through as a number. Expand date-only values to ISO-8601 datetime (UTC)."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    return s if "T" in s else f"{s}T00:00:00+00:00"


def _native(v):
    """Coerce numpy scalars to JSON-native python types (requests' json encoder needs native)."""
    if isinstance(v, np.floating):
        return None if np.isnan(v) else float(v)
    if isinstance(v, np.integer):
        return int(v)
    return v


def _profile_properties(row: dict) -> dict:
    """All v4 prop_* properties for one profile (registry-driven). Missing -> null (clears stale)."""
    props = {}
    for k in _PROP_KEYS:
        v = row.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            props[k] = None
        elif k in _DATE_KEYS:
            props[k] = _as_klaviyo_date(v)                 # ISO-8601 datetime so Klaviyo reads a date
        else:
            props[k] = _native(v)
    props["propensity_source"] = True   # flag so Klaviyo can identify propensity-pipeline profiles
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


def _chunk_by_size(records, max_bytes: int, max_count: int):
    """Split profiles into bulk jobs each under Klaviyo's request-size AND count limits.

    The v4 payload is ~2KB/profile (67 properties), so a fixed count of 5,000 blew past the
    5MB body limit -> 413. Sizing by the serialized profile keeps every job safe and adapts
    automatically if the payload grows (more dimensions/fields)."""
    chunks, cur, cur_bytes = [], [], 0
    for r in records:
        sz = len(json.dumps(_profile_obj(r)))
        if cur and (cur_bytes + sz > max_bytes or len(cur) >= max_count):
            chunks.append(cur); cur, cur_bytes = [], 0
        cur.append(r); cur_bytes += sz
    if cur:
        chunks.append(cur)
    return chunks


def _submit_bulk(objs, headers, label, sleep, max_retries: int = 25):
    """POST one bulk-import job. On a 400 for invalid email(s), drop the flagged profile(s)
    and retry — so a few bad addresses can't sink the whole batch. Returns (job_id|None, n_dropped)."""
    objs = list(objs)
    dropped = 0
    for _ in range(max_retries):
        if not objs:
            return None, dropped
        body = {"data": {"type": "profile-bulk-import-job", "attributes": {"profiles": {"data": objs}}}}
        try:
            r = requests.post(KLAVIYO_BULK_IMPORT_URL, headers=headers, json=body, timeout=60)
        except Exception as e:
            logger.error(f"{label} exception: {e}")
            return None, dropped
        if r.status_code in (200, 201, 202):
            return r.json().get("data", {}).get("id"), dropped
        if r.status_code == 400:
            try:
                errs = r.json().get("errors", [])
            except Exception:
                errs = []
            idxs = sorted({int(m.group(1)) for e in errs
                           for m in [_INVALID_EMAIL_PTR.search((e.get("source") or {}).get("pointer", "") or "")] if m},
                          reverse=True)
            if idxs:
                for i in idxs:
                    if 0 <= i < len(objs):
                        objs.pop(i)
                        dropped += 1
                time.sleep(sleep)
                continue
        logger.warning(f"{label} failed [{r.status_code}]: {r.text[:200]}")
        return None, dropped
    logger.warning(f"{label} gave up after {max_retries} retries (too many invalid emails)")
    return None, dropped


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
    # send only rows flagged valid_email=True (invalid ones are excluded up front, in profiles)
    if "valid_email" in df_profiles.columns:
        n0 = len(df_profiles)
        df_profiles = df_profiles[df_profiles["valid_email"] == True]  # noqa: E712
        skipped = n0 - len(df_profiles)
        if skipped:
            logger.info(f"Excluded {skipped:,} invalid-email profiles (valid_email=False) before send")
    records = df_profiles.to_dict(orient="records")
    total = len(records)
    chunks = _chunk_by_size(records, MAX_BULK_BYTES, min(batch_size, BULK_MAX))
    mode = "[DRY RUN]" if dry_run else "[LIVE]"
    logger.info(f"{mode} Klaviyo bulk sync — {total:,} profiles in {len(chunks)} size-capped job(s) "
                f"(<= {MAX_BULK_BYTES // 1_000_000}MB / {min(batch_size, BULK_MAX):,} profiles each)")

    if dry_run:
        if records:
            logger.info(f"{mode} sample profile:\n{json.dumps(_profile_obj(records[0]), indent=2)}")
        logger.info(f"{mode} Done — {len(chunks)} bulk job(s) prepared, nothing sent.")
        return {"total": total, "jobs": len(chunks), "skipped_dry_run": total}

    job_ids, failed, dropped_total = [], 0, 0
    for ci, chunk in enumerate(chunks, 1):
        objs = [_profile_obj(r) for r in chunk]
        jid, dropped = _submit_bulk(objs, headers, f"job {ci}/{len(chunks)}", rate_limit_sleep)
        dropped_total += dropped
        if jid:
            job_ids.append(jid)
            extra = f", {dropped} invalid dropped" if dropped else ""
            logger.info(f"job {ci}/{len(chunks)} accepted: {jid} ({len(chunk) - dropped:,} profiles{extra})")
        else:
            failed += 1
        time.sleep(rate_limit_sleep)

    result = {"total": total, "jobs": len(job_ids), "failed_jobs": failed,
              "invalid_dropped": dropped_total, "job_ids": job_ids}
    if poll and job_ids:
        result["job_status"] = _poll_jobs(job_ids, headers)
    logger.info(f"{mode} Sync complete — {result}")
    return result
