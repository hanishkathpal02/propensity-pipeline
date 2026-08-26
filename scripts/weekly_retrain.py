#!/usr/bin/env python
"""
Manual / fallback WEEKLY RETRAIN for the propensity pipeline.

PRIMARY path is the Airflow DAG `propensity_pipeline` (scheduled Mondays 06:00 PT, UI-visible,
watchdog-protected). Use this script ONLY when you need to run the weekly outside Airflow —
e.g. the scheduler is down, or a one-off backfill.

Does the full weekly, self-contained and resumable (skips dimensions already ranked for the
run_date): warehouse loads -> retrain all 7 dimension models -> re-score the full baseline ->
sync to Klaviyo -> audit-log. train_score writes the "current" models the daily-score DAG rides.

Run detached for the ~3-4h job, e.g.:

    docker exec -d airflow-airflow-scheduler-1 \
        bash -lc 'python /opt/propensity/scripts/weekly_retrain.py 2026-08-31 --live > /tmp/weekly.log 2>&1'

Args:
    [run_date]   YYYY-MM-DD as-of date (default: geo-yesterday for US).
    --live       actually push to Klaviyo. DEFAULT IS DRY (no send) unless --live is passed.
"""
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2

from propensity import pipeline
from propensity.config.dimensions import DIMENSION_KEYS
from propensity.config.params import GEO_UTC_OFFSET_HOURS, START_DATE
from propensity.config.settings import REDSHIFT_CONFIG

COUNTRY = "US"


def resolve_run_date(argv):
    """First non-flag arg, else geo-yesterday (latest completed day in the geo's tz)."""
    for a in argv[1:]:
        if not a.startswith("-"):
            return a
    off = GEO_UTC_OFFSET_HOURS.get(COUNTRY, 0)
    geo_today = (datetime.now(timezone.utc) + timedelta(hours=off)).date()
    return (geo_today - timedelta(days=1)).strftime("%Y-%m-%d")


def load_facts(country, run_date):
    """Refresh the warehouse tables the retrain reads (self-watermarking, idempotent)."""
    to_date = (datetime.strptime(run_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    calls = [
        "CALL profiles.sp_load_dim_visitor_email_map();",
        f"CALL propensity.sp_load_fact_propensity_attributes('{country}', DATE '{to_date}', DATE '{START_DATE}');",
        f"CALL propensity.sp_load_fact_subscription('{country}', DATE '{to_date}', DATE '{START_DATE}');",
        f"CALL propensity.sp_load_fact_klaviyo_engagements('{country}', DATE '{to_date}', DATE '{START_DATE}');",
    ]
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        for c in calls:
            print(f"## LOAD: {c}", flush=True)
            cur.execute(c)
    finally:
        cur.close()
        conn.close()


def main():
    run_date = resolve_run_date(sys.argv)
    dry = "--live" not in sys.argv
    art_root = os.getenv("PROPENSITY_ARTIFACTS_DIR", "/tmp/propensity_artifacts")
    art = f"{art_root}/{COUNTRY}/{run_date}"
    have = lambda n: os.path.exists(f"{art}/{n}")

    t0 = time.time()
    print(f"WEEKLY RETRAIN run_date={run_date}  send={'DRY' if dry else 'LIVE'}", flush=True)

    print("## warehouse loads", flush=True)
    load_facts(COUNTRY, run_date)

    if not (have("shared_klaviyo.parquet") and have("shared_subscriber.parquet")):
        print("## build_shared_frames", flush=True)
        pipeline.build_shared_frames(COUNTRY, run_date)
    else:
        print("shared frames present, skip", flush=True)

    for k in DIMENSION_KEYS:
        if have(f"ranked_{k}.parquet"):
            print(f"skip {k} (ranked exists)", flush=True)
            continue
        t = time.time()
        print(f"## {k}: build_features", flush=True)
        pipeline.build_features_dim(COUNTRY, run_date, k)
        print(f"## {k}: train_score", flush=True)
        pipeline.train_score_dim(COUNTRY, run_date, k)
        print(f"## {k} DONE in {(time.time()-t)/60:.1f}m", flush=True)

    print("## build_profiles", flush=True)
    pipeline.build_profiles_all_dims(COUNTRY, run_date)
    print(f"## sync_klaviyo (dry_run={dry})", flush=True)
    pipeline.sync_klaviyo(COUNTRY, run_date, dry_run=dry)
    print("## log_sync", flush=True)
    pipeline.log_klaviyo_sync(COUNTRY, run_date, dry_run=dry)
    print(f"ALL DONE in {(time.time()-t0)/60:.1f}m", flush=True)


if __name__ == "__main__":
    main()
