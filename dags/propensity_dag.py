"""
Propensity pipeline DAG.

    load_dim ─┐                                             build_features_{dim} ─► train_score_{dim}  (×7, pool=1)
    load_fact_attributes ─┼─► analyze ─► freshness_gate ─► build_shared ─┤ … one pair per dimension …  ├─► build_profiles ─► sync_klaviyo ─► log_sync
    load_fact_subscription ─┤                                            └───────── strict fan-in ─────┘
    load_fact_klaviyo ─┘

v4: every dimension (category, stone, jewelry, metal, stone_origin, product_gender, price_range)
is scored by the SAME generalized pipeline; the propensity_models pool (slots=1) serializes the
heavy per-dimension tasks. build_profiles is strict all_success — one dimension failure => no send.

Warehouse refreshes run inside Redshift (SQL CALLs). Everything from build_shared
onward is a PythonOperator calling propensity.pipeline; steps pass data via the
artifact store (io.artifacts), so each is independently retryable.

Keep this file THIN — orchestration only.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from propensity import pipeline
from propensity.config.dimensions import DIMENSION_KEYS
from propensity.config.params import GEO_UTC_OFFSET_HOURS, START_DATE

REDSHIFT_CONN = "redshift_default"
COUNTRY       = "US"
MODEL_POOL    = "propensity_models"   # slots=1 -> only one dimension's heavy task runs at a time


def resolve_run_dates(country, **_):
    """Resolve the run's dates ONCE and freeze them (returned -> pushed to XCom).

    run_date = latest COMPLETED day in the geo's timezone (geo 'yesterday') — the as-of
    date for snapshots, the model folder, and the fact-load TO bound. Computed from the
    current time (so the run is dynamic), but frozen in XCom so every task — and any later
    single-task re-run — uses the SAME date. Optional Airflow Variable
    'propensity_run_date_override' (YYYY-MM-DD) forces a specific as-of day (for backfills).
    """
    from airflow.models import Variable
    override = (Variable.get("propensity_run_date_override", default_var="") or "").strip()
    if override:
        run_date = override
    else:
        off = GEO_UTC_OFFSET_HOURS.get(country, 0)
        geo_today = (datetime.now(timezone.utc) + timedelta(hours=off)).date()
        run_date = (geo_today - timedelta(days=1)).strftime("%Y-%m-%d")   # latest completed geo day
    to_date = (datetime.strptime(run_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return {"run_date": run_date, "to_date": to_date}


# Dates are resolved ONCE by the `resolve_dates` task and read from XCom everywhere, so the
# whole run (and any single-task re-run) shares one frozen as-of date. No --logical-date needed.
# The fact loaders self-compute FROM from each table's high-water mark (MAX(date) - 3d); only
# TO + the backfill floor are passed -> gap-free at any run cadence.
RUN_DATE = "{{ ti.xcom_pull(task_ids='resolve_dates')['run_date'] }}"
TO_DS    = "{{ ti.xcom_pull(task_ids='resolve_dates')['to_date'] }}"


def _py(task_id, fn, pool=None, **extra_kwargs):
    return PythonOperator(
        task_id=task_id,
        python_callable=fn,
        pool=pool,
        op_kwargs={"country": COUNTRY, "run_date": RUN_DATE, **extra_kwargs},
    )


with DAG(
    dag_id="propensity_pipeline",
    schedule=None,                     # manual only (run ~every 14 days; dates resolved at run time)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["propensity"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:

    # ---- Resolve the run's as-of dates ONCE, freeze in XCom ----
    resolve_dates = PythonOperator(
        task_id="resolve_dates", python_callable=resolve_run_dates,
        op_kwargs={"country": COUNTRY},
    )

    # ---- Warehouse refreshes (run inside Redshift, in parallel) ----
    load_dim = SQLExecuteQueryOperator(
        task_id="load_dim_visitor_email_map", conn_id=REDSHIFT_CONN,
        sql="CALL profiles.sp_load_dim_visitor_email_map();",
    )
    load_fact_attributes = SQLExecuteQueryOperator(
        task_id="load_fact_propensity_attributes", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_propensity_attributes('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');",
    )
    load_fact_subscription = SQLExecuteQueryOperator(
        task_id="load_fact_subscription", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_subscription('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');",
    )
    load_fact_klaviyo = SQLExecuteQueryOperator(
        task_id="load_fact_klaviyo_engagements", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_klaviyo_engagements('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');",
    )
    analyze = SQLExecuteQueryOperator(
        task_id="analyze_tables", conn_id=REDSHIFT_CONN,
        sql=[
            "ANALYZE profiles.dim_visitor_email_map;",
            "ANALYZE propensity.fact_propensity_attributes;",
            "ANALYZE propensity.fact_subscription;",
            "ANALYZE propensity.fact_klaviyo_engagements;",
        ],
    )

    # ---- Model + delivery (Python; pass data via artifact store) ----
    # v4: one (build_features -> train_score) pair PER DIMENSION, all reusing the same
    # generalized machinery. The propensity_models pool (slots=1) serializes them so only
    # one dimension's CatBoost fit is in memory at a time.
    freshness_gate = _py("freshness_gate",      pipeline.freshness_gate)
    build_shared   = _py("build_shared_frames", pipeline.build_shared_frames)

    train_tasks = []
    for _key in DIMENSION_KEYS:
        bf = _py(f"build_features_{_key}", pipeline.build_features_dim, pool=MODEL_POOL, dimension=_key)
        ts = _py(f"train_score_{_key}",    pipeline.train_score_dim,    pool=MODEL_POOL, dimension=_key)
        build_shared >> bf >> ts
        train_tasks.append(ts)

    # Strict fan-in: build_profiles (default trigger_rule=all_success) runs ONLY if every
    # dimension trained -> a single dimension failure blocks the whole send. Resume = clear the
    # failed dimension (+ build_profiles/sync/log) in the SAME run; already-successful
    # dimensions stay `success` and are NOT re-trained.
    build_profiles = _py("build_profiles", pipeline.build_profiles_all_dims)
    sync_klaviyo   = _py("sync_klaviyo",    pipeline.sync_klaviyo,
                         dry_run="{{ var.value.get('propensity_sync_dry_run', 'true') }}")
    log_sync       = _py("log_sync",        pipeline.log_klaviyo_sync,
                         dry_run="{{ var.value.get('propensity_sync_dry_run', 'true') }}")

    refreshes = [load_dim, load_fact_attributes, load_fact_subscription, load_fact_klaviyo]
    resolve_dates >> refreshes
    refreshes >> analyze >> freshness_gate >> build_shared
    train_tasks >> build_profiles >> sync_klaviyo >> log_sync
