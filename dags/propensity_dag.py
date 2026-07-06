"""
Propensity pipeline DAG.

    load_dim ─┐
    load_fact_category ─┼─► analyze ─► freshness_gate ─► build_features ─► train_score ─┬─► analysis
    load_fact_subscription ─┤                                                           ├─► verify
    load_fact_klaviyo ─┘                                                                └─► build_profiles ─► sync_klaviyo

Warehouse refreshes run inside Redshift (SQL CALLs). Everything from build_features
onward is a PythonOperator calling propensity.pipeline; steps pass data via the
artifact store (io.artifacts), so each is independently retryable.

Keep this file THIN — orchestration only.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from propensity import pipeline
from propensity.config.params import GEO_UTC_OFFSET_HOURS, START_DATE

REDSHIFT_CONN = "redshift_default"
COUNTRY       = "US"


def geo_date(logical_date, country, delta=0):
    """Geo-local calendar date (YYYY-MM-DD) from the run's FIXED logical timestamp.

    Shifts the UTC logical_date by the geo's fixed offset so every task in a run
    resolves the SAME geo-local date (matches reporting.dim_date's per-geo day
    bucketing, and keeps artifact run_date consistent across tasks).
    """
    off = GEO_UTC_OFFSET_HOURS.get(country, 0)
    return (logical_date + timedelta(hours=off, days=delta)).strftime("%Y-%m-%d")


# Geo-local run dates (manual triggers MUST pass --logical-date).
#   RUN_DATE = latest COMPLETED day in the geo (as-of date: snapshots, model folder).
#   TO_DS    = RUN_DATE + 1 (exclusive upper bound for the fact load).
# The fact loaders self-compute their FROM from each table's high-water mark (MAX(date) - 3d),
# so only TO + the backfill floor are passed — gap-free at any run cadence.
RUN_DATE = "{{ geo_date(logical_date, '" + COUNTRY + "', -1) }}"
TO_DS    = "{{ geo_date(logical_date, '" + COUNTRY + "', 0) }}"


def _py(task_id, fn, **extra_kwargs):
    return PythonOperator(
        task_id=task_id,
        python_callable=fn,
        op_kwargs={"country": COUNTRY, "run_date": RUN_DATE, **extra_kwargs},
    )


with DAG(
    dag_id="propensity_pipeline",
    schedule=None,                     # manual only (run ~every 14 days; pass --logical-date)
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["propensity"],
    user_defined_macros={"geo_date": geo_date},
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:

    # ---- Warehouse refreshes (run inside Redshift, in parallel) ----
    load_dim = SQLExecuteQueryOperator(
        task_id="load_dim_visitor_email_map", conn_id=REDSHIFT_CONN,
        sql="CALL profiles.sp_load_dim_visitor_email_map();",
    )
    load_fact_category = SQLExecuteQueryOperator(
        task_id="load_fact_propensity_category", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_propensity_category('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');",
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
            "ANALYZE propensity.fact_propensity_category;",
            "ANALYZE propensity.fact_subscription;",
            "ANALYZE propensity.fact_klaviyo_engagements;",
        ],
    )

    # ---- Model + delivery (Python; pass data via artifact store) ----
    freshness_gate = _py("freshness_gate",  pipeline.freshness_gate)
    build_features = _py("build_features",  pipeline.build_features)
    train_score    = _py("train_score",     pipeline.train_score)
    analysis       = _py("analysis",        pipeline.analysis)
    verify         = _py("verify",          pipeline.verify)
    build_profiles = _py("build_profiles",  pipeline.build_profiles)
    sync_klaviyo   = _py("sync_klaviyo",     pipeline.sync_klaviyo,
                         dry_run="{{ var.value.get('propensity_sync_dry_run', 'true') }}")

    refreshes = [load_dim, load_fact_category, load_fact_subscription, load_fact_klaviyo]
    refreshes >> analyze >> freshness_gate >> build_features >> train_score
    train_score >> [analysis, verify, build_profiles]
    build_profiles >> sync_klaviyo
