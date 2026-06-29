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

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from propensity import pipeline

REDSHIFT_CONN = "redshift_default"
COUNTRY       = "US"
RUN_DATE      = "{{ ds }}"
FROM_DS       = "{{ macros.ds_add(ds, -3) }}"   # trailing 3-day reprocess window
TO_DS         = "{{ macros.ds_add(ds, 1) }}"    # exclusive upper bound (includes today)


def _py(task_id, fn, **extra_kwargs):
    return PythonOperator(
        task_id=task_id,
        python_callable=fn,
        op_kwargs={"country": COUNTRY, "run_date": RUN_DATE, **extra_kwargs},
    )


with DAG(
    dag_id="propensity_pipeline",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["propensity"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:

    # ---- Warehouse refreshes (run inside Redshift, in parallel) ----
    load_dim = SQLExecuteQueryOperator(
        task_id="load_dim_visitor_email_map", conn_id=REDSHIFT_CONN,
        sql="CALL profiles.sp_load_dim_visitor_email_map();",
    )
    load_fact_category = SQLExecuteQueryOperator(
        task_id="load_fact_propensity_category", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_propensity_category('{COUNTRY}', DATE '{FROM_DS}', DATE '{TO_DS}');",
    )
    load_fact_subscription = SQLExecuteQueryOperator(
        task_id="load_fact_subscription", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_subscription('{COUNTRY}', DATE '{FROM_DS}', DATE '{TO_DS}');",
    )
    load_fact_klaviyo = SQLExecuteQueryOperator(
        task_id="load_fact_klaviyo_engagements", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_klaviyo_engagements('{COUNTRY}', DATE '{FROM_DS}', DATE '{TO_DS}');",
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
    sync_klaviyo   = _py("sync_klaviyo",     pipeline.sync_klaviyo, dry_run=True)

    refreshes = [load_dim, load_fact_category, load_fact_subscription, load_fact_klaviyo]
    refreshes >> analyze >> freshness_gate >> build_features >> train_score
    train_score >> [analysis, verify, build_profiles]
    build_profiles >> sync_klaviyo
