"""
Propensity DAILY score DAG — cheap nightly refresh, NO retrain.

    load_dim ─┐                                             score_{dim} (×7, pool=1)
    load_fact_attributes ─┼─► analyze ─► freshness_gate ─► build_shared ─┤ … one per dimension …  ├─► build_profiles ─► sync_klaviyo ─► log_sync
    load_fact_subscription ─┤                                            └────── strict fan-in ────┘
    load_fact_klaviyo ─┘

Loads the models from the last WEEKLY retrain (propensity_pipeline writes the "current" model
per dimension) and scores ONLY customers active in the last 30 days (the as-of snapshot) — the
inactive keep their existing Klaviyo profile (upsert). ~15 min vs the weekly ~4 h. Shares the
propensity_models pool (slots=1) so it never contends with a weekly retrain for memory.

Runs 9 PM America/Los_Angeles. Created PAUSED — unpause at go-live. sync stays dry via the
propensity_sync_dry_run Variable, exactly like the weekly DAG.
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
MODEL_POOL    = "propensity_models"


def resolve_run_dates(country, **_):
    """run_date = latest completed day in the geo tz (or the override Variable), frozen in XCom."""
    from airflow.models import Variable
    override = (Variable.get("propensity_run_date_override", default_var="") or "").strip()
    if override:
        run_date = override
    else:
        off = GEO_UTC_OFFSET_HOURS.get(country, 0)
        geo_today = (datetime.now(timezone.utc) + timedelta(hours=off)).date()
        run_date = (geo_today - timedelta(days=1)).strftime("%Y-%m-%d")
    to_date = (datetime.strptime(run_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    return {"run_date": run_date, "to_date": to_date}


RUN_DATE = "{{ ti.xcom_pull(task_ids='resolve_dates')['run_date'] }}"
TO_DS    = "{{ ti.xcom_pull(task_ids='resolve_dates')['to_date'] }}"


def _py(task_id, fn, pool=None, **extra_kwargs):
    return PythonOperator(task_id=task_id, python_callable=fn, pool=pool,
                          op_kwargs={"country": COUNTRY, "run_date": RUN_DATE, **extra_kwargs})


with DAG(
    dag_id="propensity_daily_score",
    schedule="0 21 * * *",                       # 9 PM America/Los_Angeles (tz from start_date)
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Los_Angeles"),
    catchup=False,
    is_paused_upon_creation=True,                # stays OFF until go-live
    tags=["propensity", "daily"],
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
) as dag:

    resolve_dates = PythonOperator(task_id="resolve_dates", python_callable=resolve_run_dates,
                                   op_kwargs={"country": COUNTRY})

    load_dim = SQLExecuteQueryOperator(
        task_id="load_dim_visitor_email_map", conn_id=REDSHIFT_CONN,
        sql="CALL profiles.sp_load_dim_visitor_email_map();")
    load_fact_attributes = SQLExecuteQueryOperator(
        task_id="load_fact_propensity_attributes", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_propensity_attributes('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');")
    load_fact_subscription = SQLExecuteQueryOperator(
        task_id="load_fact_subscription", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_subscription('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');")
    load_fact_klaviyo = SQLExecuteQueryOperator(
        task_id="load_fact_klaviyo_engagements", conn_id=REDSHIFT_CONN,
        sql=f"CALL propensity.sp_load_fact_klaviyo_engagements('{COUNTRY}', DATE '{TO_DS}', DATE '{START_DATE}');")
    analyze = SQLExecuteQueryOperator(
        task_id="analyze_tables", conn_id=REDSHIFT_CONN,
        sql=["ANALYZE profiles.dim_visitor_email_map;",
             "ANALYZE propensity.fact_propensity_attributes;",
             "ANALYZE propensity.fact_subscription;",
             "ANALYZE propensity.fact_klaviyo_engagements;"])

    freshness_gate = _py("freshness_gate",      pipeline.freshness_gate)
    build_shared   = _py("build_shared_frames", pipeline.build_shared_frames)

    score_tasks = []
    for _key in DIMENSION_KEYS:
        s = _py(f"score_{_key}", pipeline.score_dim, pool=MODEL_POOL, dimension=_key)
        build_shared >> s
        score_tasks.append(s)

    build_profiles = _py("build_profiles", pipeline.build_profiles_all_dims)
    sync_klaviyo   = _py("sync_klaviyo", pipeline.sync_klaviyo,
                         dry_run="{{ var.value.get('propensity_sync_dry_run', 'true') }}")
    log_sync       = _py("log_sync", pipeline.log_klaviyo_sync,
                         dry_run="{{ var.value.get('propensity_sync_dry_run', 'true') }}")

    refreshes = [load_dim, load_fact_attributes, load_fact_subscription, load_fact_klaviyo]
    resolve_dates >> refreshes
    refreshes >> analyze >> freshness_gate >> build_shared
    score_tasks >> build_profiles >> sync_klaviyo >> log_sync
