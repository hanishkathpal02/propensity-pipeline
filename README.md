# propensity (v2)

Customer purchase-propensity pipeline — star-schema sourced, Airflow-orchestrated.
Predicts each customer's likelihood to buy by product category and syncs the top
categories to Klaviyo.

Production rebuild of `propensity_model2`: reads curated warehouse tables (built in
`warehouse/`) instead of raw clickstream, and is structured so Airflow orchestrates and
observes each step.

## Layout

```
warehouse/            # ETL — SQL only (run in Redshift)
  ddl/                #   CREATE TABLE per table
  procs/              #   sp_load_* loaders (CALLed by Airflow)
  README.md           #   table defs, build order, backfill, datashare dependency
src/propensity/       # the Python ML package (installable; imported by Airflow workers)
  config/             #   settings.py (secrets/conns), params.py (tunables)
  io/                 #   read.py (star queries), redshift.py, s3.py, freshness.py, artifacts.py
  features/           #   build.py — snapshots, labels, cleaning
  model/              #   train.py, analysis.py, verify.py
  delivery/           #   profiles.py (top-3 wide), klaviyo.py (sync)
  pipeline.py         #   task callables (one per DAG step)
  util.py             #   logger + date helpers
dags/                 # Airflow DAGs (thin — orchestration only)
scripts/run_local.py  # run the whole pipeline locally (Klaviyo dry-run)
pyproject.toml        # makes `propensity` importable: from propensity.model import ...
```

## Pipeline shape (DAG)

```
load_dim ─┐
load_fact_propensity_category ─┼─► analyze ─► freshness_gate ─► build_features ─► train_score ─┬─► analysis
load_fact_subscription ─┤                          (→features.parquet)  (→model.pkl, scored)  ├─► verify
load_fact_klaviyo_engagements ─┘                                                              └─► build_profiles ─► sync_klaviyo
```
- The four warehouse refreshes are `CALL` tasks (run inside Redshift, in parallel).
- Everything from `build_features` on is a PythonOperator in `pipeline.py`; steps pass
  data via the artifact store (`io/artifacts.py`), so each is independently retryable.

## Run locally

```bash
pip install -e .                 # install package + deps
cp .env.example .env             # then fill in credentials (gitignored)
python scripts/run_local.py US   # full pipeline; Klaviyo is DRY-RUN (sends nothing)
```
Outputs: artifacts → `$PROPENSITY_ARTIFACTS_DIR` (default `/tmp/propensity_artifacts`);
plots/CSVs → `$PROPENSITY_OUTPUT_DIR` (default `/tmp/propensity_output`).

## Where to add what

| I want to… | Edit |
|---|---|
| Add a country / geo | `config/params.py` `COUNTRY_LIST` → then warehouse grants + backfill for it; `dags` country |
| Retune the score bands (high/medium/low) | `config/params.py` `SCORE_BANDS` |
| Change feature windows (lookback / label / snapshot freq) | `config/params.py` |
| Change model version stamp | `config/params.py` `MODEL_VERSION` |
| Add / change a model feature | `features/build.py` (`FEATURE_COLS` + aggregation) **and** the read query in `io/read.py`; if it's a new raw column, also add it to the fact (`warehouse/ddl/*` + the proc) |
| Change read SQL / add a source table | `io/read.py` + a new `warehouse/ddl/*.sql` + `warehouse/procs/*.sql` |
| Tune LightGBM hyperparameters | `model/train.py` `LGB_PARAMS` |
| Change analysis plots / metrics | `model/analysis.py` |
| Change the Klaviyo payload / profile shape | `delivery/profiles.py` (wide format) + `delivery/klaviyo.py` (payload) |
| Add / reorder a pipeline step | add a fn in `pipeline.py`, wire it in `dags/propensity_dag.py` |
| Change a warehouse table | `warehouse/ddl/*.sql` + `warehouse/procs/*.sql` (re-register proc, re-backfill) |
| Change credentials / connections | `.env` (template in `.env.example`); read in `config/settings.py` |
| Move the artifact store to S3 | `io/artifacts.py` (swap the read/write bodies; callers unchanged) |
| Go live to Klaviyo (stop dry-run) | `dags/propensity_dag.py` → `sync_klaviyo` `dry_run=False` |

## Deploy to Airflow (EC2)

1. **Code**: `git pull` this repo on the EC2; point Airflow's `dags_folder` at `dags/`
   (or symlink it). `pip install -e .` into the Airflow environment so workers can
   `import propensity`.
2. **Connection**: Airflow → Admin → Connections → `redshift_default`
   (Amazon Redshift; host / db `dev` / user / password / port 5439).
3. **Env vars**: provide `REDSHIFT_*_DEV`, `AWS_*`, `KLAVIYO_*` to the worker
   (a `.env` at the worker working dir, or Airflow env vars). Set
   `PROPENSITY_ARTIFACTS_DIR` / `PROPENSITY_OUTPUT_DIR` to durable paths (not `/tmp`).
4. **Redshift grants** for the Airflow DB user (procs run SECURITY INVOKER — caller's perms):
   ```sql
   GRANT USAGE ON SCHEMA propensity, profiles TO <airflow_user>;
   GRANT SELECT, INSERT, DELETE ON ALL TABLES IN SCHEMA propensity, profiles TO <airflow_user>;
   GRANT EXECUTE ON PROCEDURE
     profiles.sp_load_dim_visitor_email_map(),
     propensity.sp_load_fact_propensity_category(varchar,date,date),
     propensity.sp_load_fact_subscription(varchar,date,date),
     propensity.sp_load_fact_klaviyo_engagements(varchar,date,date) TO <airflow_user>;
   GRANT USAGE ON DATABASE shared_klaviyo TO <airflow_user>;   -- datashare (klaviyo proc)
   ```
5. **Trigger** the `propensity_pipeline` DAG. `sync_klaviyo` stays **dry-run** until you
   set `dry_run=False` in the DAG.

## Notes
- v2 is sourced from the `reporting` star schema; funnel features are flag-based (not raw
  counts) and channel/device come from the normalized dims — **scores are not comparable
  to v1**.
- `propensity_model2/` (under `Documents/devHk/propensity`) is kept as the untouched v1
  baseline for reconciliation.
