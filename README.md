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
  delivery/           #   profiles.py (top-3 wide), klaviyo.py (bulk profile-import, per-geo)
  pipeline.py         #   task callables (one per DAG step)
  util.py             #   logger + date helpers
dags/                 # Airflow DAGs (thin — orchestration only)
scripts/run_local.py  # run the whole pipeline locally (Klaviyo dry-run)
pyproject.toml        # makes `propensity` importable: from propensity.model import ...
```

## How Airflow and the package communicate

Design is **thin DAG, fat package**: Airflow only *orchestrates*; all logic lives in the
`propensity` package. The package has **no Airflow dependency** — it runs standalone
(`scripts/run_local.py`). They connect through three channels:

1. **Import + function calls.** The DAG does `from propensity import pipeline` and each
   PythonOperator calls one function (e.g. `pipeline.build_features(country, run_date)`).
   On the EC2 the repo is bind-mounted and on `PYTHONPATH`, so `import propensity` resolves.
2. **Secrets + DB.** The package reads credentials via `os.getenv(...)` in
   `config/settings.py` (Airflow injects them as env vars); the SQL tasks reach Redshift via
   the Airflow-managed `redshift_default` connection.
3. **Data hand-off = the artifact store, not XCom.** Tasks are separate processes, so large
   frames pass through files in `io/artifacts.py`, keyed by `(country, run_date)` — task A
   writes `features.parquet`, task B reads it. (XCom carries only small values, e.g. the
   dates from `resolve_dates`.)

The warehouse SQL in `warehouse/` is **not executed by the pipeline** — the procs/tables live
in Redshift (deployed manually from those files, which are the version-controlled source of
truth); the DAG just `CALL`s the procs by name.

## Pipeline shape (DAG)

```
resolve_dates ─► load_dim ─┐
                 load_fact_propensity_category ─┼─► analyze ─► freshness_gate ─► build_features ─► train_score ─┬─► analysis
                 load_fact_subscription ─┤                     (→features.parquet)  (→model.pkl, scored)        ├─► verify
                 load_fact_klaviyo_engagements ─┘                                                               └─► build_profiles ─► sync_klaviyo
```
- **`resolve_dates`** runs first and freezes the run's dates in XCom (see *Run dates* below),
  so every task — and any later single-task re-run — shares one as-of date.
- The four warehouse refreshes are `CALL` tasks (run inside Redshift, in parallel). They are
  **self-watermarking**: each derives its load `FROM` from the table's own `MAX(date) − 3d`,
  so the load is **gap-free at any cadence** (daily, 14-day, or after a missed run).
- Everything from `build_features` on is a PythonOperator in `pipeline.py`; steps pass
  data via the artifact store (`io/artifacts.py`), so each is independently retryable.

## Run dates (as-of)
`resolve_dates` computes **`run_date` = the latest *completed* day in the geo's timezone**
("geo yesterday", from `GEO_UTC_OFFSET_HOURS` in `config/params.py`) and freezes it — that's
the as-of date for snapshots, the model folder, and the fact-load upper bound. **No
`--logical-date` needed**; just trigger the DAG. To force a specific as-of day (e.g. a
backfill), set the Airflow Variable `propensity_run_date_override=YYYY-MM-DD`.

## Training & evaluation window
The model retrains on the **full history** every run. The train/validation split uses
**mature labels only** (so metrics and early-stopping aren't biased by unobserved future
orders). With `run_date` as the as-of day and `LABEL_WINDOW = 30`:
- **train** = snapshots ≤ `train_end` (default `run_date − LABEL_WINDOW − VALIDATION_WINDOW`);
- **validation** = snapshots in `(train_end, run_date − LABEL_WINDOW]` — their label window has
  fully elapsed, so reported AUC / precision / recall and early-stopping are trustworthy;
- snapshots **newer than `run_date − LABEL_WINDOW`** have incomplete labels — they are **scored**
  (the latest snapshot per email drives the Klaviyo push) but **excluded from train/validation**.

With `REFIT_ON_ALL_MATURE = True` (default), the deployed model is then **refit on all mature data
(train + val)** at the early-stopped iteration count — so it learns from the most recent mature
month too. Reported metrics stay **honest** (computed from the train-only model on the held-out
val, which it never trained on); only the *deployed/scoring* model is the refit. Costs ~2× the
fit time; set the flag `False` to skip the second fit.

Tune via `LABEL_WINDOW` / `VALIDATION_WINDOW` / `REFIT_ON_ALL_MATURE` in `config/params.py`.

## Run locally

```bash
pip install -e .                 # install package + deps
cp .env.example .env             # then fill in credentials (gitignored)
python scripts/run_local.py US   # full pipeline; Klaviyo is DRY-RUN (sends nothing)
```
Outputs are keyed by `(country, run_date)`: artifacts (`model.pkl`, `*.parquet`) →
`$PROPENSITY_ARTIFACTS_DIR/<country>/<run_date>/`; analysis plots/CSVs →
`$PROPENSITY_OUTPUT_DIR/<country>/<run_date>/analysis/` (defaults under `/tmp`). Each run keeps
its own dated folder (no overwrite).

## Where to add what

| I want to… | Edit |
|---|---|
| Add a country / geo | `config/params.py` `COUNTRY_LIST` **and** `GEO_UTC_OFFSET_HOURS`; add `KLAVIYO_API_KEY_<CC>` (that geo's Klaviyo account); then warehouse grants + backfill |
| Retune the score bands (high/medium/low) | `config/params.py` `SCORE_BANDS` |
| Change feature windows (lookback / label / snapshot freq) | `config/params.py` |
| Change the train/validation split | `config/params.py` `LABEL_WINDOW` (label maturity) + `VALIDATION_WINDOW` + `REFIT_ON_ALL_MATURE`; split/refit logic in `model/train.py` |
| Change model version stamp | `config/params.py` `MODEL_VERSION` |
| Add / change a model feature | `features/build.py` (`FEATURE_COLS` + aggregation) **and** the read query in `io/read.py`; if it's a new raw column, also add it to the fact (`warehouse/ddl/*` + the proc) |
| Change read SQL / add a source table | `io/read.py` + a new `warehouse/ddl/*.sql` + `warehouse/procs/*.sql` |
| Tune LightGBM hyperparameters | `model/train.py` `LGB_PARAMS` |
| Change analysis plots / metrics | `model/analysis.py` |
| Change the Klaviyo payload / profile shape | `delivery/profiles.py` (wide format) + `delivery/klaviyo.py` (payload) |
| Add / reorder a pipeline step | add a fn in `pipeline.py`, wire it in `dags/propensity_dag.py` |
| Change a warehouse table | `warehouse/ddl/*.sql` + `warehouse/procs/*.sql` (re-register proc, re-backfill) |
| Change credentials / connections | `.env` (template in `.env.example`); read in `config/settings.py`. Klaviyo is **per-geo**: `KLAVIYO_API_KEY_<CC>` (prod), `KLAVIYO_SANDBOX_API_KEY` (dev) |
| Move the artifact store to S3 | `io/artifacts.py` (swap the read/write bodies; callers unchanged) |
| Go live to Klaviyo (stop dry-run) | set Airflow **Variable `propensity_sync_dry_run=false`**, then re-run the `sync_klaviyo` task (no code change) |
| Force a specific run / as-of date | set Airflow **Variable `propensity_run_date_override=YYYY-MM-DD`** (else = geo latest-completed day) |
| Change run cadence / schedule | `dags/propensity_dag.py` `schedule` (currently `None` = manual; run ~every 14 days) |

## Deploy to Airflow (EC2)

1. **Code**: `git pull` this repo on the EC2; point Airflow's `dags_folder` at `dags/`
   (or symlink it). `pip install -e .` into the Airflow environment so workers can
   `import propensity`.
2. **Connection**: Airflow → Admin → Connections → `redshift_default`
   (Amazon Redshift; host / db `dev` / user / password / port 5439).
3. **Env vars**: provide `REDSHIFT_*_DEV`, `AWS_*`, and Klaviyo keys to the worker.
   Klaviyo is **per-geo**: `KLAVIYO_SANDBOX_API_KEY` (used when `ENV=dev`) and one
   `KLAVIYO_API_KEY_<CC>` per geo for prod (e.g. `KLAVIYO_API_KEY_US`) — each key needs the
   **`profiles:write`** scope. Set `PROPENSITY_ARTIFACTS_DIR` / `PROPENSITY_OUTPUT_DIR` to
   durable paths (not `/tmp`).
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
5. **Trigger** the `propensity_pipeline` DAG — `airflow dags trigger propensity_pipeline`
   (no `--logical-date`; `resolve_dates` picks the geo latest-completed day). The DAG is
   `schedule=None` (manual), intended ~every 14 days. `sync_klaviyo` stays **dry-run** until
   you set Variable `propensity_sync_dry_run=false` and re-run just that task. Klaviyo is
   delivered via **bulk profile-import jobs** (≤10k/job, upsert), and every profile carries
   `propensity_source=true` so the Klaviyo team can identify pipeline-sourced profiles.

## Notes
- v2 is sourced from the `reporting` star schema; funnel features are flag-based (not raw
  counts) and channel/device come from the normalized dims — **scores are not comparable
  to v1**.
- `propensity_model2/` (under `Documents/devHk/propensity`) is kept as the untouched v1
  baseline for reconciliation.
