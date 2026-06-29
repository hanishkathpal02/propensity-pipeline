# warehouse/ — star-schema ETL for the propensity pipeline

SQL for the tables the propensity model reads. Each table has a one-time **DDL**
(`ddl/`) and a **stored procedure** (`procs/`) that loads it. Airflow calls the
procs on a schedule; the DDL is run once per environment.

Naming: files are `<schema>.<object>.sql`.

## Schemas
- `profiles`   — shared identity (`dim_visitor_email_map`). Reusable beyond propensity.
- `propensity` — propensity-owned facts and model tables.
- `reporting`  — existing curated star schema (read-only source; not managed here).

## Tables

| Table | DDL | Loader | Refresh | Source |
|---|---|---|---|---|
| `profiles.dim_visitor_email_map` | `ddl/profiles.dim_visitor_email_map.sql` | `procs/profiles.sp_load_dim_visitor_email_map.sql` | FULL, daily | `public.dw_click_stream`, `public.email_info` |
| `propensity.fact_propensity_category` | `ddl/propensity.fact_propensity_category.sql` | `procs/propensity.sp_load_fact_propensity_category.sql` | incremental (trailing 3d) | `reporting.fact_web_analytics` + dims |
| `propensity.fact_subscription` | `ddl/propensity.fact_subscription.sql` | `procs/propensity.sp_load_fact_subscription.sql` | incremental (trailing 3d) | `reporting.fact_web_analytics` (`subscription_uv_flag`) + dims |
| `propensity.fact_klaviyo_engagements` | `ddl/propensity.fact_klaviyo_engagements.sql` | `procs/propensity.sp_load_fact_klaviyo_engagements.sql` | incremental (trailing 3d) | `shared_klaviyo.public` (datashare from `cdp-angara-redshift` / `rudderstack_prod`) |

> **Datashare dependency:** Klaviyo lives on the CDP cluster (`cdp-angara-redshift` /
> `rudderstack_prod`). It is read on the dev cluster via the **`shared_klaviyo`** datashare
> (producer = CDP, consumer = `redshift-cluster-angara`/`dev`, `PUBLICACCESSIBLE TRUE`).
> The proc references `shared_klaviyo.public.klaviyo_*`. If the datashare is ever removed,
> this fact breaks — re-point or fall back to a Python loader.

## Build order (one-time, per environment)
1. Create schemas: `CREATE SCHEMA IF NOT EXISTS profiles; CREATE SCHEMA IF NOT EXISTS propensity;`
2. Run every file in `ddl/`.
3. Run every file in `procs/` (registers the procedures; loads no data).

## First load (backfill)
```sql
CALL profiles.sp_load_dim_visitor_email_map();
ANALYZE profiles.dim_visitor_email_map;

CALL propensity.sp_load_fact_propensity_category('US', DATE '2024-01-01', CURRENT_DATE + 1);
ANALYZE propensity.fact_propensity_category;

CALL propensity.sp_load_fact_subscription('US', DATE '2024-01-01', CURRENT_DATE + 1);
ANALYZE propensity.fact_subscription;

CALL propensity.sp_load_fact_klaviyo_engagements('US', DATE '2024-01-01', CURRENT_DATE + 1);
ANALYZE propensity.fact_klaviyo_engagements;
```

## Daily refresh (Airflow DAG order)
```
load_dim_visitor_email_map            -> CALL profiles.sp_load_dim_visitor_email_map();
load_fact_propensity_category (US)    -> CALL propensity.sp_load_fact_propensity_category('US', CURRENT_DATE - 3, CURRENT_DATE + 1);
analyze                               -> ANALYZE ...;
freshness_gate                        -> assert MAX(date)/MAX(etl_loaded_at) is recent
run_model -> sync_klaviyo
```
Dependency: the dim must finish before the model reads facts (read-time email join).

## Maintenance
- Weekly `VACUUM` on incrementally-loaded facts (daily DELETE+INSERT leaves dead rows).
- Identity is resolved at READ time (model joins fact -> dim_visitor_email_map), so the
  daily full refresh of the dim retro-attributes newly-identified visitors. Do NOT bake
  email into the facts.

## Model impact
Funnel metrics (`product_views`/`cart_additions`/`checkouts`) in
`fact_propensity_category` are derived from reporting UV flags, not raw event counts —
different scale/semantics than the old `dw_click_stream` queries. Treat as a new model
version (`prop_model_version` v2); new scores are not comparable to old ones.
