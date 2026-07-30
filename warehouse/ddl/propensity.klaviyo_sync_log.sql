-- Audit/debug log of every Klaviyo sync the pipeline performs.
-- Append-only: one row per profile built per run, with the send metadata + the exact payload.
-- Written by the DAG's final `log_sync` task via CSV -> S3 -> COPY (delivery/sync_log.py).
-- Column order MUST match LOG_COLS in delivery/sync_log.py (the COPY names them explicitly,
-- so order is not load-critical, but keep them aligned for clarity).

CREATE TABLE IF NOT EXISTS propensity.klaviyo_sync_log (
    country             VARCHAR(8),
    run_date            DATE,            -- run's as-of date (geo latest-completed day)
    synced_at           TIMESTAMP,       -- when this log row was written (UTC)
    dry_run             BOOLEAN,         -- TRUE = payload prepared but nothing sent to Klaviyo
    email               VARCHAR(1000),
    prop_1_score        FLOAT,
    prop_1_band         VARCHAR(16),
    prop_1_stone        VARCHAR(256),
    prop_1_jewelry      VARCHAR(256),
    prop_1_category     VARCHAR(512),
    prop_2_score        FLOAT,
    prop_2_band         VARCHAR(16),
    prop_2_stone        VARCHAR(256),
    prop_2_jewelry      VARCHAR(256),
    prop_2_category     VARCHAR(512),
    prop_3_score        FLOAT,
    prop_3_band         VARCHAR(16),
    prop_3_stone        VARCHAR(256),
    prop_3_jewelry      VARCHAR(256),
    prop_3_category     VARCHAR(512),
    prop_model_version  VARCHAR(16),     -- e.g. v3
    prop_scored_at      DATE,            -- data as-of date stamped on the profile
    test_bucket         SMALLINT,        -- 1..100 A/B bucket
    valid_email         BOOLEAN
)
DISTSTYLE KEY
DISTKEY (email)                          -- co-locate a customer's history for per-email debugging
SORTKEY (run_date, country);             -- typical filter: a given run / geo

-- Grant so the pipeline's Redshift user can append (SECURITY INVOKER path; adjust the role name):
-- GRANT INSERT, SELECT ON propensity.klaviyo_sync_log TO hanish_kathpal;
