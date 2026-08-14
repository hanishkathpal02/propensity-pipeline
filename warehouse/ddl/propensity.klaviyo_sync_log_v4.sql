-- propensity.klaviyo_sync_log_v4 — audit log of the v4 (per-dimension) Klaviyo payload.
--
-- Identity/metadata are explicit columns (filter/join on these); every prop_* attribute field
-- (legacy + per-dimension) is serialized into the single SUPER `payload` column. This makes the
-- table FUTURE-PROOF: adding a new dimension changes only the registry (config/dimensions.py) and
-- needs NO schema migration here. New table (v3 klaviyo_sync_log left untouched).
--
-- One row per profile per run. Append-only. Loaded via CSV -> S3 -> COPY (delivery/sync_log.py);
-- Redshift's COPY parses the JSON string straight into SUPER, so payload.prop_1_metal is queryable.

CREATE TABLE IF NOT EXISTS propensity.klaviyo_sync_log_v4 (
    country            VARCHAR(8),
    run_date           DATE,
    synced_at          TIMESTAMP,
    dry_run            BOOLEAN,
    email              VARCHAR(320),
    prop_model_version VARCHAR(16),
    prop_scored_at     DATE,
    test_bucket        SMALLINT,
    valid_email        BOOLEAN,
    payload            SUPER          -- every prop_* field lives here as JSON
)
DISTSTYLE KEY
DISTKEY (email)
COMPOUND SORTKEY (run_date, country);

-- Query examples:
--   SELECT email, payload.prop_1_metal, payload."prop_1_metal_score"
--   FROM propensity.klaviyo_sync_log_v4 WHERE run_date = CURRENT_DATE AND dry_run = false;
--
-- Re-apply the same grants used on propensity.klaviyo_sync_log, e.g.:
--   GRANT SELECT ON propensity.klaviyo_sync_log_v4 TO <reporting_role>;
