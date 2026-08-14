-- propensity.klaviyo_sync_log_v4 — audit log of the v4 (per-dimension) Klaviyo payload.
-- Generated from the dimension registry (propensity.config.dimensions) so it matches
-- delivery.sync_log.LOG_COLS exactly. New table (v3 klaviyo_sync_log left untouched).
--
-- One row per profile per run. Append-only. Loaded via CSV -> S3 -> COPY (delivery/sync_log.py).
-- DISTKEY(email) so a profile's history colocates; SORTKEY(run_date) for run-scoped scans.
-- (Redshift folds the mixed-case column names to lowercase, as it did for the v3 table.)

CREATE TABLE IF NOT EXISTS propensity.klaviyo_sync_log_v4 (
    country                    VARCHAR(8),
    run_date                   DATE,
    synced_at                  TIMESTAMP,
    dry_run                    BOOLEAN,
    email                      VARCHAR(320),
    prop_1_score               REAL,
    prop_1_band                VARCHAR(8),
    prop_2_score               REAL,
    prop_2_band                VARCHAR(8),
    prop_3_score               REAL,
    prop_3_band                VARCHAR(8),
    prop_1_Category            VARCHAR(128),
    prop_1_Category_score      REAL,
    prop_1_Category_band       VARCHAR(8),
    prop_2_Category            VARCHAR(128),
    prop_2_Category_score      REAL,
    prop_2_Category_band       VARCHAR(8),
    prop_3_Category            VARCHAR(128),
    prop_3_Category_score      REAL,
    prop_3_Category_band       VARCHAR(8),
    prop_1_stone               VARCHAR(128),
    prop_1_stone_score         REAL,
    prop_1_stone_band          VARCHAR(8),
    prop_2_stone               VARCHAR(128),
    prop_2_stone_score         REAL,
    prop_2_stone_band          VARCHAR(8),
    prop_3_stone               VARCHAR(128),
    prop_3_stone_score         REAL,
    prop_3_stone_band          VARCHAR(8),
    prop_1_Jewelry             VARCHAR(128),
    prop_1_Jewelry_score       REAL,
    prop_1_Jewelry_band        VARCHAR(8),
    prop_2_Jewelry             VARCHAR(128),
    prop_2_Jewelry_score       REAL,
    prop_2_Jewelry_band        VARCHAR(8),
    prop_3_Jewelry             VARCHAR(128),
    prop_3_Jewelry_score       REAL,
    prop_3_Jewelry_band        VARCHAR(8),
    prop_1_metal               VARCHAR(128),
    prop_1_metal_score         REAL,
    prop_1_metal_band          VARCHAR(8),
    prop_2_metal               VARCHAR(128),
    prop_2_metal_score         REAL,
    prop_2_metal_band          VARCHAR(8),
    prop_3_metal               VARCHAR(128),
    prop_3_metal_score         REAL,
    prop_3_metal_band          VARCHAR(8),
    prop_stone_origin          VARCHAR(128),
    prop_stone_origin_score    REAL,
    prop_stone_origin_band     VARCHAR(8),
    prop_product_gender        VARCHAR(128),
    prop_product_gender_score  REAL,
    prop_product_gender_band   VARCHAR(8),
    prop_price_range           VARCHAR(128),
    prop_price_range_score     REAL,
    prop_price_range_band      VARCHAR(8),
    prop_model_version         VARCHAR(16),
    prop_scored_at             DATE,
    test_bucket                SMALLINT,
    valid_email                BOOLEAN
)
DISTSTYLE KEY
DISTKEY (email)
COMPOUND SORTKEY (run_date, country);

-- Re-apply the same grants used on propensity.klaviyo_sync_log, e.g.:
-- GRANT SELECT ON propensity.klaviyo_sync_log_v4 TO <reporting_role>;
