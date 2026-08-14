-- propensity.fact_propensity_attributes   (v4 — per-dimension propensity)
-- Grain : (date, visitor_id, country, product_category, stone_type, jewelry_type,
--          metal_type, lab_category, gender, product_price_range, device, channel)
-- Key   : visitor_id = actual Adobe id (matches profiles.dim_visitor_email_map);
--         email is resolved at READ time, never stored here.
-- Refresh: incremental, trailing-reprocess via sp_load_fact_propensity_attributes (mirrors the
--          category proc's self-watermark).
-- Source : reporting.fact_web_analytics + dims (dim_product carries every attribute).
--
-- WHY ONE FACT: v4 scores each dimension independently. Rather than one fact per dimension, this
--   single fact carries one VARCHAR column per attribute; the read layer aggregates per
--   dimension with GROUP BY email, {dimension_column}. Plain typed columns (no SUPER/JSON) so
--   Redshift columnar scan/aggregation stays fast.
-- METRICS: product_views / cart_additions / checkouts are reporting funnel UV FLAGS (same as
--   fact_propensity_category), NOT raw counts.
-- NOISE : attribute values are stored VERBATIM (incl. 'Not Available' / 'CWA'). Do NOT filter
--   them here — a product with no metal still has a stone/category. Filtering is PER-DIMENSION
--   at read time (e.g. WHERE metal_type NOT IN ('Not Available','CWA')).

CREATE TABLE IF NOT EXISTS propensity.fact_propensity_attributes (
    date                 date          NOT NULL  ENCODE az64,
    visitor_id           VARCHAR(100)  NOT NULL  ENCODE zstd,   -- actual Adobe id (matches the map)
    country              VARCHAR(10)   NOT NULL  ENCODE zstd,
    -- dimension attributes (one column per v4 dimension) --
    product_category     VARCHAR(255)            ENCODE zstd,
    stone_type           VARCHAR(100)            ENCODE zstd,
    jewelry_type         VARCHAR(100)            ENCODE zstd,
    metal_type           VARCHAR(100)            ENCODE zstd,   -- kept WITH karat ("14K Yellow Gold")
    lab_category         VARCHAR(50)             ENCODE zstd,   -- stone_origin: Natural / Lab Grown
    gender               VARCHAR(50)             ENCODE zstd,   -- product_gender
    product_price_range  VARCHAR(50)             ENCODE zstd,   -- price_range: pre-bucketed tier
    -- context --
    device               VARCHAR(20)             ENCODE zstd,
    channel              VARCHAR(50)             ENCODE zstd,
    -- metrics --
    sessions             INTEGER                 ENCODE az64,
    product_views        INTEGER                 ENCODE az64,
    cart_additions       INTEGER                 ENCODE az64,
    checkouts            INTEGER                 ENCODE az64,
    orders               INTEGER                 ENCODE az64,
    etl_loaded_at        TIMESTAMP               ENCODE az64  DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (visitor_id)                 -- same distkey as dim_visitor_email_map -> co-located email join
COMPOUND SORTKEY (country, date);    -- model filters by country + date window
