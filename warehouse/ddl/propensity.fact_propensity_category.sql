-- propensity.fact_propensity_category
-- Grain : (date, visitor_id, country, product_category, device, channel)
-- Key   : visitor_id = actual Adobe id (matches profiles.dim_visitor_email_map);
--         email is resolved at READ time, never stored here.
-- Refresh: incremental, trailing-reprocess via sp_load_fact_propensity_category.
-- Source : reporting.fact_web_analytics + dims (dim_visitor bridges surrogate -> actual id).
--
-- NOTE: product_views / cart_additions / checkouts are derived from reporting funnel
--       UV FLAGS (pdp/cart/checkout_uv_flag), not raw event counts. Scale/semantics
--       differ from the old dw_click_stream version -> retrain (prop_model_version v2).

CREATE TABLE IF NOT EXISTS propensity.fact_propensity_category (
    date              date          NOT NULL  ENCODE az64,
    visitor_id        VARCHAR(100)  NOT NULL  ENCODE zstd,   -- actual Adobe id (matches the map)
    country           VARCHAR(10)   NOT NULL  ENCODE zstd,
    product_category  VARCHAR(255)            ENCODE zstd,
    device            VARCHAR(20)             ENCODE zstd,
    channel           VARCHAR(50)             ENCODE zstd,
    sessions          INTEGER                 ENCODE az64,
    product_views     INTEGER                 ENCODE az64,
    cart_additions    INTEGER                 ENCODE az64,
    checkouts         INTEGER                 ENCODE az64,
    orders            INTEGER                 ENCODE az64,
    etl_loaded_at     TIMESTAMP               ENCODE az64  DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (visitor_id)                 -- same distkey as dim_visitor_email_map -> co-located email join
COMPOUND SORTKEY (country, date);    -- model filters by country + date window
