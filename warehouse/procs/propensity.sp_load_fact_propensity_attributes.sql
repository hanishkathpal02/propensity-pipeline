-- propensity.sp_load_fact_propensity_attributes   (v4)
-- Self-watermarking incremental load (identical pattern to sp_load_fact_propensity_category):
-- FROM = table's own high-water mark (MAX(date) - 3d overlap) -> gap-free at any cadence. TO is
-- the exclusive upper bound (geo latest-completed day + 1). Empty table -> full backfill from
-- p_backfill_start. Same fact_web_analytics -> dim_product join as the category proc; the extra
-- attribute columns just ride along in the SELECT + GROUP BY.
--   Backfill : DELETE FROM propensity.fact_propensity_attributes WHERE country='US';   -- so MAX is NULL
--              CALL propensity.sp_load_fact_propensity_attributes('US', DATE '2026-07-28', DATE '2024-01-01');
--   Normal   : CALL propensity.sp_load_fact_propensity_attributes('US', DATE '<geo_latest+1>', DATE '2024-01-01');
--   Then     : ANALYZE propensity.fact_propensity_attributes;    (separate statement)

CREATE OR REPLACE PROCEDURE propensity.sp_load_fact_propensity_attributes(
    p_country        VARCHAR,
    p_to_date        DATE,
    p_backfill_start DATE
)
AS $$
DECLARE
    v_from DATE;
BEGIN
    -- high-watermark: continue from where the table ends, minus a 3-day overlap
    v_from := COALESCE(
        (SELECT MAX(date) FROM propensity.fact_propensity_attributes WHERE country = p_country) - 3,
        p_backfill_start
    );

    -- idempotent: clear the window for this country, then reload it
    DELETE FROM propensity.fact_propensity_attributes
    WHERE country = p_country
      AND date >= v_from
      AND date <  p_to_date;

    INSERT INTO propensity.fact_propensity_attributes
        (date, visitor_id, country, product_category, stone_type, jewelry_type,
         metal_type, lab_category, gender, product_price_range, device, channel,
         sessions, product_views, cart_additions, checkouts, orders)
    SELECT
        dt.full_date                          AS date,
        dv.visitor_id_actual                  AS visitor_id,   -- bridge surrogate -> actual
        f.country,
        dp.product_category,
        dp.stone_type,
        dp.jewelry_type,
        dp.metal_type,                                          -- WITH karat, verbatim
        dp.lab_category,                                        -- stone_origin
        dp.gender,                                             -- product_gender
        dp.product_price_range,                                -- price_range tier
        ddev.device_name                      AS device,
        dcmp.channel                          AS channel,
        COUNT(DISTINCT f.session_key_id)      AS sessions,
        SUM(f.pdp_uv_flag)                    AS product_views,    -- flag-based proxy
        SUM(f.cart_uv_flag)                   AS cart_additions,   -- flag-based proxy
        SUM(f.checkout_uv_flag)               AS checkouts,        -- flag-based proxy
        SUM(CASE WHEN dord.is_valid_order > 0 THEN f.orders ELSE 0 END) AS orders
    FROM reporting.fact_web_analytics f
    JOIN      reporting.dim_visitor  dv    ON f.visitor_id  = dv.visitor_id
    JOIN      reporting.dim_date     dt    ON f.date_id     = dt.date_id
    JOIN      reporting.dim_product  dp    ON f.product_id  = dp.product_id
                                          AND dp.country    = f.country
                                          AND dp.is_valid_product > 0
    LEFT JOIN reporting.dim_device   ddev  ON f.device_id   = ddev.device_id
    LEFT JOIN reporting.dim_campaign dcmp  ON f.campaign_id_lt = dcmp.campaign_id
    LEFT JOIN reporting.dim_order    dord  ON f.order_id    = dord.order_id
    WHERE f.country = p_country
      -- prune on the fact's own date_id (zone-map friendly); dim_date join still gives full_date
      AND f.date_id >= CAST(TO_CHAR(v_from,    'YYYYMMDD') AS INT)
      AND f.date_id <  CAST(TO_CHAR(p_to_date, 'YYYYMMDD') AS INT)
      -- NOTE: no single-attribute NOT NULL filter here (a product missing metal still has a
      -- stone/category); per-dimension NULL/noise filtering happens at READ time.
    GROUP BY dt.full_date, dv.visitor_id_actual, f.country,
             dp.product_category, dp.stone_type, dp.jewelry_type, dp.metal_type,
             dp.lab_category, dp.gender, dp.product_price_range,
             ddev.device_name, dcmp.channel;
END;
$$ LANGUAGE plpgsql;
