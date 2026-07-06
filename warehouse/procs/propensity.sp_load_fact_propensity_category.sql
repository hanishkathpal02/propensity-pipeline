-- propensity.sp_load_fact_propensity_category
-- Self-watermarking incremental load. FROM is derived from the table's own high-water
-- mark (MAX(date) - 3d overlap), so it is gap-free at ANY run cadence (daily, 14-day, or
-- after a missed/failed run). TO is the exclusive upper bound (pass the geo latest-completed
-- day + 1). Empty table -> loads from p_backfill_start (full backfill).
--   Normal   : CALL propensity.sp_load_fact_propensity_category('US', DATE '2026-07-06', DATE '2024-01-01');
--   Backfill : DELETE the country's rows first (so MAX is NULL), then CALL as above.
--   Then     : ANALYZE propensity.fact_propensity_category;   (separate statement)

CREATE OR REPLACE PROCEDURE propensity.sp_load_fact_propensity_category(
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
        (SELECT MAX(date) FROM propensity.fact_propensity_category WHERE country = p_country) - 3,
        p_backfill_start
    );

    -- idempotent: clear the window for this country, then reload it
    DELETE FROM propensity.fact_propensity_category
    WHERE country = p_country
      AND date >= v_from
      AND date <  p_to_date;

    INSERT INTO propensity.fact_propensity_category
        (date, visitor_id, country, product_category, device, channel,
         sessions, product_views, cart_additions, checkouts, orders)
    SELECT
        dt.full_date                          AS date,
        dv.visitor_id_actual                  AS visitor_id,   -- bridge surrogate -> actual
        f.country,
        dp.product_category,
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
      AND dp.product_category IS NOT NULL
    GROUP BY dt.full_date, dv.visitor_id_actual, f.country,
             dp.product_category, ddev.device_name, dcmp.channel;
END;
$$ LANGUAGE plpgsql;
