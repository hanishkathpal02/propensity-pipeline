-- propensity.sp_load_fact_subscription
-- Self-watermarking incremental load. FROM is derived from the table's own high-water
-- mark (MAX(subscription_date) - 3d overlap), so it is gap-free at ANY run cadence.
-- TO is the exclusive upper bound (pass the geo latest-completed day + 1). Empty table
-- -> loads from p_backfill_start (full backfill).
--   Normal   : CALL propensity.sp_load_fact_subscription('US', DATE '2026-07-06', DATE '2024-01-01');
--   Backfill : DELETE the country's rows first (so MAX is NULL), then CALL as above.
--   Then     : ANALYZE propensity.fact_subscription;   (separate statement)

CREATE OR REPLACE PROCEDURE propensity.sp_load_fact_subscription(
    p_country        VARCHAR,
    p_to_date        DATE,
    p_backfill_start DATE
)
AS $$
DECLARE
    v_from DATE;
BEGIN
    v_from := COALESCE(
        (SELECT MAX(subscription_date) FROM propensity.fact_subscription WHERE country = p_country) - 3,
        p_backfill_start
    );

    -- idempotent: clear the window for this country, then reload it
    DELETE FROM propensity.fact_subscription
    WHERE country = p_country
      AND subscription_date >= v_from
      AND subscription_date <  p_to_date;

    INSERT INTO propensity.fact_subscription
        (subscription_date, visitor_id, country, subscriber_channel)
    SELECT
        dt.full_date           AS subscription_date,
        dv.visitor_id_actual   AS visitor_id,      -- bridge surrogate -> actual
        f.country,
        dcmp.channel           AS subscriber_channel
    FROM reporting.fact_web_analytics f
    JOIN      reporting.dim_visitor  dv    ON f.visitor_id     = dv.visitor_id
    JOIN      reporting.dim_date     dt    ON f.date_id        = dt.date_id
    LEFT JOIN reporting.dim_campaign dcmp  ON f.campaign_id_lt = dcmp.campaign_id
    WHERE f.country = p_country
      AND f.subscription_uv_flag > 0
      AND f.date_id >= CAST(TO_CHAR(v_from,    'YYYYMMDD') AS INT)
      AND f.date_id <  CAST(TO_CHAR(p_to_date, 'YYYYMMDD') AS INT)
    GROUP BY dt.full_date, dv.visitor_id_actual, f.country, dcmp.channel;
END;
$$ LANGUAGE plpgsql;
