-- propensity.sp_load_fact_subscription
-- Incremental, trailing-reprocess load. Same proc does backfill and daily refresh.
--   Backfill : CALL propensity.sp_load_fact_subscription('US', DATE '2024-01-01', CURRENT_DATE + 1);
--   Daily    : CALL propensity.sp_load_fact_subscription('US', CURRENT_DATE - 3, CURRENT_DATE + 1);
--   Then     : ANALYZE propensity.fact_subscription;   (separate statement)
-- p_to_date is an EXCLUSIVE upper bound — pass CURRENT_DATE + 1 to include today.

CREATE OR REPLACE PROCEDURE propensity.sp_load_fact_subscription(
    p_country   VARCHAR,
    p_from_date DATE,
    p_to_date   DATE
)
AS $$
BEGIN
    -- idempotent: clear the window for this country, then reload it
    DELETE FROM propensity.fact_subscription
    WHERE country = p_country
      AND subscription_date >= p_from_date
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
      AND f.date_id >= CAST(TO_CHAR(p_from_date, 'YYYYMMDD') AS INT)
      AND f.date_id <  CAST(TO_CHAR(p_to_date,   'YYYYMMDD') AS INT)
    GROUP BY dt.full_date, dv.visitor_id_actual, f.country, dcmp.channel;
END;
$$ LANGUAGE plpgsql;
