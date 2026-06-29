-- propensity.sp_load_fact_klaviyo_engagements
-- Incremental, trailing-reprocess load. Same proc does backfill and daily refresh.
--   Backfill : CALL propensity.sp_load_fact_klaviyo_engagements('US', DATE '2024-01-01', CURRENT_DATE + 1);
--   Daily    : CALL propensity.sp_load_fact_klaviyo_engagements('US', CURRENT_DATE - 3, CURRENT_DATE + 1);
--   Then     : ANALYZE propensity.fact_klaviyo_engagements;   (separate statement)
-- p_to_date is an EXCLUSIVE upper bound — pass CURRENT_DATE + 1 to include today.
--
-- Klaviyo lives on the CDP cluster (cdp-angara-redshift / rudderstack_prod) and is read
-- here via the `shared_klaviyo` datashare mounted on the dev cluster. Country is stamped
-- as a literal (Klaviyo source is US-only for now); when the Klaviyo tables gain a country
-- column, add `AND <country_col> = p_country` to both source references.

CREATE OR REPLACE PROCEDURE propensity.sp_load_fact_klaviyo_engagements(
    p_country   VARCHAR,
    p_from_date DATE,
    p_to_date   DATE
)
AS $$
BEGIN
    -- idempotent: clear the window for this country, then reload it
    DELETE FROM propensity.fact_klaviyo_engagements
    WHERE country = p_country
      AND date >= p_from_date
      AND date <  p_to_date;

    INSERT INTO propensity.fact_klaviyo_engagements
        (date, email, country, email_opens, email_clicks)
    WITH profile_id_email AS (
        SELECT DISTINCT
            profile_id AS id,
            COALESCE(
                NULLIF(email, ''),
                TRIM(BOTH '"' FROM (event_properties.email_address::varchar))
            ) AS email
        FROM shared_klaviyo.public.klaviyo_events_new
        WHERE email IS NOT NULL OR event_properties.email_address IS NOT NULL

        UNION

        SELECT DISTINCT id, email
        FROM shared_klaviyo.public.klaviyo_profiles_new
        WHERE email IS NOT NULL AND email <> ''
    )
    SELECT
        DATE(e."timestamp")  AS date,
        p.email,
        p_country            AS country,          -- literal; Klaviyo source is US-only for now
        COUNT(DISTINCT CASE WHEN e.metric_name = 'Opened Email'  THEN e.event_id END) AS email_opens,
        COUNT(DISTINCT CASE WHEN e.metric_name = 'Clicked Email' THEN e.event_id END) AS email_clicks
    FROM shared_klaviyo.public.klaviyo_events_new e
    LEFT JOIN profile_id_email p ON e.profile_id = p.id
    WHERE e.metric_name IN ('Opened Email', 'Clicked Email')
      AND DATE(e."timestamp") >= p_from_date
      AND DATE(e."timestamp") <  p_to_date
      AND (e.event_properties."Bot Click" IS NULL OR e.event_properties."Bot Click" = 'false')
    GROUP BY DATE(e."timestamp"), p.email
    HAVING p.email IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
