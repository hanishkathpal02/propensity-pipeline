-- propensity.fact_klaviyo_engagements
-- Grain : (date, email, country)   [EMAIL-grain — Klaviyo events have no visitor_id, no bridge]
-- Refresh: incremental, trailing-reprocess via sp_load_fact_klaviyo_engagements.
-- Source : public.klaviyo_events_new + public.klaviyo_profiles_new (no reporting equivalent).
--
-- NOTE: Klaviyo source is currently US-only and has no country column, so `country` is
--       stamped as the literal p_country at load time. When the Klaviyo tables gain a
--       country column, add a `country = p_country` filter in the proc (see TODO there).

CREATE TABLE IF NOT EXISTS propensity.fact_klaviyo_engagements (
    date           date          NOT NULL  ENCODE az64,
    email          VARCHAR(255)  NOT NULL  ENCODE zstd,
    country        VARCHAR(10)   NOT NULL  ENCODE zstd,
    email_opens    INTEGER                 ENCODE az64,
    email_clicks   INTEGER                 ENCODE az64,
    etl_loaded_at  TIMESTAMP               ENCODE az64  DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (email)                      -- email-grain fact; model merges on email
COMPOUND SORTKEY (country, date);
