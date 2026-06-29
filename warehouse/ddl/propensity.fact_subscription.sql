-- propensity.fact_subscription
-- Grain : (subscription_date, visitor_id, country, subscriber_channel)
-- Key   : visitor_id = actual Adobe id (matches profiles.dim_visitor_email_map);
--         email resolved at READ time, never stored here.
-- Refresh: incremental, trailing-reprocess via sp_load_fact_subscription.
-- Source : reporting.fact_web_analytics (subscription_uv_flag > 0) + dims.
--
-- NOTE: subscriber_channel = dim_campaign.channel (last-touch, via campaign_id_lt),
--       not the old last_channel_evar25 -> consistent with fact_propensity_category's
--       channel, part of the v2 retrain.

CREATE TABLE IF NOT EXISTS propensity.fact_subscription (
    subscription_date   date          NOT NULL  ENCODE az64,
    visitor_id          VARCHAR(100)  NOT NULL  ENCODE zstd,   -- actual Adobe id (matches map)
    country             VARCHAR(10)   NOT NULL  ENCODE zstd,
    subscriber_channel  VARCHAR(100)            ENCODE zstd,
    etl_loaded_at       TIMESTAMP               ENCODE az64  DEFAULT SYSDATE
)
DISTSTYLE KEY
DISTKEY (visitor_id)
COMPOUND SORTKEY (country, subscription_date);
