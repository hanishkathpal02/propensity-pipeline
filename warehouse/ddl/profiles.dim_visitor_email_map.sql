-- profiles.dim_visitor_email_map
-- Grain : one row per (visitor_id, country) — the resolved email for that visitor.
-- Refresh: FULL refresh daily (DELETE + INSERT) via sp_load_dim_visitor_email_map.
--          Never incremental — global email resolution must be recomputed each run
--          so newly-identified visitors are attributed across all history.
-- Source : public.dw_click_stream + public.email_info (email exists only in raw clickstream).

CREATE TABLE IF NOT EXISTS profiles.dim_visitor_email_map (
    visitor_id     VARCHAR(100)  NOT NULL  ENCODE zstd,   -- actual Adobe id ("big_big")
    email          VARCHAR(255)  NOT NULL  ENCODE zstd,
    country        VARCHAR(10)   NOT NULL  ENCODE zstd,
    etl_loaded_at  TIMESTAMP               ENCODE az64  DEFAULT SYSDATE,
    PRIMARY KEY (visitor_id, country)                     -- not enforced by Redshift; documents grain
)
DISTSTYLE KEY
DISTKEY (visitor_id)                 -- co-located with fact tables that key on visitor_id
COMPOUND SORTKEY (country, visitor_id);
