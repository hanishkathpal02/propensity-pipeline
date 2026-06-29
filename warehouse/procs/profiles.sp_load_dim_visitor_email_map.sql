-- profiles.sp_load_dim_visitor_email_map
-- FULL refresh of the visitor -> email identity map. Run daily.
--   CALL profiles.sp_load_dim_visitor_email_map();
--   ANALYZE profiles.dim_visitor_email_map;   -- separate statement; ANALYZE can't run inside a txn

CREATE OR REPLACE PROCEDURE profiles.sp_load_dim_visitor_email_map()
AS $$
BEGIN
    -- DELETE (transactional) not TRUNCATE: keeps the refresh atomic so consumers
    -- never see an empty table if the INSERT fails. TRUNCATE auto-commits in Redshift.
    DELETE FROM profiles.dim_visitor_email_map;

    INSERT INTO profiles.dim_visitor_email_map (visitor_id, country, email)
    WITH emails AS (
        SELECT visitor_id, country, hashed_email_id_evar83   AS encrypted_email, 1 AS priority, date_time
        FROM dw_click_stream
        WHERE hashed_email_id_evar83 IS NOT NULL

        UNION ALL

        SELECT visitor_id, country, encrypted_user_id_evar129 AS encrypted_email, 2 AS priority, date_time
        FROM dw_click_stream
        WHERE encrypted_user_id_evar129 IS NOT NULL
    ),
    ranked_emails AS (
        SELECT visitor_id, country, encrypted_email,
               ROW_NUMBER() OVER (
                   PARTITION BY visitor_id, country
                   ORDER BY priority ASC, date_time DESC
               ) AS rn
        FROM emails
    ),
    visitor_email_map AS (
        SELECT visitor_id, country, encrypted_email
        FROM ranked_emails
        WHERE rn = 1
    ),
    resolved AS (
        SELECT vem.visitor_id, vem.country, ei.decrypted_email AS email,
               ROW_NUMBER() OVER (
                   PARTITION BY vem.visitor_id, vem.country
                   ORDER BY ei.decrypted_email
               ) AS dedup_rn
        FROM visitor_email_map vem
        JOIN email_info ei                      -- INNER: only resolvable visitors land in the dim
          ON ei.encrypted_email = vem.encrypted_email
         AND ei.is_valid_email = 'True'
        WHERE ei.decrypted_email IS NOT NULL
    )
    SELECT visitor_id, country, email
    FROM resolved
    WHERE dedup_rn = 1;                          -- one row per (visitor_id, country)
END;
$$ LANGUAGE plpgsql;
