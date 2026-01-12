WITH sfmc AS (
    SELECT sfmc_ua.subscriber_key               AS shiro_user_id,
           sfmc_ua.last_open_date               AS sfmc_last_email_open_date,
           sfmc_ua.last_click_date              AS sfmc_last_email_click_date,
           sfmc_ua.last_spv_date                AS sfmc_last_sale_pageview_date,
           sfmc_ua.last_purchase_date           AS sfmc_last_purchase_date,
           GREATEST(sfmc_ua.last_open_date,
                    sfmc_ua.last_click_date,
                    sfmc_ua.last_spv_date,
                    sfmc_ua.last_purchase_date) AS sfmc_last_activity
    FROM archive.sfmc.user_activity sfmc_ua
)
   , snowflake AS (
    SELECT iupa.shiro_user_id,
           iupa.last_email_open_tstamp::DATE    AS snowflake_last_email_open_date,
           iupa.last_email_click_tstamp::DATE   AS snowflake_last_email_click_date,
           iupa.last_sale_pageview_tstamp::DATE AS snowflake_last_sale_pageview_date,
           iupa.last_purchase_tstamp::DATE      AS snowflake_last_purchase_date,
           GREATEST(last_email_open_tstamp::DATE,
                    last_email_click_tstamp::DATE,
                    last_sale_pageview_tstamp::DATE,
                    last_purchase_tstamp::DATE) AS snowflake_last_activity
    FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
    WHERE snowflake_last_activity >= CURRENT_DATE - 31
)
   , modelling AS (
    SELECT s.shiro_user_id,
           s.sfmc_last_email_open_date,
           sf.snowflake_last_email_open_date,
           s.sfmc_last_email_open_date IS NOT DISTINCT FROM sf.snowflake_last_email_open_date       AS last_email_open_date_match,
           s.sfmc_last_email_click_date,
           sf.snowflake_last_email_click_date,
           s.sfmc_last_email_click_date IS NOT DISTINCT FROM sf.snowflake_last_email_click_date     AS last_email_click_date_match,
           s.sfmc_last_sale_pageview_date,
           sf.snowflake_last_sale_pageview_date,
           s.sfmc_last_sale_pageview_date IS NOT DISTINCT FROM sf.snowflake_last_sale_pageview_date AS last_sale_pageview_date_match,
           s.sfmc_last_purchase_date,
           sf.snowflake_last_purchase_date,
           s.sfmc_last_purchase_date IS NOT DISTINCT FROM sf.snowflake_last_purchase_date           AS last_purchase_date_match,
           s.sfmc_last_activity,
           sf.snowflake_last_activity,
           s.sfmc_last_activity IS NOT DISTINCT FROM sf.snowflake_last_activity                     AS greatest_activity_match
    FROM sfmc s
        INNER JOIN snowflake sf ON s.shiro_user_id = sf.shiro_user_id
)
SELECT *
FROM modelling m
WHERE m.greatest_activity_match = FALSE

-- SELECT modelling.greatest_activity_match,
--        COUNT(*)
-- FROM modelling
-- GROUP BY 1

-- SELECT *
-- FROM modelling m
-- WHERE m.greatest_activity_match = FALSE
--   AND m.last_purchase_date_match = FALSE

-- SELECT m.sfmc_last_purchase_date IS NULL      AS sfmc_is_null,
--        m.snowflake_last_purchase_date IS NULL AS snowflake_is_null,
--        COUNT(*)
-- FROM modelling m
-- WHERE m.greatest_activity_match = FALSE
--   AND m.last_purchase_date_match = FALSE
-- GROUP BY 1, 2
;

SELECT COUNT(*)
FROM archive.sfmc.user_activity sfmc_ua;



