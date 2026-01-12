USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
--last non direct
SELECT b.touch_start_tstamp::DATE                              AS date,
       c.touch_mkt_channel                                     AS last_non_direct_channel,
       b.touch_experience,
       c.touch_affiliate_territory,
       b.touch_hostname_territory,
       COUNT(DISTINCT b.touch_id)                              AS sessions,
       SUM(CASE WHEN t.touch_id IS NOT NULL THEN 1 ELSE 0 END) AS bookings

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions t ON b.touch_id = t.touch_id
WHERE b.touch_start_tstamp >= DATEADD(WEEK, -4, current_date)
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;

------------------------------------------------------------------------------------------------------------------------
--direct
SELECT b.touch_start_tstamp::DATE                              AS date,
       c.touch_mkt_channel,
       b.touch_experience,
       c.touch_affiliate_territory,
       b.touch_hostname_territory,
       COUNT(DISTINCT b.touch_id)                              AS sessions,
       COUNT(DISTINCT b.attributed_user_id)                    AS users,
       SUM(CASE WHEN t.touch_id IS NOT NULL THEN 1 ELSE 0 END) AS bookings
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON b.touch_id = c.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions t ON b.touch_id = t.touch_id
WHERE b.touch_start_tstamp >= DATEADD(WEEK, -4, current_date)
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5;


------------------------------------------------------------------------------------------------------------------------
--tableau sample query

SELECT b.touch_start_tstamp::DATE           AS date,
--        c.touch_mkt_channel,
--        b.touch_experience,
       CASE
           WHEN b.touch_experience = 'native app' THEN 'Native App'
           WHEN c.touch_mkt_channel LIKE 'Email%' THEN 'Web Email'
           ELSE 'Web Non Email'
           END                              AS category,
       c.touch_affiliate_territory,
       COUNT(DISTINCT b.touch_id)           AS sessions,
       COUNT(DISTINCT b.attributed_user_id) AS users,
       COUNT(DISTINCT t.booking_id)         AS bookings,
       COUNT(DISTINCT fb.booking_id)        AS complete_bookings
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON b.touch_id = c.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions t ON b.touch_id = t.touch_id
         LEFT JOIN se.data.fact_complete_booking fb ON t.booking_id = fb.booking_id
WHERE b.touch_start_tstamp >= DATEADD(WEEK, -4, current_date)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3, 4, 5;


