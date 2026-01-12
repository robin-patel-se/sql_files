USE WAREHOUSE pipe_xlarge;

WITH temptable AS (
    SELECT tr.touch_id,
           tr.event_tstamp,
           tr.booking_id,
           CASE
               WHEN LEFT(tr.booking_id, 2) != 'TB' THEN 'se_booking'
               WHEN LEFT(tr.booking_id, 2) = 'TB' THEN 'tb_booking'
               ELSE 'other' END AS booking_code,
           b.attributed_user_id AS se_user_id,
           ch.touch_hostname_territory,
           ch.touch_affiliate_territory,
           b.stitched_identity_type,
           ch.touch_mkt_channel AS last_non_direct_channel,
           cb.margin_gross_of_toms_gbp
    FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tr
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                        ON tr.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel ch
                        ON ta.attributed_touch_id = ch.touch_id
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
                        ON ta.attributed_touch_id = b.touch_id
             INNER JOIN se.data.fact_complete_booking cb ON tr.booking_id = cb.booking_id
    WHERE tr.event_tstamp >= '2020-02-28'
)

SELECT event_tstamp::DATE                                           AS booking_date,
       last_non_direct_channel,
       booking_code,
       touch_hostname_territory,
       touch_affiliate_territory,
       count(DISTINCT se_user_id)                                   AS members,
       SUM(CASE WHEN LEFT(booking_id, 2) != 'TB' THEN 1 ELSE 0 END) AS se_bookings,
       SUM(CASE WHEN LEFT(booking_id, 2) = 'TB' THEN 1 ELSE 0 END)  AS tb_bookings,
       sum(margin_gross_of_toms_gbp)                                AS margins
FROM temptable
GROUP BY 1, 2, 3, 4, 5



SELECT b.touch_start_tstamp::DATE                                      AS date,
       c.touch_mkt_channel                                             AS last_non_direct_mkt_channel,
       c.touch_affiliate_territory,
       COUNT(DISTINCT b.attributed_user_id)                            AS users,
       COUNT(DISTINCT spv.touch_id)                                    AS touches,
       COUNT(spv.event_hash)                                           AS spvs,
       COUNT(DISTINCT spv.se_sale_id, spv.touch_id)                    AS unique_spvs,
       SUM(CASE WHEN LEFT(fb.booking_id, 2) != 'TB' THEN 1 ELSE 0 END) AS se_bookings,
       SUM(CASE WHEN LEFT(fb.booking_id, 2) = 'TB' THEN 1 ELSE 0 END)  AS tb_bookings,
       SUM(fb.margin_gross_of_toms_gbp)                                AS margin_gross_toms
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs spv ON b.touch_id = spv.touch_id
         LEFT JOIN se.data.dim_sale s ON spv.se_sale_id = s.sale_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions tr ON b.touch_id = tr.touch_id
         LEFT JOIN se.data.fact_complete_booking fb ON tr.booking_id = fb.booking_id
WHERE b.touch_start_tstamp::DATE >= '2020-02-28'
GROUP BY 1, 2, 3
ORDER BY 1;

SELECT DISTINCT event_subcategory
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions;

SELECT event_tstamp::DATE,
       event_subcategory,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
GROUP BY 1, 2
ORDER BY 1, 2

SELECT event_tstamp::DATE,
       SUM(CASE WHEN event_subcategory != 'backfill_booking' THEN 1 ELSE 0 END) AS non_backfill_bookings,
       SUM(CASE WHEN event_subcategory = 'backfill_booking' THEN 1 ELSE 0 END)  AS backfill_bookings
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
GROUP BY 1
ORDER BY 1;

------------------------------------------------------------------------------------------------------------------------

--rose original query
WITH temptable
         AS
         (SELECT tr.touch_id,
                 tr.event_tstamp,
                 tr.booking_id,
--                  CASE
--                      WHEN LEFT(tr.booking_id, 2) != 'TB' THEN 'se_booking'
--                      WHEN LEFT(tr.booking_id, 2) = 'TB' THEN 'tb_booking'
--                      ELSE 'other' END AS booking_code,
                 b.attributed_user_id AS se_user_id,
                 ch.touch_hostname_territory,
                 ch.touch_affiliate_territory,
                 b.stitched_identity_type,
                 ch.touch_mkt_channel AS last_non_direct_channel,
                 cb.margin_gross_of_toms_gbp
          FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions tr
                   INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution ta
                              ON tr.touch_id = ta.touch_id AND ta.attribution_model = 'last non direct'
                   INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel ch
                              ON ta.attributed_touch_id = ch.touch_id
                   INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
                              ON ta.attributed_touch_id = b.touch_id
                   INNER JOIN se.data.fact_complete_booking cb ON tr.booking_id = cb.booking_id
          WHERE tr.event_tstamp >= '2020-02-28')

SELECT event_tstamp::DATE                                                        AS booking_date,
--        last_non_direct_channel,
--        booking_code,
--        touch_hostname_territory,
--        touch_affiliate_territory,
--        count(DISTINCT se_user_id)                                                   AS members,
--        SUM(CASE WHEN LEFT(booking_id, 2) != 'TB' THEN 1 ELSE 0 END) AS se_bookings,
--        SUM(CASE WHEN LEFT(booking_id, 2) = 'TB' THEN 1 ELSE 0 END)  AS tb_bookings,
       COUNT(DISTINCT CASE WHEN LEFT(booking_id, 2) != 'TB' THEN booking_id END) AS se_bookings,
       COUNT(DISTINCT CASE WHEN LEFT(booking_id, 2) = 'TB' THEN booking_id END)  AS tb_bookings
--        sum(margin_gross_of_toms_gbp)                                                AS margins
FROM temptable
GROUP BY 1--, 2, 3, 4, 5
ORDER BY 1
;

USE WAREHOUSE pipe_xlarge;

--robin version
SELECT b.touch_start_tstamp::DATE                                                               AS date,
       c.touch_mkt_channel                                                                      AS last_non_direct_mkt_channel,
       c.touch_affiliate_territory,
       COUNT(DISTINCT b.attributed_user_id)                                                     AS users,
       COUNT(DISTINCT b.touch_id)                                                               AS touches,
       SUM(CASE WHEN LEFT(fb.booking_id, 2) != 'TB' THEN 1 ELSE 0 END)                          AS se_bookings,
       SUM(CASE WHEN LEFT(fb.booking_id, 2) = 'TB' THEN 1 ELSE 0 END)                           AS tb_bookings,
       COUNT(DISTINCT CASE WHEN LEFT(tr.booking_id, 2) != 'TB' THEN tr.booking_id END)          AS se_bookings,
       COUNT(DISTINCT
             CASE WHEN LEFT(tr.booking_id, 2) = 'TB' THEN tr.booking_id END)                    AS tb_bookings,
       SUM(fb.margin_gross_of_toms_gbp)                                                         AS margin_gross_toms,
       SUM(CASE
               WHEN LEFT(fb.booking_id, 2) != 'TB' THEN fb.margin_gross_of_toms_gbp
               ELSE 0 END)                                                                      AS se_margin_gross_toms,
       SUM(CASE WHEN LEFT(fb.booking_id, 2) = 'TB' THEN fb.margin_gross_of_toms_gbp ELSE 0 END) AS tb_margin_gross_toms
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions tr ON b.touch_id = tr.touch_id
         LEFT JOIN se.data.fact_complete_booking fb ON tr.booking_id = fb.booking_id
WHERE b.touch_start_tstamp::DATE >= '2020-02-28'
GROUP BY 1, 2, 3
ORDER BY 1;


SELECT b.touch_start_tstamp::DATE                                                      AS date,
       count(DISTINCT booking_id)                                                      AS bookings,
       SUM(CASE WHEN LEFT(tr.booking_id, 2) != 'TB' THEN 1 ELSE 0 END)                 AS se_bookings,
       SUM(CASE WHEN LEFT(tr.booking_id, 2) = 'TB' THEN 1 ELSE 0 END)                  AS tb_bookings,
       COUNT(DISTINCT CASE WHEN LEFT(tr.booking_id, 2) != 'TB' THEN tr.booking_id END) AS se_bookings_new,
       COUNT(DISTINCT CASE WHEN LEFT(tr.booking_id, 2) = 'TB' THEN tr.booking_id END)  AS tb_bookings_new
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions tr ON tr.touch_id = b.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
WHERE touch_start_tstamp >= '2020-02-28'
GROUP BY 1;





