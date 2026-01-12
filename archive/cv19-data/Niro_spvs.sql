USE WAREHOUSE pipe_large;



SELECT spv.event_tstamp::DATE                       AS date,
       b.touch_experience,
       b.touch_hostname,
       c.touch_affiliate_territory,
       c.touch_mkt_channel                          AS last_non_direct_mkt_channel,
       COUNT(DISTINCT b.attributed_user_id)         AS users,
       COUNT(DISTINCT spv.touch_id)                 AS touches,
       COUNT(spv.event_hash)                        AS spvs,
       COUNT(DISTINCT spv.se_sale_id, spv.touch_id) AS unique_spvs
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs spv ON b.touch_id = spv.touch_id
WHERE spv.event_tstamp::DATE >= '2019-01-01'
  AND c.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1;

------------------------------------------------------------------------------------------------------------------------

SELECT utm_campaign,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
WHERE touch_mkt_channel = 'Email - Newsletter'
GROUP BY 1
ORDER BY 2 DESC;

SELECT utm_campaign,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
WHERE touch_mkt_channel = 'Email - Other'
GROUP BY 1
ORDER BY 2 DESC;

SELECT touch_id,
       touch_mkt_channel,
       touch_landing_page,
       touch_hostname,
       touch_hostname_territory,
       attributed_user_id,
       utm_campaign,
       utm_medium,
       utm_source,
       utm_term,
       utm_content,
       click_id,
       sub_affiliate_name,
       affiliate,
       touch_affiliate_territory,
       awadgroupid,
       awcampaignid,
       referrer_hostname,
       referrer_medium
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
WHERE touch_mkt_channel IN ('Email - Other', 'Email - Newsletter')
GROUP BY 1
ORDER BY 2 DESC;

------------------------------------------------------------------------------------------------------------------------

SELECT spv.event_tstamp::DATE                       AS date,
       b.touch_experience,
       b.touch_hostname,
       c.touch_affiliate_territory,
       c.touch_mkt_channel                          AS last_non_direct_mkt_channel,
       s.product_configuration,
       s.product_type,
       COUNT(DISTINCT b.attributed_user_id)         AS users,
       COUNT(DISTINCT spv.touch_id)                 AS touches,
       COUNT(spv.event_hash)                        AS spvs,
       COUNT(DISTINCT spv.se_sale_id, spv.touch_id) AS unique_spvs
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs spv ON b.touch_id = spv.touch_id
         LEFT JOIN se.data.dim_sale s ON spv.se_sale_id = s.sale_id
WHERE spv.event_tstamp::DATE >= '2019-01-01'
  AND c.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1;

USE WAREHOUSE pipe_xlarge;

SELECT spv.event_tstamp::DATE                       AS date,
       b.touch_experience,
       b.touch_hostname,
       c.touch_affiliate_territory,
       c.touch_mkt_channel                          AS last_non_direct_mkt_channel,
       s.product_configuration,
       s.product_type,
       COUNT(DISTINCT b.attributed_user_id)         AS users,
       COUNT(DISTINCT spv.touch_id)                 AS touches,
       COUNT(spv.event_hash)                        AS spvs,
       COUNT(DISTINCT spv.se_sale_id, spv.touch_id) AS unique_spvs,
       COUNT(DISTINCT fb.booking_id)                AS bookings,
       SUM(fb.margin_gross_of_toms_gbp)             AS margin_gross_toms
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs spv ON b.touch_id = spv.touch_id
         INNER JOIN se.data.dim_sale s ON spv.se_sale_id = s.sale_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions tr ON b.touch_id = tr.touch_id
         LEFT JOIN se.data.fact_complete_booking fb ON tr.booking_id = fb.booking_id
WHERE spv.event_tstamp::DATE >= '2019-01-01'
  AND c.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1;


SELECT tr.event_tstamp::DATE         AS booking_date,
       COUNT(DISTINCT fb.booking_id) AS bookings

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs spv ON b.touch_id = spv.touch_id
         LEFT JOIN se.data.dim_sale s ON spv.se_sale_id = s.sale_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions tr ON b.touch_id = tr.touch_id
         LEFT JOIN se.data.fact_complete_booking fb ON tr.booking_id = fb.booking_id
WHERE spv.event_tstamp::DATE >= '2019-01-01'
  AND c.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1
ORDER BY 1;

USE WAREHOUSE pipe_large;

SELECT tr.event_tstamp::DATE         AS booking_date,
       b.touch_experience,
       c.touch_mkt_channel           AS last_non_direct_mkt_channel,
       COUNT(DISTINCT fb.booking_id) AS bookings

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                    ON a.attributed_touch_id = c.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_transactions tr ON b.touch_id = tr.touch_id
         LEFT JOIN se.data.fact_complete_booking fb ON tr.booking_id = fb.booking_id
WHERE tr.event_tstamp::DATE >= '2019-01-01'
  AND c.touch_affiliate_territory IN ('UK', 'DE')
GROUP BY 1, 2, 3
ORDER BY 1;

