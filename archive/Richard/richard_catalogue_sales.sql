SELECT e.app_id
     , count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
         JOIN data_science.mart_analytics.vw_deal_features vdf
              ON e.se_sale_id = vdf.deal_id
WHERE vdf.cms_model = 'Catalogue (CMS 3.0) '
GROUP BY 1
LIMIT 50;



SELECT is_server_side_event,
       count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
         JOIN data_science.mart_analytics.vw_deal_features vdf
              ON e.se_sale_id = vdf.deal_id
WHERE vdf.cms_model = 'Catalogue (CMS 3.0) '
  AND e.collector_tstamp::DATE = '2020-05-04'
GROUP BY 1
LIMIT 50;


SELECT is_server_side_event,
       count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
         JOIN data_science.mart_analytics.vw_deal_features vdf
              ON e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR = vdf.deal_id
WHERE vdf.cms_model = 'Catalogue (CMS 3.0) '
  AND e.collector_tstamp::DATE = '2020-05-04'
GROUP BY 1
LIMIT 50;



SELECT *
FROM snowplow.atomic.events e
         JOIN data_science.mart_analytics.vw_deal_features vdf
              ON e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR = vdf.deal_id
WHERE vdf.cms_model = 'Catalogue (CMS 3.0) '
  AND e.collector_tstamp::DATE = '2020-05-04'
LIMIT 50;


SELECT contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE is_server_side_event
  AND se_sale_id IS NOT NULL

SELECT e.event_name,
       CASE WHEN t.touch_id IS NULL THEN 'sessionised' ELSE 'not_sessioned' END AS session_status,
       count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touchification t ON e.event_hash = t.event_hash
WHERE e.event_tstamp::DATE = '2020-05-04'
GROUP BY 1, 2


SELECT is_robot_spider_event, count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.event_tstamp::DATE = '2020-05-04'
GROUP BY 1;


SELECT e.event_name,
       CASE WHEN t.touch_id IS NOT NULL THEN 'sessionised' ELSE 'not_sessioned' END AS session_status,
       CASE WHEN s.touch_id IS NOT NULL THEN 'spv' ELSE 'non_spv' END               AS spv_status,
       count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touchification t ON e.event_hash = t.event_hash
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs s ON t.touch_id = s.touch_id
WHERE e.event_tstamp::DATE = '2020-05-04'
GROUP BY 1, 2, 3;

------------------------------------------------------------------------------------------------------------------------

SELECT e.event_tstamp::DATE AS date,
       mc.touch_affiliate_territory,
       count(*)
FROM hygiene_vault_mvp.snowplow.event_stream e
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
                   ON e.event_hash = mt.event_hash
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mc
                   ON mc.touch_id = mt.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                   ON a.touch_id = mt.touch_id
WHERE e.se_sale_id IS NOT NULL
  AND a.stitched_identity_type = 'se_user_id'
  AND a.attributed_user_id IS NOT NULL
  AND e.event_tstamp IS NOT NULL
  AND e.event_tstamp >= CURRENT_DATE - (5 + 7)
  AND mc.touch_affiliate_territory IN ('TB-NL', 'TB-BE_NL', 'TB-BE_FR')
GROUP BY 1, 2
;

SELECT touch_hostname, touch_affiliate_territory, count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
WHERE touch_hostname LIKE '%travelbird%'
GROUP BY 1, 2;
