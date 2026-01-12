--user snapshot
SELECT u.current_affiliate_territory_id                                    AS territory_id,
       t.name                                                              AS territory_code,
       COUNT(DISTINCT u.user_id),
       SUM(IFF(u.last_booking_complete_tstamp > CURRENT_DATE - 150, 1, 0)) AS booking_complete_tstamp_users,
       SUM(IFF(u.last_sale_pageview_tstamp > CURRENT_DATE - 150, 1, 0))    AS sale_pageview_complete_tstamp_users
FROM data_vault_mvp.engagement_stg.user_snapshot u
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON a.id = u.current_affiliate_id
         INNER JOIN se.data.se_territory t ON u.current_affiliate_territory_id = t.id
WHERE u.last_sale_pageview_tstamp > CURRENT_DATE - 150
   OR u.last_booking_complete_tstamp > CURRENT_DATE - 150
GROUP BY 1, 2
ORDER BY 1 ASC;

SELECT *
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_territory_id = 26
  AND sua.last_sale_pageview_tstamp > CURRENT_DATE - 150;
------------------------------------------------------------------------------------------------------------------------

SELECT ude.territory_id,
       t.name AS territory_code,
       COUNT(DISTINCT ude.user_id)
FROM data_science.predictive_modeling.user_deal_events ude
         INNER JOIN se.data.se_territory t ON ude.territory_id = t.id
WHERE ude.evt_date > CURRENT_DATE - 150
GROUP BY 1, 2
ORDER BY 1 ASC;


USE WAREHOUSE pipe_xlarge;
--event_stream
WITH events AS (
    SELECT t.id                    AS territory_id,
           mc.touch_affiliate_territory,
           mt.attributed_user_id   AS user_id,
           e.se_sale_id            AS deal_id,
           TO_DATE(e.event_tstamp) AS evt_date,
           MAX(e.event_tstamp)     AS max_event_ts
    FROM hygiene_vault_mvp.snowplow.event_stream e
             INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON e.event_hash = mt.event_hash
             INNER JOIN se.data.scv_touch_marketing_channel mc ON mc.touch_id = mt.touch_id
             INNER JOIN se.data.se_territory t ON t.name = mc.touch_affiliate_territory
    WHERE e.is_server_side_event = TRUE
      AND e.event_tstamp >= CURRENT_DATE - 150
      AND mt.stitched_identity_type = 'se_user_id'
    GROUP BY 1, 2, 3, 4, 5
)
SELECT events.touch_affiliate_territory,
       COUNT(DISTINCT user_id)
FROM events
GROUP BY 1;



SELECT e.page_url,
       mc.touch_affiliate_territory
FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON e.event_hash = mt.event_hash
         INNER JOIN se.data.scv_touch_marketing_channel mc ON mc.touch_id = mt.touch_id
WHERE e.is_server_side_event = TRUE
  AND e.event_tstamp >= CURRENT_DATE - 150
  AND mt.stitched_identity_type = 'se_user_id'
  AND TRY_TO_NUMBER(mt.attributed_user_id) IN (
    SELECT DISTINCT sua.shiro_user_id
    FROM se.data.se_user_attributes sua
    WHERE sua.current_affiliate_territory_id = 26
      AND sua.last_sale_pageview_tstamp > CURRENT_DATE - 150
)
;


SELECT DISTINCT
       mt.attributed_user_id AS user_id

FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON e.event_hash = mt.event_hash
         INNER JOIN se.data.scv_touch_marketing_channel mc ON mc.touch_id = mt.touch_id
         INNER JOIN se.data.se_territory t ON t.name = mc.touch_affiliate_territory
WHERE e.is_server_side_event = TRUE
  AND e.event_tstamp >= CURRENT_DATE - 150
  AND mt.stitched_identity_type = 'se_user_id'
  AND mc.touch_affiliate_territory = 'TB-BE_NL'
    EXCEPT

SELECT sua.shiro_user_id
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_territory_id = 26
  AND sua.last_sale_pageview_tstamp > CURRENT_DATE - 150;

