USE WAREHOUSE pipe_2xlarge;

------------------------------------------------------------------------------------------------------------------------
--original code
WITH valid_deals_filter AS (
    SELECT DISTINCT
           sa.se_sale_id AS deal_id
         , ts.id         AS territory_id
    FROM se.data.sale_active sa
        INNER JOIN se.data.dim_sale ds
                   ON sa.se_sale_id = ds.se_sale_id
        INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot ts
                   ON ds.posa_territory = ts.name
    WHERE sa.view_date >= CURRENT_DATE - 60
      AND ts.id IN (1, 2, 4, 8, 9, 10, 11, 12, 14, 15, 25, 26, 27)
),

     events AS (
         SELECT t.id                                          AS territory_id,
                COALESCE(e.se_user_id, mt.attributed_user_id) AS user_id,
                e.se_sale_id                                  AS deal_id,
                CASE
                    WHEN (RLIKE
                        (
                            e.page_url,
                            '.*\/sale\/book.*', -- secret escapes and flash
                            'i'
                        )) OR (RLIKE
                        (
                            e.page_url,
                            '.*sales.*\/booking.*', -- travel bird and catalogue
                            'i'
                        )) THEN 'book-form'
                    WHEN RLIKE
                        (
                            e.page_url,
                            '.*sale.*',
                            'i'
                        ) THEN 'deal-view'
                    ELSE 'other'
                    END                                       AS evt_name,
                TO_DATE(e.event_tstamp)                       AS evt_date,
                MAX(e.event_tstamp)                           AS max_event_ts
         FROM hygiene_vault_mvp.snowplow.event_stream e
             JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt
                  ON e.event_hash = mt.event_hash
             JOIN se.data.scv_touch_marketing_channel mc
                  ON mc.touch_id = mt.touch_id
             JOIN se.data.se_territory t
                  ON t.name = mc.touch_affiliate_territory
         WHERE mt.stitched_identity_type = 'se_user_id'
           AND e.page_url IS NOT NULL
           AND e.derived_tstamp IS NOT NULL
           AND e.event_name IN ('page_view', 'screen_view', 'event')
           AND e.is_server_side_event = TRUE
           -- TODO adjust to include more dates:
           AND e.event_tstamp >= CURRENT_DATE - 5
         GROUP BY 1, 2, 3, 4, 5
     )
SELECT e.territory_id,
       e.user_id,
       e.deal_id,
       e.evt_name,
       e.evt_date,
       e.max_event_ts
FROM events e
    JOIN valid_deals_filter ss
         ON ss.deal_id = e.deal_id
             AND ss.territory_id = e.territory_id;

------------------------------------------------------------------------------------------------------------------------
/*We really wanna track all the events which combine deal_id, user_id and timestamp and which can be used as an interest signal. Currently, these are:
SPV
booking-form view
booking
*/

USE WAREHOUSE pipe_2xlarge;

SELECT t.id,
       ssel.attributed_user_id AS user_id,
       'deal-view'             AS evt_name,
       sts.event_tstamp::DATE  AS evt_date,
       MAX(sts.event_tstamp)   AS max_event_ts
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data_pii.scv_session_events_link ssel ON sts.touch_id = ssel.touch_id AND ssel.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    INNER JOIN se.data.se_territory t ON stmc.touch_affiliate_territory = t.name
-- TODO adjust to include more dates:
WHERE sts.event_tstamp >= CURRENT_DATE - 5
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT t.id,
       ssel.attributed_user_id AS user_id,
       'booking'               AS evt_name,
       stt.event_tstamp::DATE  AS evt_date,
       MAX(stt.event_tstamp)   AS max_event_ts
FROM se.data.scv_touched_transactions stt
    INNER JOIN se.data_pii.scv_session_events_link ssel ON stt.touch_id = ssel.touch_id AND ssel.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
    INNER JOIN se.data.se_territory t ON stmc.touch_affiliate_territory = t.name
-- TODO adjust to include more dates:
WHERE stt.event_tstamp >= CURRENT_DATE - 5
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT t.id,
       ssel.attributed_user_id AS user_id,
       'booking form'          AS evt_name,
       ses.event_tstamp::DATE  AS evt_date,
       MAX(ses.event_tstamp)   AS max_event_ts
FROM se.data_pii.scv_event_stream ses
    INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash AND ssel.stitched_identity_type = 'se_user_id'
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
    INNER JOIN se.data.se_territory t ON stmc.touch_affiliate_territory = t.name
-- TODO adjust to include more dates:
WHERE ses.event_tstamp >= CURRENT_DATE - 5
  AND ses.contexts_com_secretescapes_content_context_1[0]:sub_category::VARCHAR = 'booking form'
GROUP BY 1, 2, 3, 4;


SELECT * FROm hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo