SELECT k.send_id
     , k.mapped_territory
     , k.email_name
     , k.data_source_name
     , k.event_date
     , k.sends
     , k.uniqueopens
     , k.opens
     , k.uniqueclicks
     , k.clicks
     , k.unsubs
     , k.mapped_objective
     , k.mapped_platform
     , k.mapped_campaign
     , k.segment
     , k.dow
     , k.year
     , k.week
     , y.bookings
     , y.margin
FROM (
         SELECT b.send_id,
                b.mapped_territory,
                b.email_name,
                b.data_source_name,
                b.event_date,
                b.sends,
                b.uniqueopens,
                b.opens,
                COUNT(DISTINCT c.shiro_user_id)                                                        AS uniqueclicks,
                COUNT(c.event_hash)                                                                    AS clicks,
                '',
                b.mapped_objective,
                b.mapped_platform,
                b.mapped_campaign,
                CASE WHEN b.data_source_name LIKE '%ACT_24M' THEN '720 days active' ELSE b.segment END AS segment,
                DAYNAME(b.event_date)                                                                  AS dow,
                YEAR(b.event_date)                                                                     AS year,
                WEEK(b.event_date)                                                                     AS week
         FROM (
                  SELECT a.send_id
                       , a.mapped_territory
                       , a.email_name
                       , a.data_source_name
                       , a.email_segment_key
                       , a.mapped_objective
                       , a.mapped_platform
                       , a.mapped_campaign
                       , a.segment
                       , a.event_date
                       , a.sent_tstamp
                       , a.sends
                       , COUNT(DISTINCT o.shiro_user_id) AS uniqueopens
                       , COUNT(o.event_hash)             AS opens
                  FROM (
                           SELECT s.send_id
                                , j.mapped_territory
                                , j.email_name
                                , g.data_source_name
                                , s.email_segment_key
                                , j.mapped_objective
                                , j.mapped_platform
                                , j.mapped_campaign
                                , g.segment
                                , s.event_date
                                , j.sent_tstamp
                                , COUNT(s.shiro_user_id) AS sends
                           FROM se.data.crm_events_sends s
                                    JOIN se.data.crm_jobs_list j ON s.send_id = j.send_id
                                    JOIN se.data.crm_email_segments g ON s.email_segment_key = g.email_segment_key
                           WHERE s.event_date > '2021-01-01 00:00:00'
                             AND j.mapped_territory IN ('UK', 'DE')
                             AND j.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
                             AND g.data_source_name NOT LIKE '%_CORE_CH_%'
                             AND g.data_source_name NOT LIKE '%_LLUX_%'
                             AND g.data_source_name NOT LIKE '%_TRBK_%'
                             AND g.data_source_name NOT LIKE '%_UPLS_%'
                           GROUP BY s.send_id, j.mapped_territory, j.email_name, g.data_source_name, s.email_segment_key,
                                    j.mapped_objective, j.mapped_platform, j.mapped_campaign, g.segment, s.event_date,
                                    j.sent_tstamp
                       ) a
                           LEFT OUTER JOIN se.data.crm_events_opens o
                                           ON a.send_id = o.send_id AND a.email_segment_key = o.email_segment_key AND
                                              DATEDIFF(HH, a.sent_tstamp, o.event_tstamp) < 24
                  GROUP BY a.send_id, a.mapped_territory, a.email_name, a.data_source_name, a.email_segment_key
                         , a.mapped_objective, a.mapped_platform, a.mapped_campaign, a.segment, a.event_date, a.sent_tstamp
                         , a.sends
              ) b
                  LEFT OUTER JOIN se.data.crm_events_clicks c
                                  ON b.send_id = c.send_id AND b.email_segment_key = c.email_segment_key AND
                                     DATEDIFF(HH, b.sent_tstamp, c.event_tstamp) < 24
         GROUP BY b.send_id, b.mapped_territory, b.email_name, b.data_source_name, b.event_date,
                  b.sends, b.uniqueopens, b.opens, b.mapped_objective,
                  b.mapped_platform, b.mapped_campaign,
                  CASE WHEN b.data_source_name LIKE '%ACT_24M' THEN '720 days active' ELSE b.segment END,
                  DAYOFWEEKISO(b.event_date), YEAR(b.event_date), WEEK(b.event_date) + 1
     ) k
         LEFT OUTER JOIN
     (
         SELECT ces.send_id, seg.data_source_name, w.week, SUM(w.bookings) AS bookings, SUM(w.margin) AS margin
         FROM (
                  SELECT stmc.utm_campaign                            AS send_id
                       , fcb.shiro_user_id
                       , WEEK(fcb.booking_transaction_completed_date) AS week
                       , COUNT(fcb.booking_id)                        AS bookings
                       , SUM(fcb.margin_gross_of_toms_gbp)            AS margin
                  FROM se.data.scv_touch_marketing_channel stmc
                           JOIN se.data.scv_touch_attribution att
                                ON att.touch_id = stmc.touch_id
                                    AND att.attribution_model = 'last non direct'
                           JOIN se.data.scv_touched_transactions stt
                                ON att.attributed_touch_id = stt.touch_id
                           JOIN se.data.fact_complete_booking fcb
                                ON fcb.booking_id = stt.booking_id
                                    AND fcb.booking_status = 'COMPLETE'
                                    AND fcb.booking_transaction_completed_date > {ts '2021-01-01 00:00:00'}
                  GROUP BY 1, 2, 3
              ) w
                  JOIN se.data.crm_events_sends ces
                       ON ces.shiro_user_id = w.shiro_user_id
                           AND CAST(ces.send_id AS VARCHAR) = w.send_id
                  JOIN se.data.crm_email_segments seg
                       ON seg.email_segment_key = ces.email_segment_key
         GROUP BY 1, 2, 3
     ) y
     ON k.send_id = y.send_id
         AND k.data_source_name = y.data_source_name
         AND k.week = y.week
;
------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

WITH send_list AS (
    --filter sends and segments based on input requirement
    --this will then be used to filter downstream ctes
    SELECT j.send_id,
           j.mapped_territory,
           j.email_name,
           g.data_source_name,
           j.mapped_objective,
           j.mapped_platform,
           j.mapped_campaign,
           g.email_segment_key,
           CASE WHEN g.data_source_name LIKE '%ACT_24M' THEN '720 days active' ELSE g.segment END AS segment,
           j.sent_tstamp
    FROM se.data.crm_jobs_list j
             JOIN se.data.crm_email_segments g ON j.send_id = g.send_id
    WHERE j.mapped_territory IN ('UK', 'DE')
      AND j.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
      AND g.data_source_name NOT LIKE '%_CORE_CH_%'
      AND g.data_source_name NOT LIKE '%_LLUX_%'
      AND g.data_source_name NOT LIKE '%_TRBK_%'
      AND g.data_source_name NOT LIKE '%_UPLS_%'
    AND j.sent_date >= '2021-01-01 00:00:00'
),
     send_counts AS (
         SELECT s.send_id,
                s.data_source_name,
                ces.event_date,
                COUNT(DISTINCT ces.shiro_user_id) AS unique_sends,
                COUNT(DISTINCT ces.event_hash)    AS sends
         FROM se.data.crm_events_sends ces
                  INNER JOIN send_list s ON ces.send_id = s.send_id AND s.email_segment_key = ces.email_segment_key
         WHERE ces.event_date >= '2021-01-01'
         GROUP BY 1, 2, 3
     ),
     open_counts AS (
         SELECT s.send_id,
                s.data_source_name,
                ceo.event_date,
                COUNT(DISTINCT ceo.shiro_user_id) AS unique_opens,
                COUNT(DISTINCT ceo.event_hash)    AS opens
         FROM se.data.crm_events_opens ceo
                  INNER JOIN send_list s ON ceo.send_id = s.send_id AND s.email_segment_key = ceo.email_segment_key
         WHERE ceo.event_date >= '2021-01-01'
         GROUP BY 1, 2, 3
     ),
     click_counts AS (
         SELECT s.send_id,
                s.data_source_name,
                cec.event_date,
                COUNT(DISTINCT cec.shiro_user_id) AS unique_clicks,
                COUNT(DISTINCT cec.event_hash)    AS clicks
         FROM se.data.crm_events_clicks cec
                  INNER JOIN send_list s ON cec.send_id = s.send_id AND s.email_segment_key = cec.email_segment_key
         WHERE cec.event_date >= '2021-01-01'
         GROUP BY 1, 2, 3
     ),
     transactions AS (
         SELECT TRY_TO_NUMBER(stmc.utm_campaign)                    AS send_id,
                UPPER(stmc.utm_content)                             AS data_source_name,
                fcb.booking_completed_date,
                COUNT(DISTINCT fcb.booking_id)                      AS bookings,
                SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
         FROM se.data.scv_touched_transactions stt
                  --filter to complete bookings
                  INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
                  INNER JOIN se.data.scv_touch_attribution sta
                             ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
                  INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
--                   INNER JOIN send_list s
--                              ON s.send_id = TRY_TO_NUMBER(stmc.utm_campaign) AND s.data_source_name = UPPER(stmc.utm_content)
         WHERE fcb.booking_completed_date >= '2021-01-01'
         AND stmc.utm_campaign IS NOT NULL
         AND stmc.utm_content IS NOT NULL
         GROUP BY 1, 2, 3

     )
SELECT sl.send_id,
       sl.mapped_territory,
       sl.email_name,
       sl.data_source_name,
       sl.mapped_objective,
       sl.mapped_platform,
       sl.mapped_campaign,
       sl.email_segment_key,
       sl.segment,
       sl.sent_tstamp,
       c.date_value AS event_date,
       c.se_week,
       c.se_year,
       c.day_name,
       sc.sends,
       sc.unique_sends,
       oc.opens,
       oc.unique_opens,
       cc.clicks,
       cc.unique_clicks,
       t.bookings,
       t.margin
FROM send_list sl
         -- create a grain of per send id, data source and after days prior to the send
         LEFT JOIN se.data.se_calendar c
                   ON sl.sent_tstamp::DATE <= c.date_value AND DATEDIFF(DAY, sl.sent_tstamp::DATE, c.date_value) <= 7
         LEFT JOIN send_counts sc
                   ON sl.send_id = sc.send_id AND sl.data_source_name = sc.data_source_name AND c.date_value = sc.event_date
         LEFT JOIN open_counts oc
                   ON sl.send_id = oc.send_id AND sl.data_source_name = oc.data_source_name AND c.date_value = oc.event_date
         LEFT JOIN click_counts cc
                   ON sl.send_id = cc.send_id AND sl.data_source_name = cc.data_source_name AND c.date_value = cc.event_date
         LEFT JOIN transactions t
                   ON sl.send_id = t.send_id AND sl.data_source_name = t.data_source_name AND
                      c.date_value = t.booking_completed_date
WHERE sl.send_id = 1190721 --TODO, REMOVE FOR TESTING
;