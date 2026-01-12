/* 24 HOUR REPORT \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ VERSION 1 - OM STYLE */

SET report_start = {ts '2021-06-12 00:00:00'};
SET report_end = {ts '2021-06-14 00:00:00'};

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
     , CONCAT(k.week, ' - ', k.year) AS weekyear
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
                ''                              AS unsubs,
                b.mapped_objective,
                b.mapped_platform,
                b.mapped_campaign,
                b.segment,
                sec.day_name                    AS dow,
                sec.se_week                     AS week,
                sec.se_year                     AS year,
                COUNT(DISTINCT c.shiro_user_id) AS uniqueclicks,
                COUNT(c.event_hash)             AS clicks
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
                           SELECT s.send_id,
                                  j.mapped_territory,
                                  j.email_name,
                                  g.data_source_name,
                                  s.email_segment_key,
                                  j.mapped_objective,
                                  j.mapped_platform,
                                  j.mapped_campaign,
                                  CASE WHEN g.data_source_name LIKE '%ACT_24M' THEN '720 days active' ELSE g.segment END AS segment,
                                  s.event_date,
                                  j.sent_tstamp
                                   ,
                                  COUNT(s.shiro_user_id)                                                                 AS sends
                           FROM se.data.crm_events_sends s
                                    JOIN se.data.crm_jobs_list j ON s.send_id = j.send_id
                                    JOIN se.data.crm_email_segments g ON s.email_segment_key = g.email_segment_key
                           WHERE s.event_date BETWEEN $report_start AND $report_end
                             AND j.mapped_territory IN ('DE', 'UK')
                             AND j.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
                             AND g.data_source_name NOT LIKE '%_CORE_CH_%'
                             AND g.data_source_name NOT LIKE '%_LLUX_%'
                             AND g.data_source_name NOT LIKE '%_TRBK_%'
                             AND g.data_source_name NOT LIKE '%_UPLS_%'

                           GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
                       ) a
                           LEFT OUTER JOIN se.data.crm_events_opens o ON a.send_id = o.send_id AND a.email_segment_key = o.email_segment_key
                      AND DATEDIFF(HH, o.event_tstamp, a.sent_tstamp) > -24
                      AND o.event_date <= $report_end
                  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
              ) b
                  JOIN            se.data.se_calendar sec ON sec.date_value = b.event_date
                  LEFT OUTER JOIN se.data.crm_events_clicks c ON b.send_id = c.send_id AND b.email_segment_key = c.email_segment_key
             AND DATEDIFF(HH, c.event_tstamp, b.sent_tstamp) > -24
             AND c.event_date <= $report_end
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
     ) k
         LEFT OUTER JOIN
     (
         SELECT ces.send_id, seg.data_source_name, w.week, w.year, SUM(w.bookings) AS bookings, SUM(w.margin) AS margin
         FROM (
                  SELECT stmc.utm_campaign AS send_id, fcb.shiro_user_id, sdsc.se_week AS week, sdsc.se_year AS year, COUNT(fcb.booking_id) AS bookings, SUM(fcb.margin_gross_of_toms_gbp) AS margin
                  FROM se.data.scv_touch_marketing_channel stmc
                           JOIN se.data.scv_touch_attribution att ON att.touch_id = stmc.touch_id AND att.attribution_model = 'last non direct'
                           JOIN se.data.scv_touched_transactions stt ON att.attributed_touch_id = stt.touch_id
                           JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = stt.booking_id
                      AND fcb.booking_transaction_completed_date BETWEEN $report_start AND $report_end
                           JOIN se.data.se_calendar sdsc ON sdsc.date_value = fcb.booking_transaction_completed_date
                  WHERE stmc.utm_source = 'newsletter'
                  GROUP BY 1, 2, 3, 4
              ) w
                  JOIN se.data.crm_events_sends ces ON ces.shiro_user_id = w.shiro_user_id AND CAST(ces.send_id AS VARCHAR) = w.send_id
                  JOIN se.data.crm_email_segments seg ON seg.email_segment_key = ces.email_segment_key
         GROUP BY 1, 2, 3, 4
     ) y
     ON k.send_id = y.send_id
         AND k.data_source_name = y.data_source_name
         AND k.week = y.week AND k.year = y.year

