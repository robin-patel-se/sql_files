SET report_start = {ts '2021-11-06 00:00:00'};
SET report_end = {ts '2021-11-08 00:00:00'};

USE WAREHOUSE pipe_2xlarge;

WITH sends_list AS (
    --isolate sends from partners
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
           j.sent_tstamp,
           COUNT(s.shiro_user_id)                                                                 AS sends
    FROM se.data.crm_events_sends s
        JOIN se.data.crm_jobs_list j ON s.send_id = j.send_id
        JOIN se.data.crm_email_segments g ON s.email_segment_key = g.email_segment_key
    WHERE s.event_date BETWEEN $report_start AND $report_end
      AND j.mapped_territory IN ('NL')
--       AND j.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
--       AND g.data_source_name NOT LIKE '%_TRBK_%'
--       AND g.data_source_name NOT LIKE '%_UPLS_%'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
   , attach_opens AS (
--attaching aggregated opens to the input sends
    SELECT sl.send_id,
           sl.mapped_territory,
           sl.email_name,
           sl.data_source_name,
           sl.email_segment_key,
           sl.mapped_objective,
           sl.mapped_platform,
           sl.mapped_campaign,
           sl.segment,
           sl.event_date,
           sl.sent_tstamp,
           sl.sends,
           COUNT(DISTINCT o.shiro_user_id) AS uniqueopens,
           COUNT(o.event_hash)             AS opens
    FROM sends_list sl
        LEFT OUTER JOIN se.data.crm_events_opens o ON sl.send_id = o.send_id AND sl.email_segment_key = o.email_segment_key
        AND DATEDIFF(HH, o.event_tstamp, sl.sent_tstamp) > -24
        AND o.event_date <= $report_end
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)
   , attach_clicks AS (
    --attaching aggregated clicks to the opens
    SELECT o.send_id,
           o.mapped_territory,
           o.email_name,
           o.data_source_name,
           o.event_date,
           o.sends,
           o.uniqueopens,
           o.opens,
           ''                              AS unsubs,
           o.mapped_objective,
           o.mapped_platform,
           o.mapped_campaign,
           o.segment,
           sec.day_name                    AS dow,
           sec.se_week                     AS week,
           sec.se_year                     AS year,
           COUNT(DISTINCT c.shiro_user_id) AS uniqueclicks,
           COUNT(c.event_hash)             AS clicks
    FROM attach_opens o
        INNER JOIN se.data.se_calendar sec ON sec.date_value = o.event_date
        LEFT JOIN  se.data.crm_events_clicks c ON o.send_id = c.send_id AND o.email_segment_key = c.email_segment_key
        AND DATEDIFF(HH, c.event_tstamp, o.sent_tstamp) > -24
        AND c.event_date <= $report_end
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
)
   , w AS (
    SELECT stmc.utm_campaign                 AS send_id,
           fcb.shiro_user_id,
           sdsc.se_week                      AS week,
           sdsc.se_year                      AS year,
           COUNT(fcb.booking_id)             AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp) AS margin
    FROM se.data.scv_touch_marketing_channel stmc
        JOIN se.data.scv_touch_attribution att
             ON att.touch_id = stmc.touch_id AND att.attribution_model = 'last non direct'
        JOIN se.data.scv_touched_transactions stt ON att.attributed_touch_id = stt.touch_id
        JOIN se.data.fact_complete_booking fcb ON fcb.booking_id = stt.booking_id
        AND fcb.booking_transaction_completed_date BETWEEN $report_start AND $report_end
        JOIN se.data.se_calendar sdsc ON sdsc.date_value = fcb.booking_transaction_completed_date
    WHERE stmc.utm_source = 'newsletter'
    GROUP BY 1, 2, 3, 4
)
   , y AS (
    SELECT ces.send_id,
           seg.data_source_name,
           w.week,
           w.year,
           SUM(w.bookings) AS bookings,
           SUM(w.margin)   AS margin
    FROM w
        JOIN se.data.crm_events_sends ces
             ON ces.shiro_user_id = w.shiro_user_id AND CAST(ces.send_id AS VARCHAR) = w.send_id
        JOIN se.data.crm_email_segments seg ON seg.email_segment_key = ces.email_segment_key
    GROUP BY 1, 2, 3, 4
)

SELECT ac.send_id,
       ac.mapped_territory,
       ac.email_name,
       ac.data_source_name,
       ac.event_date,
       ac.sends,
       ac.uniqueopens,
       ac.opens,
       ac.uniqueclicks,
       ac.clicks,
       ac.unsubs,
       ac.mapped_objective,
       ac.mapped_platform,
       ac.mapped_campaign,
       ac.segment,
       ac.dow,
       CONCAT(ac.week, ' - ', ac.year) AS weekyear,
       y.bookings,
       y.margin
FROM attach_clicks ac
    LEFT OUTER JOIN y ON ac.send_id = y.send_id
    AND ac.data_source_name = y.data_source_name
    AND ac.week = y.week AND ac.year = y.year;


SELECT *
FROM se.data.email_performance ep
WHERE ep.mapped_territory IN ('UK')
  AND ep.mapped_objective IN ('PARTNER');

SELECT *
FROM se.data.email_performance ep
WHERE ep.send_id = 1147574;


SELECT sua.shiro_user_id,
       sua.signup_tstamp::DATE,
       ua.*
FROM se.data.se_user_attributes sua
    INNER JOIN se.data.user_activity ua ON sua.shiro_user_id = ua.shiro_user_id
    AND sua.signup_tstamp::DATE + 1 = ua.date -- when the user joined
WHERE sua.shiro_user_id = 75159038;


SELECT *
FROM se.data.user_subscription us;
SELECT *
FROM se.data.user_subscription_event u;


SELECT DISTINCT cjl.send_id, cjl.email_name, ceo.list_id
FROM se.data_pii.crm_jobs_list cjl
    LEFT JOIN se.data_pii.crm_events_opens ceo ON cjl.send_id = ceo.send_id
WHERE cjl.send_id IN (
                      '1262165',
                      '1262165',
                      '1262156',
                      '1262165',
                      '1262170',
                      '1262155',
                      '1262467',
                      '1262467',
                      '1262467',
                      '1262462',
                      '1262461'
    )

SELECT TRIM(' 20211108_NL_CORE_ATHENA_StaycationNewT')


SELECT *
FROM hygiene_vault_mvp.sfmc.jobs_list
WHERE created_at::DATE = CURRENT_DATE
  AND send_id IN (
                  '1262165',
                  '1262165',
                  '1262156',
                  '1262165',
                  '1262170',
                  '1262155',
                  '1262467',
                  '1262467',
                  '1262467',
                  '1262462',
                  '1262461'
    );

