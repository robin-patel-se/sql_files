SET report_start = {ts '2021-12-22 00:00:00'};
SET report_end = {ts '2021-12-31 00:00:00'};
USE WAREHOUSE pipe_xlarge;

WITH sends AS (
    SELECT events_sends.email_id,
           jobs_list.mapped_territory,
           jobs_list.email_name,
           events_sends.email_segment_key,
           jobs_list.mapped_objective,
           jobs_list.mapped_platform,
           jobs_list.mapped_campaign,
           events_sends.event_date,
           jobs_list.sent_tstamp,
           COUNT(events_sends.shiro_user_id) AS sends
    FROM se.data.crm_events_sends events_sends
        INNER JOIN se.data.crm_jobs_list jobs_list
                   ON events_sends.email_id = jobs_list.email_id
    WHERE events_sends.event_date
        BETWEEN $report_start AND $report_end
      AND jobs_list.mapped_territory IN ('DE', 'UK')
      AND jobs_list.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
     opens AS (
         SELECT sends.email_id,
                sends.mapped_territory,
                sends.email_name,
                sends.mapped_objective,
                sends.mapped_platform,
                sends.mapped_campaign,
                sends.event_date,
                sends.sent_tstamp,
                sends.sends,
                COUNT(DISTINCT events_opens.shiro_user_id) AS uniqueopens,
                COUNT(events_opens.event_hash)             AS opens
         FROM sends
             LEFT JOIN se.data.crm_events_opens events_opens
                       ON sends.email_id = events_opens.email_id
                           AND sends.email_segment_key = events_opens.email_segment_key
                           AND DATEDIFF(HH, events_opens.event_tstamp, sends.sent_tstamp) > -24
                           AND events_opens.event_date <= $report_end
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
     ),
     unsubscribes AS (
         SELECT unsubs.email_id,
                COUNT(*) AS unsubs
         FROM se.data.crm_events_unsubscribes AS unsubs
             INNER JOIN sends ON sends.email_id = unsubs.email_id
         WHERE unsubs.event_date BETWEEN $report_start AND $report_end
         GROUP BY 1
     ),
     events_clicks AS (
         SELECT opens.email_id,
                opens.mapped_territory,
                opens.email_name,
                opens.event_date,
                opens.sends,
                opens.uniqueopens,
                opens.opens,
                unsubs.unsubs                               AS unsubs,
                opens.mapped_objective,
                opens.mapped_platform,
                opens.mapped_campaign,
                sec.day_name                                AS dow,
                sec.se_week                                 AS week,
                sec.se_year                                 AS year,
                COUNT(DISTINCT events_clicks.shiro_user_id) AS uniqueclicks,
                COUNT(events_clicks.event_hash)             AS clicks
         FROM opens
             INNER JOIN se.data.se_calendar AS sec ON sec.date_value = opens.event_date
             LEFT JOIN  se.data.crm_events_clicks events_clicks
                        ON opens.email_id = events_clicks.email_id
                            AND DATEDIFF(HH, events_clicks.event_tstamp, opens.sent_tstamp) > -24
                            AND events_clicks.event_date <= $report_end
             LEFT JOIN  unsubscribes AS unsubs
                        ON opens.email_id = unsubs.email_id
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
     ),
     bookings AS (
         SELECT CASE
                    -- newly hardcoded url parameter to avoid id collision between send_id (SFMC) and campaign_id (iterable)
                    WHEN LOWER(stmc.landing_page_parameters:utm_platform::VARCHAR) = 'iterable' THEN 'IT-' || stmc.utm_campaign
                    -- before the hardcoded url parameter based on interrogation of data there is an identifiable mutuatally extinct range of ids
                    -- from the 1st of Nov 2021, campaign id min 3,114,669, send id 949,936
                    WHEN stt.event_tstamp::DATE >= '2021-11-01' AND LENGTH(stmc.utm_campaign) = 7 THEN 'IT-' || stmc.utm_campaign
                    ELSE 'SFMC-' || stmc.utm_campaign
                    END                           AS email_id,
                fcb.shiro_user_id::VARCHAR        AS shiro_user_id,
                sdsc.se_week                      AS week,
                sdsc.se_year                      AS year,
                COUNT(fcb.booking_id)             AS bookings,
                SUM(fcb.margin_gross_of_toms_gbp) AS margin
         FROM se.data.scv_touch_marketing_channel stmc
             INNER JOIN se.data.scv_touch_attribution att
                        ON att.touch_id = stmc.touch_id
                            AND att.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touched_transactions stt
                        ON att.attributed_touch_id = stt.touch_id
             INNER JOIN se.data.fact_complete_booking fcb
                        ON fcb.booking_id = stt.booking_id
                            AND fcb.booking_transaction_completed_date
                               BETWEEN $report_start AND $report_end
             INNER JOIN se.data.se_calendar sdsc
                        ON sdsc.date_value = fcb.booking_transaction_completed_date
         WHERE LOWER(stmc.touch_mkt_channel) LIKE 'email%'
         GROUP BY 1, 2, 3, 4
     )
SELECT events_clicks.email_id,
       events_clicks.mapped_territory,
       events_clicks.email_name,
       events_clicks.event_date,
       events_clicks.sends,
       events_clicks.uniqueopens,
       events_clicks.opens,
       events_clicks.uniqueclicks,
       events_clicks.clicks,
       events_clicks.unsubs,
       events_clicks.mapped_objective,
       events_clicks.mapped_platform,
       events_clicks.mapped_campaign,
       events_clicks.dow,
       CONCAT(events_clicks.week, ' - ', events_clicks.year) AS weekyear,
       bookings.bookings,
       bookings.margin
FROM events_clicks
    LEFT JOIN bookings
              ON events_clicks.email_id = bookings.email_id
;


SELECT *
FROM se.data.email_performance ep
WHERE ep.mapped_territory IN ('DE', 'UK')
  AND ep.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
  AND ep.sent_date BETWEEN '2021-12-22' AND '2021-12-31';


SET report_start = {ts '2021-12-22 00:00:00'};
SET report_end = {ts '2021-12-31 00:00:00'};

WITH sends AS (
    SELECT events_sends.send_id,
           jobs_list.mapped_territory,
           jobs_list.email_name,
           email_segments.data_source_name,
           events_sends.email_segment_key,
           jobs_list.mapped_objective,
           jobs_list.mapped_platform,
           jobs_list.mapped_campaign,
           IFF(email_segments.data_source_name LIKE '%ACT_24M', '720 days active', email_segments.segment) AS segment,
           events_sends.event_date,
           jobs_list.sent_tstamp,
           COUNT(events_sends.shiro_user_id)                                                               AS sends
    FROM se.data.crm_events_sends events_sends
        INNER JOIN se.data.crm_jobs_list jobs_list
                   ON events_sends.send_id = jobs_list.send_id
        INNER JOIN se.data.crm_email_segments email_segments
                   ON events_sends.email_segment_key = email_segments.email_segment_key
    WHERE events_sends.event_date
        BETWEEN $report_start AND $report_end
      AND jobs_list.mapped_territory IN ('DE', 'UK')
      AND jobs_list.mapped_objective NOT IN ('SERVICE', 'AME', 'PARTNER')
      AND email_segments.data_source_name NOT LIKE '%_CORE_CH_%'
      AND email_segments.data_source_name NOT LIKE '%_LLUX_%'
      AND email_segments.data_source_name NOT LIKE '%_TRBK_%'
      AND email_segments.data_source_name NOT LIKE '%_UPLS_%'
    GROUP BY events_sends.send_id,
             jobs_list.mapped_territory,
             jobs_list.email_name,
             email_segments.data_source_name,
             events_sends.email_segment_key,
             jobs_list.mapped_objective,
             jobs_list.mapped_platform,
             jobs_list.mapped_campaign,
             IFF(email_segments.data_source_name LIKE '%ACT_24M', '720 days active', email_segments.segment),
             events_sends.event_date,
             jobs_list.sent_tstamp
),
     opens AS (
         SELECT sends.send_id,
                sends.mapped_territory,
                sends.email_name,
                sends.data_source_name,
                sends.email_segment_key,
                sends.mapped_objective,
                sends.mapped_platform,
                sends.mapped_campaign,
                sends.segment,
                sends.event_date,
                sends.sent_tstamp,
                sends.sends,
                COUNT(DISTINCT events_opens.shiro_user_id) AS uniqueopens,
                COUNT(events_opens.event_hash)             AS opens
         FROM sends
             LEFT OUTER JOIN se.data.crm_events_opens events_opens
                             ON sends.send_id = events_opens.send_id
                                 AND sends.email_segment_key = events_opens.email_segment_key
                                 AND DATEDIFF(HH, events_opens.event_tstamp, sends.sent_tstamp) > -24
                                 AND events_opens.event_date <= $report_end
         GROUP BY sends.send_id,
                  sends.mapped_territory,
                  sends.email_name,
                  sends.data_source_name,
                  sends.email_segment_key,
                  sends.mapped_objective,
                  sends.mapped_platform,
                  sends.mapped_campaign,
                  sends.segment,
                  sends.event_date,
                  sends.sent_tstamp,
                  sends.sends
     ),
     unsubscribes AS (
         SELECT unsubs.send_id,
                COUNT(*) AS unsubs,
                sends.email_segment_key
         FROM se.data.crm_events_unsubscribes AS unsubs
             INNER JOIN sends
                        ON sends.send_id = unsubs.send_id
                            AND sends.email_segment_key = unsubs.email_segment_key
         WHERE unsubs.event_date
                   BETWEEN $report_start AND $report_end
         GROUP BY unsubs.send_id,
                  sends.email_segment_key
     ),
     events_clicks AS (
         SELECT opens.send_id,
                opens.mapped_territory,
                opens.email_name,
                opens.data_source_name,
                opens.event_date,
                opens.sends,
                opens.uniqueopens,
                opens.opens,
                unsubs.unsubs                               AS unsubs,
                opens.mapped_objective,
                opens.mapped_platform,
                opens.mapped_campaign,
                opens.segment,
                sec.day_name                                AS dow,
                sec.se_week                                 AS week,
                sec.se_year                                 AS year,
                COUNT(DISTINCT events_clicks.shiro_user_id) AS uniqueclicks,
                COUNT(events_clicks.event_hash)             AS clicks
         FROM opens
             INNER JOIN      se.data.se_calendar AS sec
                             ON sec.date_value = opens.event_date
             LEFT OUTER JOIN se.data.crm_events_clicks events_clicks
                             ON opens.send_id = events_clicks.send_id
                                 AND opens.email_segment_key = events_clicks.email_segment_key
                                 AND DATEDIFF(HH, events_clicks.event_tstamp, opens.sent_tstamp) > -24
                                 AND events_clicks.event_date <= $report_end
             LEFT OUTER JOIN unsubscribes AS unsubs
                             ON opens.send_id = unsubs.send_id
                                 AND opens.email_segment_key = unsubs.email_segment_key
         GROUP BY opens.send_id,
                  opens.mapped_territory,
                  opens.email_name,
                  opens.data_source_name,
                  opens.event_date,
                  opens.sends,
                  opens.uniqueopens,
                  opens.opens,
                  unsubs.unsubs,
                  opens.mapped_objective,
                  opens.mapped_platform,
                  opens.mapped_campaign,
                  opens.segment,
                  sec.day_name,
                  sec.se_week,
                  sec.se_year
     ),
     complete_booking AS (
         SELECT stmc.utm_campaign                 AS send_id,
                fcb.shiro_user_id::VARCHAR        AS shiro_user_id,
                sdsc.se_week                      AS week,
                sdsc.se_year                      AS year,
                COUNT(fcb.booking_id)             AS bookings,
                SUM(fcb.margin_gross_of_toms_gbp) AS margin
         FROM se.data.scv_touch_marketing_channel stmc
             INNER JOIN se.data.scv_touch_attribution att
                        ON att.touch_id = stmc.touch_id
                            AND att.attribution_model = 'last non direct'
             INNER JOIN se.data.scv_touched_transactions stt
                        ON att.attributed_touch_id = stt.touch_id
             INNER JOIN se.data.fact_complete_booking fcb
                        ON fcb.booking_id = stt.booking_id
                            AND fcb.booking_transaction_completed_date
                               BETWEEN $report_start AND $report_end
             INNER JOIN se.data.se_calendar sdsc
                        ON sdsc.date_value = fcb.booking_transaction_completed_date
         WHERE stmc.utm_source = 'newsletter'
         GROUP BY stmc.utm_campaign,
                  fcb.shiro_user_id::VARCHAR,
                  sdsc.se_week,
                  sdsc.se_year
     ),
     bookings AS
         (
             SELECT ces.send_id,
                    seg.data_source_name,
                    complete_booking.week,
                    complete_booking.year,
                    SUM(complete_booking.bookings) AS bookings,
                    SUM(complete_booking.margin)   AS margin
             FROM complete_booking
                 INNER JOIN se.data.crm_events_sends ces
                            ON ces.shiro_user_id = complete_booking.shiro_user_id
                                AND CAST(ces.send_id AS VARCHAR) = complete_booking.send_id
                 INNER JOIN se.data.crm_email_segments seg
                            ON seg.email_segment_key = ces.email_segment_key
             GROUP BY ces.send_id,
                      seg.data_source_name,
                      complete_booking.week,
                      complete_booking.year
         )
SELECT events_clicks.send_id,
       events_clicks.mapped_territory,
       events_clicks.email_name,
       events_clicks.data_source_name,
       events_clicks.event_date,
       events_clicks.sends,
       events_clicks.uniqueopens,
       events_clicks.opens,
       events_clicks.uniqueclicks,
       events_clicks.clicks,
       events_clicks.unsubs,
       events_clicks.mapped_objective,
       events_clicks.mapped_platform,
       events_clicks.mapped_campaign,
       events_clicks.segment,
       events_clicks.dow,
       CONCAT(events_clicks.week, ' - ', events_clicks.year) AS weekyear,
       bookings.bookings,
       bookings.margin
FROM events_clicks
    LEFT OUTER JOIN bookings
                    ON events_clicks.send_id = bookings.send_id
                        AND events_clicks.data_source_name = bookings.data_source_name
                        AND events_clicks.week = bookings.week AND events_clicks.year = bookings.year