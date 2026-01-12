SET report_start = {ts '2021-08-30 00:00:00'};
SET report_end = {ts '2021-09-05 00:00:00'};

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
     )

SELECT SUM(opens)
FROM opens
WHERE opens.segment = '30 day active'
  AND opens.mapped_objective = 'CORE'
  AND opens.mapped_territory = 'DE';

USE WAREHOUSE pipe_xlarge;
SELECT COUNT(*)
FROM se.data_pii.crm_events_opens ceo
WHERE ceo.send_id = '1242914'
  AND ceo.event_date::DATE = '2021-09-06';
--249032 opens

SELECT DISTINCT aer.se_sale_id
FROM se.data.athena_email_reporting aer
WHERE aer.send_id = '1242914'
  AND aer.event_date = '2021-09-06';
--1,763 sales

SELECT DISTINCT aer.se_sale_id
FROM se.data.athena_email_reporting aer
WHERE aer.send_id = '1242913'
  AND aer.event_date = '2021-09-06';
--1,712

SELECT DISTINCT aer.se_sale_id
FROM se.data.athena_email_reporting aer
WHERE aer.send_id = '1243013'
  AND aer.event_date = '2021-09-06';
--57

SELECT aer.se_sale_id, aer.impressions
FROM se.data.athena_email_reporting aer
WHERE aer.send_id = '1242914'
  AND aer.event_date = '2021-09-06'
ORDER BY aer.impressions DESC NULLS LAST;

SELECT COUNT(*)
FROM se.data_pii.crm_events_opens ceo
WHERE ceo.send_id = '1242914'
  AND ceo.event_date = '2021-09-06';