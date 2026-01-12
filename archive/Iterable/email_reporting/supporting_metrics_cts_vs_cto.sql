USE WAREHOUSE pipe_2xlarge;

USE WAREHOUSE pipe_2xlarge;

WITH clicks AS (
    SELECT cec.send_id,
           cec.event_date,
           COUNT(*) AS clicks
    FROM se.data_pii.crm_events_clicks cec
    WHERE cec.event_date >= CURRENT_DATE - 30
    GROUP BY 1, 2
)
   , opens AS (

    SELECT ceo.send_id,
           ceo.event_date,
           COUNT(*) AS opens
    FROM se.data_pii.crm_events_opens ceo
    WHERE ceo.event_date >= CURRENT_DATE - 30
    GROUP BY 1, 2
)
   , sends AS (
    SELECT ces.send_id,
           ces.event_date,
           COUNT(*) AS sends
    FROM se.data_pii.crm_events_sends ces
    WHERE ces.event_date >= CURRENT_DATE - 30
    GROUP BY 1, 2
)
   , model AS (
    SELECT s.send_id,
           cjl.sent_date,
           cjl.email_name,
           cjl.mapped_crm_date,
           cjl.mapped_territory,
           cjl.mapped_objective,
           cjl.mapped_platform,
           cjl.mapped_campaign,
           cjl.subject,
           s.sends,
           o.opens,
           c.clicks,
           clicks / opens AS cto_rate,
           clicks / sends AS cts_rate
    FROM sends s --using sends as master as needed for comparative metrics
        LEFT JOIN se.data.crm_jobs_list cjl ON s.send_id = cjl.send_id
        LEFT JOIN opens o ON s.send_id = o.send_id
        LEFT JOIN clicks c ON s.send_id = c.send_id
)
SELECT *
FROM model m;


CREATE OR REPLACE VIEW scratch.robinpatel.email_metrics AS
(
SELECT ep.send_id,
       ep.sent_date,
       ep.sent_tstamp,
       ep.email_name,
       ep.is_email_name_remapped,
       ep.mapped_crm_date,
       ep.mapped_territory,
       ep.mapped_objective,
       ep.mapped_platform,
       ep.mapped_campaign,
       ep.client_id,
       ep.from_name,
       ep.from_email,
       ep.sched_time,
       ep.sent_time,
       ep.subject,
       ep.triggered_send_external_key,
       ep.send_definition_external_key,
       ep.job_status,
       ep.preview_url,
       ep.is_multipart,
       ep.additional,
       ep.is_athena_email,
       ep.email_sends,
       ep.unique_email_opens,
       ep.email_opens,
       ep.unique_email_clicks,
       ep.email_clicks,
       ep.email_unsubs,
       ep.sessions,
       ep.spvs,
       ep.bookings,
       ep.domestic_bookings,
       ep.international_bookings,
       ep.margin,
       ep.gross_revenue,
       ep.email_clicks / ep.email_opens AS cto_rate,
       ep.email_clicks / ep.email_sends AS cts_rate
FROM se.data.email_performance ep
WHERE DATE_TRUNC('year', ep.sent_date) = DATE_TRUNC('year', CURRENT_DATE)
  AND ep.mapped_crm_date IS NOT NULL
    );


SELECT em.sent_date,
       DATE_PART('month', em.sent_date)       AS month,
       DATE_PART('week', em.sent_date)        AS week,
       SUM(em.email_sends)                    AS total_email_sends,
       SUM(em.email_opens)                    AS total_email_opens,
       SUM(em.email_clicks)                   AS total_email_clicks,
       SUM(em.bookings)                       AS total_bookings,
       SUM(em.margin)                         AS total_margin,
       total_email_clicks / total_email_sends AS cts_rate,
       total_email_clicks / total_email_opens AS cto_rate
FROM scratch.robinpatel.email_metrics em
GROUP BY 1, 2, 3;

SELECT *
FROM se.data.user_emails ue;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM se.data.athena_email_reporting aer;

SELECT * FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa WHERE iupa.segment_name IS NOT NULL and iupa.athena_segment_name IS NOT NULL;

SELECT * FROM unload_vault_mvp.iterable.user_profile_activity__20211130t030000__daily_at_03h00 u;