WITH booking_table AS (
    SELECT b.shiro_user_id::VARCHAR      AS user_id,
           b.booking_completed_date      AS event_date,
           b.booking_completed_timestamp AS event_tstamp,
           b.sale_id                     AS deal_id,
           t.id                          AS territory_id,
           b.margin_gross_of_toms_gbp
    FROM se.data.fact_complete_booking b
             LEFT JOIN se.data.dim_sale ds ON b.sale_id = ds.se_sale_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON ds.posa_territory = t.name
    WHERE t.id = 1
      AND b.booking_completed_date >= CURRENT_DATE - 30
      AND b.booking_completed_date <= CURRENT_DATE
),
     email_sends AS (
         SELECT es.user_id,
                es.territory_id,
                es.deal_id,
                TO_DATE(es.send_log__date) AS send_date
         FROM data_vault_mvp.athena.email_sends es
         WHERE es.territory_id = 1
           AND es.send_log__campaign_type = 'CORE'
           AND es.send_log__source_table = 'Selections'
           AND es.send_log__date >= CURRENT_DATE - 30
     )
-- SELECT COUNT(DISTINCT b.user_id)      AS user_booking_count,     -- 200k
--        COUNT(DISTINCT b.deal_id)      AS deal_booking_count,     -- 200
--        COUNT(DISTINCT b.territory_id) AS territory_booking_count -- 1
-- FROM email_sends e
--          LEFT JOIN booking_table b
--                    ON e.user_id = b.user_id
--                        AND e.territory_id = b.territory_id
--                        AND e.deal_id = b.deal_id
-- WHERE b.event_date > e.send_date
--   AND b.deal_id IS NOT NULL

SELECT *
FROM email_sends e
         LEFT JOIN booking_table b
                   ON e.user_id = b.user_id
                       AND e.territory_id = b.territory_id
                       AND e.deal_id = b.deal_id
WHERE b.event_date > e.send_date
  AND b.deal_id IS NOT NULL;

SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send aersis
WHERE aersis.send_id = '1227293';


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log CLONE hygiene_snapshot_vault_mvp.sfmc.athena_send_log;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_sources;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting CLONE data_vault_mvp.dwh.athena_email_reporting;

self_describing_task --include 'dv/dwh/athena/sales_in_send.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_sales_in_send;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.crm_email_segments CLONE data_vault_mvp.dwh.crm_email_segments;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting
WHERE athena_email_reporting.schedule_tstamp = (
    SELECT MAX(aer.schedule_tstamp)
    FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting aer
)

SELECT aer.send_date, SUM(aer.impressions), SUM(aer.clicks)
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.send_date = CURRENT_DATE - 1
GROUP BY 1;
SELECT aer.send_date, SUM(aer.impressions), SUM(aer.clicks)
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting aer
WHERE aer.send_date = CURRENT_DATE - 1
GROUP BY 1;


SELECT DATE_TRUNC(WEEK, sb.booking_completed_date)        AS week,
       sb.se_sale_id,
       stmc.touch_mkt_channel,
       stmc.touch_affiliate_territory,
       COUNT(sb.booking_id)                               AS transactions,
       SUM(sb.margin_gross_of_toms_gbp_constant_currency) AS margin
FROM se.data.scv_touched_transactions tt
         INNER JOIN se.data.se_booking sb ON sb.booking_id = tt.booking_id
         INNER JOIN se.data.scv_touch_marketing_channel AS stmc ON tt.touch_id = stmc.touch_id
         INNER JOIN se.data.fact_complete_booking fb ON tt.booking_id = fb.booking_id
GROUP BY 1, 2, 3, 4

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_performance AS (
    WITH athena_emails AS (
        --used to produce flag on email to state if it is an athena email or not
        SELECT DISTINCT aer.send_id
        FROM data_vault_mvp.dwh.athena_email_reporting aer
    ),
         email_details AS (
             SELECT jl.sent_date,
                    jl.sent_tstamp,
                    jl.email_name,
                    jl.is_email_name_remapped,
                    jl.mapped_crm_date,
                    jl.mapped_territory,
                    jl.mapped_objective,
                    jl.mapped_platform,
                    jl.mapped_campaign,
                    jl.client_id,
                    jl.send_id,
                    jl.from_name,
                    jl.from_email,
                    jl.sched_time,
                    jl.sent_time,
                    jl.subject,
                    jl.email_name__o,
                    jl.triggered_send_external_key,
                    jl.send_definition_external_key,
                    jl.job_status,
                    jl.preview_url,
                    jl.is_multipart,
                    jl.additional,
                    IFF(ae.send_id IS NOT NULL, TRUE, FALSE) AS is_athena_email
             FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl
                      LEFT JOIN athena_emails ae ON jl.send_id = ae.send_id
             WHERE jl.sent_date >= CURRENT_DATE - 7 --get batch of sends that have occurred in the last 7 days
         ),
         sends AS (
             SELECT es.send_id,
                    COUNT(*) AS email_sends
             FROM se.data_pii.crm_events_sends es
                      INNER JOIN email_details ed ON es.send_id = ed.send_id
             WHERE es.event_date::DATE >= CURRENT_DATE - 7
               AND es.event_date <= CURRENT_DATE
             GROUP BY 1
         ),
         opens AS (
             SELECT eo.send_id,
                    COUNT(DISTINCT eo.shiro_user_id) AS unique_email_opens,
                    COUNT(*)                         AS email_opens
             FROM se.data_pii.crm_events_opens eo
                      INNER JOIN email_details ed ON eo.send_id = ed.send_id
             WHERE eo.event_date::DATE >= CURRENT_DATE - 7
               AND eo.event_date <= CURRENT_DATE
             GROUP BY 1
         ),
         clicks AS (
             SELECT ec.send_id,
                    COUNT(DISTINCT ec.shiro_user_id) AS unique_email_clicks,
                    COUNT(*)                         AS email_clicks
             FROM se.data_pii.crm_events_clicks ec
                      INNER JOIN email_details ed ON ec.send_id = ed.send_id
             WHERE ec.event_date::DATE >= CURRENT_DATE - 7
               AND ec.event_date <= CURRENT_DATE
             GROUP BY 1
         ),
         unsubs AS (
             SELECT eu.send_id,
                    COUNT(*) AS email_unsubs
             FROM se.data_pii.crm_events_unsubscribes eu
                      INNER JOIN email_details ed ON eu.send_id = ed.send_id
             WHERE eu.event_date::DATE >= CURRENT_DATE - 7
               AND eu.event_date <= CURRENT_DATE
             GROUP BY 1
         ),
         spvs AS (
             SELECT stmc.utm_campaign AS send_id,
                    COUNT(*)          AS spvs
             FROM se.data.scv_touched_spvs sts
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                      INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = ed.send_id
                 AND sts.event_tstamp::DATE >= CURRENT_DATE - 7
                 AND sts.event_tstamp::DATE <= CURRENT_DATE

             GROUP BY 1
         ),
         bookings AS (
             SELECT stmc.utm_campaign                                AS send_id,
                    COUNT(DISTINCT tt.booking_id)                    AS bookings,
                    SUM(IFF(fb.travel_type = 'Domestic', 1, 0))      AS domestic_bookings,
                    SUM(IFF(fb.travel_type = 'International', 1, 0)) AS international_bookings,
                    SUM(fb.margin_gross_of_toms_cc)                  AS margin,
                    SUM(fb.gross_revenue_gbp_constant_currency)      AS gross_revenue
             FROM se.data.scv_touched_transactions tt
                      INNER JOIN data_vault_mvp.dwh.fact_booking fb ON fb.booking_id = tt.booking_id AND fb.booking_status_type = 'live'
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON tt.touch_id = stmc.touch_id
                      INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = ed.send_id
             WHERE tt.event_tstamp::DATE >= CURRENT_DATE - 7
               AND tt.event_tstamp::DATE <= CURRENT_DATE
             GROUP BY 1
         ),
         sessions AS (
             SELECT stmc.utm_campaign AS send_id,
                    COUNT(*)          AS sessions
             FROM se.data_pii.scv_touch_basic_attributes tba
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON tba.touch_id = stmc.touch_id
                      INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = ed.send_id
             WHERE tba.touch_start_tstamp::DATE >= CURRENT_DATE - 7
               AND tba.touch_start_tstamp::DATE <= CURRENT_DATE
             GROUP BY 1
         )

    SELECT ed.send_id,
           ed.sent_date,
           ed.sent_tstamp,
           ed.email_name,
           ed.is_email_name_remapped,
           ed.mapped_crm_date,
           ed.mapped_territory,
           ed.mapped_objective,
           ed.mapped_platform,
           ed.mapped_campaign,
           ed.client_id,
           ed.from_name,
           ed.from_email,
           ed.sched_time,
           ed.sent_time,
           ed.subject,
           ed.triggered_send_external_key,
           ed.send_definition_external_key,
           ed.job_status,
           ed.preview_url,
           ed.is_multipart,
           ed.additional,
           ed.is_athena_email,
           s.email_sends,
           o.unique_email_opens,
           o.email_opens,
           c.unique_email_clicks,
           c.email_clicks,
           uns.email_unsubs,
           ss.sessions,
           spvs.spvs,
           b.bookings,
           b.domestic_bookings,
           b.international_bookings,
           b.margin,
           b.gross_revenue
    FROM email_details ed
             LEFT JOIN sends s ON ed.send_id = s.send_id
             LEFT JOIN opens o ON ed.send_id = o.send_id
             LEFT JOIN clicks c ON ed.send_id = c.send_id
             LEFT JOIN unsubs uns ON ed.send_id = uns.send_id
             LEFT JOIN sessions ss ON ed.send_id = ss.send_id
             LEFT JOIN spvs ON ed.send_id = spvs.send_id
             LEFT JOIN bookings b ON ed.send_id = b.send_id
);


SELECT GET_DDL('table', 'scratch.robinpatel.email_performance');

CREATE OR REPLACE TRANSIENT TABLE email_performance
(
    sent_date                    DATE,
    sent_tstamp                  TIMESTAMP,
    email_name                   VARCHAR,
    is_email_name_remapped       BOOLEAN,
    mapped_crm_date              VARCHAR,
    mapped_territory             VARCHAR,
    mapped_objective             VARCHAR,
    mapped_platform              VARCHAR,
    mapped_campaign              VARCHAR,
    client_id                    NUMBER,
    send_id                      NUMBER,
    from_name                    VARCHAR,
    from_email                   VARCHAR,
    sched_time                   VARCHAR,
    sent_time                    VARCHAR,
    subject                      VARCHAR,
    triggered_send_external_key  VARCHAR,
    send_definition_external_key VARCHAR,
    job_status                   VARCHAR,
    preview_url                  VARCHAR,
    is_multipart                 VARCHAR,
    additional                   VARCHAR,
    is_athena_email              BOOLEAN,
    email_sends                  NUMBER,
    unique_email_opens           NUMBER,
    email_opens                  NUMBER,
    unique_email_clicks          NUMBER,
    email_clicks                 NUMBER,
    email_unsubs                 NUMBER,
    sessions                     NUMBER,
    spvs                         NUMBER,
    bookings                     NUMBER,
    domestic_bookings            NUMBER,
    international_bookings       NUMBER,
    margin                       NUMBER,
    gross_booking_value          NUMBER,
    gross_revenue                DECIMAL(13, 4)
);

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting CLONE data_vault_mvp.dwh.athena_email_reporting;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends CLONE hygiene_snapshot_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks CLONE hygiene_snapshot_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_unsubscribes CLONE hygiene_snapshot_vault_mvp.sfmc.events_unsubscribes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.module_fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;

SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eopi;

self_describing_task --include 'dv/dwh/email/email_performance.py'  --method 'run' --start '2021-07-01 00:00:00' --end '2021-07-01 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.email_performance;

SELECT MIN(sent_date)
FROM data_vault_mvp.dwh.email_performance;

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
       ep.gross_revenue
FROM se.data.email_performance ep;

------------------------------------------------------------------------------------------------------------------------
--historical data

USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_performance_history AS (
    WITH athena_emails AS (
        --used to produce flag on email to state if it is an athena email or not
        SELECT DISTINCT aer.send_id
        FROM data_vault_mvp.dwh.athena_email_reporting aer
    ),
         email_details AS (
             SELECT jl.sent_date,
                    jl.sent_tstamp,
                    jl.email_name,
                    jl.is_email_name_remapped,
                    jl.mapped_crm_date,
                    jl.mapped_territory,
                    jl.mapped_objective,
                    jl.mapped_platform,
                    jl.mapped_campaign,
                    jl.client_id,
                    jl.send_id,
                    jl.from_name,
                    jl.from_email,
                    jl.sched_time,
                    jl.sent_time,
                    jl.subject,
                    jl.email_name__o,
                    jl.triggered_send_external_key,
                    jl.send_definition_external_key,
                    jl.job_status,
                    jl.preview_url,
                    jl.is_multipart,
                    jl.additional,
                    IFF(ae.send_id IS NOT NULL, TRUE, FALSE) AS is_athena_email
             FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl
                      LEFT JOIN athena_emails ae ON jl.send_id = ae.send_id
             WHERE jl.sent_date >= '2018-01-01'
         ),
         sends AS (
             SELECT es.send_id,
                    COUNT(*) AS email_sends
             FROM se.data_pii.crm_events_sends es
                      INNER JOIN email_details ed ON es.send_id = ed.send_id
                 AND es.event_date >= ed.sent_date
                 AND es.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE es.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         opens AS (
             SELECT eo.send_id,
                    COUNT(DISTINCT eo.shiro_user_id) AS unique_email_opens,
                    COUNT(*)                         AS email_opens
             FROM se.data_pii.crm_events_opens eo
                      INNER JOIN email_details ed ON eo.send_id = ed.send_id
                 AND eo.event_date >= ed.sent_date
                 AND eo.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE eo.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         clicks AS (
             SELECT ec.send_id,
                    COUNT(DISTINCT ec.shiro_user_id) AS unique_email_clicks,
                    COUNT(*)                         AS email_clicks
             FROM se.data_pii.crm_events_clicks ec
                      INNER JOIN email_details ed ON ec.send_id = ed.send_id
                 AND ec.event_date >= ed.sent_date
                 AND ec.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE ec.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         unsubs AS (
             SELECT eu.send_id,
                    COUNT(*) AS email_unsubs
             FROM se.data_pii.crm_events_unsubscribes eu
                      INNER JOIN email_details ed ON eu.send_id = ed.send_id
                 AND eu.event_date >= ed.sent_date
                 AND eu.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE eu.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         spvs AS (
             SELECT stmc.utm_campaign AS send_id,
                    COUNT(*)          AS spvs
             FROM se.data.scv_touched_spvs sts
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                      INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = ed.send_id
                 AND sts.event_tstamp >= ed.sent_date
                 AND sts.event_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE sts.event_tstamp::DATE >= '2018-01-01'

             GROUP BY 1
         ),
         bookings AS (
             SELECT stmc.utm_campaign                                AS send_id,
                    COUNT(DISTINCT tt.booking_id)                    AS bookings,
                    SUM(IFF(fb.travel_type = 'Domestic', 1, 0))      AS domestic_bookings,
                    SUM(IFF(fb.travel_type = 'International', 1, 0)) AS international_bookings,
                    SUM(fb.margin_gross_of_toms_cc)                  AS margin,
                    SUM(fb.gross_revenue_gbp_constant_currency)      AS gross_revenue
             FROM se.data.scv_touched_transactions tt
                      INNER JOIN data_vault_mvp.dwh.fact_booking fb ON fb.booking_id = tt.booking_id AND fb.booking_status_type = 'live'
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON tt.touch_id = stmc.touch_id
                      INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = ed.send_id
                 AND tt.event_tstamp >= ed.sent_date
                 AND tt.event_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE tt.event_tstamp::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         sessions AS (
             SELECT stmc.utm_campaign AS send_id,
                    COUNT(*)          AS sessions
             FROM se.data_pii.scv_touch_basic_attributes tba
                      INNER JOIN se.data.scv_touch_marketing_channel stmc ON tba.touch_id = stmc.touch_id
                      INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = ed.send_id
                 AND tba.touch_start_tstamp >= ed.sent_date
                 AND tba.touch_start_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE tba.touch_start_tstamp::DATE >= '2018-01-01'
             GROUP BY 1
         )

    SELECT ed.send_id,
           ed.sent_date,
           ed.sent_tstamp,
           ed.email_name,
           ed.is_email_name_remapped,
           ed.mapped_crm_date,
           ed.mapped_territory,
           ed.mapped_objective,
           ed.mapped_platform,
           ed.mapped_campaign,
           ed.client_id,
           ed.from_name,
           ed.from_email,
           ed.sched_time,
           ed.sent_time,
           ed.subject,
           ed.triggered_send_external_key,
           ed.send_definition_external_key,
           ed.job_status,
           ed.preview_url,
           ed.is_multipart,
           ed.additional,
           ed.is_athena_email,
           s.email_sends,
           o.unique_email_opens,
           o.email_opens,
           c.unique_email_clicks,
           c.email_clicks,
           uns.email_unsubs,
           ss.sessions,
           spvs.spvs,
           b.bookings,
           b.domestic_bookings,
           b.international_bookings,
           b.margin,
           b.gross_revenue
    FROM email_details ed
             LEFT JOIN sends s ON ed.send_id = s.send_id
             LEFT JOIN opens o ON ed.send_id = o.send_id
             LEFT JOIN clicks c ON ed.send_id = c.send_id
             LEFT JOIN unsubs uns ON ed.send_id = uns.send_id
             LEFT JOIN sessions ss ON ed.send_id = ss.send_id
             LEFT JOIN spvs ON ed.send_id = spvs.send_id
             LEFT JOIN bookings b ON ed.send_id = b.send_id
)
;

------------------------------------------------------------------------------------------------------------------------

MERGE INTO data_vault_mvp.dwh.email_performance AS target
    USING scratch.robinpatel.email_performance AS batch ON target.send_id = batch.send_id
    WHEN NOT MATCHED
        THEN
        INSERT
            VALUES (CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP,
                    'historic backfill',
                    CURRENT_TIMESTAMP()::TIMESTAMP,
                    CURRENT_TIMESTAMP()::TIMESTAMP,
                    batch.send_id,
                    batch.sent_date,
                    batch.sent_tstamp,
                    batch.email_name,
                    batch.is_email_name_remapped,
                    batch.mapped_crm_date,
                    batch.mapped_territory,
                    batch.mapped_objective,
                    batch.mapped_platform,
                    batch.mapped_campaign,
                    batch.client_id,
                    batch.from_name,
                    batch.from_email,
                    batch.sched_time,
                    batch.sent_time,
                    batch.subject,
                    batch.triggered_send_external_key,
                    batch.send_definition_external_key,
                    batch.job_status,
                    batch.preview_url,
                    batch.is_multipart,
                    batch.additional,
                    batch.is_athena_email,
                    batch.email_sends,
                    batch.unique_email_opens,
                    batch.email_opens,
                    batch.unique_email_clicks,
                    batch.email_clicks,
                    batch.email_unsubs,
                    batch.sessions,
                    batch.spvs,
                    batch.bookings,
                    batch.domestic_bookings,
                    batch.international_bookings,
                    batch.margin,
                    batch.gross_revenue);

SELECT count(*)FROM data_vault_mvp.dwh.email_performance ep
SELECT count(*)FROM data_vault_mvp.dwh.email_performance_20210706 ep;

SELECT * FROM data_vault_mvp.dwh.email_performance ep WHERE ep.sent_date = '2021-01-01';


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.email_performance_20210706 CLONE data_vault_mvp.dwh.email_performance;

MERGE INTO data_vault_mvp.dwh.email_performance AS target
    USING scratch.robinpatel.email_performance_history AS batch ON target.send_id = batch.send_id
    WHEN NOT MATCHED
        THEN
        INSERT
            VALUES (CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP,
                    'historic backfill',
                    CURRENT_TIMESTAMP()::TIMESTAMP,
                    CURRENT_TIMESTAMP()::TIMESTAMP,
                    batch.send_id,
                    batch.sent_date,
                    batch.sent_tstamp,
                    batch.email_name,
                    batch.is_email_name_remapped,
                    batch.mapped_crm_date,
                    batch.mapped_territory,
                    batch.mapped_objective,
                    batch.mapped_platform,
                    batch.mapped_campaign,
                    batch.client_id,
                    batch.from_name,
                    batch.from_email,
                    batch.sched_time,
                    batch.sent_time,
                    batch.subject,
                    batch.triggered_send_external_key,
                    batch.send_definition_external_key,
                    batch.job_status,
                    batch.preview_url,
                    batch.is_multipart,
                    batch.additional,
                    batch.is_athena_email,
                    batch.email_sends,
                    batch.unique_email_opens,
                    batch.email_opens,
                    batch.unique_email_clicks,
                    batch.email_clicks,
                    batch.email_unsubs,
                    batch.sessions,
                    batch.spvs,
                    batch.bookings,
                    batch.domestic_bookings,
                    batch.international_bookings,
                    batch.margin,
                    batch.gross_revenue);

SELECT * FROM se.data.email_performance ep;

SELECT se.data.CHANNEL_CATEGORY();


SELECT * FROM raw_vault_mvp.cms_mongodb.sales s

SELECT * FROM data_vault_mvp.bi.daily_spv_weight dsw

