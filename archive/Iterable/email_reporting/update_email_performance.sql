CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting CLONE data_vault_mvp.dwh.athena_email_reporting;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks CLONE hygiene_snapshot_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends CLONE hygiene_snapshot_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_unsubscribes CLONE hygiene_snapshot_vault_mvp.sfmc.events_unsubscribes;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_click CLONE latest_vault.iterable.email_click;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_open CLONE latest_vault.iterable.email_open;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_send CLONE latest_vault.iterable.email_send;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.email_unsubscribe CLONE latest_vault.iterable.email_unsubscribe;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign CLONE latest_vault.iterable.campaign;


SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.email_send_event');

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.email_performance_20211203 CLONE data_vault_mvp.dwh.email_performance;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_unsubscribe_event AS
SELECT *
FROM data_vault_mvp.dwh.email_unsubscribe_event;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_list AS
SELECT *
FROM data_vault_mvp.dwh.email_list;

self_describing_task --include 'dv/dwh/email/email_performance.py'  --method 'run' --start '2021-12-02 00:00:00' --end '2021-12-02 00:00:00'


SELECT *
FROM se.data.crm_jobs_list cjl;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list jl;

ALTER TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list
    RENAME COLUMN scheduled_tstmap TO scheduled_tstamp;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_list el;

SELECT MIN(sent_date)
FROM data_vault_mvp.dwh.email_performance ep;

USE WAREHOUSE pipe_2xlarge;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.email_performance_history AS (
    WITH athena_emails AS (
        --used to produce flag on email to state if it is an athena email or not
        SELECT DISTINCT aer.send_id
        FROM data_vault_mvp.dwh.athena_email_reporting aer
    ),
         email_details AS (
             SELECT jl.email_id,
                    jl.send_id,
                    jl.campaign_id,
                    jl.scheduled_date,
                    jl.scheduled_tstmap,
                    jl.email_name,
                    jl.mapped_crm_date,
                    jl.mapped_territory,
                    jl.mapped_objective,
                    jl.mapped_platform,
                    jl.mapped_campaign,
                    jl.sent_date,
                    jl.sent_tstamp,
                    jl.is_email_name_remapped,
                    jl.client_id,
                    jl.from_name,
                    jl.from_email,
                    jl.subject,
                    jl.triggered_send_external_key,
                    jl.send_definition_external_key,
                    jl.job_status,
                    jl.preview_url,
                    jl.is_multipart,
                    jl.additional,
                    jl.campaign_created_at,
                    jl.campaign_updated_at,
                    jl.ended_at,
                    jl.template_id,
                    jl.message_medium,
                    jl.created_by_user_id,
                    jl.updated_by_user_id,
                    jl.campaign_state,
                    jl.list_ids,
                    jl.suppression_list_ids,
                    jl.send_size,
                    jl.labels,
                    jl.type,
                    jl.crm_platform,
                    IFF(ae.send_id IS NOT NULL, TRUE, FALSE) AS is_athena_email
             FROM data_vault_mvp.dwh.email_list jl
                 LEFT JOIN athena_emails ae ON jl.send_id = ae.send_id
             WHERE jl.sent_date >= '2018-01-01'
         ),
         sends AS (
             SELECT es.email_id,
                    COUNT(*) AS email_sends
             FROM se.data_pii.crm_events_sends es
                 INNER JOIN email_details ed ON es.email_id = ed.email_id
                 AND es.event_date >= ed.sent_date
                 AND es.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE es.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         opens AS (
             SELECT eo.email_id,
                    COUNT(DISTINCT eo.shiro_user_id) AS unique_email_opens,
                    COUNT(*)                         AS email_opens
             FROM se.data_pii.crm_events_opens eo
                 INNER JOIN email_details ed ON eo.email_id = ed.email_id
                 AND eo.event_date >= ed.sent_date
                 AND eo.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE eo.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         clicks AS (
             SELECT ec.email_id,
                    COUNT(DISTINCT ec.shiro_user_id) AS unique_email_clicks,
                    COUNT(*)                         AS email_clicks
             FROM se.data_pii.crm_events_clicks ec
                 INNER JOIN email_details ed ON ec.email_id = ed.email_id
                 AND ec.event_date >= ed.sent_date
                 AND ec.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE ec.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         unsubs AS (
             SELECT eu.email_id,
                    COUNT(*) AS email_unsubs
             FROM se.data_pii.crm_events_unsubscribes eu
                 INNER JOIN email_details ed ON eu.email_id = ed.email_id
                 AND eu.event_date >= ed.sent_date
                 AND eu.event_date::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE eu.event_date::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         spvs AS (
             SELECT CASE
                        -- newly hardcoded url parameter to avoid id collision between send_id (SFMC) and campaign_id (iterable)
                        WHEN LOWER(stmc.landing_page_parameters:utm_platform::VARCHAR) = 'iterable' THEN 'IT-' || stmc.utm_campaign
                        -- before the hardcoded url parameter based on interrogation of data there is an identifiable mutuatally extinct range of ids
                        -- from the 1st of Nov 2021, campaign id min 3,114,669, send id 949,936
                        WHEN sts.event_tstamp::DATE >= '2021-11-01' AND LENGTH(stmc.utm_campaign) = 7 THEN 'IT-' || stmc.utm_campaign
                        ELSE 'SFMC-' || stmc.utm_campaign
                        END  AS email_id,
                    COUNT(*) AS spvs
             FROM se.data.scv_touched_spvs sts
                 INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
                 INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = COALESCE(ed.send_id, ed.campaign_id)
                 AND sts.event_tstamp >= ed.sent_date
                 AND sts.event_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE sts.event_tstamp::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         bookings AS (
             SELECT CASE
                        -- newly hardcoded url parameter to avoid id collision between send_id (SFMC) and campaign_id (iterable)
                        WHEN LOWER(stmc.landing_page_parameters:utm_platform::VARCHAR) = 'iterable' THEN 'IT-' || stmc.utm_campaign
                        -- before the hardcoded url parameter based on interrogation of data there is an identifiable mutuatally extinct range of ids
                        -- from the 1st of Nov 2021, campaign id min 3,114,669, send id 949,936
                        WHEN tt.event_tstamp::DATE >= '2021-11-01' AND LENGTH(stmc.utm_campaign) = 7 THEN 'IT-' || stmc.utm_campaign
                        ELSE 'SFMC-' || stmc.utm_campaign
                        END                                          AS email_id,
                    COUNT(DISTINCT tt.booking_id)                    AS bookings,
                    SUM(IFF(fb.travel_type = 'Domestic', 1, 0))      AS domestic_bookings,
                    SUM(IFF(fb.travel_type = 'International', 1, 0)) AS international_bookings,
                    SUM(fb.margin_gross_of_toms_cc)                  AS margin,
                    SUM(fb.gross_revenue_gbp_constant_currency)      AS gross_revenue
             FROM se.data.scv_touched_transactions tt
                 INNER JOIN data_vault_mvp.dwh.fact_booking fb ON fb.booking_id = tt.booking_id AND fb.booking_status_type = 'live'
                 INNER JOIN se.data.scv_touch_marketing_channel stmc ON tt.touch_id = stmc.touch_id
                 INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = COALESCE(ed.send_id, ed.campaign_id)
                 AND tt.event_tstamp >= ed.sent_date
                 AND tt.event_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE tt.event_tstamp::DATE >= '2018-01-01'
             GROUP BY 1
         ),
         sessions AS (
             SELECT CASE
                        -- newly hardcoded url parameter to avoid id collision between send_id (SFMC) and campaign_id (iterable)
                        WHEN LOWER(stmc.landing_page_parameters:utm_platform::VARCHAR) = 'iterable' THEN 'IT-' || stmc.utm_campaign
                        -- before the hardcoded url parameter based on interrogation of data there is an identifiable mutuatally extinct range of ids
                        -- from the 1st of Nov 2021, campaign id min 3,114,669, send id 949,936
                        WHEN tba.touch_start_tstamp::DATE >= '2021-11-01' AND LENGTH(stmc.utm_campaign) = 7 THEN 'IT-' || stmc.utm_campaign
                        ELSE 'SFMC-' || stmc.utm_campaign
                        END  AS email_id,
                    COUNT(*) AS sessions
             FROM se.data_pii.scv_touch_basic_attributes tba
                 INNER JOIN se.data.scv_touch_marketing_channel stmc ON tba.touch_id = stmc.touch_id
                 INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = COALESCE(ed.send_id, ed.campaign_id)
                 AND tba.touch_start_tstamp >= ed.sent_date
                 AND tba.touch_start_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
             WHERE tba.touch_start_tstamp::DATE >= '2018-01-01'
             GROUP BY 1
         )

    SELECT ed.email_id,
           ed.send_id,
           ed.campaign_id,
           ed.scheduled_date,
           ed.scheduled_tstmap,
           ed.email_name,
           ed.mapped_crm_date,
           ed.mapped_territory,
           ed.mapped_objective,
           ed.mapped_platform,
           ed.mapped_campaign,
           ed.sent_date,
           ed.sent_tstamp,
           ed.is_email_name_remapped,
           ed.client_id,
           ed.from_name,
           ed.from_email,
           ed.subject,
           ed.triggered_send_external_key,
           ed.send_definition_external_key,
           ed.job_status,
           ed.preview_url,
           ed.is_multipart,
           ed.additional,
           ed.campaign_created_at,
           ed.campaign_updated_at,
           ed.ended_at,
           ed.template_id,
           ed.message_medium,
           ed.created_by_user_id,
           ed.updated_by_user_id,
           ed.campaign_state,
           ed.list_ids,
           ed.suppression_list_ids,
           ed.send_size,
           ed.labels,
           ed.type,
           ed.crm_platform,
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
        LEFT JOIN sends s ON ed.email_id = s.email_id
        LEFT JOIN opens o ON ed.email_id = o.email_id
        LEFT JOIN clicks c ON ed.email_id = c.email_id
        LEFT JOIN unsubs uns ON ed.email_id = uns.email_id
        LEFT JOIN sessions ss ON ed.email_id = ss.email_id
        LEFT JOIN spvs ON ed.email_id = spvs.email_id
        LEFT JOIN bookings b ON ed.email_id = b.email_id
)
;

USE WAREHOUSE pipe_2xlarge;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.email_performance
(

    -- (lineage) metadata for the current job
    schedule_tstamp              TIMESTAMP,
    run_tstamp                   TIMESTAMP,
    operation_id                 VARCHAR,
    created_at                   TIMESTAMP,
    updated_at                   TIMESTAMP,

    -- data columns
    email_id                     VARCHAR PRIMARY KEY NOT NULL,
    send_id                      NUMBER,
    campaign_id                  NUMBER,
    scheduled_date               DATE,
    scheduled_tstmap             TIMESTAMP_NTZ,
    email_name                   VARCHAR,
    mapped_crm_date              VARCHAR,
    mapped_territory             VARCHAR,
    mapped_objective             VARCHAR,
    mapped_platform              VARCHAR,
    mapped_campaign              VARCHAR,
    sent_date                    DATE,
    sent_tstamp                  TIMESTAMP_NTZ,
    is_email_name_remapped       BOOLEAN,
    client_id                    NUMBER,
    from_name                    VARCHAR,
    from_email                   VARCHAR,
    subject                      VARCHAR,
    triggered_send_external_key  VARCHAR,
    send_definition_external_key VARCHAR,
    job_status                   VARCHAR,
    preview_url                  VARCHAR,
    is_multipart                 VARCHAR,
    additional                   VARCHAR,
    campaign_created_at          TIMESTAMP_NTZ,
    campaign_updated_at          TIMESTAMP_NTZ,
    ended_at                     TIMESTAMP_NTZ,
    template_id                  NUMBER,
    message_medium               VARCHAR,
    created_by_user_id           VARCHAR,
    updated_by_user_id           VARCHAR,
    campaign_state               VARCHAR,
    list_ids                     ARRAY,
    suppression_list_ids         ARRAY,
    send_size                    NUMBER,
    labels                       ARRAY,
    type                         VARCHAR,
    crm_platform                 VARCHAR,
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
    margin                       DECIMAL(13, 4),
    gross_revenue                DECIMAL(13, 4)
);
;
`

INSERT INTO data_vault_mvp_dev_robin.dwh.email_performance
SELECT '2021-12-03 04:00:00',
       CURRENT_TIMESTAMP,
       'historic backfill',
       CURRENT_TIMESTAMP()::TIMESTAMP,
       CURRENT_TIMESTAMP()::TIMESTAMP,
       email_id,
       send_id,
       campaign_id,
       scheduled_date,
       scheduled_tstmap,
       email_name,
       mapped_crm_date,
       mapped_territory,
       mapped_objective,
       mapped_platform,
       mapped_campaign,
       sent_date,
       sent_tstamp,
       is_email_name_remapped,
       client_id,
       from_name,
       from_email,
       subject,
       triggered_send_external_key,
       send_definition_external_key,
       job_status,
       preview_url,
       is_multipart,
       additional,
       campaign_created_at,
       campaign_updated_at,
       ended_at,
       template_id,
       message_medium,
       created_by_user_id,
       updated_by_user_id,
       campaign_state,
       list_ids,
       suppression_list_ids,
       send_size,
       labels,
       type,
       crm_platform,
       is_athena_email,
       email_sends,
       unique_email_opens,
       email_opens,
       unique_email_clicks,
       email_clicks,
       email_unsubs,
       sessions,
       spvs,
       bookings,
       domestic_bookings,
       international_bookings,
       margin,
       gross_revenue
FROM scratch.robinpatel.email_performance_history;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_performance ep
WHERE ep.crm_platform = 'iterable';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_performance ep
WHERE ep.crm_platform = 'salesforce marketing cloud';
SELECT *
FROM data_vault_mvp.dwh.email_performance ep
WHERE ep.send_id = 1245073;

------------------------------------------------------------------------------------------------------------------------
--check for send id / campaign id cross over


SELECT DISTINCT MIN(id)
FROM latest_vault.iterable.campaign c; --3114669


SELECT MIN(send_id),
       MAX(send_id)
FROM se.data.crm_jobs_list cjl; --MIN(SEND_ID) 177	MAX(SEND_ID) 1270931


SELECT MIN(send_id),
       MAX(send_id)
FROM se.data.crm_jobs_list cjl
WHERE cjl.sent_date >= CURRENT_DATE - 30;
-- ran on the 6th dec
-- MIN(SEND_ID)949936	MAX(SEND_ID)1270931


SELECT LOWER(stmc.landing_page_parameters:utm_platform::VARCHAR) = 'iterable'
FROM se.data.scv_touch_marketing_channel stmc;

SELECT *
FROM se.data.crm_jobs_list cjl
WHERE cjl.crm_platform = 'salesforce marketing cloud';


SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_performance
WHERE crm_platform = 'iterable';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_performance__step08__email_bookings;


SELECT *
FROM scratch.robinpatel.email_performance_history;

SELECT *
FROM data_vault_mvp.dwh.email_performance ep
WHERE ep.send_id = 1267545;

WITH email_details AS (
    SELECT jl.email_id,
           jl.send_id,
           jl.campaign_id,
           jl.scheduled_date,
           jl.scheduled_tstmap,
           jl.email_name,
           jl.mapped_crm_date,
           jl.mapped_territory,
           jl.mapped_objective,
           jl.mapped_platform,
           jl.mapped_campaign,
           jl.sent_date,
           jl.sent_tstamp,
           jl.is_email_name_remapped,
           jl.client_id,
           jl.from_name,
           jl.from_email,
           jl.subject,
           jl.triggered_send_external_key,
           jl.send_definition_external_key,
           jl.job_status,
           jl.preview_url,
           jl.is_multipart,
           jl.additional,
           jl.campaign_created_at,
           jl.campaign_updated_at,
           jl.ended_at,
           jl.template_id,
           jl.message_medium,
           jl.created_by_user_id,
           jl.updated_by_user_id,
           jl.campaign_state,
           jl.list_ids,
           jl.suppression_list_ids,
           jl.send_size,
           jl.labels,
           jl.type,
           jl.crm_platform
    FROM data_vault_mvp.dwh.email_list jl
    WHERE jl.sent_date >= '2018-01-01'
)

SELECT CASE
           -- newly hardcoded url parameter to avoid id collision between send_id (SFMC) and campaign_id (iterable)
           WHEN LOWER(stmc.landing_page_parameters:utm_platform::VARCHAR) = 'iterable' THEN 'IT-' || stmc.utm_campaign
           -- before the hardcoded url parameter based on interrogation of data there is an identifiable mutuatally extinct range of ids
           -- from the 1st of Nov 2021, campaign id min 3,114,669, send id 949,936
           WHEN sts.event_tstamp::DATE >= '2021-11-01' AND LENGTH(stmc.utm_campaign) = 7 THEN 'IT-' || stmc.utm_campaign
           ELSE 'SFMC-' || stmc.utm_campaign
           END  AS email_id,
       COUNT(*) AS spvs
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    INNER JOIN email_details ed ON TRY_TO_NUMBER(stmc.utm_campaign) = COALESCE(ed.send_id, ed.campaign_id)
    AND sts.event_tstamp >= ed.sent_date
    AND sts.event_tstamp::DATE <= DATEADD(DAY, 7, ed.sent_date)
WHERE sts.event_tstamp::DATE >= '2018-01-01'
  AND email_id IN (
    SELECT DISTINCT ed.email_id
    FROM email_details ed
)
GROUP BY 1;


SELECT *
FROM se.data.email_performance ep
WHERE crm_platform = 'iterable';

SELECT *
FROM se.data.crm_jobs_list;
SELECT *
FROM se.data.crm_events_sends;
SELECT *
FROM se.data.crm_events_opens;
SELECT *
FROM se.data.crm_events_clicks;
SELECT *
FROM se.data.crm_events_unsubscribes;

SELECT *
FROM se.data.user_activity ua;
SELECT *
FROM se.data.active_user_base aub;

SELECT *
FROM se.data.email_performance ep
WHERE ep.crm_platform = 'iterable';


--1test
SELECT *
FROM collab.muse_1450_check_se_calendar.check_revised_calendar
WHERE date_value BETWEEN '2021-12-24' AND '2022-01-08';
--2test
SELECT *
FROM data_vault_mvp.dwh.se_calendar
WHERE date_value BETWEEN '2021-12-24' AND '2022-01-08';


SELECT *
FROM se.data.email_performance ep
WHERE ep.crm_platform = 'iterable';


SELECT es.event_created_at::DATE AS date,
       COUNT(*)
FROM latest_vault.iterable.email_send es
GROUP BY 1;


SELECT ces.event_date::DATE AS date,
       COUNT(*)
FROM se.data.crm_events_sends ces
WHERE ces.crm_platform = 'iterable'
GROUP BY 1
;

WITH step01 AS (
    SELECT DISTINCT
           aer.send_id
    FROM data_vault_mvp.dwh.athena_email_reporting aer
)

   , step02 AS (
    SELECT jl.email_id,
           jl.send_id,
           jl.campaign_id,
           jl.scheduled_date,
           jl.scheduled_tstmap,
           jl.email_name,
           jl.mapped_crm_date,
           jl.mapped_territory,
           jl.mapped_objective,
           jl.mapped_platform,
           jl.mapped_campaign,
           jl.sent_date,
           jl.sent_tstamp,
           jl.is_email_name_remapped,
           jl.client_id,
           jl.from_name,
           jl.from_email,
           jl.subject,
           jl.triggered_send_external_key,
           jl.send_definition_external_key,
           jl.job_status,
           jl.preview_url,
           jl.is_multipart,
           jl.additional,
           jl.campaign_created_at,
           jl.campaign_updated_at,
           jl.ended_at,
           jl.template_id,
           jl.message_medium,
           jl.created_by_user_id,
           jl.updated_by_user_id,
           jl.campaign_state,
           jl.list_ids,
           jl.suppression_list_ids,
           jl.send_size,
           jl.labels,
           jl.type,
           jl.crm_platform,
           IFF(ae.send_id IS NOT NULL, TRUE, FALSE) AS is_athena_email
    FROM data_vault_mvp.dwh.email_list jl
        LEFT JOIN step01 ae ON jl.send_id = ae.send_id
    WHERE jl.sent_date >= TO_DATE('2021-12-06 04:00:00') - 7 --get batch of sends that have occurred in the last 7 days
)

SELECT eo.email_id,
       COUNT(DISTINCT eo.shiro_user_id) AS unique_email_opens,
       COUNT(*)                         AS email_opens
FROM data_vault_mvp.dwh.email_open_event eo
    INNER JOIN step02 ed ON eo.send_id = ed.send_id
WHERE eo.event_date >= TO_DATE('2021-12-06 04:00:00') - 7
  AND eo.event_date <= TO_DATE('2021-12-06 04:00:00')
GROUP BY 1
;



self_describing_task --include 'se/data_pii/scv/scv_event_stream.py'  --method 'run' --start '2021-12-08 00:00:00' --end '2021-12-08 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.email_performance CLONE data_vault_mvp.dwh.email_performance;

self_describing_task --include 'dv/dwh/email/email_performance.py'  --method 'run' --start '2021-12-08 00:00:00' --end '2021-12-08 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.email_performance ep
WHERE ep.crm_platform = 'iterable'
  AND ep.sent_date::DATE = CURRENT_DATE - 2;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_performance ep
WHERE ep.crm_platform = 'iterable'
  AND ep.sent_date::DATE = CURRENT_DATE - 2

SELECT *
FROM scratch.robinpatel.email_performance_history
WHERE  sent_date::DATE = CURRENT_DATE - 1
;

