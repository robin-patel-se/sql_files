SELECT ses.event_hash,
       ses.event_date,
       ses.event_tstamp,
       ses.data_source_key,
       ses.shiro_user_id,
       ses.client_id,
       'SFMC-' || ses.send_id       AS email_id,
       ses.send_id,
       NULL                         AS campaign_id,
       ses.subscriber_key,
       ses.email_address,
       ses.subscriber_id,
       ses.list_id,
       --ses.event_date__o,
       ses.event_type,
       ses.batch_id,
       ses.triggered_send_external_key,
       NULL                         AS catalog_collection_count,
       NULL                         AS catalog_lookup_count,
       NULL                         AS channel_id,
       NULL                         AS content_id,
       NULL                         AS message_bus_id,
       NULL                         AS message_id,
       NULL                         AS message_type_id,
       NULL                         AS product_recommendation_count,
       NULL                         AS template_id,
       'salesforce marketing cloud' AS crm_platform
FROM hygiene_snapshot_vault_mvp.sfmc.events_sends ses

UNION ALL

SELECT SHA2(
                   COALESCE(ies.campaign_id, 0) ||
                   COALESCE(ies.email, '') ||
                   COALESCE(ies.event_created_at, '1970-01-01')
           , 256)                 AS event_hash,
       ies.event_created_at::DATE AS event_date,
       ies.event_created_at       AS event_tstamp,
       NULL                       AS data_source_key,
       su.id                      AS shiro_user_id,
       NULL                       AS client_id,
       'IT-' || ies.campaign_id   AS email_id,
       NULL                       AS send_id,
       ies.campaign_id,
       su.id::VARCHAR             AS subscriber_key,
       ies.email                  AS email_address,
       NULL                       AS subscriber_id,
       NULL                       AS list_id,
       NULL                       AS event_type,
       NULL                       AS batch_id,
       NULL                       AS triggered_send_external_key,
       ies.catalog_collection_count,
       ies.catalog_lookup_count,
       ies.channel_id,
       ies.content_id,
       ies.message_bus_id,
       ies.message_id,
       ies.message_type_id,
       ies.product_recommendation_count,
       ies.template_id,
       'iterable'                 AS crm_platform
--        ies.record
FROM latest_vault.iterable.email_send ies
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON ies.email = su.username;
;

self_describing_task --include 'dv/dwh/email/email_events/email_send_event.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'


------------------------------------------------------------------------------------------------------------------------

SELECT seo.event_hash,
       seo.event_date,
       seo.event_tstamp,
       seo.data_source_key,
       seo.shiro_user_id,
       seo.client_id,
       'SFMC-' || seo.send_id       AS email_id,
       seo.send_id,
       NULL                         AS campaign_id,
       seo.subscriber_key,
       seo.email_address,
       seo.subscriber_id,
       seo.list_id,
       seo.city,
       seo.country,
       seo.region,
       seo.ip_address, -- NOTE: This column is considered PII
       seo.event_type,
       seo.batch_id,
       seo.triggered_send_external_key,
       seo.latitude,
       seo.longitude,
       seo.metrocode,
       seo.area_code,
       seo.browser,
       seo.email_client,
       seo.operating_system,
       seo.device,
       NULL                         AS event_name,
       NULL                         AS message_id,
       NULL                         AS template_id,
       NULL                         AS user_agent,
       NULL                         AS user_agent_device,
       'salesforce marketing cloud' AS crm_platform
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred seo

UNION ALL

SELECT SHA2(
                   COALESCE(ieo.campaign_id, 0) ||
                   COALESCE(ieo.email, '') ||
                   COALESCE(ieo.event_created_at, '1970-01-01')
           , 256)                 AS event_hash,
       ieo.event_created_at::DATE AS event_date,
       ieo.event_created_at       AS event_tstamp,
       NULL                       AS data_source_key,
       su.id                      AS shiro_user_id,
       NULL                       AS client_id,
       'IT-' || ieo.campaign_id   AS email_id,
       NULL                       AS send_id,
       ieo.campaign_id,
       su.id::VARCHAR             AS subscriber_key,
       ieo.email                  AS email_address,
       NULL                       AS subscriber_id,
       NULL                       AS list_id,
       ieo.city,
       ieo.country,
       ieo.region,
       ieo.ip, -- NOTE: This column is considered PII
       NULL                       AS event_type,
       NULL                       AS batch_id,
       NULL                       AS triggered_send_external_key,
       NULL                       AS latitude,
       NULL                       AS longitude,
       NULL                       AS metrocode,
       NULL                       AS area_code,
       NULL                       AS browser,
       NULL                       AS email_client,
       NULL                       AS operating_system,
       NULL                       AS device,
       ieo.event_name,
       ieo.message_id,
       ieo.template_id,
       ieo.user_agent,
       ieo.user_agent_device,
       'iterable'                 AS crm_platform
FROM latest_vault.iterable.email_open ieo
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON ieo.email = su.username;

self_describing_task --include 'dv/dwh/email/email_events/email_open_event.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.email_send_event
--WHERE crm_platform = 'iterable';
------------------------------------------------------------------------------------------------------------------------
SELECT sec.event_hash,
       sec.event_date,
       sec.event_tstamp,
       sec.data_source_key,
       sec.shiro_user_id,
       sec.client_id,
       'SFMC-' || sec.send_id       AS email_id,
       sec.send_id,
       NULL                         AS campaign_id,
       sec.subscriber_key,
       sec.email_address,
       sec.subscriber_id,
       sec.list_id,
       sec.city,
       sec.country,
       sec.region,
       sec.ip_address, -- NOTE: This column is considered PII
       'click'                      AS event_type,
       sec.send_url_id,
       sec.url_id,
       sec.url,
       sec.alias,
       sec.batch_id,
       sec.triggered_send_external_key,
       sec.latitude,
       sec.longitude,
       sec.metrocode,
       sec.area_code,
       sec.browser,
       sec.email_client,
       sec.operating_system,
       sec.device,
       NULL                         AS content_id,
       NULL                         AS event_created_at,
       NULL                         AS event_name,
       NULL                         AS href_index,
       NULL                         AS message_id,
       NULL                         AS template_id,
       NULL                         AS url,
       NULL                         AS user_agent,
       NULL                         AS user_agent_device,
       'salesforce marketing cloud' AS crm_platform
FROM hygiene_snapshot_vault_mvp.sfmc.events_clicks sec

UNION ALL

SELECT SHA2(
                   COALESCE(iec.campaign_id, 0) ||
                   COALESCE(iec.email, '') ||
                   COALESCE(iec.event_created_at, '1970-01-01')
           , 256)                 AS event_hash,
       iec.event_created_at::DATE AS event_date,
       iec.event_created_at       AS event_tstamp,
       NULL                       AS data_source_key,
       su.id                      AS shiro_user_id,
       NULL                       AS client_id,
       'IT-' || iec.campaign_id   AS email_id,
       NULL                       AS send_id,
       iec.campaign_id,
       su.id::VARCHAR             AS subscriber_key,
       iec.email                  AS email_address,
       NULL                       AS subscriber_id,
       NULL                       AS list_id,
       iec.city,
       iec.country,
       iec.region,
       iec.ip, -- NOTE: This column is considered PII
       'click'                    AS event_type,
       NULL                       AS send_url_id,
       NULL                       AS url_id,
       NULL                       AS url,
       NULL                       AS alias,
       NULL                       AS batch_id,
       NULL                       AS triggered_send_external_key,
       NULL                       AS latitude,
       NULL                       AS longitude,
       NULL                       AS metrocode,
       NULL                       AS area_code,
       NULL                       AS browser,
       NULL                       AS email_client,
       NULL                       AS operating_system,
       NULL                       AS device,
       iec.content_id,
       iec.event_created_at,
       iec.event_name,
       iec.href_index,
       iec.message_id,
       iec.template_id,
       iec.url,
       iec.user_agent,
       iec.user_agent_device,
       'iterable'                 AS crm_platform
FROM latest_vault_dev_robin.iterable.email_click iec
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON iec.email = su.username;


self_describing_task --include 'dv/dwh/email/email_events/email_click_event.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
USE WAREHOUSE pipe_2xlarge;


------------------------------------------------------------------------------------------------------------------------
SELECT seu.event_hash,
       seu.event_date,
       seu.event_tstamp,
       seu.data_source_key,
       seu.shiro_user_id,
       seu.client_id,
       'SFMC-' || seu.send_id       AS email_id,
       seu.send_id,
       NULL                         AS campaign_id,
       seu.subscriber_key,
       seu.email_address,
       seu.subscriber_id,
       seu.list_id,
       'unsubscribe'                AS event_type,
       seu.batch_id,
       seu.triggered_send_external_key,
       seu.unsub_reason,
       NULL                         AS channel_ids,
       NULL                         AS bounce_message,
       NULL                         AS email_list_ids,
       NULL                         AS message_id,
       NULL                         AS recipient_state,
       NULL                         AS template_id,
       NULL                         AS unsub_source,
       'salesforce marketing cloud' AS crm_platform
FROM hygiene_snapshot_vault_mvp.sfmc.events_unsubscribes seu
UNION ALL
SELECT SHA2(
                   COALESCE(ieu.campaign_id, 0) ||
                   COALESCE(ieu.email, '') ||
                   COALESCE(ieu.event_created_at, '1970-01-01')
           , 256)                 AS event_hash,
       ieu.event_created_at::DATE AS event_date,
       ieu.event_created_at       AS event_tstamp,
       NULL                       AS data_source_key,
       su.id                      AS shiro_user_id,
       NULL                       AS client_id,
       'IT-' || ieu.campaign_id   AS email_id,
       NULL                       AS send_id,
       ieu.campaign_id,
       su.id::VARCHAR             AS subscriber_key,
       ieu.email                  AS email_address,
       NULL                       AS subscriber_id,
       NULL                       AS list_id,
       'unsubscribe'              AS event_type,
       NULL                       AS batch_id,
       NULL                       AS triggered_send_external_key,
       NULL                       AS unsub_reason,
       ieu.channel_ids,
       ieu.bounce_message,
       ieu.email_list_ids,
       ieu.message_id,
       ieu.recipient_state,
       ieu.template_id,
       ieu.unsub_source,
       'iterable'                 AS crm_platform
FROM latest_vault_dev_robin.iterable.email_unsubscribe ieu
    INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su ON ieu.email = su.username;

self_describing_task --include 'dv/dwh/email/email_events/email_unsubscribe_event.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
------------------------------------------------------------------------------------------------------------------------

SELECT 'SFMC-' || jl.send_id        AS email_id,
       jl.send_id,
       NULL                         AS campaign_id,
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
       jl.sched_time,
       jl.sent_time,
       jl.subject,
       jl.triggered_send_external_key,
       jl.send_definition_external_key,
       jl.job_status,
       jl.preview_url,
       jl.is_multipart,
       jl.additional,
       NULL                         AS campaign_created_at,
       NULL                         AS campaign_updated_at,
       NULL                         AS ended_at,
       NULL                         AS template_id,
       NULL                         AS message_medium,
       NULL                         AS created_by_user_id,
       NULL                         AS updated_by_user_id,
       NULL                         AS campaign_state,
       NULL                         AS list_ids,
       NULL                         AS suppression_list_ids,
       NULL                         AS send_size,
       NULL                         AS labels,
       NULL                         AS type,
       'salesforce marketing cloud' AS crm_platform
FROM hygiene_snapshot_vault_mvp.sfmc.jobs_list jl
UNION ALL

SELECT 'IT-' || c.id    AS email_id,
       NULL             AS send_id,
       c.id             AS campaign_id,
       c.start_at::DATE AS scheduled_date,
       c.start_at       AS scheduled_tstamp,
       c.name           AS email_name,
       c.mapped_crm_date,
       c.mapped_territory,
       c.mapped_objective,
       c.mapped_platform,
       c.mapped_campaign,
       c.start_at::DATE AS sent_date,
       c.start_at       AS sent_tstamp,
       NULL             AS is_email_name_remapped,
       NULL             AS client_id,
       NULL             AS from_name,
       NULL             AS from_email,
       NULL             AS sched_time,
       NULL             AS sent_time,
       NULL             AS subject,
       NULL             AS triggered_send_external_key,
       NULL             AS send_definition_external_key,
       NULL             AS job_status,
       NULL             AS preview_url,
       NULL             AS is_multipart,
       NULL             AS additional,
       c.campaign_created_at,
       c.campaign_updated_at,
       c.ended_at,
       c.template_id,
       c.message_medium,
       c.created_by_user_id, -- NOTE: This column is considered PII
       c.updated_by_user_id, -- NOTE: This column is considered PII
       c.campaign_state,
       c.list_ids,
       c.suppression_list_ids,
       c.send_size,
       c.labels,
       c.type,
       'iterable'       AS crm_platform
FROM latest_vault.iterable.campaign c
WHERE c.message_medium = 'Email' -- to avoid push events accidentally coming through in the future
;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign CLONE latest_vault.iterable.campaign;

self_describing_task --include 'dv/dwh/email/email_events/email_list.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
------------------------------------------------------------------------------------------------------------------------
WITH a1 AS (
    SELECT *
    FROM data_vault_mvp.dwh.email_click_event ece
    WHERE crm_platform = 'iterable'
    LIMIT 10
),
     b1 AS (
         SELECT *
         FROM data_vault_mvp.dwh.email_click_event e
         WHERE crm_platform = 'salesforce marketing cloud'
         LIMIT 10
     )
SELECT *
FROM a1
UNION ALL
SELECT *
FROM b1;



SELECT ece.event_hash,
       ece.event_date,
       ece.event_tstamp,
       ece.data_source_key,
       ece.shiro_user_id,
       ece.client_id,
       ece.email_id,
       ece.send_id,
       ece.campaign_id,
       ece.subscriber_key,
       ece.email_address,
       ece.subscriber_id,
       ece.list_id,
       ece.city,
       ece.country,
       ece.region,
       ece.ip_address,
       ece.url,
       ece.event_type,
       ece.send_url_id,
       ece.url_id,
       ece.alias,
       ece.batch_id,
       ece.triggered_send_external_key,
       ece.latitude,
       ece.longitude,
       ece.metrocode,
       ece.area_code,
       ece.browser,
       ece.email_client,
       ece.operating_system,
       ece.device,
       ece.content_id,
       ece.event_created_at,
       ece.href_index,
       ece.message_id,
       ece.template_id,
       ece.user_agent,
       ece.user_agent_device,
       ece.crm_platform
FROM data_vault_mvp.dwh.email_click_event ece;


------------------------------------------------------------------------------------------------------------------------
    self_describing_task --include 'se/data_pii/crm/crm_events_clicks.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
    self_describing_task --include 'se/data/crm/crm_events_clicks.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'


SELECT ece.event_hash,
       ece.event_date,
       ece.event_tstamp,
       ece.data_source_key,
       ece.shiro_user_id,
       ece.client_id,
       ece.email_id,
       ece.send_id,
       ece.campaign_id,
       ece.subscriber_key,
       SHA2(ece.email_address) AS email_address_hash,
       ece.subscriber_id,
       ece.list_id,
       ece.city,
       ece.country,
       ece.region,
       SHA2(ece.ip_address)    AS ip_address_hash,
       ece.url,
       ece.event_type,
       ece.send_url_id,
       ece.url_id,
       ece.alias,
       ece.batch_id,
       ece.triggered_send_external_key,
       ece.latitude,
       ece.longitude,
       ece.metrocode,
       ece.area_code,
       ece.browser,
       ece.email_client,
       ece.operating_system,
       ece.device,
       ece.content_id,
       ece.event_created_at,
       ece.href_index,
       ece.message_id,
       ece.template_id,
       ece.user_agent,
       ece.user_agent_device,
       ece.crm_platform,
       SHA2(
                   COALESCE(ece.send_id::VARCHAR, '') ||
                   COALESCE(ece.list_id::VARCHAR, '')
           )                   AS email_segment_key
FROM data_vault_mvp.dwh.email_click_event ece;


SELECT eoe.event_hash,
       eoe.event_date,
       eoe.event_tstamp,
       eoe.data_source_key,
       eoe.shiro_user_id,
       eoe.client_id,
       eoe.email_id,
       eoe.send_id,
       eoe.campaign_id,
       eoe.subscriber_key,
       eoe.email_address,
       eoe.subscriber_id,
       eoe.list_id,
       eoe.city,
       eoe.country,
       eoe.region,
       eoe.ip_address,
       eoe.event_type,
       eoe.batch_id,
       eoe.triggered_send_external_key,
       eoe.latitude,
       eoe.longitude,
       eoe.metrocode,
       eoe.area_code,
       eoe.browser,
       eoe.email_client,
       eoe.operating_system,
       eoe.device,
       eoe.message_id,
       eoe.template_id,
       eoe.user_agent,
       eoe.user_agent_device,
       eoe.crm_platform
FROM data_vault_mvp.dwh.email_open_event eoe;
    self_describing_task --include 'se/data_pii/crm/crm_events_opens.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
    self_describing_task --include 'se/data/crm/crm_events_opens.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'

------------------------------------------------------------------------------------------------------------------------

SELECT ese.event_hash,
       ese.event_date,
       ese.event_tstamp,
       ese.data_source_key,
       ese.shiro_user_id,
       ese.client_id,
       ese.email_id,
       ese.send_id,
       ese.campaign_id,
       ese.subscriber_key,
       ese.email_address,
       ese.subscriber_id,
       ese.list_id,
       ese.event_type,
       ese.batch_id,
       ese.triggered_send_external_key,
       ese.catalog_collection_count,
       ese.catalog_lookup_count,
       ese.channel_id,
       ese.content_id,
       ese.message_bus_id,
       ese.message_id,
       ese.message_type_id,
       ese.product_recommendation_count,
       ese.template_id,
       ese.crm_platform
FROM data_vault_mvp.dwh.email_send_event ese;
    self_describing_task --include 'se/data_pii/crm/crm_events_sends.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
    self_describing_task
--include 'se/data/crm/crm_events_sends.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'

------------------------------------------------------------------------------------------------------------------------
;
SELECT eue.event_hash,
       eue.event_date,
       eue.event_tstamp,
       eue.data_source_key,
       eue.shiro_user_id,
       eue.client_id,
       eue.email_id,
       eue.send_id,
       eue.campaign_id,
       eue.subscriber_key,
       eue.email_address,
       eue.subscriber_id,
       eue.list_id,
       eue.event_type,
       eue.batch_id,
       eue.triggered_send_external_key,
       eue.unsub_reason,
       eue.channel_ids,
       eue.bounce_message,
       eue.email_list_ids,
       eue.message_id,
       eue.recipient_state,
       eue.template_id,
       eue.unsub_source,
       eue.crm_platform
FROM data_vault_mvp.dwh.email_unsubscribe_event eue;
    self_describing_task --include 'se/data_pii/crm/crm_events_unsubscribes.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
    self_describing_task
--include 'se/data/crm/crm_events_unsubscribes.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'

------------------------------------------------------------------------------------------------------------------------

SELECT el.email_id,
       el.send_id,
       el.campaign_id,
       el.scheduled_date,
       el.scheduled_tstmap,
       el.email_name,
       el.mapped_crm_date,
       el.mapped_territory,
       el.mapped_objective,
       el.mapped_platform,
       el.mapped_campaign,
       el.sent_date,
       el.sent_tstamp,
       el.is_email_name_remapped,
       el.client_id,
       el.from_name,
       el.from_email,
       el.subject,
       el.triggered_send_external_key,
       el.send_definition_external_key,
       el.job_status,
       el.preview_url,
       el.is_multipart,
       el.additional,
       el.campaign_created_at,
       el.campaign_updated_at,
       el.ended_at,
       el.template_id,
       el.message_medium,
       el.created_by_user_id,
       el.updated_by_user_id,
       el.campaign_state,
       el.list_ids,
       el.suppression_list_ids,
       el.send_size,
       el.labels,
       el.type,
       el.crm_platform
FROM data_vault_mvp.dwh.email_list el;
    self_describing_task --include 'se/data_pii/crm/crm_jobs_list.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'
    self_describing_task --include 'se/data/crm/crm_jobs_list.py'  --method 'run' --start '2021-11-29 00:00:00' --end '2021-11-29 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_sends CLONE hygiene_snapshot_vault_mvp.sfmc.events_sends;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_clicks CLONE hygiene_snapshot_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_unsubscribes CLONE hygiene_snapshot_vault_mvp.sfmc.events_unsubscribes;

USE WAREHOUSE pipe_2xlarge;

WITH grain AS (
    SELECT DISTINCT jl.email_id, jl.scheduled_date::DATE AS date
    FROM se_dev_robin.data.crm_jobs_list jl

    UNION

    SELECT DISTINCT es.email_id, es.event_date::DATE AS date
    FROM se_dev_robin.data.crm_events_sends es

    UNION

    SELECT DISTINCT eo.email_id, eo.event_date::DATE AS date
    FROM se_dev_robin.data.crm_events_opens eo

    UNION

    SELECT DISTINCT ec.email_id, ec.event_date::DATE AS date
    FROM se_dev_robin.data.crm_events_clicks ec

    UNION

    SELECT DISTINCT eu.email_id, eu.event_date::DATE AS date
    FROM se_dev_robin.data.crm_events_unsubscribes eu
),
     agg_sends AS (
         SELECT es.event_date::DATE        AS send_date,
                es.email_id,
                COUNT(*)                         AS sends,
                COUNT(DISTINCT es.shiro_user_id) AS unique_sends
         FROM se_dev_robin.data.crm_events_sends es
         GROUP BY 1, 2

     ),
     agg_opens AS (
         SELECT eo.event_date::DATE        AS open_date,
                eo.email_id,
                COUNT(*)                         AS opens,
                COUNT(DISTINCT eo.shiro_user_id) AS unique_opens
         FROM se_dev_robin.data.crm_events_opens eo
         GROUP BY 1, 2
     ),
     agg_clicks AS (
         SELECT ec.event_date::DATE        AS click_date,
                ec.email_id,
                COUNT(*)                         AS clicks,
                COUNT(DISTINCT ec.shiro_user_id) AS unique_clicks
         FROM se_dev_robin.data.crm_events_clicks ec
         GROUP BY 1, 2
     ),
     agg_unsubscribes AS (
         SELECT eu.event_date::DATE        AS unsubscribe_date,
                eu.email_id,
                COUNT(*)                         AS unsubscribes,
                COUNT(DISTINCT eu.shiro_user_id) AS unique_unsubscribes
         FROM se_dev_robin.data.crm_events_unsubscribes eu
         GROUP BY 1, 2
     )
SELECT g.email_id,
       g.date,
       cjl.email_name,
       cjl.mapped_crm_date,
       cjl.mapped_territory,
       cjl.mapped_objective,
       cjl.mapped_platform,
       cjl.mapped_campaign,
       ags.sends,
       ags.unique_sends,
       ago.opens,
       ago.unique_opens,
       agc.clicks,
       agc.unique_clicks,
       agu.unsubscribes,
       agu.unique_unsubscribes
FROM grain g
    LEFT JOIN se_dev_robin.data.crm_jobs_list cjl ON g.email_id = cjl.email_id
    LEFT JOIN agg_sends ags ON g.email_id = ags.email_id AND g.date = ags.send_date
    LEFT JOIN agg_opens ago ON g.email_id = ago.email_id AND g.date = ago.open_date
    LEFT JOIN agg_clicks agc ON g.email_id = agc.email_id AND g.date = agc.click_date
    LEFT JOIN agg_unsubscribes agu ON g.email_id = agu.email_id AND g.date = agu.unsubscribe_date
WHERE cjl.mapped_crm_date >= '2021-10-01'
AND cjl.crm_platform = 'iterable'
-- AND cjl.crm_platform = 'salesforce marketing cloud'
;

