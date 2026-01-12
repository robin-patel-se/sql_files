CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.sfmc.athena_send_log CLONE hygiene_vault_mvp.sfmc.athena_send_log;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_sources;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.crm_email_segments CLONE data_vault_mvp.dwh.crm_email_segments;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2021-02-09 00:00:00' --end '2021-02-09 00:00:00'

--data_vault_mvp.dwh.athena_email_reporting__sales_in_send need to adjust the data in this table

SELECT asl.section,
       asl.position_in_section
FROM hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl;

DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;
DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send
WHERE athena_email_reporting__sales_in_send.se_sale_id = 'A24077'
  AND athena_email_reporting__sales_in_send.send_id = 1198928;



SELECT DISTINCT
       asl.deal_id                                                                                              AS se_sale_id,
       asl.job_id                                                                                               AS send_id,
       asl.log_date::DATE                                                                                       AS send_date,
       cjl.email_name,
       cjl.mapped_territory,
       js.data_source_name,
       asl.section,
       asl.position_in_section,
       ROW_NUMBER()
               OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) AS list_position,
       ROW_NUMBER()
               OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) <=
       10                                                                                                       AS is_in_top_10_position,
       ss.sale_name,
       ss.company_name,
       ss.start_date::DATE                                                                                      AS start_date,
       ss.end_date::DATE                                                                                        AS end_date,
       ss.sale_type,
       ss.sale_product,
       ss.destination_type,
       ss.posu_city,
       ss.posu_country,
       ss.posu_division,
       js.subscriber_key
FROM hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list cjl ON asl.job_id = cjl.send_id
         INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources js ON asl.job_id = js.send_id
    AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ss ON asl.deal_id = ss.se_sale_id
WHERE asl.updated_at >= TIMESTAMPADD('day', -1, '2021-02-08 06:00:00'::TIMESTAMP)::DATE
  AND js.data_source_name IS NOT NULL
  AND js.send_id = 1198928
  AND js.subscriber_key = 39052520;


--adjust richard code
SELECT ROW_NUMBER()
               OVER (PARTITION BY es.send_log__job_id, es.user_id ORDER BY es.send_log__section, es.send_log__position_in_section) AS email_position,
       es.*
FROM data_vault_mvp.athena.email_sends es;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send__prod CLONE data_vault_mvp.dwh.athena_email_reporting__sales_in_send;

SELECT sis.schedule_tstamp,
       sis.run_tstamp,
       sis.created_at,
       sis.updated_at,
       sis.se_sale_id,
       sis.send_id,
       sis.send_date,
       sis.email_name,
       sis.mapped_territory,
       sis.data_source_name,
       'unknown' AS sale_position_group,
       sis.sale_name,
       sis.company_name,
       sis.start_date,
       sis.end_date,
       sis.sale_type,
       sis.sale_product,
       sis.destination_type,
       sis.posu_city,
       sis.posu_country,
       sis.posu_division
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send__prod sis;

SELECT aer.send_id,
       aer.data_source_name,
       aer.sale_position_group,
       SUM(aer.impressions),
       SUM(clicks)
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10833'
GROUP BY 1, 2, 3;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send_test CLONE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;
DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;
DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting aer;

SELECT aer.send_id,
       aer.data_source_name,
       SUM(aer.impressions),
       SUM(clicks)
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10833'
  AND aer.send_id = 1198521
GROUP BY 1, 2;

SELECT aer.send_id,
       aer.data_source_name,
       aer.sale_position_group,
       SUM(aer.impressions),
       SUM(clicks)
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting aer
WHERE aer.se_sale_id = 'A10833'
  AND aer.send_id = 1198521
GROUP BY 1, 2, 3;


SELECT ep.url_parameters:utm_content::VARCHAR,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs ts
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_extracted_params ep ON ts.page_url = ep.url
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes tba ON ts.touch_id = tba.touch_id
--          LEFT JOIN hygiene_vault_mvp.sfmc.athena_send_log asl ON tba.attributed_user_id = asl.subscriber_key::VARCHAR
--     AND ep.url_parameters:utm_campaign::VARCHAR = asl.job_id::VARCHAR
--     AND ts.se_sale_id = asl.deal_id
WHERE ep.url_parameters:utm_campaign::VARCHAR = '1198521'
GROUP BY 1;

SELECT
--        *,
--        PARSE_URL(mts.page_url),
PARSE_URL(mts.page_url, 1):host::VARCHAR                     AS host,
PARSE_URL(mts.page_url, 1): PARAMETERS:utm_campaign::VARCHAR AS utm_campaign,
PARSE_URL(mts.page_url, 1): PARAMETERS:utm_content::VARCHAR  AS utm_content,
mts.event_tstamp::DATE                                       AS date,
COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE PARSE_URL(mts.page_url, 1): PARAMETERS:utm_campaign::VARCHAR = '1198521'
GROUP BY 1, 2, 3, 4;


SELECT data_source_name,
       sale_position_group,
       SUM(impressions),
       SUM(clicks)
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting
WHERE se_sale_id = 'A17160'
  AND send_id = 1199596
GROUP BY 1, 2;


SELECT data_source_name,
       SUM(impressions),
       SUM(clicks)
FROM data_vault_mvp.dwh.athena_email_reporting
WHERE se_sale_id = 'A17160'
  AND send_id = 1199596
GROUP BY 1;


SELECT
--        *,
--        PARSE_URL(mts.page_url),
PARSE_URL(mts.page_url, 1):host::VARCHAR                     AS host,
PARSE_URL(mts.page_url, 1): PARAMETERS:utm_campaign::VARCHAR AS utm_campaign,------------------------------------------------------------------------------------------------------------------------
PARSE_URL(mts.page_url, 1): PARAMETERS:utm_content::VARCHAR  AS utm_content,
mts.event_tstamp::DATE                                       AS date,
COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE PARSE_URL(mts.page_url, 1): PARAMETERS:utm_campaign::VARCHAR = '1199596'
GROUP BY 1, 2, 3, 4;

SELECT MIN(updated_at)
FROM hygiene_vault_mvp.sfmc.athena_send_log asl;

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2021-01-31 00:00:00' --end '2021-01-31 00:00:00'

------------------------------------------------------------------------------------------------------------------------
--how to process existing data,
--update sales in send to include 3 versions of all the existing sales, one with 'inside top 10', one with 'outside top 10' and
--one with 'unknown', this will ensure all possibilities are available for join
--insert existing data into the new table with sale_position_group as unknown,

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210216 CLONE data_vault_mvp.dwh.athena_email_reporting__sales_in_send;


CREATE TABLE IF NOT EXISTS data_vault_mvp.dwh.athena_email_reporting
(

    -- (lineage) metadata for the current job
    schedule_tstamp     TIMESTAMP,
    run_tstamp          TIMESTAMP,
    operation_id        VARCHAR,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    -- data columns
    se_sale_id          VARCHAR,
    send_id             NUMBER,
    send_date           DATE,
    email_name          VARCHAR,
    mapped_territory    VARCHAR,
    data_source_name    VARCHAR,
    sale_position_group VARCHAR,
    sale_name           VARCHAR,
    company_name        VARCHAR,
    sale_start_date     DATE,
    sale_end_date       DATE,
    sale_type           VARCHAR,
    sale_product        VARCHAR,
    destination_type    VARCHAR,
    posu_city           VARCHAR,
    posu_country        VARCHAR,
    posu_division       VARCHAR,
    event_date          DATE,
    unique_impressions  NUMBER,
    impressions         NUMBER,
    unique_clicks       NUMBER,
    clicks              NUMBER,

    PRIMARY KEY (se_sale_id, send_id, data_source_name, sale_position_group, event_date)
)
    CLUSTER BY (se_sale_id, send_id);
;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting
SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'unknown' AS sale_position_group,
       sale_name,
       company_name,
       sale_start_date,
       sale_end_date,
       sale_type,
       sale_product,
       destination_type,
       posu_city,
       posu_country,
       posu_division,
       event_date,
       unique_impressions,
       impressions,
       unique_clicks,
       clicks
FROM data_vault_mvp.dwh.athena_email_reporting_20210216;

CREATE TABLE IF NOT EXISTS data_vault_mvp.dwh.athena_email_reporting__sales_in_send
(
    schedule_tstamp     TIMESTAMP,
    run_tstamp          TIMESTAMP,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    se_sale_id          VARCHAR,
    send_id             NUMBER,
    send_date           DATE,
    email_name          VARCHAR,
    mapped_territory    VARCHAR,
    data_source_name    VARCHAR,
    sale_position_group VARCHAR, --generic group to allow for flexibility
    sale_name           VARCHAR,
    company_name        VARCHAR,
    start_date          DATE,
    end_date            DATE,
    sale_type           VARCHAR,
    sale_product        VARCHAR,
    destination_type    VARCHAR,
    posu_city           VARCHAR,
    posu_country        VARCHAR,
    posu_division       VARCHAR,

    PRIMARY KEY (se_sale_id, send_id, data_source_name, sale_position_group)
)
    CLUSTER BY (send_id);
;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting__sales_in_send
SELECT schedule_tstamp,
       run_tstamp,
       created_at,
       updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'inside top 10' AS sale_position_group,
       sale_name,
       company_name,
       start_date,
       end_date,
       sale_type,
       sale_product,
       destination_type,
       posu_city,
       posu_country,
       posu_division
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210216;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting__sales_in_send
SELECT schedule_tstamp,
       run_tstamp,
       created_at,
       updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'outside top 10' AS sale_position_group,
       sale_name,
       company_name,
       start_date,
       end_date,
       sale_type,
       sale_product,
       destination_type,
       posu_city,
       posu_country,
       posu_division
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210216;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting__sales_in_send
SELECT schedule_tstamp,
       run_tstamp,
       created_at,
       updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'unknown' AS sale_position_group,
       sale_name,
       company_name,
       start_date,
       end_date,
       sale_type,
       sale_product,
       destination_type,
       posu_city,
       posu_country,
       posu_division
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210216;


