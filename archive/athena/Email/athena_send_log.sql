SELECT asl.deal_id,
       count(DISTINCT coalesce(asl.job_id, asl.subscriber_key)) AS emails
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
GROUP BY 1
ORDER BY 2 DESC;


SELECT *
FROM se.data.crm_jobs_list cjl;

SELECT *
FROM hygiene_vault_mvp.sfmc.athena_send_log asl;
SELECT *
FROM se.data_pii.crm_jobs_list cjl
LIMIT 1;

WITH sales_within_send AS (
    SELECT asl.deal_id,
           asl.job_id         AS send_id,
           asl.log_date::DATE AS send_date,
           asl.subscriber_key,
           cjl.email_name
    FROM hygiene_vault_mvp.sfmc.athena_send_log asl
             LEFT JOIN se.data.crm_jobs_list cjl ON asl.job_id = cjl.send_id
),
     impressions AS (

         SELECT ceo.send_id,
                ceo.subscriber_key,
                event_tstamp::DATE AS open_date,
                1                  AS unque_impressions,
                count(*)           AS impressions
         FROM se.data.crm_events_opens ceo
         GROUP BY 1, 2, 3
     )
SELECT *
FROM sales_within_send sws
         LEFT JOIN impressions i ON sws.send_id = i.send_id AND sws.subscriber_key = i.subscriber_key
WHERE sws.send_id = 1171954;

USE WAREHOUSE pipe_xlarge;


WITH sales_in_send AS (
    --get a list of all sales that are included in a send
    SELECT asl.deal_id,
           asl.job_id         AS send_id,
           asl.subscriber_key,
           asl.log_date::DATE AS send_date,
           cjl.email_name,
           js.data_source_name,
           1                  AS sends
    FROM hygiene_vault_mvp.sfmc.athena_send_log asl
             LEFT JOIN se.data.crm_jobs_list cjl ON asl.job_id = cjl.send_id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js
                       ON asl.job_id = js.send_id AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
    WHERE asl.job_id = 1171954
),

     impressions AS (
         --aggregate opens based on send id, by user id, by date. We assume that
         --if someone opens an email with 10 sales in it, all 10 sales should
         --be assigned an impression.
         SELECT ceo.send_id,
                ceo.subscriber_key,
                event_tstamp::DATE AS open_date,
                1                  AS unque_impressions,
                count(*)           AS impressions
         FROM se.data.crm_events_opens ceo
         GROUP BY 1, 2, 3
     )
SELECT *

FROM sales_in_send sis
         LEFT JOIN impressions i ON sis.subscriber_key = i.subscriber_key AND sis.send_id = i.send_id
;


------------------------------------------------------------------------------------------------------------------------

SELECT ces.list_id, count(*)
FROM se.data.crm_events_sends ces
WHERE ces.send_id = 1171954
GROUP BY 1;

SELECT ceo.list_id, count(*)
FROM se.data.crm_events_opens ceo
WHERE ceo.send_id = 1171954
GROUP BY 1;

SELECT js.data_source_name,
       count(DISTINCT asl.subscriber_key)
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
         LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js
                   ON asl.job_id = js.send_id AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
WHERE asl.job_id = 1171954
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

SELECT count(*),
       count(DISTINCT asl.subscriber_key)
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
         LEFT JOIN se.data.crm_jobs_list cjl ON asl.job_id = cjl.send_id
         LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js
                   ON asl.job_id = js.send_id AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
WHERE asl.job_id = 1171954;


SELECT *
FROM raw_vault_mvp.sfmc.athena_send_log asl
WHERE TRY_TO_NUMBER(asl.subscriber_key) IS NULL;


--opens
SELECT ceo.send_id,
       ceo.list_id,
       count(DISTINCT ceo.subscriber_key) AS unque_impressions,
       count(*)                           AS impressions
FROM se.data.crm_events_opens ceo
WHERE ceo.send_id = 1171954
GROUP BY 1, 2;

SELECT ceo.subscriber_key, ceo.event_date, count(*)
FROM se.data.crm_events_opens ceo
WHERE ceo.send_id = 1171954
--   AND ceo.subscriber_key IN (27727863, 52275091)
GROUP BY 1, 2
ORDER BY 3 DESC;
--25324371 user id with 163 opens

--granular view
WITH sales_in_send AS (
    --get a list of all sales that are included in a send
    SELECT asl.deal_id,
           asl.job_id         AS send_id,
           asl.subscriber_key,
           asl.log_date::DATE AS send_date,
           cjl.email_name,
           js.data_source_name
    FROM hygiene_vault_mvp.sfmc.athena_send_log asl
             LEFT JOIN se.data.crm_jobs_list cjl ON asl.job_id = cjl.send_id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js
                       ON asl.job_id = js.send_id AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
    WHERE asl.job_id = 1171954
      AND js.subscriber_key IN (21441170, 47457890, 72104760, 68237600)
),
     impressions AS (
         --aggregate opens based on send id, by user id, by date. We assume that
         --if someone opens an email with 10 sales in it, all 10 sales should
         --be assigned an impression.
         SELECT ceo.send_id,
                ceo.subscriber_key,
                event_tstamp::DATE AS open_date,
                1                  AS unque_impressions,
                count(*)           AS impressions
         FROM se.data.crm_events_opens ceo
         GROUP BY 1, 2, 3
     )
SELECT sis.deal_id,
       sis.send_id,
       sis.subscriber_key,
       sis.send_date,
       sis.email_name,
       sis.data_source_name,
       i.open_date,
       i.unque_impressions,
       i.impressions

FROM sales_in_send sis
         LEFT JOIN impressions i ON sis.subscriber_key = i.subscriber_key AND sis.send_id = i.send_id
;

--aggregate w/o ds
WITH sales_in_send AS (
    --get a list of all sales that are included in a send
    --content varies from email to email based on the recipient
    SELECT asl.deal_id,
           asl.job_id         AS send_id,
           asl.subscriber_key,
           asl.log_date::DATE AS send_date,
           cjl.email_name,
           js.data_source_name,
           1                  AS sends
    FROM hygiene_vault_mvp.sfmc.athena_send_log asl
             LEFT JOIN se.data.crm_jobs_list cjl ON asl.job_id = cjl.send_id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js
                       ON asl.job_id = js.send_id AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
    WHERE asl.job_id = 1171954
      AND js.subscriber_key IN (21441170, 47457890, 72104760, 68237600)
),
     impressions AS (
         --aggregate opens based on send id, by user id, by date. We assume that
         --if someone opens an email with 10 sales in it, all 10 sales should
         --be assigned an impression.
         SELECT ceo.send_id,
                ceo.subscriber_key,
                event_tstamp::DATE AS open_date,
                1                  AS unque_impressions,
                count(*)           AS impressions
         FROM se.data.crm_events_opens ceo
         GROUP BY 1, 2, 3
     )
SELECT sis.deal_id,
       sis.send_id,
       sis.send_date,
       sis.email_name,
       i.open_date,
       SUM(i.unque_impressions),
       SUM(i.impressions)

FROM sales_in_send sis
         LEFT JOIN impressions i ON sis.subscriber_key = i.subscriber_key AND sis.send_id = i.send_id
GROUP BY 1, 2, 3, 4, 5;

USE WAREHOUSE pipe_xlarge;

--sessions starting with utm
SELECT mtba.touch_start_tstamp::DATE AS click_date,
       mtba.attributed_user_id       AS subscriber_key,
       mtmc.utm_content              AS data_source_name,
       es.se_sale_id,
       COUNT(DISTINCT mtba.touch_id) AS clicks
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
    --touch id is set by the first event_hash in the session
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mtba.touch_id = es.event_hash
WHERE mtmc.touch_mkt_channel LIKE 'Email%'
  AND mtba.stitched_identity_type = 'se_user_id'
  AND mtba.touch_start_tstamp >= '2020-10-06'
GROUP BY 1, 2, 3, 4;


--multiple clicks can occur within the same session so need to establish this on an event level
SELECT mts.event_tstamp::DATE                  AS click_date,
       mt.attributed_user_id,
       mep.url_parameters:utm_content::VARCHAR AS data_source_name,
       mts.se_sale_id,
       mep.url_parameters,
       COUNT(DISTINCT mts.event_hash)          AS clicks
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_extracted_params mep ON mts.page_url = mep.url
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON mts.event_hash = mt.event_hash
WHERE mts.event_tstamp >= '2020-10-06'
  AND mep.url_parameters:utm_medium::VARCHAR = 'email'
GROUP BY 1, 2, 3, 4, 5;


------------------------------------------------------------------------------------------------------------------------
--aggregate view
WITH sales_in_send AS (
    --get a list of all sales that are included in a send
    --content varies from email to email based on the recipient
    SELECT asl.deal_id,
           asl.job_id          AS send_id,
           asl.subscriber_key,
           asl.log_date::DATE  AS send_date,
           cjl.email_name,
           cjl.mapped_territory,
           js.data_source_name,
           ss.sale_name,
           ss.company_name,
           ss.start_date::DATE AS start_date,
           ss.end_date::DATE   AS end_date,
           ss.sale_type,
           ss.sale_product,
           ss.destination_type,
           ss.posu_city,
           ss.posu_country,
           ss.posu_division
    FROM hygiene_vault_mvp.sfmc.athena_send_log asl
             LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_list cjl ON asl.job_id = cjl.send_id
             LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js
                       ON asl.job_id = js.send_id AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
             LEFT JOIN data_vault_mvp.dwh.se_sale ss ON asl.deal_id = ss.se_sale_id
    WHERE asl.job_id = 1171954
      AND js.subscriber_key IN (21441170, 47457890, 72104760, 68237600)
),
     impressions AS (
         --aggregate opens based on send id, by user id, by date. We assume that
         --if someone opens an email with 10 sales in it, all 10 sales should
         --be assigned an impression.
         SELECT ceo.send_id,
                ceo.subscriber_key,
                event_tstamp::DATE AS open_date,
                1                  AS unque_impressions,
                count(*)           AS impressions
         FROM se.data.crm_events_opens ceo
         WHERE ceo.event_date >= '2020-10-06' --hard date when athena went live
         GROUP BY 1, 2, 3
     ),
     clicks AS (
         --model scv sessions based on landing age parameter
         SELECT mts.event_tstamp::DATE                   AS click_date,
                mt.attributed_user_id                    AS subscriber_key,
                mep.url_parameters:utm_campaign::VARCHAR AS send_id,
                mts.se_sale_id,
                1                                        AS unique_clicks,
                COUNT(DISTINCT mts.event_hash)           AS clicks
         FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
                  --to get the parameters of each url
                  INNER JOIN data_vault_mvp.single_customer_view_stg.module_extracted_params mep ON mts.page_url = mep.url
             --to get the attributed user id for each spv
                  INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON mts.event_hash = mt.event_hash
         WHERE mts.event_tstamp >= '2020-10-06' --hard date when athena went live
           AND mep.url_parameters:utm_medium::VARCHAR = 'email'
           AND TRY_TO_NUMBER(mt.attributed_user_id) IS NOT NULL
         GROUP BY 1, 2, 3, 4
     ),
     join_data AS (
         --attach impressions on sends based on just the subscriber key and send id and infer
         --that all sales within send (that was opened) have an impression
         SELECT sis.deal_id,
                sis.send_id,
                sis.send_date,
                sis.email_name,
                sis.mapped_territory,
                sis.subscriber_key,
                sis.data_source_name,
                sis.sale_name,
                sis.company_name,
                sis.start_date,
                sis.end_date,
                sis.sale_type,
                sis.sale_product,
                sis.destination_type,
                sis.posu_city,
                sis.posu_country,
                sis.posu_division,
                i.open_date,
                i.unque_impressions,
                i.impressions,
                c.unique_clicks,
                c.clicks
         FROM sales_in_send sis
                  LEFT JOIN impressions i ON sis.subscriber_key = i.subscriber_key AND sis.send_id = i.send_id
                  LEFT JOIN clicks c ON sis.subscriber_key = c.subscriber_key AND
                                        TRY_TO_NUMBER(sis.send_id) = TRY_TO_NUMBER(c.send_id) AND
                                        sis.deal_id = c.se_sale_id AND
                                        i.open_date = c.click_date
     )
SELECT jd.deal_id,
       jd.send_id,
       jd.send_date,
       jd.email_name,
       jd.mapped_territory,
       jd.data_source_name,
       jd.sale_name,
       jd.company_name,
       jd.start_date,
       jd.end_date,
       jd.sale_type,
       jd.sale_product,
       jd.destination_type,
       jd.posu_city,
       jd.posu_country,
       jd.posu_division,
       jd.open_date,
       SUM(jd.unque_impressions) AS unque_impressions,
       SUM(jd.impressions)       AS impressions,
       SUM(jd.unique_clicks)     AS unique_clicks,
       SUM(jd.clicks)            AS clicks
FROM join_data jd
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17;
;



USE WAREHOUSE pipe_xlarge;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touched_spvs mts ON mtba.touch_id = mts.touch_id
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_extracted_params mep ON mts.page_url = mep.url
WHERE TRY_TO_NUMBER(mtba.attributed_user_id) IN (27727863, 52275091, 27849683)
  AND mtba.touch_start_tstamp >= '2020-10-06';


--pull data for one user


SELECT ssa.data_model, count(*)
FROM se.data.se_sale_attributes ssa
GROUP BY 1;

SELECT DISTINCT bs.sale_product, bs.sale_type
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.sfmc.athena_send_log CLONE hygiene_vault_mvp.sfmc.athena_send_log;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_sources;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_sources;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.crm_email_segments CLONE data_vault_mvp.dwh.crm_email_segments;


self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2020-10-20 00:00:00' --end '2020-10-20 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__step01__sales_in_send;
SELECT *
FROM hygiene_vault_mvp.sfmc.athena_send_log;

SELECT COUNT(*),
       count(DISTINCT asl.job_id)
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
WHERE asl.log_date >= TIMESTAMPADD('day', -1, '2020-10-19 04:00:00'::TIMESTAMP);

SELECT COUNT(*)                           AS table_rows,
       count(DISTINCT deal_id)            AS sales,
       count(DISTINCT asl.subscriber_key) AS users,
       count(DISTINCT asl.job_id)         AS emails
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
WHERE asl.log_date >= TIMESTAMPADD('day', -1, '2020-10-19 04:00:00'::TIMESTAMP)

SELECT count(*)
FROM snowplow.atomic.events e;
SELECT MIN(e.etl_tstamp)
FROM snowplow.atomic.events e;

USE WAREHOUSE pipe_xlarge;
SELECT count
FROM snowplow.atomic.events e;
-- 11,066,833,084

SELECT count
FROM hygiene_vault_mvp.sfmc.athena_send_log asl;
-- 2,654,181,363;

SELECT asl.log_date::DATE,
       count AS count_rows
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
GROUP BY 1;

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2020-10-21 00:00:00' --end '2020-10-21 00:00:00'


SELECT get_ddl('table', 'data_vault_mvp_dev_robin.dwh.athena_email_reporting__step01__sales_in_send');

CREATE OR REPLACE TRANSIENT TABLE athena_email_reporting__step01__sales_in_send
(
    deal_id          VARCHAR,
    send_id          NUMBER,
    subscriber_key   NUMBER,
    send_date        DATE,
    email_name       VARCHAR,
    mapped_territory VARCHAR,
    data_source_name VARCHAR,
    sale_name        VARCHAR,
    company_name     VARCHAR,
    start_date       DATE,
    end_date         DATE,
    sale_type        VARCHAR,
    sale_product     VARCHAR,
    destination_type VARCHAR,
    posu_city        VARCHAR,
    posu_country     VARCHAR,
    posu_division    VARCHAR
);

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2020-10-22 00:00:00' --end '2020-10-22 00:00:00'


SELECT sis.se_sale_id,
       sis.send_id,
       sis.send_date,
       sis.email_name,
       sis.mapped_territory,
       sis.subscriber_key,
       sis.data_source_name,
       sis.sale_name,
       sis.company_name,
       sis.start_date,
       sis.end_date,
       sis.sale_type,
       sis.sale_product,
       sis.destination_type,
       sis.posu_city,
       sis.posu_country,
       sis.posu_division,
       c.date_value AS date,
       i.unique_impressions,
       i.impressions,
       c.unique_clicks,
       c.clicks
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send sis
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_calendar c
                   ON sis.send_date <= c.date_value AND c.date_value <= TO_DATE(2020 - 10 - 21 04:00:00)
         LEFT JOIN data_vault_mvp_dev_robin.dwh.athena_email_reporting__step01__impressions i
                   ON sis.subscriber_key = i.subscriber_key
                       AND sis.send_id = i.send_id
                       AND c.date_value = i.impression_date
         LEFT JOIN data_vault_mvp_dev_robin.dwh.athena_email_reporting__step02__clicks c
                   ON sis.subscriber_key = c.subscriber_key
                       AND sis.send_id = c.send_id
                       AND sis.se_sale_id = c.se_sale_id
                       AND c.date_value = c.click_date;



SELECT sis.se_sale_id,
       sis.send_id,
       sis.send_date,
       sis.email_name,
       sis.mapped_territory,
       sis.subscriber_key,
       sis.data_source_name,
       sis.sale_name,
       sis.company_name,
       sis.start_date,
       sis.end_date,
       sis.sale_type,
       sis.sale_product,
       sis.destination_type,
       sis.posu_city,
       sis.posu_country,
       sis.posu_division,
       sc.date_value AS date
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send sis
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_calendar sc
                   ON sis.send_date <= sc.date_value AND sc.date_value <= TO_DATE('2020-10-21 04:00:00')
WHERE sis.se_sale_id = 'A6422';

-- CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send_bkup CLONE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send CLONE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send_bkup;
DELETE
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send
WHERE send_id != 1175561;


SELECT ceo.send_id,
       ceo.subscriber_key,
       ces.data_source_name,
       event_tstamp::DATE AS open_date,
       1                  AS unque_impressions,
       count(*)           AS impressions
FROM se.data.crm_events_opens ceo
         LEFT JOIN se.data.crm_email_segments ces ON ceo.email_segment_key = ces.email_segment_key
WHERE ceo.event_date >= '2020-10-06' --hard date when athena went live
GROUP BY 1, 2, 3, 4;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__step03__join_data
WHERE se_sale_id = 'A11211';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__step01__impressions
WHERE athena_email_reporting__step01__impressions.send_id = 1175561;

DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;

SELECT jd.se_sale_id,
       jd.send_id,
       jd.email_name,
       jd.data_source_name,
       jd.mapped_territory,
       jd.event_date,
       jd.unique_impressions,
       jd.impressions,
       jd.unique_clicks,
       jd.clicks
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__step03__join_data jd
WHERE jd.se_sale_id = 'A12499';

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting__step03__join_data;

DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting;

SELECT ceo.event_date::DATE,
       COUNT(*)
FROM se.data.crm_events_opens ceo
WHERE ceo.send_id = 1175375
GROUP BY 1


--content varies from email to email based on the recipient
SELECT DISTINCT
       asl.deal_id         AS se_sale_id,
       asl.job_id          AS send_id,
       asl.log_date::DATE  AS send_date,
       cjl.email_name,
       cjl.mapped_territory,
       js.data_source_name,
       ss.sale_name,
       ss.company_name,
       ss.start_date::DATE AS start_date,
       ss.end_date::DATE   AS end_date,
       ss.sale_type,
       ss.sale_product,
       ss.destination_type,
       ss.posu_city,
       ss.posu_country,
       ss.posu_division
FROM hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list cjl ON asl.job_id = cjl.send_id
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources js ON asl.job_id = js.send_id
    AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ss ON asl.deal_id = ss.se_sale_id
WHERE asl.updated_at >= '2020-10-21';

SELECT MIN(view_date)
FROM se.data.se_hotel_rooms_and_rates_snapshot shrars

SELECT se_sale_id, send_id, data_source_name, event_date, *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting
WHERE athena_email_reporting.se_sale_id = 'A17878'
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, send_id, data_source_name, event_date) > 1;

DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting;

DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;

------------------------------------------------------------------------------------------------------------------------
--add sale id grain to email segments

SELECT *
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
         LEFT JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_list jl ON asl.job_id = jl.send_id
         INNER JOIN hygiene_snapshot_vault_mvp.sfmc.jobs_sources js ON asl.job_id = js.send_id
    AND asl.subscriber_key::VARCHAR = js.subscriber_key::VARCHAR;



SELECT eo.event_tstamp::DATE             AS impression_date,
       eo.send_id,
       es.data_source_name,
       asl.deal_id                       AS se_sale_id,
       COUNT(DISTINCT eo.subscriber_key) AS unique_impressions,
       COUNT(1)                          AS impressions
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
         LEFT JOIN data_vault_mvp.dwh.crm_email_segments es ON eo.send_id = es.send_id AND eo.list_id = es.list_id
         LEFT JOIN hygiene_vault_mvp.sfmc.athena_send_log asl
                   ON eo.subscriber_key = asl.subscriber_key AND eo.send_id = asl.job_id
WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
  AND eo.updated_at >= TO_DATE(current_date) - 7
--AND eo.updated_at >= TO_DATE('{schedule.tstamp}') - {days_sales_in_send} --#TODO replace previous line with this
  AND eo.send_id = 1179749
GROUP BY 1, 2, 3, 4;


SELECT eo.event_tstamp::DATE AS impression_date,
       eo.send_id,
       es.data_source_name,
       eo.subscriber_key,
       asl.deal_id
FROM hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred eo
         LEFT JOIN data_vault_mvp.dwh.crm_email_segments es ON eo.send_id = es.send_id AND eo.list_id = es.list_id
         LEFT JOIN hygiene_vault_mvp.sfmc.athena_send_log asl
                   ON eo.subscriber_key = asl.subscriber_key AND eo.send_id = asl.job_id
WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
  AND eo.updated_at >= TO_DATE(current_date) - 7
--AND eo.updated_at >= TO_DATE('{schedule.tstamp}') - {days_sales_in_send} --#TODO replace previous line with this
  AND eo.send_id = 1179749
  AND eo.subscriber_key = 20638202;


------------------------------------------------------------------------------------------------------------------------

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

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2020-11-26 00:00:00' --end '2020-11-26 00:00:00'

SELECT * FROM data_vault_mvp.dwh.athena_email_reporting aer;

TRUNCATE data_vault_mvp.dwh.athena_email_reporting;
