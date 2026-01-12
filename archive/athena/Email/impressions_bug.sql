SELECT event_date,
       SUM(aer.impressions) AS impressions,
       SUM(aer.clicks)      AS clicks
FROM se.data.athena_email_reporting aer
         INNER JOIN se.data.dim_sale s ON s.se_sale_id = aer.se_sale_id
GROUP BY 1
ORDER BY 1 ASC;

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2021-03-15 00:00:00' --end '2021-03-15 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send CLONE data_vault_mvp.dwh.athena_email_reporting__sales_in_send;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin_mvp.sfmc.athena_send_log CLONE hygiene_snapshot_vault_mvp_mvp.sfmc.athena_send_log;


WITH sale_position_grouping AS (
    --compute sale position in group at user level
    SELECT eo.event_tstamp::DATE                  AS impression_date,
           eo.send_id,
           es.data_source_name,
           asl.deal_id                            AS se_sale_id,
           eo.subscriber_key,
           IFF(ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) <=
               10,
               'inside top 10', 'outside top 10') AS sale_position_group
    FROM hygiene_snapshot_vault_mvp_dev_robin_mvp.sfmc.events_opens_plus_inferred eo
             LEFT JOIN data_vault_mvp_dev_robin.dwh.crm_email_segments es ON eo.send_id = es.send_id AND eo.list_id = es.list_id
        --to get the position within an email the sale was
             LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin_mvp.sfmc.athena_send_log asl
                       ON eo.subscriber_key::VARCHAR = asl.subscriber_key::VARCHAR AND eo.send_id = asl.job_id
    WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
)
     --aggregate to remove user grain
SELECT spg.impression_date,
       spg.send_id,
       spg.data_source_name,
       spg.se_sale_id,
       spg.sale_position_group,
       COUNT(DISTINCT spg.subscriber_key) AS unique_impressions,
       COUNT(1)                           AS impressions
FROM sale_position_grouping spg
GROUP BY 1, 2, 3, 4, 5
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin_mvp.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp_mvp.sfmc.events_opens_plus_inferred;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.offer_id = 117011;
SELECT *
FROM se.data.tb_booking tb
WHERE tb.offer_id = 116985;

SELECT *
FROM se.data.tb_offer t
WHERE t.tb_offer_id = 116985;
SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.offers_offer_snapshot oos
WHERE id = 116985

SELECT *
FROM se.data.fact_booking fb
WHERE fb.se_sale_id = 'A24680';


USE WAREHOUSE pipe_xlarge;
SELECT stmc.touch_affiliate_territory, COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-01-01'
  AND se.data.posa_category_from_territory(stmc.touch_affiliate_territory) IS NULL
GROUP BY 1;


SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_id = 'TB-21904505';

SELECT *
FROM se.data.tb_offer t
WHERE t.tb_offer_id = 114979;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

WITH list_of_opens AS (
    --compute sale position in group at user level
    SELECT eo.event_tstamp::DATE AS impression_date,
           eo.event_hash,
           eo.send_id,
           es.data_source_name,
           eo.subscriber_key
    FROM hygiene_snapshot_vault_mvp_mvp.sfmc.events_opens_plus_inferred eo
             LEFT JOIN data_vault_mvp.dwh.crm_email_segments es
                       ON eo.send_id = es.send_id AND eo.list_id = es.list_id
    WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
      AND eo.event_tstamp >= CURRENT_DATE - 1
),
     list_position AS (
         --for each spv blow out based on athena send log data to retrieve all the sales sent to
         --a subscriber in one send in order to compute the list position.
         SELECT lo.event_hash,
                asl.deal_id           AS se_sale_id,
                IFF(ROW_NUMBER()
                            OVER (PARTITION BY asl.job_id, asl.subscriber_key, lo.event_hash
                                ORDER BY asl.section, asl.position_in_section) <= 10, 'inside top 10',
                    'outside top 10') AS sale_position_group
         FROM list_of_opens lo
                  --join all sales in send to spv to count position
                  INNER JOIN hygiene_vault_mvp.sfmc.athena_send_log asl ON lo.subscriber_key = asl.subscriber_key
             AND lo.send_id = asl.job_id
--aggregate to remove user grain
SELECT spg.impression_date,
       spg.send_id,
       spg.data_source_name,
       spg.se_sale_id,
       spg.sale_position_group,
       COUNT(DISTINCT spg.subscriber_key) AS unique_impressions,
       COUNT(1)                           AS impressions
FROM sale_position_grouping spg
GROUP BY 1, 2, 3, 4, 5;



WITH list_of_opens AS (
    --compute sale position in group at user level
    SELECT eo.event_tstamp::DATE      AS impression_date,
           eo.event_hash,
           eo.send_id,
           es.data_source_name,
           eo.subscriber_key::VARCHAR AS subscriber_key
    FROM hygiene_snapshot_vault_mvp_mvp.sfmc.events_opens_plus_inferred eo
             LEFT JOIN data_vault_mvp.dwh.crm_email_segments es
                       ON eo.send_id = es.send_id AND eo.list_id = es.list_id
    WHERE eo.event_date >= '2020-10-06' --hard date when athena went live
      AND eo.event_tstamp >= CURRENT_DATE - 1
),
     list_position AS (
         --for each spv blow out based on athena send log data to retrieve all the sales sent to
         --a subscriber in one send in order to compute the list position.
         SELECT lo.event_hash,
                asl.deal_id                                                AS se_sale_id,
                lo.send_id,
                asl.subscriber_key,
                ROW_NUMBER()
                        OVER (PARTITION BY asl.job_id, asl.subscriber_key, lo.event_hash
                            ORDER BY asl.section, asl.position_in_section) AS index,
                IFF(ROW_NUMBER()
                            OVER (PARTITION BY asl.job_id, asl.subscriber_key, lo.event_hash
                                ORDER BY asl.section, asl.position_in_section) <= 10, 'inside top 10',
                    'outside top 10')                                      AS sale_position_group
         FROM list_of_opens lo
                  --join all sales in send to spv to count position
                  INNER JOIN hygiene_vault_mvp.sfmc.athena_send_log asl ON lo.subscriber_key::VARCHAR = asl.subscriber_key::VARCHAR
             AND lo.send_id = asl.job_id
     )
SELECT lo.impression_date,
       lo.send_id,
       lo.data_source_name,
       lp.se_sale_id,
       lp.sale_position_group,
       COUNT(DISTINCT lo.subscriber_key) AS unique_impressions,
       COUNT(1)                          AS impressions
FROM list_of_opens lo
         INNER JOIN list_position lp ON lo.event_hash = lp.event_hash
WHERE lo.send_id = 1207082
GROUP BY 1, 2, 3, 4, 5;



CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.athena_send_log CLONE hygiene_snapshot_vault_mvp.sfmc.athena_send_log;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_list CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.jobs_sources CLONE hygiene_snapshot_vault_mvp.sfmc.jobs_sources;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.events_opens_plus_inferred CLONE hygiene_snapshot_vault_mvp.sfmc.events_opens_plus_inferred;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.crm_email_segments CLONE data_vault_mvp.dwh.crm_email_segments;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;


SELECT *
FROM se.data.athena_email_reporting aer
WHERE aer.send_id = 1207082;

SELECT aer.event_date,
       SUM(aer.impressions)

FROM se.data.athena_email_reporting aer
GROUP BY 1;



SELECT *
FROM hygiene_vault_mvp.sfmc.athena_send_log asl
WHERE asl.job_id = 1206834
  AND asl.subscriber_key = 22830046;

SELECT COUNT(*)
FROM se.data.crm_events_opens ceo
WHERE ceo.send_id = 1207082;


DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting;
DROP TABLE data_vault_mvp_dev_robin.dwh.athena_email_reporting__sales_in_send;

self_describing_task --include 'dv/dwh/athena/email_reporting.py'  --method 'run' --start '2021-03-18 00:00:00' --end '2021-03-18 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting aer
WHERE aer.send_id = 1207082;



SELECT aer.event_date,
       SUM(aer.impressions),
       SUM(aer.clicks)
FROM se.data.athena_email_reporting aer
GROUP BY 1;


SELECT aer.send_id,
       SUM(aer.impressions),
       SUM(aer.clicks)
FROM se.data.athena_email_reporting aer
WHERE aer.event_date = CURRENT_DATE - 1
GROUP BY 1
ORDER BY 2 DESC;

SELECT *
FROM se.data.athena_email_reporting aer
WHERE aer.send_id = 1207682;

SELECT aer.send_id,
       aer.se_sale_id,
       aer.data_source_name,
       aer.event_date,
       SUM(aer.impressions),
       SUM(aer.clicks)
FROM se.data.athena_email_reporting aer
WHERE aer.event_date = CURRENT_DATE - 1
  AND aer.send_id = 1207682
GROUP BY 1,2,3,4;
