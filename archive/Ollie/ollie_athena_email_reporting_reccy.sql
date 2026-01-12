SELECT r.send_date,
       r.send_id,
       r.sale_position_group,
       r.se_sale_id,
       r.event_date,
       SUM(r.impressions) AS impressions,
       SUM(r.clicks)      AS clicks
FROM se.data.athena_email_reporting r
WHERE r.send_id = 1201065
  AND r.se_sale_id = 'A17281'
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE TABLE scratch.robinpatel.athena_send_log_1201065_a17281 AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) <= 10
    FROM hygiene_vault_mvp.sfmc.athena_send_log asl
    WHERE asl.job_id = 1201065
      AND asl.deal_id = 'A17281'
);

--dev
SELECT r.send_date,
       r.send_id,
       r.sale_position_group,
       r.se_sale_id,
       r.event_date,
       SUM(r.impressions) AS impressions,
       SUM(r.clicks)      AS clicks
FROM data_vault_mvp_dev_robin.dwh.athena_email_reporting r
WHERE r.send_id = 1201065
  AND r.se_sale_id = 'A22418'
GROUP BY 1, 2, 3, 4, 5;

--prod
SELECT r.send_date,
       r.send_id,
       r.sale_position_group,
       r.se_sale_id,
       r.event_date,
       SUM(r.impressions) AS impressions,
       SUM(r.clicks)      AS clicks
FROM data_vault_mvp.dwh.athena_email_reporting r
WHERE r.send_id = 1201065
  AND r.se_sale_id = 'A22418'
GROUP BY 1, 2, 3, 4, 5;

--to update history

CREATE OR REPLACE TABLE data_vault_mvp.dwh.athena_email_reporting_20210226 CLONE data_vault_mvp.dwh.athena_email_reporting;
TRUNCATE data_vault_mvp.dwh.athena_email_reporting;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting
SELECT aer.schedule_tstamp,
       aer.run_tstamp,
       aer.operation_id,
       aer.created_at,
       aer.updated_at,
       aer.se_sale_id,
       aer.send_id,
       aer.send_date,
       aer.email_name,
       aer.mapped_territory,
       aer.data_source_name,
       'unknown'                   AS sale_position_group,
       aer.sale_name,
       aer.company_name,
       aer.sale_start_date,
       aer.sale_end_date,
       aer.sale_type,
       aer.sale_product,
       aer.destination_type,
       aer.posu_city,
       aer.posu_country,
       aer.posu_division,
       aer.event_date,
       SUM(aer.unique_impressions) AS unique_impressions,
       SUM(aer.impressions)        AS impressions,
       SUM(aer.unique_clicks)      AS unique_clicks,
       SUM(aer.clicks)             AS clicks
FROM data_vault_mvp.dwh.athena_email_reporting_20210226 aer
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23;

CREATE OR REPLACE TABLE data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210226 CLONE data_vault_mvp.dwh.athena_email_reporting__sales_in_send;

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
SELECT MIN(schedule_tstamp) AS schedule_tstamp,
       MIN(run_tstamp)      AS run_tstamp,
       MIN(created_at)      AS created_at,
       MIN(updated_at)      AS updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'inside top 10'      AS sale_position_group,
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
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210226
GROUP BY 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting__sales_in_send
SELECT MIN(schedule_tstamp) AS schedule_tstamp,
       MIN(run_tstamp)      AS run_tstamp,
       MIN(created_at)      AS created_at,
       MIN(updated_at)      AS updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'outside top 10'     AS sale_position_group,
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
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210226
GROUP BY 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21;

INSERT INTO data_vault_mvp.dwh.athena_email_reporting__sales_in_send
SELECT MIN(schedule_tstamp) AS schedule_tstamp,
       MIN(run_tstamp)      AS run_tstamp,
       MIN(created_at)      AS created_at,
       MIN(updated_at)      AS updated_at,
       se_sale_id,
       send_id,
       send_date,
       email_name,
       mapped_territory,
       data_source_name,
       'unknown'            AS sale_position_group,
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
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send_20210226
GROUP BY 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21;

------------------------------------------------------------------------------------------------------------------------


WITH sale_position_grouping AS (
    --model scv sessions based on landing page parameter
    SELECT ts.event_tstamp::DATE                   AS click_date,
           ts.event_hash,
           ep.url_parameters:utm_campaign::VARCHAR AS send_id,
           ep.url_parameters:utm_content::VARCHAR  AS data_source_name,
           ts.se_sale_id,
           tba.attributed_user_id                  AS subscriber_key,
           IFF(ROW_NUMBER() OVER (PARTITION BY asl.job_id, asl.subscriber_key ORDER BY asl.section, asl.position_in_section) <=
               10,
               'inside top 10', 'outside top 10')  AS sale_position_group
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs ts
             --to get the parameters of each url
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params ep ON ts.page_url = ep.url
        --to get the attributed user id for each spv
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba
                        ON ts.touch_id = tba.touch_id
        --to get the position within an email the sale was
             LEFT JOIN hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl ON tba.attributed_user_id = asl.subscriber_key::VARCHAR
        AND ep.url_parameters:utm_campaign::VARCHAR = asl.job_id::VARCHAR
    WHERE ts.event_tstamp >= '2020-10-06' --hard date when athena went live
      AND ts.updated_at >= TO_DATE('2021-02-08 06:00:00') - 7
      AND ep.url_parameters:utm_medium::VARCHAR = 'email'
      AND TRY_TO_NUMBER(tba.attributed_user_id) IS NOT NULL
      AND ts.se_sale_id = 'A22418'
      AND ep.url_parameters:utm_campaign::VARCHAR = '1201065'
)
SELECT spg.click_date,
       spg.send_id,
       spg.data_source_name,
       spg.se_sale_id,
       spg.sale_position_group,
       COUNT(DISTINCT spg.subscriber_key) AS unique_clicks,
       COUNT(DISTINCT spg.event_hash)     AS clicks
FROM sale_position_grouping spg

GROUP BY 1, 2, 3, 4, 5
;


WITH list_of_spvs AS (
    --model scv sessions based on landing page parameter
    SELECT ts.event_tstamp::DATE                  AS click_date,
           ts.event_hash,
           ep.url_parameters:utm_campaign::INT    AS send_id,
           ep.url_parameters:utm_content::VARCHAR AS data_source_name,
           ts.se_sale_id,
           tba.attributed_user_id                 AS subscriber_key
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs ts
             --to get the parameters of each url
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params ep ON ts.page_url = ep.url
        --to get the attributed user id for each spv
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba
                        ON ts.touch_id = tba.touch_id
    WHERE ts.event_tstamp >= '2020-10-06'                       --hard date when athena went live
      AND ts.event_tstamp >= TO_DATE('2021-02-08 06:00:00') - 7 --TODO CHANGE TO incremental
      AND ep.url_parameters:utm_medium::VARCHAR = 'email'
      AND TRY_TO_NUMBER(tba.attributed_user_id) IS NOT NULL
      AND TRY_TO_NUMBER(ep.url_parameters:utm_campaign::VARCHAR) IS NOT NULL
      AND ts.se_sale_id = 'A22418'                              --TODO REMOVE
      AND ep.url_parameters:utm_campaign::VARCHAR = '1201065' --TODO REMOVE
),
     list_position AS (
         --for each spv blow out based on athena send log data to retrieve all the sales sent to
         --a subscriber in one send in order to compute the list position.
         SELECT ls.event_hash,
                asl.deal_id           AS se_sale_id,
                IFF(ROW_NUMBER()
                            OVER (PARTITION BY asl.job_id, asl.subscriber_key, ls.event_hash ORDER BY asl.section, asl.position_in_section) <=
                    10,
                    'inside top 10',
                    'outside top 10') AS sale_position_group
         FROM list_of_spvs ls
                  --join all sales in send to spv to count position
                  INNER JOIN hygiene_vault_mvp_dev_robin.sfmc.athena_send_log asl ON ls.subscriber_key = asl.subscriber_key
             AND ls.send_id = asl.job_id
     )
SELECT los.click_date,
       los.send_id,
       los.data_source_name,
       los.se_sale_id,
       lp.sale_position_group,
       COUNT(DISTINCT los.subscriber_key) AS unique_clicks,
       COUNT(DISTINCT los.event_hash)     AS clicks
FROM list_of_spvs los
         INNER JOIN list_position lp ON los.event_hash = lp.event_hash AND los.se_sale_id = lp.se_sale_id
GROUP BY 1, 2, 3, 4, 5
;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting__step03__join_data atjd
    QUALIFY COUNT(*) OVER (PARTITION BY
        atjd.se_sale_id,
        atjd.send_id,
        atjd.data_source_name,
        atjd.event_date,
        atjd.sale_position_group
        ) > 1
ORDER BY se_sale_id,
         send_id,
         data_source_name,
         event_date,
         sale_position_group;


SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, send_id, data_source_name, sale_position_group) > 1
ORDER BY se_sale_id, send_id, data_source_name, sale_position_group;


SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting__sales_in_send
    QUALIFY ROW_NUMBER() OVER (PARTITION BY se_sale_id, send_id, data_source_name, sale_position_group ORDER BY created_at) > 1;

SELECT *
FROM data_vault_mvp.dwh.athena_email_reporting
    QUALIFY COUNT(*) OVER (PARTITION BY se_sale_id, send_id, data_source_name, sale_position_group, event_date) > 1
ORDER BY se_sale_id, send_id, data_source_name, sale_position_group, event_date;

SELECT *
FROM data_vault_mvp.dwh.se_sale ssa
WHERE ssa.se_sale_id = '112805';


SELECT se.data.se_week(ceo.event_date),
       COUNT(*) AS impressions
FROM se.data.crm_events_opens ceo
WHERE ceo.event_date >= '2021-01-01'
GROUP BY 1;


SELECT c.se_week,
       c.week_start
        ,
       SUM(a.impressions) AS impressions
        ,
       SUM(a.clicks)      AS clicks
FROM se.data.athena_email_reporting a
         JOIN se.data.crm_jobs_list j
              ON j.send_id = a.send_id
         JOIN se.data.se_calendar c
              ON c.date_value = a.send_date
WHERE a.send_date >= DATEADD('month', -1, CURRENT_DATE)
  AND j.mapped_objective = 'CORE'
GROUP BY 1, 2
ORDER BY 1