CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname CLONE data_vault_mvp.single_customer_view_stg.module_url_hostname;



SELECT url,
       parameter_index,
       CASE
           WHEN parameter = 'utm_campaign'
               THEN parameter_value END AS utm_campaign,
       CASE
           WHEN parameter = 'utm_medium'
               THEN parameter_value END AS utm_medium,
       CASE
           WHEN parameter = 'utm_source'
               THEN parameter_value END AS utm_source,
       CASE
           WHEN parameter = 'utm_term'
               THEN parameter_value END AS utm_term,
       CASE
           WHEN parameter = 'utm_content'
               THEN parameter_value END AS utm_content,
       CASE
           WHEN parameter IN ('gclid', 'msclkid', 'dclid', 'clickid', 'fbclid')
               THEN parameter_value END AS click_id,
       CASE
           WHEN parameter = 'saff'
               THEN parameter_value END AS sub_affiliate_name,
       CASE
           WHEN parameter = 'fromApp'
               THEN parameter_value END AS from_app,
       CASE
           WHEN parameter = 'Snowplow'
               THEN parameter_value END AS snowplow_id,
       CASE
           WHEN parameter = 'affiliate'
               THEN parameter_value END AS affiliate,
       CASE
           WHEN parameter = 'awcampaignid'
               THEN parameter_value END AS awcampaignid,
       CASE
           WHEN parameter = 'awadgroupid'
               THEN parameter_value END AS awadgroupid,
       CASE
           WHEN parameter = 'accountVerified'
               THEN parameter_value END AS account_verified
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
WHERE module_url_params.schedule_tstamp >= current_date - 2;



SELECT url,
       parameter_index,
       CASE
           WHEN parameter = 'utm_campaign'
               THEN parameter_value END AS utm_campaign,
       CASE
           WHEN parameter = 'utm_medium'
               THEN parameter_value END AS utm_medium,
       CASE
           WHEN parameter = 'utm_source'
               THEN parameter_value END AS utm_source,
       CASE
           WHEN parameter = 'utm_term'
               THEN parameter_value END AS utm_term,
       CASE
           WHEN parameter = 'utm_content'
               THEN parameter_value END AS utm_content,
       CASE
           WHEN parameter IN ('gclid', 'msclkid', 'dclid', 'clickid', 'fbclid')
               THEN parameter_value END AS click_id,
       CASE
           WHEN parameter = 'saff'
               THEN parameter_value END AS sub_affiliate_name,
       CASE
           WHEN parameter = 'fromApp'
               THEN parameter_value END AS from_app,
       CASE
           WHEN parameter = 'Snowplow'
               THEN parameter_value END AS snowplow_id,
       CASE
           WHEN parameter = 'affiliate'
               THEN parameter_value END AS affiliate,
       CASE
           WHEN parameter = 'awcampaignid'
               THEN parameter_value END AS awcampaignid,
       CASE
           WHEN parameter = 'awadgroupid'
               THEN parameter_value END AS awadgroupid,
       CASE
           WHEN parameter = 'accountVerified'
               THEN parameter_value END AS account_verified
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
WHERE module_url_params.schedule_tstamp >= current_date - 2;


--find urls with dupe params
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
WHERE up.schedule_tstamp >= current_date - 2
    QUALIFY COUNT(*) OVER (PARTITION BY up.url, up.parameter) > 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
WHERE up.schedule_tstamp >= current_date - 2
  AND up.url =
      'https://www.secretescapes.de/sale/book-hotel?startDate=2020-10-20&endDate=2020-10-24&rooms=1&offerId=12530&saleId=11395&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=6&singleResult=false&gce_perbfee=&rateCodes=SLH&rateCodes=SLH&rateCodes=SLH&rateCodes=SLH&staffBooking=false'
    QUALIFY COUNT(*) OVER (PARTITION BY up.url, up.parameter) > 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
WHERE up.schedule_tstamp >= current_date - 2
  AND up.url =
      'https://www.secretescapes.de/sale/book-hotel?startDate=2020-10-20&endDate=2020-10-24&rooms=1&offerId=12530&saleId=11395&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=6&singleResult=false&gce_perbfee=&rateCodes=SLH&rateCodes=SLH&rateCodes=SLH&rateCodes=SLH&staffBooking=false'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY up.url, up.parameter ORDER BY up.parameter_index DESC) = 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
WHERE up.schedule_tstamp >= current_date - 2
  AND up.url =
      'https://www.secretescapes.de/sale/book-hotel?startDate=2020-10-20&endDate=2020-10-24&rooms=1&offerId=12530&saleId=11395&agentId=&numberOfAdults=2&selectedFlightJsonString=&flightIndex=&numberOfFlightResults=&maxAvailableRooms=6&singleResult=false&gce_perbfee=&rateCodes=SLH&rateCodes=SLH&rateCodes=SLH&rateCodes=SLH&staffBooking=false';


WITH dedupe AS (
    SELECT up.schedule_tstamp,
           up.run_tstamp,
           up.operation_id,
           up.created_at,
           up.updated_at,
           up.url,
           up.parameter_index,
           up.parameter,
           up.parameter_value
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
    WHERE up.schedule_tstamp >= current_date - 3
        QUALIFY ROW_NUMBER() OVER (PARTITION BY up.url, up.parameter ORDER BY up.parameter_index DESC) = 1
),
     aggregate AS (
         SELECT dd.url,
                object_agg(dd.parameter::VARCHAR, dd.parameter_value::VARIANT) AS url_parameters
         FROM dedupe dd
         GROUP BY 1
     )

SELECT a.url,
       a.url_parameters:utm_campaign::VARCHAR    AS utm_campaign,
       a.url_parameters:utm_medium::VARCHAR      AS utm_medium,
       a.url_parameters:tm_source::VARCHAR       AS utm_source,
       a.url_parameters:utm_term::VARCHAR        AS utm_term,
       a.url_parameters:utm_content::VARCHAR     AS utm_content,
       COALESCE(a.url_parameters:gclid::VARCHAR,
                a.url_parameters:msclkid::VARCHAR,
                a.url_parameters:dclid::VARCHAR,
                a.url_parameters:clickid::VARCHAR,
                a.url_parameters:fbclid::VARCHAR
           )                                     AS click_id,
       a.url_parameters:saff::VARCHAR            AS sub_affiliate_name,
       a.url_parameters:fromApp::VARCHAR         AS from_app,
       a.url_parameters:Snowplow::VARCHAR        AS snowplow_id,
       a.url_parameters:affiliate::VARCHAR       AS affiliate,
       a.url_parameters:awcampaignid::VARCHAR    AS awcampaignid,
       a.url_parameters:awadgroupid::VARCHAR     AS awadgroupid,
       a.url_parameters:accountVerified::VARCHAR AS account_verified,
       a.url_parameters
FROM aggregate a;


------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2020-10-08 00:00:00' --end '2020-10-08 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params;

self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2020-10-08 00:00:00' --end '2020-10-08 00:00:00'


SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.promotion_label IS NOT NULL;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
WHERE module_touch_utm_referrer.landing_page_parameters IS NOT NULL;

self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2020-10-08 00:00:00' --end '2020-10-08 00:00:00';

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
WHERE landing_page_parameters IS NOT NULL;

------------------------------------------------------------------------------------------------------------------------
--historic update of extracted_params module
USE WAREHOUSE pipe_2xlarge;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

--to test update
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
WHERE updated_at < current_date - 2;

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
WHERE updated_at < current_date - 2;
--

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
    ADD COLUMN url_parameters OBJECT;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params target
SET target.url_parameters = batch.url_parameters
FROM (
         WITH dedupe AS (
             SELECT up.schedule_tstamp,
                    up.run_tstamp,
                    up.operation_id,
                    up.created_at,
                    up.updated_at,
                    up.url,
                    up.parameter_index,
                    up.parameter,
                    up.parameter_value
             FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
                  --the right most parameter
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY up.url, up.parameter ORDER BY up.parameter_index DESC) = 1
         )
         SELECT dd.url,
                object_agg(dd.parameter::VARCHAR, dd.parameter_value::VARIANT) AS url_parameters
         FROM dedupe dd
         GROUP BY 1
     ) batch
WHERE target.url = batch.url;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params;

CREATE OR REPLACE TABLE scratch.robinpatel.module_extracted_params AS (
    WITH dedupe AS (
        SELECT up.schedule_tstamp,
               up.run_tstamp,
               up.operation_id,
               up.created_at,
               up.updated_at,
               up.url,
               up.parameter_index,
               up.parameter,
               up.parameter_value
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params up
            QUALIFY ROW_NUMBER() OVER (PARTITION BY up.url, up.parameter ORDER BY up.parameter_index DESC) = 1
    ),
         object_agg AS (
             SELECT url,
                    object_agg(dd.parameter::VARCHAR, dd.parameter_value::VARIANT) AS url_parameters
             FROM dedupe dd
             GROUP BY 1
         )
    SELECT m.schedule_tstamp,
           m.run_tstamp,
           m.operation_id,
           m.created_at,
           m.updated_at,
           m.url,
           m.utm_campaign,
           m.utm_medium,
           m.utm_source,
           m.utm_term,
           m.utm_content,
           m.click_id,
           m.sub_affiliate_name,
           m.from_app,
           m.snowplow_id,
           m.affiliate,
           m.awcampaignid,
           m.awadgroupid,
           m.account_verified,
           oa.url_parameters
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params m
             LEFT JOIN object_agg oa ON m.url = oa.url
);

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params clone scratch.robinpatel.module_extracted_params;
SELECT * FROM scratch.robinpatel.module_extracted_params;

------------------------------------------------------------------------------------------------------------------------

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
    ADD COLUMN landing_page_parameters OBJECT;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer target
SET target.landing_page_parameters = batch.url_parameters
FROM (
         SELECT url,
                url_parameters
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
     ) batch
WHERE target.touch_landing_page = batch.url;

SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer WHERE MODULE_TOUCH_UTM_REFERRER.landing_page_parameters IS  NULL;

------------------------------------------------------------------------------------------------------------------------

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
    ADD COLUMN landing_page_parameters OBJECT;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.landing_page_parameters = batch.landing_page_parameters
FROM (
         SELECT touch_id,
                landing_page_parameters
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
     ) batch
WHERE target.touch_id = batch.touch_id;

self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2020-10-08 00:00:00' --end '2020-10-08 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer.py'  --method 'run' --start '2020-10-08 00:00:00' --end '2020-10-08 00:00:00'
self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2020-10-08 00:00:00' --end '2020-10-08 00:00:00'
SELECT * FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params ep WHERE ep.url_parameters IS NOT NULL;

SELECT * FROM data_vault_mvp.single_customer_view_stg.module_extracted_params mep;
SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc WHERE mtmc.landing_page_parameters IS NOT NULL;

SELECT *,
       stmc.landing_page_parameters:
       FROM se.data.scv_touch_marketing_channel stmc;


SELECT PARSE_JSON('{
  "j": "914491",
  "jb": "133",
  "jl_cmpn": "914491",
  "jl_uid": "22365633",
  "l": "13_HTML",
  "mid": "6352156",
  "noPasswordSignIn": "true",
  "sfmc_sub": "25025435",
  "u": "19469949",
  "utm_campaign": "automated_news_es_sat_trdg",
  "utm_content": "914491",
  "utm_medium": "email",
  "utm_source": "newsletter"
}'):mid

SELECT stmc.touch_landing_page,
       stmc.landing_page_parameters:mid
FROM se.data.scv_touch_marketing_channel stmc

