-- donald gatfield
-- 31st May 2022
-- SPI-2245 - add 2x new fields to extracted parameters (messageId + utm_platform)


-- take row-count before: 771416182
SELECT COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_extracted_params;

-- backup in case a rollback is required
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_para_spi_2245_clone
CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;

-- rename the 'old' table into dev schema
ALTER TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params
RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params_rename;

-- create the production table to include new field: iterable_email_send_message_id
CREATE TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params
(
schedule_tstamp                TIMESTAMP,
run_tstamp                     TIMESTAMP,
operation_id                   VARCHAR,
created_at                     TIMESTAMP,
updated_at                     TIMESTAMP,
url                            VARCHAR PRIMARY KEY NOT NULL,
utm_campaign                   VARCHAR,
utm_medium                     VARCHAR,
utm_source                     VARCHAR,
utm_term                       VARCHAR,
utm_content                    VARCHAR,
click_id                       VARCHAR,
sub_affiliate_name             VARCHAR,
from_app                       VARCHAR,
snowplow_id                    VARCHAR,
affiliate                      VARCHAR,
awcampaignid                   VARCHAR,
awadgroupid                    VARCHAR,
account_verified               VARCHAR,
message_id                     VARCHAR,
utm_platform                   VARCHAR,
url_parameters                 OBJECT
)
CLUSTER BY (url)
;

USE WAREHOUSE pipe_xlarge;


INSERT INTO data_vault_mvp.single_customer_view_stg.module_extracted_params
SELECT
schedule_tstamp,
run_tstamp,
operation_id,
created_at,
updated_at,
url,
utm_campaign,
utm_medium,
utm_source,
utm_term,
utm_content,
click_id,
sub_affiliate_name,
from_app,
snowplow_id,
affiliate,
awcampaignid,
awadgroupid,
account_verified,
NULL message_id,
NULL AS utm_platform,
url_parameters
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params_rename
;

-- ensure row-count matches the initial row-count [ X ]
SELECT COUNT(*) FROM  data_vault_mvp.single_customer_view_stg.module_extracted_params;

-- mop-up a few days worth of message_ids (if any)
-- we do this here as the module DOES NOT UPDATE
MERGE INTO data_vault_mvp.single_customer_view_stg.module_extracted_params AS TARGET
USING (
  WITH dedupe AS (
      -- create a distinct list and select the last versions of the utm params in any query
      -- (found cases where there are duplicates)
      SELECT
          up.url,
          up.parameter_index,
          up.parameter,
          up.parameter_value
      FROM data_vault_mvp.single_customer_view_stg.module_url_params up
      WHERE schedule_tstamp >= TIMESTAMPADD('day', -1, '2022-05-25 03:00:00'::TIMESTAMP)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY up.url, up.parameter ORDER BY up.parameter_index DESC) = 1

  ),
  aggregate AS (
      --create url parameters object for future reference
      SELECT
          dd.url,
          object_agg(dd.parameter::VARCHAR, dd.parameter_value::VARIANT) AS url_parameters
      FROM dedupe dd
      GROUP BY 1
  )
  SELECT
      a.url,
      a.url_parameters:utm_campaign::VARCHAR AS utm_campaign,
      a.url_parameters:utm_medium::VARCHAR AS utm_medium,
      a.url_parameters:utm_source::VARCHAR AS utm_source,
      a.url_parameters:utm_term::VARCHAR AS utm_term,
      a.url_parameters:utm_content::VARCHAR AS utm_content,
      COALESCE(
          a.url_parameters:gclid::VARCHAR,
          a.url_parameters:msclkid::VARCHAR,
          a.url_parameters:dclid::VARCHAR,
          a.url_parameters:clickid::VARCHAR,
          a.url_parameters:fbclid::VARCHAR
      ) AS click_id,
      a.url_parameters:saff::VARCHAR AS sub_affiliate_name,
      a.url_parameters:fromApp::VARCHAR AS from_app,
      a.url_parameters:Snowplow::VARCHAR AS snowplow_id,
      a.url_parameters:affiliate::VARCHAR AS affiliate,
      a.url_parameters:awcampaignid::VARCHAR AS awcampaignid,
      a.url_parameters:awadgroupid::VARCHAR AS awadgroupid,
      a.url_parameters:accountVerified::VARCHAR AS account_verified,
      a.url_parameters:messageId::VARCHAR AS message_id,
      a.url_parameters:utm_platform::VARCHAR AS utm_platform,
      a.url_parameters
  FROM aggregate a
) AS batch ON target.url = batch.url
WHEN MATCHED
  THEN UPDATE SET
      target.schedule_tstamp = '2022-05-31 18:33:00',
      target.run_tstamp = '2022-05-31 18:33:55',
      target.operation_id = 'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/01_url_manipulation/03_module_extracted_params.py__20220520T030000__daily_at_03h00',
      target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,
      target.url = batch.url,
      target.utm_campaign = batch.utm_campaign,
      target.utm_medium = batch.utm_medium,
      target.utm_source = batch.utm_source,
      target.utm_term = batch.utm_term,
      target.utm_content = batch.utm_content,
      target.click_id = batch.click_id,
      target.sub_affiliate_name = batch.sub_affiliate_name,
      target.from_app = batch.from_app,
      target.snowplow_id = batch.snowplow_id,
      target.affiliate = batch.affiliate,
      target.awcampaignid = batch.awcampaignid,
      target.awadgroupid = batch.awadgroupid,
      target.account_verified = batch.account_verified,
      target.message_id = batch.message_id,
      target.utm_platform = batch.utm_platform,
      target.url_parameters = batch.url_parameters
WHEN NOT MATCHED
  THEN INSERT VALUES (
           '2022-05-31 18:33:00',
           '2022-05-31 18:33:55',
            'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/01_url_manipulation/03_module_extracted_params.py__20220520T030000__daily_at_03h00',
            CURRENT_TIMESTAMP()::TIMESTAMP,
            CURRENT_TIMESTAMP()::TIMESTAMP,

            batch.url,
            batch.utm_campaign,
            batch.utm_medium,
            batch.utm_source,
            batch.utm_term,
            batch.utm_content,
            batch.click_id,
            batch.sub_affiliate_name,
            batch.from_app,
            batch.snowplow_id,
            batch.affiliate,
            batch.awcampaignid,
            batch.awadgroupid,
            batch.account_verified,
            batch.message_id,
            batch.utm_platform,
            batch.url_parameters
            );

-- check (success == you see some values)
SELECT utm_platform, COUNT(*) FROM   data_vault_mvp.single_customer_view_stg.module_extracted_params GROUP BY 1;

-- check (success == you see some values)
SELECT message_id, COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_extracted_params GROUP BY 1;