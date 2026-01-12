CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls CLONE data_vault_mvp.single_customer_view_stg.module_unique_urls;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params CLONE data_vault_mvp.single_customer_view_stg.module_url_params;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_extracted_params mep
WHERE mep.created_at >= '2020-10-19';

self_describing_task --include 'dv/dwh/events/01_url_manipulation/03_module_extracted_params.py'  --method 'run' --start '2020-10-19 00:00:00' --end '2020-10-19 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params;

USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params CLONE data_vault_mvp.single_customer_view_stg.module_extracted_params;
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params target
SET target.utm_source = batch.url_parameters:utm_source::VARCHAR
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params batch
WHERE target.url = batch.url
  AND target.updated_at > '2020-10-12 06:53:54.905000000'
;

SELECT COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_extracted_params mtur;
SELECT COUNT(*) FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params mtur;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params mep
WHERE mep.created_at > '2020-10-12 06:53:54.905000000'
  AND mep.utm_medium = 'email'
ORDER BY created_at;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer;
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer target
SET target.utm_source = batch.landing_page_parameters:utm_source::VARCHAR
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer batch
WHERE target.touch_id = batch.touch_id
  AND target.updated_at > '2020-10-12 06:53:54.905000000'
;
SELECT COUNT(*) FROM data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer mtur;
SELECT COUNT(*) FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer mtur;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes clone data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification clone data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot clone data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.affiliate_snapshot clone data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.affiliate clone raw_vault_mvp.cms_mysql.affiliate;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel clone data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

self_describing_task --include 'dv/dwh/events/05_touch_channelling/02_module_touch_marketing_channel.py'  --method 'run' --start '2020-10-12 00:00:00' --end '2020-10-12 00:00:00';

--live
SELECT mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp >= '2020-10-12'
GROUP BY 1;

--prod
SELECT mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp >= '2020-10-12'
GROUP BY 1;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution clone data_vault_mvp.single_customer_view_stg.module_touch_attribution;



--live att
SELECT mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta ON mtba.touch_id = mta.touch_id AND attribution_model = 'last non direct'
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp >= '2020-10-12'
GROUP BY 1;

--prod att
SELECT mtmc.touch_mkt_channel,
       count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta ON mtba.touch_id = mta.touch_id AND attribution_model = 'last non direct'
INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp >= '2020-10-12'
GROUP BY 1;


self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2020-10-12 00:00:00' --end '2020-10-12 00:00:00'


