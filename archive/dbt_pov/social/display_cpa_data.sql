biapp/
manifests/
incoming/
marketing_gsheets/
display_cpa_data.json

dataset_task --include 'marketing_gsheets.display_cpa_data' --operation ExtractOperation --method 'run'  --start '2022-08-02 00:00:00' --end '2022-08-02 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpa_data' --operation IngestOperation --method 'run'  --start '2022-08-02 00:00:00' --end '2022-08-02 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpa_data' --operation HygieneOperation --method 'run'  --start '2022-08-02 00:00:00' --end '2022-08-02 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpa_data' --operation LatestRecordsOperation --method 'run' --upstream --start '2022-08-02 00:00:00' --end '2022-08-02 00:00:00'

DROP TABLE raw_vault_dev_robin.marketing_gsheets.display_cpa_data;
DROP TABLE hygiene_vault_dev_robin.marketing_gsheets.display_cpa_data;
DROP TABLE latest_vault_dev_robin.marketing_gsheets.display_cpa_data;


SELECT *
FROM latest_vault_dev_robin.marketing_gsheets.display_cpa_data;


SELECT *
FROM latest_vault.performance_marketing.cpl_historical;

biapp/
manifests/
incoming/
performance_marketing/
display_cpl_historical.json



dataset_task --include 'performance_marketing.cpl_historical' --operation ExtractOperation --method 'run'  --start '1970-01-01 00:00:00' --end '1970-01-02 00:00:00'
dataset_task --include 'performance_marketing.cpl_historical' --operation IngestOperation --method 'run'  --start '1970-01-01 00:00:00' --end '1970-01-02 00:00:00'
dataset_task --include 'performance_marketing.cpl_historical' --operation HygieneOperation --method 'run'  --start '1970-01-01 00:00:00' --end '1970-01-02 00:00:00'
dataset_task --include 'performance_marketing.cpl_historical' --operation LatestRecordsOperation --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-02 00:00:00'


CREATE SCHEMA raw_vault_dev_robin.performance_marketing;


SELECT *



FROM snowflake.account_usage.query_history qh  WHERE qh.query_text LIKE '%cpl_historical%';



SELECT * FROM raw_vault_dev_robin.marketing_gsheets.display_cpa_data;
SELECT * FROM hygiene_vault_dev_robin.marketing_gsheets.display_cpa_data;
SELECT * FROm latest_vault_dev_robin.marketing_gsheets.display_cpa_data;