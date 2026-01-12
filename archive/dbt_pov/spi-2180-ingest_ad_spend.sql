biapp/
manifests/
incoming/
marketing_gsheets/
facebook_ads_affiliate_mapping.json

dataset_task --include 'marketing_gsheets.facebook_ads_affiliate_mapping' --operation ExtractOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.facebook_ads_affiliate_mapping' --operation IngestOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.facebook_ads_affiliate_mapping' --operation HygieneOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.facebook_ads_affiliate_mapping' --operation LatestRecordsOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'



ExtractOperation


SELECT *
FROM latest_vault_dev_robin.marketing_gsheets.facebook_ads_affiliate_mapping;

SELECT *
FROM hygiene_vault_dev_robin.marketing_gsheets.facebook_ads_affiliate_mapping
    QUALIFY COUNT(*) OVER (PARTITION BY affiliate_id, account_id) > 1;


DROP TABLE raw_vault_dev_robin.marketing_gsheets.facebook_ads_affiliate_mapping;
DROP TABLE hygiene_vault_dev_robin.marketing_gsheets.facebook_ads_affiliate_mapping;
DROP TABLE latest_vault_dev_robin.marketing_gsheets.facebook_ads_affiliate_mapping;


dataset_task --include 'marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping' --operation ExtractOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping' --operation IngestOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping' --operation HygieneOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping' --operation LatestRecordsOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'


SELECT *
FROM latest_vault_dev_robin.marketing_gsheets.google_ads_and_bing_ads_affiliate_mapping;


dataset_task --include 'marketing_gsheets.goals' --operation ExtractOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.goals' --operation IngestOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.goals' --operation HygieneOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.goals' --operation LatestRecordsOperation --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'
dataset_task --include 'marketing_gsheets.goals' --operation LatestRecordsOperation --upstream --method 'run' --start '2022-05-09 00:00:00' --end '2022-05-09 00:00:00'

SELECT *
FROM latest_vault_dev_robin.marketing_gsheets.goals;

SELECT
    month_start,
    TRY_TO_DATE(month_start, 'dd/MM/yyyy') AS month_start
FROM raw_vault_dev_robin.marketing_gsheets.goals;


DROP TABLE raw_vault_dev_robin.marketing_gsheets.goals;
DROP TABLE hygiene_vault_dev_robin.marketing_gsheets.goals;
DROP TABLE latest_vault_dev_robin.marketing_gsheets.goals;


SELECT * FROM raw_vault_dev_robin.marketing_gsheets.goals;
SELECT * FROM hygiene_vault_dev_robin.marketing_gsheets.goals;
SELECT * FROM latest_vault.marketing_gsheets.goals;



SELECT * FROM data_vault_mvp.bi.ppc_performance;
SELECT * FROM data_vault_mvp.bi.ppc_leads;
SELECT * FROM data_vault_mvp.bi.ppc_performance_and_goals;