dataset_task --include 'marketing_gsheets.tv_production_data' --operation ExtractOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'
dataset_task --include 'marketing_gsheets.tv_production_data' --operation IngestOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'
dataset_task --include 'marketing_gsheets.tv_production_data' --operation HygieneOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'
dataset_task --include 'marketing_gsheets.tv_production_data' --operation LatestRecordsOperation --method 'run'  --start '2022-06-12 00:00:00' --end '2022-06-13 00:00:00'

SELECT * FROM raw_vault_dev_robin.marketing_gsheets.tv_production_data tpd;
SELECT * FROM latest_vault_dev_robin.marketing_gsheets.tv_production_data tpd;