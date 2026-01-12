


dataset_task --include 'marketing_gsheets.display_cpl_data' --operation ExtractOperation --method 'run'  --start '2022-06-14 00:00:00' --end '2022-06-14 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpl_data' --operation IngestOperation --method 'run'  --start '2022-06-14 00:00:00' --end '2022-06-14 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpl_data' --operation HygieneOperation --method 'run'  --start '2022-06-14 00:00:00' --end '2022-06-14 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpl_data' --operation LatestRecordsOperation --method 'run' --start '2022-06-14 00:00:00' --end '2022-06-14 00:00:00'
dataset_task --include 'marketing_gsheets.display_cpl_data' --operation LatestRecordsOperation --method 'run' --upstream --start '2022-06-14 00:00:00' --end '2022-06-14 00:00:00'

SELECT * FROM raw_vault_dev_robin.marketing_gsheets.display_cpl_data;
SELECT * FROM hygiene_vault_dev_robin.marketing_gsheets.display_cpl_data;
SELECT * FROM latest_vault_dev_robin.marketing_gsheets.display_cpl_data;;
