/*
"Key"	VARCHAR
"Cluster"	NUMBER
"Cluster Region"	VARCHAR
"Cluster Sub Region"	VARCHAR
"Month of View Date"	VARCHAR
"Days Start Range"	NUMBER
"Days End Range"	NUMBER
"2019 LID"	NUMBER
"2021 LID"	NUMBER
"2022 LID"	NUMBER
"Overall LID"	NUMBER
*/


dataset_task --include 'cro_gsheets.key_dates_definition' --operation ExtractOperation --method 'run' --upstream --start '2022-07-06 00:30:00' --end '2022-07-06 00:30:00'
dataset_task --include 'cro_gsheets.key_dates_definition' --operation IngestOperation --method 'run' --upstream --start '2022-07-06 00:30:00' --end '2022-07-06 00:30:00'
dataset_task --include 'cro_gsheets.key_dates_definition' --operation HygieneOperation --method 'run' --upstream --start '2022-07-06 00:30:00' --end '2022-07-06 00:30:00'
dataset_task --include 'cro_gsheets.key_dates_definition' --operation LatestRecordsOperation --method 'run' --upstream --start '2022-07-06 00:30:00' --end '2022-07-06 00:30:00'

SELECT * FROM latest_vault_dev_robin.cro_gsheets.key_dates_definition;





