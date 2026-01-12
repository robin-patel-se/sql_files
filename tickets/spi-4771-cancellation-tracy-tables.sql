python biapp/bau/manifests/generate_manifest_from_sql_table.py \
    --connector 'travelbird_mysql' \
    --table_names 'contracts_producttermlink' 'contracts_productterms' \
    --mode 'regenerative' \
    --start_date '2024-02-07 00:00:00'

dataset_task --include 'travelbird_mysql.contracts_producttermlink' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-02-07 00:30:00' --end '2024-02-07 00:30:00'
dataset_task --include 'travelbird_mysql.contracts_productterms' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-02-07 00:30:00' --end '2024-02-07 00:30:00'