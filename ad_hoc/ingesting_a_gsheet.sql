-- https://secretescapes.atlassian.net/wiki/spaces/DP/pages/2080145550/Ingest+a+GSheet+Dataset

-- https://secretescapes.atlassian.net/wiki/spaces/DP/pages/2080702549/Consolidated+Ingest+Manifest

-- https://secretescapes.atlassian.net/wiki/spaces/DP/pages/2364801025/Setting+up+the+Data+Pipeline

dataset_task
\
    --include 'incoming.airwallex.balance_history' \
    --kind 'incoming' \
    --operation LatestRecordsOperation \
    --method 'run' \
    --upstream \
    --start '2024-05-08 00:00:00' \
    --end '2024-05-08 00:00:00'

-- https://docs.google.com/spreadsheets/d/176zZB4ygf8Ww7ua1HsGfcxishPC8_ef-M4PrRLKPrjY/edit?gid=0#gid=0;



SELECT *
FROM raw_vault_dev_robin.fpa_gsheets.constant_currency_robin;

SELECT *
FROM hygiene_vault_dev_robin.fpa_gsheets.constant_currency_robin

SELECT *
FROM latest_vault_dev_robin.fpa_gsheets.constant_currency_robin