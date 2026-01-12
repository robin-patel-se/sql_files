dataset_task --include 'fpa_gsheets.posu_categorisation' --operation LatestRecordsOperation --method 'run' --upstream --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'

SELECT *
FROM latest_vault_dev_robin.fpa_gsheets.posu_categorisation;
DROP TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation;


SELECT * FROM se.data.se_offer_attributes soa;