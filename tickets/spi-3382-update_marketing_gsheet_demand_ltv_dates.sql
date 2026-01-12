dataset_task --include 'marketing_gsheets.demand_ltvs' --operation LatestRecordsOperation --method 'run' --upstream --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'

DROP SCHEMA raw_vault_dev_robin.marketing_gsheets;
DROP SCHEMA hygiene_vault_dev_robin.marketing_gsheets;
DROP SCHEMA latest_vault_dev_robin.marketing_gsheets;

SELECT *
FROM latest_vault_dev_robin.marketing_gsheets.demand_ltvs;



SELECT *
FROM latest_vault.marketing_gsheets.demand_ltvs;


SELECT *
FROM se.data.se_sale_tags sst
WHERE sst.tag_name = 'zz_flash';

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active;

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.is_server_side_event = FALSE
  AND es.email_address IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 10;



            LEFT JOIN {target_table_ref}__step02__booking_metrics b on mdpu.se_sale_id = b.se_sale_id AND mdpu.date = b.date AND mdpu.posa_territory = b.posa_territory
            LEFT JOIN {target_table_ref}__step02__booking_metrics b ON mdpu.se_sale_id = b.se_sale_id AND mdpu.date = b.date AND mdpu.posa_territory = b.posa_territory
