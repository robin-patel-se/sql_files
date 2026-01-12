SELECT *
FROM data_vault_mvp.sfsc_snapshots.opportunity_snapshot os;


SELECT os.dataset_name,
       os.dataset_source,
       os.schedule_interval,
       os.schedule_tstamp,
       os.run_tstamp,
       os.loaded_at,
       os.filename,
       os.file_row_number,
       os.id,
       os.created_date,
       os.last_modified_date,
       os.account_id,
       os.owner_id,
       os.sale_id,
       os.agency_id,
       os.giata_id,
       os.record_type_id,
       os.deal_profile,
       os.deal_category,
       os.allocation_start,
       os.allocation_end,
       os.proposed_start_date,
       os.deal_label_multi,
       os.stage_name,
       os.extract_metadata
FROM se.data.se_sale_attributes ssa
         LEFT JOIN data_vault_mvp.sfsc_snapshots.opportunity_snapshot os ON ssa.salesforce_opportunity_id = LEFT(os.id, 15)
WHERE ssa.sale_active;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.sfsc_snapshots.opportunity_snapshot CLONE data_vault_mvp.sfsc_snapshots.opportunity_snapshot;


self_describing_task --include 'staging/hygiene/snowplow/events'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT ss.sale_name,
       ss.product_configuration,
       ss.product_line,
       ss.product_type,
       ss.salesforce_opportunity_id,
       salesforce_opportunity_id_full,
       salesforce_account_id,
       deal_profile,
       salesforce_proposed_start_date,
       salesforce_deal_label_multi,
       salesforce_stage_name
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.sale_active;

SELECT min(loaded_at)
FROM raw_vault_mvp.sfsc.opportunity o; --2019-10-17 09:01:51.461547000

airflow backfill --start_date '2019-10-17 03:00:00' --end_date '2019-10-17 03:00:00' --task_regex '.*' sfsc_snapshot__daily_at_03h00


SELECT ssa.company_name,
       SUM(sb.margin_gross_of_toms_gbp_constant_currency) AS margin,
       SUM(sb.customer_total_price_gbp_constant_currency) AS customer_total_price,
       COUNT(*)                                           AS bookings
FROM se.data.se_booking sb
         INNER JOIN se.data.se_sale_attributes ssa ON sb.sale_id = ssa.se_sale_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.booking_completed_date >= '2020-01-01'
GROUP BY 1
ORDER BY 4 DESC;

SELECT DISTINCT name, ts.country_name
FROM data_vault_mvp.cms_mysql_snapshots.territory_snapshot ts;



airflow backfill --start_date '2020-10-05 03:00:00' --end_date '2020-10-05 03:00:00' --task_regex '.*' -m cms_mysql_snapshot_wave3__daily_at_03h00