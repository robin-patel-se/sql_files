DROP SCHEMA collab.supplier_comms;

CREATE OR REPLACE VIEW collab.fpa.supplier_comms_data COPY GRANTS
AS
SELECT bs.record__o['supplier']::VARCHAR AS supplier_name,
       sb.sale_id,
       ss.sale_name,
       ss.product_configuration,
       ss.product_type,
       ss.start_date,
       bs.record__o['country']::VARCHAR  AS posu_country,
       count(*)                          AS trx,
       SUM(IFF(sb.has_flights, 1, 0))    AS trx_with_flights,
       SUM(IFF(sb.has_flights, 0, 1))    AS trx_without_flights
FROM data_vault_mvp.dwh.se_booking sb
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.booking_id = bs.booking_id
WHERE sb.booking_status = 'COMPLETE'
--   AND ss.product_type = 'Hotel' --product in cube
  AND sb.booking_completed_date < '2020-05-01'
  AND sb.check_in_date >= '2020-07-01'
  AND sb.check_out_date <= '2020-10-31'
  AND sb.territory != 'PL'
GROUP BY 1, 2, 3, 4, 5, 6, 7
;

SELECT sum(scd.trx),
       sum(scd.trx_without_flights),
       sum(scd.trx_with_flights)
FROM collab.fpa.supplier_comms_data scd;

SELECT * FROM collab.fpa.supplier_comms_data scd;

SELECT bs.record__o['supplier']::VARCHAR AS supplier_name,
       sb.sale_id,
       ss.sale_name,
       ss.product_configuration,
       ss.product_type,
       ss.start_date,
       bs.record__o['country']::VARCHAR  AS posu_country,
       sb.has_flights
--        count(*)                          AS trx,
--        SUM(IFF(sb.has_flights, 1, 0))    AS trx_with_flights,
--        SUM(IFF(sb.has_flights, 0, 1))    AS trx_without_flights
FROM data_vault_mvp.dwh.se_booking sb
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.booking_id = bs.booking_id
WHERE sb.booking_status = 'COMPLETE'
  AND ss.product_type = 'Hotel' --product in cube
  AND sb.booking_completed_date < '2020-05-01'
  AND sb.check_in_date >= '2020-07-01'
  AND sb.check_out_date <= '2020-10-31'
  AND sb.territory != 'PL'
AND supplier_name = 'Travel Partner GmbH';



SqlColumnList
SELECT scd.supplier_name,
       scd.sale_id,
       scd.sale_name,
       scd.product_configuration,
       scd.product_type,
       scd.start_date,
       scd.posu_country,
       scd.trx,
       scd.trx_with_flights,
       scd.trx_without_flights
FROM collab.fpa.supplier_comms_data scd
ORDER BY trx DESC;



WITH adjusted_dates AS (
    SELECT
        -- a booking can have multiple adjustments but we needs the dates associated with
        -- the most recent one
        -- TODO: if we need this elsewhere too we should define it in a `dv` module
        -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
        COALESCE(a.booking_id::VARCHAR, 'A' || pr.reservation_id)::VARCHAR AS booking_id,
        a.check_in_date::DATE                                              AS check_in_date,
        a.check_out_date::DATE                                             AS check_out_date,
        a.stay_by_date                                                     AS voucher_stay_by_date
    FROM data_vault_mvp.cms_mysql_snapshots.amendment_snapshot a
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot pr
                       ON a.product_reservation_id = pr.id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.booking_id ORDER BY a.date_created DESC) = 1
)

SELECT
--        cs.name                           AS company_name,
bs.record__o['supplier']::VARCHAR AS supplier_name,
sb.sale_id,
ss.sale_name,
ss.product_configuration,
ss.product_type,
ss.start_date,
bs.record__o['country']::VARCHAR  AS posu_country,
count(*)                          AS trx
FROM data_vault_mvp.dwh.se_booking sb
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cs ON ss.company_id = cs.id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.booking_id = bs.booking_id
         LEFT JOIN adjusted_dates aj ON sb.booking_id = aj.booking_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.has_flights = FALSE
  AND ss.product_type = 'Hotel' --product in cube
  AND sb.booking_completed_date < '2020-05-01'
  AND COALESCE(aj.check_in_date, sb.original_check_in_date) >= '2020-07-01'
  AND COALESCE(aj.check_out_date, sb.original_check_out_date) <= '2020-10-31'
  AND sb.territory != 'PL'
GROUP BY 1, 2, 3, 4, 5, 6, 7;



SELECT sum(scd.trx)
FROM collab.fpa.supplier_comms_data scd;

WITH adjusted_dates AS (
    SELECT
        -- a booking can have multiple adjustments but we needs the dates associated with
        -- the most recent one
        -- TODO: if we need this elsewhere too we should define it in a `dv` module
        -- or change `cms_mysql_snapshot_bulk_wave2.booking_adjustment` to deduplicate on `booking_id` instead of `id`
        COALESCE(a.booking_id::VARCHAR, 'A' || pr.reservation_id)::VARCHAR AS booking_id,
        a.check_in_date::DATE                                              AS check_in_date,
        a.check_out_date::DATE                                             AS check_out_date,
        a.stay_by_date                                                     AS voucher_stay_by_date
    FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot a
             LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot pr
                       ON a.product_reservation_id = pr.id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a.booking_id ORDER BY a.date_created DESC) = 1
)
SELECT count(*)
FROM data_vault_mvp.dwh.se_booking sb
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cs ON ss.company_id = cs.id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.booking_id = bs.booking_id
         LEFT JOIN adjusted_dates aj ON sb.booking_id = aj.booking_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.has_flights = FALSE
  AND ss.product_type = 'Hotel' --product in cube
  AND sb.booking_completed_date < '2020-05-01'
  AND COALESCE(aj.check_in_date, sb.original_check_in_date) >= '2020-07-01'
  AND COALESCE(aj.check_out_date, sb.original_check_out_date) <= '2020-10-31'
  AND sb.territory != 'PL'
;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs;

SELECT COUNT(*)
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.booking_completed_date < '2020-05-01'
  AND sb.check_in_date >= '2020-07-01'
  AND sb.check_out_date <= '2020-10-31';

SELECT cs.name                           AS company_name,
       bs.record__o['supplier']::VARCHAR AS supplier_name,
       sb.sale_id,
       ss.sale_name,
       ss.product_configuration,
       ss.product_type,
       ss.start_date,
       ss.posu_country,
       count(*)                          AS trx
FROM data_vault_mvp.dwh.se_booking sb
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cs ON ss.company_id = cs.id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs ON sb.booking_id = bs.booking_id
WHERE sb.booking_status = 'COMPLETE'
  AND sb.has_flights = FALSE
  AND ss.product_type = 'Hotel' --product in cube
  AND sb.booking_completed_date < '2020-05-01'
  AND sb.check_in_date >= '2020-07-01'
  AND sb.check_out_date <= '2020-10-31'
  AND ss.posa_territory != 'PL'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

GRANT SELECT ON VIEW collab.fpa.supplier_comms_data TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON VIEW collab.fpa.supplier_comms_data TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.fpa.supplier_comms_data TO ROLE personal_role__samanthamandeldallal;


DROP TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_ho CLONE raw_vault_mvp.sfsc.rebooking_request_cases_ho;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_request_cases_pkg CLONE raw_vault_mvp.sfsc.rebooking_request_cases_pkg;
self_describing_task --include 'staging/hygiene/sfsc/rebooking_requests'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

self_describing_task --include 'se/data/tb_booking'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

