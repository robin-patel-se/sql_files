SELECT *
FROM data_vault_mvp.dwh.se_booking sb;

SELECT cc.dataset_name,
       cc.dataset_source,
       cc.schedule_interval,
       cc.schedule_tstamp,
       cc.run_tstamp,
       cc.loaded_at,
       cc.filename,
       cc.file_row_number,
       cc.extract_metadata,
       cc.base_currency,
       cc.currency,
       cc.category,
       cc.start_date,
       cc.end_date,
       cc.fx,
       cc.multiplier,
       cc.notes
FROM raw_vault_mvp.fpa_gsheets.constant_currency cc;

CREATE SCHEMA raw_vault_mvp_dev_robin.fpa_gsheets;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE raw_vault_mvp.fpa_gsheets.constant_currency;

CREATE TABLE fpa_gsheets.constant_currency
(
    dataset_name      VARCHAR,
    dataset_source    VARCHAR,
    schedule_interval VARCHAR,
    schedule_tstamp   TIMESTAMP,
    run_tstamp        TIMESTAMP,
    loaded_at         TIMESTAMP,
    filename          VARCHAR,
    file_row_number   NUMBER,
    extract_metadata  VARIANT,
    base_currency     VARCHAR,
    currency          VARCHAR,
    category          VARCHAR,
    start_date        DATE,
    end_date          DATE,
    fx                DOUBLE,
    multiplier        DOUBLE,
    notes             VARCHAR
);

self_describing_task --include 'staging/hygiene/fpa_gsheets/constant_currency.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM hygiene_vault_mvp_dev_robin.fpa_gsheets.constant_currency;

------------------------------------------------------------------------------------------------------------------------

SELECT cc.schedule_tstamp,
       cc.run_tstamp,
       cc.operation_id,
       cc.created_at,
       cc.updated_at,
       cc.row_dataset_name,
       cc.row_dataset_source,
       cc.row_loaded_at,
       cc.row_schedule_tstamp,
       cc.row_run_tstamp,
       cc.row_filename,
       cc.row_file_row_number,
       cc.row_extract_metadata,
       cc.base_currency,
       cc.currency,
       cc.category,
       cc.start_date,
       cc.end_date,
       cc.fx,
       cc.multiplier,
       cc.notes,
       cc.failed_some_validation,
       cc.fails_validation__base_currency__expected_nonnull,
       cc.fails_validation__currency__expected_nonnull,
       cc.fails_validation__start_date__expected_nonnull,
       cc.fails_validation__end_date__expected_nonnull,
       cc.fails_validation__multiplier__expected_nonnull
FROM hygiene_vault_mvp_dev_robin.fpa_gsheets.constant_currency cc
    QUALIFY ROW_NUMBER() OVER (PARTITION BY
        base_currency,
        currency,
        category,
        start_date,
        end_date ORDER BY row_loaded_at DESC, row_file_row_number DESC) = 1;

self_describing_task --include 'staging/hygiene_snapshots/fpa_gsheets/constant_currency.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;


CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.amendment_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_reservation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_reservation_snapshot;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

WITH cube_bookings AS (
    SELECT dbs.transaction_id,
           fbvs.margin,
           fbvs.margin_constant_currency
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_v_snapshot fbvs
             LEFT JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot dbs ON fbvs.key_booking = dbs.key_booking
),
     dwh_vs_cube AS (
         SELECT sb.booking_id,
                sb.transaction_id,
                sb.currency,
                sb.booking_completed_date,
                ssa.sale_name,
                sb.margin_gross_of_toms_gbp                   AS dwh_margin,
                cb.margin                                     AS cube_margin,
                sb.margin_gross_of_toms_gbp_constant_currency AS dwh_margin_constant_currency,
                cb.margin_constant_currency                   AS cube_margin_constant_currency
         FROM data_vault_mvp_dev_robin.dwh.se_booking sb
                  LEFT JOIN se.data.se_sale_attributes ssa ON sb.sale_id = ssa.se_sale_id
                  LEFT JOIN cube_bookings cb ON sb.transaction_id = cb.transaction_id
         WHERE sb.booking_status = 'COMPLETE'
           AND ssa.product_configuration = 'Hotel'
           AND sb.booking_completed_date::DATE < current_date - 1
     )
-- SELECT DATE_TRUNC('month', dvc.booking_completed_date)                    AS month,
--        SUM(dvc.dwh_margin)                                                AS dwh__margin,
--        SUM(dvc.cube_margin)                                               AS cube__margin,
--        dwh__margin - cube__margin                                         AS margin_diff,
--        dwh__margin / cube__margin - 1                                     AS margin_var,
--        SUM(dvc.dwh_margin_constant_currency)                              AS dwh__margin_constant_currency,
--        SUM(dvc.cube_margin_constant_currency)                             AS cube__margin_constant_currency,
--        dwh__margin_constant_currency - cube__margin_constant_currency     AS margin_constant_currency_diff,
--        dwh__margin_constant_currency / cube__margin_constant_currency - 1 AS margin_constant_currency_var
--
-- FROM dwh_vs_cube dvc
-- GROUP BY 1
-- ORDER BY 1

-- SELECT dvc.booking_id,
--        dvc.transaction_id,
--        dvc.currency,
--        dvc.booking_completed_date,
--        dvc.sale_name,
--
--        dvc.dwh_margin,
--        dvc.cube_margin,
--        dvc.dwh_margin - dvc.cube_margin AS margin_diff,
--        dvc.dwh_margin_constant_currency,
--        dvc.cube_margin_constant_currency
-- FROM dwh_vs_cube dvc
-- ORDER BY margin_diff DESC

SELECT dvc.booking_id,
       dvc.transaction_id,
       dvc.currency,
       dvc.booking_completed_date,
       dvc.sale_name,

       dvc.dwh_margin,
       dvc.cube_margin,
       dvc.dwh_margin - dvc.cube_margin AS margin_diff,
       dvc.dwh_margin_constant_currency,
       dvc.cube_margin_constant_currency
FROM dwh_vs_cube dvc
WHERE dvc.booking_completed_date >= '2018-12-01'
  AND dvc.booking_completed_date <= '2019-01-31'
ORDER BY margin_diff DESC
;

SELECT *
FROM data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot
WHERE dim_bookings_snapshot.booking_id = '12470-94509-4077822';


------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.posa_territory LIKE '%SE_TEMP%'
   OR ssa.hotel_code = '001notacompany0'
   OR LOWER(ssa.company_name) LIKE 'test';


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs
WHERE bs.salesforce_opportunity_id = '001notacompany0';

SELECT tb.se_sale_id,
       tb.sold_price_currency,
       tb.sold_price_total_gbp,
       tb.cost_price_total_gbp,
       tb.booking_fee_incl_vat_gbp,
       tb.margin_gbp,
       tb.margin_gbp_constant_currency,
       tb.margin_gbp / NULLIF(tb.margin_gbp_constant_currency, 0) - 1 AS diff

FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
ORDER BY ABS(diff) DESC;

self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SHOW GRANTS TO ROLE personal_role__robinpatel;

self_describing_task --include 'se/data/se_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/tb_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/fact_complete_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00';

WITH fx_dates AS (
    SELECT cc.base_currency,
           sc.date_value,
           cc.currency,
           cc.category,
           cc.start_date,
           cc.end_date,
           cc.fx,
           cc.multiplier
    FROM hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency cc
             LEFT JOIN se.data.se_calendar sc ON cc.start_date <= sc.date_value AND cc.end_date >= sc.date_value
    ORDER BY base_currency, currency, start_date, end_date, date_value
)
SELECT fx_dates.base_currency,
       fx_dates.date_value,
       fx_dates.currency,
       fx_dates.category,
       fx_dates.start_date,
       fx_dates.end_date,
       count(*)
FROM fx_dates
GROUP BY 1, 2, 3, 4, 5, 6
HAVING count(*) > 1
;


SELECT custom FROM data_vault_mvp.dwh.se_booking sb;
SELECT * FROM se.data.tb_offer;