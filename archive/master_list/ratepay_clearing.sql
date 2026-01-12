------------------------------------------------------------------------------------------------------------------------

SELECT shopsorder_id,
       currency,
       sum(amount_gross)           AS amount_gross,
       sum(disagio_gross)          AS disagio_gross,
       sum(transactionfee_gross)   AS transactionfee_gross,
       sum(paymentchangefee_gross) AS paymentchangefee_gross
FROM raw_vault_mvp.ratepay.clearing
WHERE lower(entry_type) IN ('5', '6', 'return', 'credit') --refund status
GROUP BY 1, 2;

SELECT clearing.dataset_name,
       clearing.dataset_source,
       clearing.schedule_interval,
       clearing.schedule_tstamp,
       clearing.run_tstamp,
       clearing.loaded_at,
       clearing.filename,
       clearing.file_row_number,
       clearing.uid,
       clearing.order_date,
       clearing.transaction_id,
       clearing.shopsorder_id,
       clearing.reference,
       clearing.entry_type,
       clearing.export_date,
       clearing.amount_gross,
       clearing.disagio_gross,
       clearing.transactionfee_gross,
       clearing.paymentchangefee_gross,
       clearing.currency,
       clearing.customergroup,
       clearing.knowncustomer,
       clearing.product,
       clearing.extract_metadata
FROM raw_vault_mvp.ratepay.clearing;

SELECT *
FROM raw_vault_mvp.ratepay.clearing;

CREATE TABLE ratepay.clearing
(
    dataset_name           VARCHAR,
    dataset_source         VARCHAR,
    schedule_interval      VARCHAR,
    schedule_tstamp        TIMESTAMPNTZ,
    run_tstamp             TIMESTAMPNTZ,
    loaded_at              TIMESTAMPNTZ,
    filename               VARCHAR,
    file_row_number        NUMBER,
    uid                    NUMBER,
    order_date             TIMESTAMPNTZ,
    transaction_id         VARCHAR,
    shopsorder_id          VARCHAR,
    reference              VARCHAR,
    entry_type             VARCHAR,
    export_date            TIMESTAMPNTZ,
    amount_gross           DOUBLE,
    disagio_gross          DOUBLE,
    transactionfee_gross   DOUBLE,
    paymentchangefee_gross DOUBLE,
    currency               VARCHAR,
    customergroup          NUMBER,
    knowncustomer          BOOLEAN,
    product                NUMBER,
    extract_metadata       VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

self_describing_task --include 'staging/hygiene/ratepay/clearing'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE SCHEMA raw_vault_mvp_dev_robin.ratepay;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.ratepay.clearing CLONE raw_vault_mvp.ratepay.clearing;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

DROP TABLE hygiene_vault_mvp_dev_robin.ratepay.clearing;

self_describing_task --include 'staging/hygiene_snapshots/ratepay/clearing'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT shopsorder_id,
       currency,
       sum(amount_gross)           AS amount_gross,
       sum(disagio_gross)          AS disagio_gross,
       sum(transactionfee_gross)   AS transactionfee_gross,
       sum(paymentchangefee_gross) AS paymentchangefee_gross
FROM hygiene_snapshot_vault_mvp_dev_robin.ratepay.clearing
WHERE lower(entry_type) IN ('5', '6', 'return', 'credit') --refund status
GROUP BY 1, 2;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.ratepay.clearing;


------------------------------------------------------------------------------------------------------------------------
--booking/reservation crossover
WITH booking_reservation AS (
    SELECT b.booking_id
    FROM hygiene_snapshot_vault_mvp.cms_mysql.booking b
    UNION ALL
    SELECT REGEXP_REPLACE(r.booking_id, 'A') AS booking_id
    FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r
),
     cross_over_booking_ids AS (
         SELECT br.booking_id,
                count(*)
         FROM booking_reservation br
         GROUP BY 1
         HAVING COUNT(*) > 1
     )
SELECT c.booking_id,
       c.order_date,
       IFF(LEFT(sb.booking_id,1) = 'A', 'ndm', 'odm') as data_model,
       sb.booking_created_date,
       sb.booking_completed_date
FROM hygiene_snapshot_vault_mvp_dev_robin.ratepay.clearing c
LEFT JOIN data_vault_mvp.dwh.se_booking sb ON c.booking_id = REGEXP_REPLACE(sb.booking_id, 'A')
WHERE c.booking_id IN (
    SELECT booking_id
    FROM cross_over_booking_ids
)
ORDER BY c.booking_id, c.reference;




