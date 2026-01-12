SELECT dataset_name,
       dataset_source,
       schedule_interval,
       schedule_tstamp,
       run_tstamp,
       loaded_at,
       filename,
       file_row_number,
       administration_code,
       merchant_code,
       order_code,
       event_date,
       payment_method,
       status,
       currency_code,
       amount,
       commission,
       batch_id,
       refusal_reason,
       extract_metadata
FROM raw_vault_mvp.worldpay.transaction_summary;

SELECT order_code,
       currency_code,
       MIN(event_date::DATE)                                      AS min_event_date,
       MAX(event_date::DATE)                                      AS max_event_date,
       COUNT(event_date)                                          AS number_events,
       --negatve because mix match of data
       sum(CASE WHEN amount < 0 THEN amount * -1 ELSE amount END) AS amount
FROM raw_vault_mvp.worldpay.transaction_summary
WHERE lower(status) IN ('refunded', 'refunded_by_merchant') --means we've given money back to customer
GROUP BY 1, 2;

CREATE TABLE worldpay.transaction_summary
(
    dataset_name        VARCHAR,
    dataset_source      VARCHAR,
    schedule_interval   VARCHAR,
    schedule_tstamp     TIMESTAMP,
    run_tstamp          TIMESTAMP,
    loaded_at           TIMESTAMP,
    filename            VARCHAR,
    file_row_number     NUMBER,
    administration_code VARCHAR,
    merchant_code       VARCHAR,
    order_code          VARCHAR,
    event_date          TIMESTAMP,
    payment_method      VARCHAR,
    status              VARCHAR,
    currency_code       VARCHAR,
    amount              DOUBLE,
    commission          DOUBLE,
    batch_id            NUMBER,
    refusal_reason      VARCHAR,
    PRIMARY KEY (order_code)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

SELECT transaction_summary.extract_metadata
FROM raw_vault_mvp.worldpay.transaction_summary
WHERE transaction_summary.extract_metadata IS NOT NULL;

SELECT extract_metadata:remote_filename AS filename,
       MAX(loaded_at)                   AS loaded_at
FROM raw_vault_mvp.worldpay.transaction_summary
GROUP BY 1;

DROP TABLE hygiene_vault_mvp_dev_robin.worldpay.transaction_summary;
SELECT *
FROM hygiene_vault_mvp_dev_robin.worldpay.transaction_summary ts;

SELECT COUNT(*)
FROM raw_vault_mvp_dev_robin.worldpay.transaction_summary ts;
SELECT COUNT(*)
FROM hygiene_vault_mvp_dev_robin.worldpay.transaction_summary ts;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.worldpay.transaction_summary CLONE raw_vault_mvp.worldpay.transaction_summary;

self_describing_task --include 'staging/hygiene/worldpay/transaction_summary'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE SCHEMA raw_vault_mvp_dev_robin.worldpay;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.worldpay.transaction_summary CLONE raw_vault_mvp.worldpay.transaction_summary;

self_describing_task --include 'staging/hygiene_snapshots/worldpay/transaction_summary'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.worldpay.transaction_summary;

SELECT wp.*
FROM raw_vault_mvp.worldpay.transaction_summary wp
         INNER JOIN (
    SELECT extract_metadata:remote_filename AS filename,
           MAX(loaded_at)                   AS loaded_at
    FROM raw_vault_mvp.worldpay.transaction_summary
    GROUP BY 1
) mx ON mx.filename = wp.extract_metadata:remote_filename
    AND mx.loaded_at = wp.loaded_at;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.worldpay.transaction_summary;

CREATE TABLE worldpay.transaction_summary
(
    dataset_name        VARCHAR      NOT NULL,
    dataset_source      VARCHAR      NOT NULL,
    schedule_interval   VARCHAR      NOT NULL,
    schedule_tstamp     TIMESTAMPNTZ NOT NULL,
    run_tstamp          TIMESTAMPNTZ NOT NULL,
    loaded_at           TIMESTAMPNTZ NOT NULL,
    filename            VARCHAR      NOT NULL,
    file_row_number     NUMBER       NOT NULL,
    administration_code VARCHAR,
    merchant_code       VARCHAR,
    order_code          VARCHAR,
    event_date          TIMESTAMPNTZ,
    payment_method      VARCHAR,
    status              VARCHAR,
    currency_code       VARCHAR,
    amount              DOUBLE,
    commission          DOUBLE,
    batch_id            NUMBER,
    refusal_reason      VARCHAR,
    extract_metadata    VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));


create transient table WORLDPAY.TRANSACTION_SUMMARY
(
    schedule_tstamp     VARCHAR,
    run_tstamp          VARCHAR,
    operation_id        VARCHAR,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,

    filename            VARCHAR,
    file_row_number     NUMBER,
    administration_code VARCHAR,
    merchant_code       VARCHAR,
    order_code          VARCHAR,
    event_date          TIMESTAMP,
    payment_method      VARCHAR,
    status              VARCHAR,
    currency_code       VARCHAR,
    amount              DOUBLE,
    commission          DOUBLE,
    batch_id            NUMBER,
    refusal_reason      VARCHAR
);
SELECT MIN(loaded_at) FROM raw_vault_mvp.worldpay.transaction_summary ts; --2020-05-29 11:25:04.541894000

airflow backfill --start_date '2020-05-29 09:30:00' --end_date '2020-05-29 09:30:00' --task_regex '.*' hygiene_snapshots__worldpay__transaction_summary_snapshot__daily_at_09h30
airflow backfill --start_date '2020-06-22 09:30:00' --end_date '2020-06-28 09:30:00' --task_regex '.*' -m hygiene_snapshots__worldpay__transaction_summary_snapshot__daily_at_09h30

SELECT COUNT(*) FROM raw_vault_mvp.worldpay.transaction_summary ts;
SELECT COUNT(*) FROM hygiene_vault_mvp.worldpay.transaction_summary;
SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.worldpay.transaction_summary;