SELECT MIN(cs.loaded_at)
FROM raw_vault_mvp.finance_gsheets.chargebacks_se cs;

SELECT cs.dataset_name,
       cs.dataset_source,
       cs.schedule_interval,
       cs.schedule_tstamp,
       cs.run_tstamp,
       cs.loaded_at,
       cs.filename,
       cs.file_row_number,
       cs.date,
       cs.order_code,
       cs.booking_id,
       cs.payment_method,
       cs.currency,
       cs.payment_amount,
       cs.cb_status,
       cs.extract_metadata
FROM raw_vault_mvp.finance_gsheets.chargebacks_se cs;

CREATE TABLE finance_gsheets.chargebacks_se
(
    dataset_name      VARCHAR,
    dataset_source    VARCHAR,
    schedule_interval VARCHAR,
    schedule_tstamp   TIMESTAMP,
    run_tstamp        TIMESTAMP,
    loaded_at         TIMESTAMP,
    filename          VARCHAR,
    file_row_number   NUMBER,
    date              DATE,
    order_code        VARCHAR,
    booking_id        NUMBER,
    payment_method    VARCHAR,
    currency          VARCHAR,
    payment_amount    DOUBLE,
    cb_status         VARCHAR,
    extract_metadata  VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.chargebacks_se CLONE raw_vault_mvp.finance_gsheets.chargebacks_se;

self_describing_task --include 'staging/hygiene/finance_gsheets/chargebacks_se'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.finance_gsheets.chargebacks_se;

SELECT cs.booking_id
FROM raw_vault_mvp.finance_gsheets.cas cs;
42687194 40000000



self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/chargebacks_se'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.chargebacks_se cs
GROUP BY 1
;