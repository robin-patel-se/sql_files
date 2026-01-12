--multiple chargebacks so sum up to booking
SELECT reference,
       ccy,
       sum(amount) AS amount
FROM raw_vault_mvp.finance_gsheets.chargebacks_catalogue
GROUP BY 1, 2;

CREATE TABLE finance_gsheets.chargebacks_catalogue
(

    cb_date          DATE,
    reference        VARCHAR,
    ccy              VARCHAR,
    amount           DOUBLE,
    reason           VARCHAR,
    defended_date    DATE,
    result           VARCHAR,
    extract_metadata VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.chargebacks_catalogue CLONE raw_vault_mvp.finance_gsheets.chargebacks_catalogue;

SELECT cc.cb_date,
       cc.reference,
       cc.ccy,
       cc.amount,
       cc.reason,
       cc.defended_date,
       cc.result,
       cc.extract_metadata
FROM raw_vault_mvp.finance_gsheets.chargebacks_catalogue cc;

q

SELECT *
FROM hygiene_vault_mvp_dev_robin.finance_gsheets.chargebacks_catalogue;

SELECT *
FROM hygiene_vault_mvp_dev_robin.finance_gsheets.chargebacks_catalogue
WHERE booking_id IN (
    SELECT booking_id
    FROM hygiene_vault_mvp_dev_robin.finance_gsheets.chargebacks_catalogue
    GROUP BY 1
    HAVING count(*) > 1
);

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/chargebacks_catalogue'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.chargebacks_catalogue;
