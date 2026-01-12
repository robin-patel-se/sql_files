SELECT mr.dataset_name,
       mr.dataset_source,
       mr.schedule_interval,
       mr.schedule_tstamp,
       mr.run_tstamp,
       mr.loaded_at,
       mr.filename,
       mr.file_row_number,
       mr.full_cms_transaction_id,
       REGEXP_SUBSTR(mr.full_cms_transaction_id, '.*-(.*)', 1, 1, 'e') AS last_portion_of_transaction_ref,
       CASE
           WHEN
               LOWER(mr.product_type) = 'catalogue' THEN 'TB-' || last_portion_of_transaction_ref
           WHEN LEFT(mr.full_cms_transaction_id, 1) = 'A' THEN 'A' || last_portion_of_transaction_ref
           ELSE last_portion_of_transaction_ref
           END                                                         AS booking_id,
       mr.refund_timestamp,
       mr.email_address,
       mr.payment_status,
       mr.customer_currency,
       mr.amount_in_customer_currency,
       mr.beneficiary_name,
       mr.bank_details_type,
       mr.product_type,
       mr.full_cms_transaction_id,
       mr.type_of_refund,
       mr.reference_transaction_id,
       mr.refund_speed,
       mr.duplicate,
       mr.cb_raised,
       mr.fraud_team_comment,
       mr.extract_metadata
FROM raw_vault_mvp.finance_gsheets.manual_refunds mr;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.manual_refunds CLONE raw_vault_mvp.finance_gsheets.manual_refunds;

CREATE TABLE finance_gsheets.manual_refunds
(
    dataset_name                VARCHAR,
    dataset_source              VARCHAR,
    schedule_interval           VARCHAR,
    schedule_tstamp             TIMESTAMP,
    run_tstamp                  TIMESTAMP,
    loaded_at                   TIMESTAMP,
    filename                    VARCHAR,
    file_row_number             NUMBER,
    refund_timestamp            VARCHAR,
    email_address               VARCHAR,
    payment_status              VARCHAR,
    customer_currency           VARCHAR,
    amount_in_customer_currency VARCHAR,
    beneficiary_name            VARCHAR,
    bank_details_type           VARCHAR,
    product_type                VARCHAR,
    full_cms_transaction_id     VARCHAR,
    type_of_refund              VARCHAR,
    reference_transaction_id    VARCHAR,
    refund_speed                VARCHAR,
    duplicate                   VARCHAR,
    cb_raised                   VARCHAR,
    fraud_team_comment          VARCHAR,
    extract_metadata            VARIANT,
    PRIMARY KEY (dataset_name, dataset_source, schedule_interval, schedule_tstamp, run_tstamp, filename, file_row_number)
)
    CLUSTER BY (TO_DATE(schedule_tstamp));

self_describing_task --include 'staging/hygiene/finance_gsheets/manual_refunds'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

DROP TABLE hygiene_vault_mvp_dev_robin.finance_gsheets.manual_refunds;

self_describing_task --include 'staging/hygiene_snapshots/finance_gsheets/manual_refunds'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.manual_refunds mr
WHERE mr.unique_transaction_id IN (
    SELECT mr.unique_transaction_id
    FROM hygiene_snapshot_vault_mvp_dev_robin.finance_gsheets.manual_refunds mr
    GROUP BY 1
    HAVING count(*) > 1
);

