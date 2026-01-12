SELECT MIN(vsr.transaction_datetime)
FROM hygiene_snapshot_vault_mvp.enett.van_settlement_report vsr;
--WHERE vsr.activity_type = 'Settlement'
-- AND vsr.merch_category_name_1 = 'RYANAIR';
SELECT vsr.ticket_no_1,
       CASE
           WHEN vsr.ticket_no_1 LIKE '% 0' THEN REGEXP_REPLACE(vsr.ticket_no_1, ' *0')
           WHEN REGEXP_COUNT(RIGHT(vsr.ticket_no_1, 6), '[A-Z]') > 1 THEN RIGHT(vsr.ticket_no_1, 6)
           ELSE NULL
           END   AS pnr,
       IFF(vsr.ticket_no_1 NOT LIKE '% 0' AND REGEXP_COUNT(RIGHT(vsr.ticket_no_1, 6), '[A-Z]') = 0, vsr.ticket_no_1,
           NULL) AS ticket_number

FROM raw_vault_mvp.enett.van_settlement_report vsr
WHERE vsr.ticket_no_1 IS NOT NULL;

SELECT REGEXP_REPLACE('VHVHQA       0', ' *0');

CREATE SCHEMA raw_vault_mvp_dev_robin.enett;
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.enett.van_settlement_report CLONE raw_vault_mvp.enett.van_settlement_report;

self_describing_task --include 'staging/hygiene/enett/van_settlement_report.py'  --method 'run' --start '2021-05-04 00:00:00' --end '2021-05-04 00:00:00'
self_describing_task --include 'staging/hygiene_snapshot/enett/van_settlement_report.py'  --method 'run' --start '2021-05-04 00:00:00' --end '2021-05-04 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp.enett.van_settlement_report vsr
WHERE vsr.issued_to_ecn = 410232;

SELECT REGEXP_COUNT(RIGHT('22400000K2Q7NV', 6), '[A-Z]')

SELECT ticket_no_1,
       pnr,
       ticket_number,
       user_reference_1
FROM hygiene_snapshot_vault_mvp_dev_robin.enett.van_settlement_report
WHERE issued_to_ecn = 410232;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.enett_van_settlement_report AS (
    SELECT 'ent_' || SHA2(vsr.remote_filename || vsr.remote_file_row_number)  AS transaction_id,
           vsr.transaction_datetime                                           AS transaction_tstamp,
           'enett'                                                            AS payment_service_provider,
           LOWER(vsr.activity_type)                                           AS payment_service_provider_transaction_type,
           IFF(vsr.transaction_amount < 0, 'money in', 'money out')           AS cashflow_direction,
           IFF(vsr.transaction_amount < 0, 'flight refund', 'flight payment') AS cashflow_type,
           vsr.transaction_amount                                             AS transaction_amount,
           vsr.transaction_currency,
           vsr.van_history_id,
           vsr.issued_to_ecn,
           se.finance.ecn_details(vsr.issued_to_ecn)                          AS ecn_details,
           IFF(ecn_details:travel_trust_ecn = TRUE, TRUE, FALSE)              AS travel_trust_ecn,
           vsr.van_transaction_id,
           vsr.issuer,
           vsr.van,
           vsr.van_curr_1,
           vsr.van_amt_1,
           vsr.max_amt_1,
           vsr.processed,
           vsr.transaction_datetime,
           vsr.approved_datetime,
           vsr.pos_curr,
           vsr.pos_amt,
           vsr.reconciliation_currency,
           vsr.reconciliation_amount,
           vsr.cb_fee_on_van_amount,
           vsr.cb_fee_on_reconciliation_amount,
           vsr.issuing_integrator_code,
           vsr.van_created_by,
           vsr.van_requested_date,
           vsr.van_activation_date,
           vsr.auth_date_1,
           vsr.auth_amt_1,
           vsr.auth_code_1,
           vsr.van_valid_until_date,
           vsr.van_visible_expiry_date,
           vsr.merchant_name,
           vsr.merchant_address,
           vsr.merchant_city,
           vsr.merchant_state,
           vsr.merchant_country,
           vsr.acquirer_id,
           vsr.merchant_id,
           vsr.merch_category_code_1,
           vsr.merch_category_name_1,
           vsr.passenger_name,
           vsr.ticket_no_1,
           CASE
               WHEN vsr.ticket_no_1 LIKE '% 0' THEN REGEXP_REPLACE(vsr.ticket_no_1, ' * 0')
               WHEN REGEXP_COUNT(RIGHT(UPPER(vsr.ticket_no_1), 8), '[A-Z]') > 1
                   THEN IFF(vsr.merch_category_name_1 = 'EASYJET', RIGHT(vsr.ticket_no_1, 7), RIGHT(vsr.ticket_no_1, 6))
               END                                                            AS pnr, --mixed ticket numbers and pnrs, PNR's appear with a series of spaces and a trailing '0'
           IFF(vsr.ticket_no_1 NOT LIKE '% 0'
                   AND REGEXP_COUNT(RIGHT(vsr.ticket_no_1, 6), '[A-Z]') = 0,
               vsr.ticket_no_1, NULL)                                         AS ticket_number,
           vsr.user_reference_1,
           vsr.user_reference_2,
           vsr.user_reference_3,
           vsr.user_reference_4,
           vsr.user_reference_5,
           vsr.user_reference_6,
           vsr.user_reference_7,
           vsr.user_reference_8,
           vsr.user_reference_9,
           vsr.user_reference_10,
           vsr.user_reference_11,
           vsr.user_reference_12,
           vsr.user_reference_13,
           vsr.user_reference_14,
           vsr.user_reference_15,
           vsr.user_reference_16,
           vsr.user_reference_17,
           vsr.user_reference_18,
           vsr.user_reference_19,
           vsr.user_reference_20,
           vsr.integrator_user_reference_1,
           vsr.integrator_user_reference_2,
           vsr.integrator_user_reference_3,
           vsr.integrator_user_reference_4,
           vsr.integrator_user_reference_5,
           vsr.integrator_user_reference_6,
           vsr.integrator_user_reference_7,
           vsr.integrator_user_reference_8,
           vsr.integrator_user_reference_9,
           vsr.integrator_user_reference_10,
           vsr.integrator_user_reference_11,
           vsr.integrator_user_reference_12,
           vsr.integrator_user_reference_13,
           vsr.integrator_user_reference_14,
           vsr.integrator_user_reference_15,
           vsr.integrator_user_reference_16,
           vsr.integrator_user_reference_17,
           vsr.integrator_user_reference_18,
           vsr.integrator_user_reference_19,
           vsr.integrator_user_reference_20,
           vsr.notes
    FROM hygiene_snapshot_vault_mvp.enett.van_settlement_report vsr
);

SELECT *
FROM hygiene_snapshot_vault_mvp.enett.van_settlement_report vsr;

SELECT REGEXP_REPLACE('CE3ZQB0      0', ' *0');


SELECT GET_DDL('table', 'scratch.robinpatel.enett_van_settlement_report');


CREATE OR REPLACE TRANSIENT TABLE enett_van_settlement_report
(
    transaction_id                            VARCHAR(132),
    transaction_tstamp                        TIMESTAMP,
    payment_service_provider                  VARCHAR(5),
    payment_service_provider_transaction_type VARCHAR,
    cashflow_direction                        VARCHAR(9),
    cashflow_type                             VARCHAR(14),
    transaction_amount                        DECIMAL(13, 4),
    transaction_currency                      VARCHAR,
    van_history_id                            NUMBER,
    issued_to_ecn                             NUMBER,
    ecn_details                               OBJECT,
    travel_trust_ecn                          BOOLEAN,
    van_transaction_id                        NUMBER,
    issuer                                    VARCHAR,
    van                                       VARCHAR,
    van_curr_1                                VARCHAR,
    van_amt_1                                 DECIMAL(13, 4),
    max_amt_1                                 DECIMAL(13, 4),
    processed                                 VARCHAR,
    transaction_datetime                      TIMESTAMP,
    approved_datetime                         TIMESTAMP,
    pos_curr                                  VARCHAR,
    pos_amt                                   DECIMAL(13, 4),
    reconciliation_currency                   VARCHAR,
    reconciliation_amount                     DECIMAL(13, 4),
    cb_fee_on_van_amount                      DECIMAL(13, 4),
    cb_fee_on_reconciliation_amount           DECIMAL(13, 4),
    issuing_integrator_code                   VARCHAR,
    van_created_by                            VARCHAR,
    van_requested_date                        DATE,
    van_activation_date                       DATE,
    auth_date_1                               TIMESTAMP,
    auth_amt_1                                DECIMAL(13, 4),
    auth_code_1                               NUMBER,
    van_valid_until_date                      DATE,
    van_visible_expiry_date                   VARCHAR,
    merchant_name                             VARCHAR,
    merchant_address                          VARCHAR,
    merchant_city                             VARCHAR,
    merchant_state                            VARCHAR,
    merchant_country                          VARCHAR,
    acquirer_id                               NUMBER,
    merchant_id                               VARCHAR,
    merch_category_code_1                     NUMBER,
    merch_category_name_1                     VARCHAR,
    passenger_name                            VARCHAR,
    ticket_no_1                               VARCHAR,
    pnr                                       VARCHAR,
    ticket_number                             VARCHAR,
    user_reference_1                          VARCHAR,
    user_reference_2                          VARCHAR,
    user_reference_3                          VARCHAR,
    user_reference_4                          VARCHAR,
    user_reference_5                          VARCHAR,
    user_reference_6                          VARCHAR,
    user_reference_7                          VARCHAR,
    user_reference_8                          VARCHAR,
    user_reference_9                          VARCHAR,
    user_reference_10                         VARCHAR,
    user_reference_11                         VARCHAR,
    user_reference_12                         VARCHAR,
    user_reference_13                         VARCHAR,
    user_reference_14                         VARCHAR,
    user_reference_15                         VARCHAR,
    user_reference_16                         VARCHAR,
    user_reference_17                         VARCHAR,
    user_reference_18                         VARCHAR,
    user_reference_19                         VARCHAR,
    user_reference_20                         VARCHAR,
    integrator_user_reference_1               VARCHAR,
    integrator_user_reference_2               VARCHAR,
    integrator_user_reference_3               VARCHAR,
    integrator_user_reference_4               VARCHAR,
    integrator_user_reference_5               VARCHAR,
    integrator_user_reference_6               VARCHAR,
    integrator_user_reference_7               VARCHAR,
    integrator_user_reference_8               VARCHAR,
    integrator_user_reference_9               VARCHAR,
    integrator_user_reference_10              VARCHAR,
    integrator_user_reference_11              VARCHAR,
    integrator_user_reference_12              VARCHAR,
    integrator_user_reference_13              VARCHAR,
    integrator_user_reference_14              VARCHAR,
    integrator_user_reference_15              VARCHAR,
    integrator_user_reference_16              VARCHAR,
    integrator_user_reference_17              VARCHAR,
    integrator_user_reference_18              VARCHAR,
    integrator_user_reference_19              VARCHAR,
    integrator_user_reference_20              VARCHAR,
    notes                                     VARCHAR
);

SELECT *
FROM raw_vault_mvp.svb.svb_statement ss
WHERE ss.description LIKE '%109043-895270-53399877%'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.enett.van_settlement_report CLONE hygiene_snapshot_vault_mvp.enett.van_settlement_report;

self_describing_task --include 'dv/finance/enett/van_settlement_report.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.finance.enett_van_settlement_report
WHERE transaction_id IS NULL;

SELECT '2021-05-09 03:00:00',
       '2021-05-11 12:28:53',
       'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/finance/enett/van_settlement_report.py__20210509T030000__daily_at_03h00',
       CURRENT_TIMESTAMP()::TIMESTAMP,
       CURRENT_TIMESTAMP()::TIMESTAMP,

       batch.transaction_id,
       batch.transaction_tstamp,
       batch.payment_service_provider,
       batch.payment_service_provider_transaction_type,
       batch.cashflow_direction,
       batch.cashflow_type,
       batch.transaction_amount,
       batch.transaction_currency,
       batch.van_history_id,
       batch.issued_to_ecn,
       batch.ecn_details,
       batch.travel_trust_ecn,
       batch.van_transaction_id,
       batch.issuer,
       batch.van,
       batch.van_curr_1,
       batch.van_amt_1,
       batch.max_amt_1,
       batch.processed,
       batch.transaction_datetime,
       batch.approved_datetime,
       batch.pos_curr,
       batch.pos_amt,
       batch.reconciliation_currency,
       batch.reconciliation_amount,
       batch.cb_fee_on_van_amount,
       batch.cb_fee_on_reconciliation_amount,
       batch.issuing_integrator_code,
       batch.van_created_by,
       batch.van_requested_date,
       batch.van_activation_date,
       batch.auth_date_1,
       batch.auth_amt_1,
       batch.auth_code_1,
       batch.van_valid_until_date,
       batch.van_visible_expiry_date,
       batch.merchant_name,
       batch.merchant_address,
       batch.merchant_city,
       batch.merchant_state,
       batch.merchant_country,
       batch.acquirer_id,
       batch.merchant_id,
       batch.merch_category_code_1,
       batch.merch_category_name_1,
       batch.passenger_name,
       batch.ticket_no_1,
       batch.pnr,
       batch.ticket_number,
       batch.user_reference_1,
       batch.user_reference_2,
       batch.user_reference_3,
       batch.user_reference_4,
       batch.user_reference_5,
       batch.user_reference_6,
       batch.user_reference_7,
       batch.user_reference_8,
       batch.user_reference_9,
       batch.user_reference_10,
       batch.user_reference_11,
       batch.user_reference_12,
       batch.user_reference_13,
       batch.user_reference_14,
       batch.user_reference_15,
       batch.user_reference_16,
       batch.user_reference_17,
       batch.user_reference_18,
       batch.user_reference_19,
       batch.user_reference_20,
       batch.integrator_user_reference_1,
       batch.integrator_user_reference_2,
       batch.integrator_user_reference_3,
       batch.integrator_user_reference_4,
       batch.integrator_user_reference_5,
       batch.integrator_user_reference_6,
       batch.integrator_user_reference_7,
       batch.integrator_user_reference_8,
       batch.integrator_user_reference_9,
       batch.integrator_user_reference_10,
       batch.integrator_user_reference_11,
       batch.integrator_user_reference_12,
       batch.integrator_user_reference_13,
       batch.integrator_user_reference_14,
       batch.integrator_user_reference_15,
       batch.integrator_user_reference_16,
       batch.integrator_user_reference_17,
       batch.integrator_user_reference_18,
       batch.integrator_user_reference_19,
       batch.integrator_user_reference_20,
       batch.notes,
       batch.remote_filename,
       batch.remote_file_row_number

FROM data_vault_mvp_dev_robin.finance.enett_van_settlement_report__step01__model_data AS batch
WHERE batch.transaction_id IS NULL;


SELECT *
FROM hygiene_snapshot_vault_mvp.enett.van_settlement_report vsr
WHERE vsr.remote_file_row_number IS NULL;

SELECT *
FROM data_vault_mvp_dev_robin.finance.enett_van_settlement_report;
airflow backfill --start_date '2021-05-10 09:00:00' --end_date '2021-05-10 09:00:00' --task_regex '.*' dv__finance__enett__van_settlement_report_netsuite__daily_at_09h00

SELECT *
FROM se.data.enett_van_settlement_report evsr
         self_describing_task --include 'se/finance/cash_flow/enett_van_settlement_report.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'

SELECT *
FROM data_vault_mvp.finance.enett__van_settlement_report evsr

SELECT GET_DDL('table', 'data_vault_mvp.finance.enett__van_settlement_report');

self_describing_task --include 'dv/finance/enett/van_settlement_report.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'
self_describing_task --include 'dv/finance/aviate/transactions.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.finance.enett_van_settlement_report__step04__add_tb_data
WHERE LEFT(tb_order_id, 3) = 'TB-';

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog CLONE data_vault_mvp.dwh.tb_order_item_changelog;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

SELECT * FROm data_vault_mvp_dev_robin.finance.enett_van_settlement_report__step01__most_recent_order_item;

SELECT * FROM data_vault_mvp_dev_robin.finance.enett_van_settlement_report__step02__flatten_multple_pnrs;

SELECT * FROm data_vault_mvp_dev_robin.finance.enett_van_settlement_report WHERE tb_order_id LIKE 'TB-%'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.aviate.tig_transaction_report clone hygiene_snapshot_vault_mvp.aviate.tig_transaction_report;

self_describing_task --include 'dv/finance/aviate/transactions.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'
self_describing_task --include 'se/finance/cash_flow/enett_van_settlement_report.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'

self_describing_task --include '/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/dv/finance/aviate/transactions.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'
self_describing_task --include 'dv/finance/svb/manual_refund.py'  --method 'run' --start '2021-05-10 00:00:00' --end '2021-05-10 00:00:00'

airflow backfill --start_date '2021-05-10 03:00:00' --end_date '2021-05-10 03:00:00' --task_regex '.*' dwh__cash_flow__enett_van_settlement_report__daily_at_03h00