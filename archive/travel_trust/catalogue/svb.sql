SELECT *
FROM hygiene_snapshot_vault_mvp.svb.svb_statement;

SELECT GET_DDL('table', 'hygiene_snapshot_vault_mvp.svb.svb_statement');
;

CREATE OR REPLACE VIEW se.finance.svb_statement AS

SELECT s.remote_filename,
       s.row_file_row_number,
       s.transaction_date,
       s.record_type,
       s.transaction_date__o,
       s.bank_id,
       s.account_number,
       s.account_name,
       s.account_owner,
       s.tran_type_description,
       s.bai_tran_code,
       s.swift_tran_code,
       s.currency,
       s.credit_amount,
       s.debit_amount,
       s.bank_ref_no,
       s.customer_ref_no,
       s.description,
       s.originator_beneficiary,
       s.additional_descriptor_1,
       s.additional_descriptor_2,
       s.additional_descriptor_3,
       s.additional_descriptor_4,
       s.additional_descriptor_5,
       s.additional_descriptor_6,
       s.additional_descriptor_7,
       s.additional_descriptor_8,
       s.additional_descriptor_9,
       s.additional_descriptor_10,
       s.additional_descriptor_11,
       s.additional_descriptor_12,
       s.opening_ledger_balance,
       s.opening_available_balance,
       s.one_day_float,
       s.two_plus_day_float,
       s.closing_ledger_balance,
       s.closing_available_balance
FROM hygiene_snapshot_vault_mvp.svb.svb_statement s;


self_describing_task --include 'se/finance/travel_trust/svb_statement.py'  --method 'run' --start '2021-03-14 00:00:00' --end '2021-03-14 00:00:00'

CREATE SCHEMA hygiene_snapshot_vault_mvp_dev_robin.svb;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.svb.svb_statement CLONE hygiene_snapshot_vault_mvp.svb.svb_statement;

-- svb debit
SELECT 'svb_' || SHA2(ss.remote_filename || ss.remote_file_row_number) AS transaction_id,
       ss.transaction_date::TIMESTAMP                                  AS transaction_tstamp,
       'silicon valley bank'                                           AS payment_service_provider,
       'credit'                                                        AS payment_service_provider_transaction_type,
       'money in'                                                      AS cashflow_direction,

       ss.remote_filename,
       ss.remote_file_row_number,
       ss.record_type,
       ss.bank_id,
       ss.account_number,
       ss.account_name,
       ss.account_owner,
       ss.tran_type_description,
       ss.bai_tran_code,
       ss.swift_tran_code,
       ss.currency,
       ss.debit_amount,
       ss.bank_ref_no,
       ss.customer_ref_no,
       ss.description,
       ss.originator_beneficiary,
       ss.additional_descriptor_1,
       ss.additional_descriptor_2,
       ss.additional_descriptor_3,
       ss.additional_descriptor_4,
       ss.additional_descriptor_5,
       ss.additional_descriptor_6,
       ss.additional_descriptor_7,
       ss.additional_descriptor_8,
       ss.additional_descriptor_9,
       ss.additional_descriptor_10,
       ss.additional_descriptor_11,
       ss.additional_descriptor_12,
       ss.opening_ledger_balance,
       ss.opening_available_balance,
       ss.one_day_float,
       ss.two_plus_day_float,
       ss.closing_ledger_balance,
       ss.closing_available_balance
FROM hygiene_snapshot_vault_mvp.svb.svb_statement ss
WHERE ss.debit_amount IS NOT NULL
  AND ss.credit_amount IS NULL
;



self_describing_task --include 'dv/finance/stripe/stripe_cash_on_booking.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'
self_describing_task --include 'dv/finance/stripe/stripe_chargeback.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'
self_describing_task --include 'dv/finance/stripe/stripe_refund.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'
self_describing_task --include 'se/finance/travel_trust/stripe_cash_on_booking.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'
self_describing_task --include 'se/finance/travel_trust/stripe_chargeback.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'
self_describing_task --include 'se/finance/travel_trust/stripe_refund.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'

DROP TABLE data_vault_mvp.dwh.stripe_cash_on_booking;
DROP TABLE data_vault_mvp.dwh.stripe_refund;
DROP TABLE data_vault_mvp.dwh.stripe_chargeback;
DROP TABLE data_vault_mvp.dwh.tb_order_payment_coupon;


-- svb debit

SELECT 'svb_' || SHA2(ss.remote_filename || ss.remote_file_row_number)                                     AS transaction_id,
       ss.transaction_date::TIMESTAMP                                                                      AS transaction_tstamp,
       'silicon valley bank'                                                                               AS payment_service_provider,
       'credit'                                                                                            AS payment_service_provider_transaction_type,
       'money in'                                                                                          AS cashflow_direction,
       'manual refund'                                                                                     AS cashflow_type,
       ss.debit_amount                                                                                     AS transaction_amount,
       ss.currency                                                                                         AS transaction_currency,
       SPLIT(ss.description, '; '),
       des_elements.value::VARCHAR                                                                         AS refund_element,
       TRY_TO_NUMBER(REGEXP_SUBSTR(des_elements.value::VARCHAR, '[REF: SE REFUND |.*-](\\d+)', 1, 1, 'e')) AS order_id,
       ss.description,
       ss.tran_type_description
FROM hygiene_snapshot_vault_mvp.svb.svb_statement ss,
     LATERAL FLATTEN(INPUT => SPLIT(ss.description, '; '), OUTER => TRUE) des_elements
WHERE ss.debit_amount IS NOT NULL
  AND ss.credit_amount IS NULL
  AND TRY_TO_NUMBER(REGEXP_SUBSTR(des_elements.value::VARCHAR, '[REF: SE REFUND |.*-](\\d+)', 1, 1, 'e')) IS NOT NULL;

--AND se.finance.TRAVEL_TRUST_BOOKING(order_id::INT)-- use this function in view;


WITH lateral_flatten AS (
    SELECT 'svb_' || SHA2(ss.remote_filename || ss.remote_file_row_number)                         AS transaction_id,
           ss.transaction_date::TIMESTAMP                                                          AS transaction_tstamp,
           'silicon valley bank'                                                                   AS payment_service_provider,
           'credit'                                                                                AS payment_service_provider_transaction_type,
           'money in'                                                                              AS cashflow_direction,
           'manual refund'                                                                         AS cashflow_type,
           ss.debit_amount                                                                         AS amount,
           ss.currency,
           COALESCE(REGEXP_SUBSTR(des_elements.value::VARCHAR, 'REF: SEMREF.*\\w*-\\w*-(\\d+)', 1, 1, 'e'),
                    REGEXP_SUBSTR(des_elements.value::VARCHAR, 'REF: SEMREF--?(\\d+)', 1, 1, 'e')) AS order_id,
           des_elements.value::VARCHAR                                                             AS refund_element,
           ss.description,
           SPLIT(ss.description, '; ')                                                             AS description_object,
           ss.tran_type_description
    FROM hygiene_snapshot_vault_mvp_dev_robin.svb.svb_statement ss,
         LATERAL FLATTEN(INPUT => SPLIT(ss.description, '; '), OUTER => TRUE) des_elements
    WHERE ss.debit_amount IS NOT NULL
      AND ss.credit_amount IS NULL
      AND ss.description LIKE '%SEMREF%'
      AND refund_element LIKE 'REF: SEMREF%'
)
SELECT lf.transaction_id,
       lf.transaction_tstamp,
       lf.payment_service_provider,
       lf.payment_service_provider_transaction_type,
       lf.cashflow_direction,
       lf.cashflow_type,
       lf.amount,
       lf.currency,
       COALESCE(tb.booking_id, sb.booking_id) AS booking_id,
       lf.order_id,
       lf.refund_element,
       lf.description,
       lf.description_object,
       lf.tran_type_description
FROM lateral_flatten lf
         LEFT JOIN data_vault_mvp.dwh.tb_booking tb ON lf.order_id = tb.order_id
         LEFT JOIN data_vault_mvp.dwh.se_booking sb ON lf.order_id = sb.booking_id
;
SELECT *
FROM data_vault_mvp_dev_robin.finance.svb_manual_refund svb
    QUALIFY COUNT(*) OVER (PARTITION BY svb.transaction_id) > 1;

SELECT *
FROM data_vault_mvp_dev_robin.finance.svb_manual_refund smr;


SELECT *
FROM hygiene_snapshot_vault_mvp.svb.svb_statement ss;

SELECT *
FROM se.finance.travel_trust_money_in ttmi;
SELECT *
FROM se.finance.travel_trust_booking_components ttbc;

--REF: SE REFUND 109714-898557-53794995
-- REF: SE REFUND 52080127


SELECT REGEXP_SUBSTR('REF: SE REFUND 52080127', 'REF: SE REFUND (\\d+)', 1, 1, 'e');
SELECT REGEXP_SUBSTR('REF: SE REFUND 52080127', 'REF: SE REFUND \\d+-\\d+-(\\d+)|REF: SE REFUND (\\d+)|', 1, 1, 'e');
SELECT REGEXP_SUBSTR('REF: SE REFUND A7400-SED-21890727', 'REF: SE REFUND \\w+-\\w+-(\\d+)', 1, 1, 'e')

SELECT REGEXP_SUBSTR('REF: SE REFUND 105465-878368-51908942', 'REF: SE REFUND (\\w+-\\w+-\\d+)', 1, 1, 'e')

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.svb.svb_statement CLONE hygiene_snapshot_vault_mvp.svb.svb_statement;
self_describing_task --include 'dv/finance/svb/manual_refund.py'  --method 'run' --start '2021-05-03 00:00:00' --end '2021-05-03 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.finance.svb_manual_refund smr
WHERE smr.transaction_tstamp >= '2020-01-01';

SELECT des_elements.value::VARCHAR AS refund_element,
       ss.description,
       *

FROM hygiene_snapshot_vault_mvp.svb.svb_statement ss,

     LATERAL FLATTEN(INPUT => SPLIT(ss.description, '; '), OUTER => TRUE) des_elements
WHERE ss.description LIKE '%SEMREF%'
  AND refund_element LIKE 'REF: SEMREF%';

SELECT REGEXP_SUBSTR('REF: SEMREF--52080710', 'REF: SEMREF--?(\\d+)', 1, 1, 'e')
SELECT REGEXP_SUBSTR('REF: SEMREF-A8188-SED-21890547', 'REF: SEMREF.*\\w*-\\w*-(\\d+)', 1, 1, 'e')
           AS self_describing_task --include 'se/finance/travel_trust/svb_manual_refund.py'  --method 'run' --start '2021-05-03 00:00:00' --end '2021-05-03 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.finance.svb_manual_refund ss
WHERE LEFT(booking_id, 3) = 'TB-';

SELECT *
FROM data_vault_mvp.finance.stripe_cash_on_booking scob;

self_describing_task --include 'se/finance/travel_trust/travel_trust_money_in.py'  --method 'run' --start '2021-05-03 00:00:00' --end '2021-05-03 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.tb_order_payment_coupon CLONE data_vault_mvp.finance.tb_order_payment_coupon;



SELECT send_date, SUM(clicks)
FROM data_vault_mvp.dwh.athena_email_reporting
WHERE se_sale_id = 'A24680'
GROUP BY 1;

SELECT *
FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl
WHERE asl.deal_id = 'A24680';



