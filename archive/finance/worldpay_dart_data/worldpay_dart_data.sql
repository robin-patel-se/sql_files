SELECT *
FROM hygiene_snapshot_vault_mvp.worldpay.dart312_transaction_reconciliation_accepted dtra;

SELECT *
FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary ts;

WITH union_data AS (
    --old world reporting system from worldpay (Transaction summary)
    SELECT administration_code,
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
           'Transaction Summary' AS reporting_platform
    FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary ts

    UNION ALL
    --new world reporting system from worldpay (DART)
    SELECT dtra.store_reference      AS administration_code,
           dtra.merchant_id          AS merchant_code,
           dtra.payee_reference_otr  AS order_code,
           dtra.transaction_datetime AS event_date,
           dtra.scheme_reference     AS payment_method,
           CASE
               WHEN LOWER(dtra.transaction_type) = 'purchase' THEN 'AUTHORISED'
               WHEN LOWER(dtra.transaction_type) = 'refund' THEN 'REFUNDED'
               ELSE UPPER(dtra.transaction_type)
               END                   AS status,
           dtra.transaction_currency AS currency_code,
           dtra.transaction_amount   AS amount,
           NULL                      AS commission,
           NULL                      AS batch_id,
           NULL                      AS refusal_reason,
           'DART'                    AS reporting_platform
    FROM hygiene_snapshot_vault_mvp.worldpay.dart312_transaction_reconciliation_accepted dtra
)

SELECT *
FROM union_data
WHERE union_data.order_code = '3c44QFYSJOlDaTFNHuB-';

SELECT DISTINCT status
FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary ts;


SELECT DISTINCT ts.*
FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary ts
         LEFT JOIN hygiene_snapshot_vault_mvp.worldpay.dart312_transaction_reconciliation_accepted dtra
                   ON ts.order_code = dtra.payee_reference_otr
WHERE dtra.payee_reference_otr IS NULL
  AND ts.status = 'AUTHORISED'
  AND ts.event_date >= CURRENT_DATE - 2;

SELECT *
FROM hygiene_snapshot_vault_mvp.worldpay.dart312_transaction_reconciliation_accepted d312tra
WHERE d312tra.payee_reference_otr = '3c68eiiUY099NeZJLrzS';

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.worldpay.transaction_summary CLONE hygiene_snapshot_vault_mvp.worldpay.transaction_summary;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.worldpay.dart312_transaction_reconciliation_accepted CLONE hygiene_snapshot_vault_mvp.worldpay.dart312_transaction_reconciliation_accepted;

self_describing_task --include 'dv/dwh/payment_service_provider/worldpay_transactions.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
