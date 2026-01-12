WITH worldpay AS (
    --aggregate worldpay transactions up to booking
    SELECT order_code,
           LISTAGG(DISTINCT currency_code, ',')                                       AS worldpay_currency,
           MIN(event_date::DATE)                                                      AS worldpay_min_event_date,
           MAX(event_date::DATE)                                                      AS worldpay_max_event_date,
           COUNT(event_date)                                                          AS worldpay_number_events,
           SUM(IFF(LOWER(status) IN ('refunded', 'refunded_by_merchant'), amount, 0)) AS worldpay_amount,
           SUM(IFF(LOWER(status) = 'cancelled', amount, 0))                           AS worldpay_canx_amount,
           LISTAGG(wts.refusal_reason, ', ')                                          AS worldpay_refusal_reason_list,
           LISTAGG(wts.status, ', ')                                                  AS worldpay_status_list
    FROM se.data.worldpay_transaction_summary wts
    WHERE LOWER(status) IN ('refunded', 'refunded_by_merchant', 'cancelled') -- filter worldpay transactions
    GROUP BY 1
),
     ratepay AS (
         --aggregate ratepay clearings up to booking
         SELECT booking_id,
                LISTAGG(DISTINCT currency, ',') AS ratepay_currency, --should be only one type but introducing this incase its not.
                SUM(amount_gross)               AS ratepay_amount,
                SUM(disagio_gross)              AS ratepay_disagio,
                SUM(transactionfee_gross)       AS ratepay_transaction_fee,
                SUM(paymentchangefee_gross)     AS ratepay_payment_change_fee,
                MAX(rc.order_date)              AS ratepay_last_order_date,
                listagg(rc.entry_type, ', ')    AS ratepay_entry_type_list
         FROM se.data.ratepay_clearing rc
         WHERE lower(entry_type) IN ('5', '6', 'return', 'credit') -- filter ratepay clearings
         GROUP BY 1
     ),
     manual_refunds AS (
         --aggregate manual refunds up to booking
         SELECT mr.transaction_id,
                MAX(mr.refund_timestamp)                            AS m_bacs_refund_timestamp,
                LISTAGG(DISTINCT mr.payment_status, ', ')           AS m_bacs_payment_status,
                LISTAGG(DISTINCT mr.customer_currency, ', ')        AS m_bacs_customer_currency,
                SUM(mr.amount_in_customer_currency)                 AS m_bacs_amount_in_customer_currency,
                LISTAGG(DISTINCT mr.bank_details_type, ', ')        AS m_bacs_bank_details_type,
                LISTAGG(DISTINCT mr.product_type, ', ')             AS m_bacs_product_type,

                LISTAGG(DISTINCT mr.type_of_refund, ', ')           AS m_bacs_type_of_refund,
                LISTAGG(DISTINCT mr.reference_transaction_id, ', ') AS m_bacs_reference_transaction_id,
                LISTAGG(DISTINCT mr.refund_speed, ', ')             AS m_bacs_refund_speed,
                LISTAGG(DISTINCT mr.duplicate, ', ')                AS m_bacs_duplicate,
                LISTAGG(DISTINCT mr.cb_raised, ', ')                AS m_bacs_cb_raised,
                LISTAGG(DISTINCT mr.fraud_team_comment, ', ')       AS m_bacs_fraud_team_comment
         FROM se.data.manual_refunds mr
         GROUP BY 1
     )
SELECT sb.booking_id,
       sb.transaction_id,
       sb.unique_transaction_reference,
       sb.booking_status,
       --manual bacs refunds
       mancb.m_bacs_refund_timestamp,
       mancb.m_bacs_payment_status,
       mancb.m_bacs_customer_currency,
       mancb.m_bacs_amount_in_customer_currency,
       mancb.m_bacs_bank_details_type,
       mancb.m_bacs_product_type,
       mancb.m_bacs_type_of_refund,
       mancb.m_bacs_reference_transaction_id,
       mancb.m_bacs_refund_speed,
       mancb.m_bacs_duplicate,
       mancb.m_bacs_cb_raised,
       mancb.m_bacs_fraud_team_comment,

       --se platform chargebacks
       cbse.date           AS cb_se_date,
       cbse.order_code     AS cb_se_order_code,
       cbse.payment_method AS cb_se_payment_method,
       cbse.currency       AS cb_se_currency,
       cbse.payment_amount AS cb_se_payment_amount,
       cbse.cb_status      AS cb_se_status,

       --worldpay refund data
       wp.worldpay_min_event_date,
       wp.worldpay_max_event_date,
       wp.worldpay_number_events,
       wp.worldpay_currency,
       wp.worldpay_amount,
       wp.worldpay_canx_amount,
       wp.worldpay_refusal_reason_list,
       wp.worldpay_status_list,

       --ratepay refund data
       rp.ratepay_currency,
       rp.ratepay_amount,
       rp.ratepay_disagio,
       rp.ratepay_transaction_fee,
       rp.ratepay_payment_change_fee,
       rp.ratepay_last_order_date,
       rp.ratepay_entry_type_list

FROM se.data.se_booking sb
         LEFT JOIN worldpay wp ON sb.unique_transaction_reference = wp.order_code
         LEFT JOIN ratepay rp ON sb.booking_id = rp.booking_id
         LEFT JOIN manual_refunds mancb ON sb.transaction_id = mancb.transaction_id
         LEFT JOIN se.data.se_chargebacks cbse ON sb.booking_id = cbse.booking_id
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED', 'HOLD_BOOKED', 'EXPIRED')
;

------------------------------------------------------------------------------------------------------------------------

SELECT mr.row_file_row_number,
       mr.transaction_id,
       mr.booking_id,
       mr.refund_timestamp,
       mr.payment_status,
       mr.customer_currency,
       mr.amount_in_customer_currency,
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
FROM hygiene_snapshot_vault_mvp.finance_gsheets.manual_refunds mr;


self_describing_task --include 'se/data/manual_refunds.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT cs.booking_id,
       cs.date,
       cs.order_code,
       cs.booking_id__o,
       cs.payment_method,
       cs.currency,
       cs.payment_amount,
       cs.cb_status,
FROM hygiene_snapshot_vault_mvp.finance_gsheets.chargebacks_se cs;

SELECT cc.booking_id,
       cc.cb_date,
       cc.reference,
       cc.ccy,
       cc.amount,
       cc.reason,
       cc.defended_date,
       cc.result
FROM hygiene_snapshot_vault_mvp.finance_gsheets.chargebacks_catalogue cc;

self_describing_task --include 'se/data/se_chargebacks.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/tb_chargebacks.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'