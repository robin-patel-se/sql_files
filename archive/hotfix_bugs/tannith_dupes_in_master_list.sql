SELECT s.transaction_id,
       s.customer_payment,
       s.unique_transaction_reference,
       s.worldpay_min_event_date,
       s.worldpay_max_event_date,
       s.worldpay_number_events,
       s.worldpay_currency,
       s.worldpay_amount,
       w.order_code,
       w.filename,
       w.event_date,
       w.amount,
       w.status
FROM se.data_pii.master_se_booking_list s
         LEFT JOIN se.data.worldpay_transaction_summary w
                   ON s.unique_transaction_reference = w.order_code
WHERE s.worldpay_amount > 0
  AND w.status LIKE 'REFUND%'
AND w.order_code = '2cc8KYB7rYv-B16LO-VB'
LIMIT 10
;

SELECT * FROM hygiene_snapshot_vault_mvp.worldpay.transaction_summary WHERE order_code = '2cc8KYB7rYv-B16LO-VB'

select TRANSACTION_ID,
       CUSTOMER_PAYMENT,
       UNIQUE_TRANSACTION_REFERENCE,
       WORLDPAY_MIN_EVENT_DATE,
       WORLDPAY_MAX_EVENT_DATE,
       WORLDPAY_NUMBER_EVENTS,
       WORLDPAY_CURRENCY,
       WORLDPAY_AMOUNT,
       W.AMOUNT,
       W.STATUS
from "SE"."DATA_PII"."MASTER_SE_BOOKING_LIST" S
LEFT JOIN "SE"."DATA"."WORLDPAY_TRANSACTION_SUMMARY" W
ON UNIQUE_TRANSACTION_REFERENCE = W.ORDER_CODE
WHERE WORLDPAY_AMOUNT > 0
AND W.STATUS LIKE 'REFUND%'
AND w.order_code = '2cc8KYB7rYv-B16LO-VB'
LIMIT 10
;