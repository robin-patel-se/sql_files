SELECT cr.credit_id,
       cr.shiro_user_id,
       cr.original_se_booking_id,
       cr.original_se_booking_tech_platform,
       cr.credit_status,
       cr.credit_date_created,
       cr.credit_last_updated,
       cr.credit_type,
       cr.credit_currency,
       cr.credit_amount,
       bk.sale_type,
       bk.sale_id,
       bk_cnx_refund_type,
       msbl.booking_id,
       msbl.transaction_id,
       bk.check_in_date,
       bk.check_out_date,
       bk.currency,
       bk.territory,
       msbl.payment_type,
       msbl.cr_credit_active,
       msbl.cr_credit_deleted,
       msbl.cr_credit_used,
       msbl.cr_credit_used_tb,
       msbl.cr_credit_refunded_cash,
       msbl.m_bacs_refund_timestamp,
       msbl.m_bacs_payment_status,
       msbl.m_bacs_customer_currency,
       msbl.m_bacs_amount_in_customer_currency,
       msbl.cb_se_date,
       msbl.cb_se_currency,
       msbl.cb_se_payment_amount,
       msbl.cb_se_status,
       msbl.worldpay_max_event_date,
       msbl.worldpay_currency,
       msbl.worldpay_amount,
       msbl.ratepay_currency,
       msbl.ratepay_amount,
       msbl.worldpay_canx_amount,
       bk.customer_total_price_cc,
       bk.credits_used_cc,
       msbl.flight_sell_rate,
       msbl.baggage_sell_rate,
       wp.status,
       wp.amount,
       msbl.unique_transaction_reference
FROM se.data.se_credit cr
         LEFT JOIN se.data.master_se_booking_list AS msbl ON cr.original_se_booking_id = msbl.booking_id
         LEFT JOIN se.data.se_booking AS bk ON cr.original_se_booking_id = bk.booking_id
         LEFT JOIN se.data.worldpay_transaction_summary AS wp
                   ON wp.order_code = msbl.unique_transaction_reference
                       AND wp.status LIKE '%REFUNDED%'
WHERE cr.credit_status = 'REFUNDED_CASH'
  AND cr.original_se_booking_tech_platform = 'SECRET_ESCAPES'
  AND cr.credit_last_updated >= '2021-04-01'
  AND cr.credit_last_updated <= '2021-04-30'
ORDER BY original_se_booking_id DESC;


SELECT * FROm se.data.worldpay_transaction_summary wts
WHERE LOWER(status) IN ('refunded', 'refunded_by_merchant', 'cancelled');

SELECT DISTINCT sc.credit_status FROm se.data.se_credit sc;
SELECT * FROm se.data.se_credit sc WHERE sc.credit_status = 'REFUNDED_CASH'