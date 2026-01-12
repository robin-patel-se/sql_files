SELECT c.shiro_user_id,
       c.original_se_booking_id,
       c.credit_id,
       c.credit_date_created::date    AS credit_date_created,
       c.credit_expiration_date::date AS credit_expiry_date,
       c.credit_type,
       c.credit_status,
       c.credit_reason,
       c.credit_currency,
       c.credit_amount,
       b.territory_name,
       b.transaction_id,
       b.customer_email,
       b.payment_amount,
       b.m_bacs_payment_status,
       b.m_bacs_amount_in_customer_currency,
       b.cb_tb_lost_amount,
       b.cb_tb_won_amount,
       b.cb_tb_pending_amount,
       b.stripe_currency,
       b.stripe_refunded_amount,
       b.airline
FROM se.data_pii.se_credit c
         LEFT JOIN se.data_pii.master_tb_booking_list b ON c.original_se_booking_id = b.booking_id
WHERE c.credit_reason LIKE '%COVID%'
--   AND territory_name = 'UK'
  AND c.credit_status IN ('ACTIVE', 'EXPIRED')
  AND c.credit_type IN ('REFUND', 'CANCELLATION_CREDIT')
  AND b.bonding_type = 'ATOL';


airflow backfill --start_date '2021-05-18 01:00:00' --end_date '2021-05-18 01:00:00' --task_regex '.*' -m incoming__finance_gsheets__safi_airlines__daily_at_01h00
airflow backfill --start_date '2021-05-18 01:00:00' --end_date '2021-05-18 01:00:00' --task_regex '.*' -m incoming__tableau_gsheets__tableau_channel_costs__daily_at_01h00


SELECT
       sc.*
FROM se.data.tb_booking tb
         INNER JOIN se.data.se_credit sc ON tb.booking_id = sc.original_se_booking_id
WHERE tb.territory = 'UK'
  AND tb.is_atol_bonded_booking
  AND tb.payment_status = 'CANCELLED'
  AND LOWER(tb.cancellation_reason) LIKE '%covid_19%'
  AND sc.credit_status IN ('ACTIVE', 'EXPIRED')



SELECT sc.credit_id,
       sc.credit_status,
       sc.credit_status__o,
       sc.original_se_booking_id,
       sb.booking_id,
       sb.transaction_id
FROM se.data.se_credit sc
INNER JOIN se.data.se_booking sb ON sc.original_se_booking_id = sb.booking_id
WHERE credit_id IN (
                    '13124630',
                    '13123070',
                    '13304995',
                    '13120936',
                    '13150949',
                    '13121564',
                    '13120623',
                    '13122773',
                    '13120559',
                    '13207640',
                    '13122360',
                    '13120674',
                    '13257990')