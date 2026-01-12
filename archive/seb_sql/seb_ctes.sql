------------------------------------------------------------------------------------------------------------------------
--CTE demo
SELECT se_sale_id,
       SUM(fcb.margin_gross_of_toms_gbp)
FROM se.data.fact_complete_booking fcb
GROUP BY 1;


SELECT fcb.se_sale_id,
       --
       SUM(fcb.margin_gross_of_toms_gbp) AS total_margin,
       AVG(fcb.margin_gross_of_toms_gbp),
       MAX(fcb.margin_gross_of_toms_gbp)
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_completed_date >= CURRENT_DATE - 1
GROUP BY 1;

SELECT *
FROM se.data.scv_touched_spvs sts;


SELECT LEFT(se_sale_id, 1) = 'A' AS new_data_model,
       SUM(total_margin)
FROM (
    SELECT *
    FROM (
        SELECT fcb.se_sale_id,
               SUM(fcb.margin_gross_of_toms_gbp) AS total_margin
        FROM se.data.fact_complete_booking fcb
        WHERE fcb.booking_completed_date >= CURRENT_DATE - 1
        GROUP BY 1
    )
    WHERE total_margin > 1000
)
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
WITH query1 AS (
    -- summarising up to sale id
    SELECT fcb.se_sale_id,
           SUM(fcb.margin_gross_of_toms_gbp) AS total_margin
    FROM se.data.fact_complete_booking fcb
    WHERE fcb.booking_completed_date >= CURRENT_DATE - 1
    GROUP BY 1
),
     query2 AS (
         -- filtering query 1 for 1000
         SELECT *
         FROM query1
         WHERE total_margin > 1000
     )
SELECT LEFT(se_sale_id, 1) = 'A' AS new_data_model,
       SUM(total_margin)
FROM query2
    LEFT JOIN query1 ON query2.se_sale_id = query1.se_sale_id
GROUP BY 1;


-- summarisation of margin to se_sale_id
-- add spvs

-- psuedo
WITH query1 AS (
    -- booking data at booking grain, aggregate to sale id
),
     query2 AS (
         -- spv data at spv grain, aggregate to sale id
     )
SELECT
FROM query1
    LEFT JOIN query2 ON sale_id = sale_id



WITH bookings_by_sale AS (
    -- booking data at booking grain, aggregate to sale id
    SELECT fcb.se_sale_id,
           SUM(fcb.margin_gross_of_toms_gbp) AS total_margin
    FROM se.data.fact_complete_booking fcb
    WHERE fcb.booking_completed_date::DATE = CURRENT_DATE - 1
    GROUP BY 1
),
     spvs_by_sale AS (
         -- spv data at spv grain, aggregate to sale id
         SELECT sts.se_sale_id,
                COUNT(*) AS spv
         FROM se.data.scv_touched_spvs sts
         WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
         GROUP BY 1
     )
SELECT bbs.se_sale_id,
       total_margin,
       spv
FROM bookings_by_sale bbs
    LEFT JOIN spvs_by_sale sbs ON bbs.se_sale_id = sbs.se_sale_id;
;


------------------------------------------------------------------------------------------------------------------------
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
    WHERE LOWER(status) IN ('refunded', 'refunded_by_merchant', 'cancelled', 'miscellaneous', 'refund_failed') -- filter worldpay transactions
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
                LISTAGG(rc.entry_type, ', ')    AS ratepay_entry_type_list
         FROM se.data.ratepay_clearing rc
         WHERE LOWER(entry_type) IN ('5', '6', 'return', 'credit') -- filter ratepay clearings
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
     ),
     credits_linked_to_booking AS (
         -- credits automatically linked to a previous booking cancellation
         SELECT sc.original_se_booking_id,
                COUNT(*)                           AS no_of_credits,
                COUNT(DISTINCT sc.credit_currency) AS no_of_currencies,
                SUM(sc.credit_amount_gbp)          AS credit_amount_gbp
         FROM se.data.se_credit sc
         WHERE sc.original_se_booking_id IS NOT NULL
         GROUP BY 1
     ),
     manually_issued_credits AS (
         -- credits associated to a booking based on user credits
         -- credit created after booking was cancelled
         -- credit created within 1 day of cancellation
         -- credit is not an automatic linked credit
         SELECT sb.booking_id,
                SUM(sc.credit_amount_gbp) AS total_credits_manually_issued
         FROM se.data.se_booking sb
             INNER JOIN se.data.se_credit sc ON sb.shiro_user_id = sc.shiro_user_id
         WHERE sb.booking_status = 'REFUNDED'
           AND sc.original_se_booking_id IS NULL
           AND sb.cancellation_tstamp < sc.credit_date_created
           AND DATEDIFF(DAY, sb.cancellation_tstamp, sc.credit_date_created) = 1
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
       rp.ratepay_entry_type_list,

       --credits linked to booking
       cltb.no_of_currencies,
       cltb.credit_amount_gbp,

       --credits linked to booking by shiro user id
       mic.total_credits_manually_issued

FROM se.data.se_booking sb
    LEFT JOIN worldpay wp ON sb.unique_transaction_reference = wp.order_code
    LEFT JOIN ratepay rp ON sb.booking_id = rp.booking_id
    LEFT JOIN manual_refunds mancb ON sb.transaction_id = mancb.transaction_id
    LEFT JOIN se.data.se_chargebacks cbse ON sb.booking_id = cbse.booking_id
    LEFT JOIN credits_linked_to_booking cltb ON sb.booking_id = cltb.original_se_booking_id
    LEFT JOIN manually_issued_credits mic ON sb.booking_id = mic.booking_id
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED', 'HOLD_BOOKED', 'EXPIRED')
;


SELECT wts.order_code,
       COUNT(*) AS count_rows
FROM se.data.worldpay_transaction_summary wts
GROUP BY 1
HAVING count_rows > 1



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
WHERE LOWER(status) IN ('refunded', 'refunded_by_merchant', 'cancelled', 'miscellaneous', 'refund_failed') -- filter worldpay transactions
GROUP BY 1;


SELECT order_code,
       status,
       wts.amount,
       LOWER(status),
       IFF(LOWER(status) IN ('refused'), amount, 0)
FROM se.data.worldpay_transaction_summary wts
WHERE wts.order_code = '2a09cFWAtWTGTIAMJ4FQ';


SELECT order_code,
       LISTAGG(DISTINCT status, ', '),
       SUM(
               IFF(
                           LOWER(status) IN ('refunded', 'refunded_by_merchant'), amount, 0
                   )
           )
FROM se.data.worldpay_transaction_summary wts
WHERE wts.order_code = '2a09cFWAtWTGTIAMJ4FQ'
GROUP BY 1;



SELECT sc.original_se_booking_id,
       COUNT(*)
FROM se.data.se_credit sc
WHERE sc.original_se_booking_id IS NOT NULL
GROUP BY 1
HAVING COUNT(*) > 1
;



SELECT sc.original_se_booking_id,
       COUNT(*)                           AS no_of_credits,
       COUNT(DISTINCT sc.credit_currency) AS no_of_currencies,
       SUM(sc.credit_amount_gbp)          AS credit_amount_gbp
FROM se.data.se_credit sc
WHERE sc.original_se_booking_id IS NOT NULL
GROUP BY 1;



SELECT sb.booking_id,
       sb.shiro_user_id,
       sb.booking_status,
       sb.cancellation_tstamp,
       sc.credit_id,
       sc.credit_date_created,
       sc.credit_amount_gbp
FROM se.data.se_booking sb
    INNER JOIN se.data.se_credit sc ON sb.shiro_user_id = sc.shiro_user_id
WHERE sb.booking_status = 'REFUNDED'
  AND sc.original_se_booking_id IS NULL
  AND sb.shiro_user_id = 17087309 -- TODO remove
  AND sb.cancellation_tstamp < sc.credit_date_created
  AND DATEDIFF(DAY, sb.cancellation_tstamp, sc.credit_date_created) = 1
;



SELECT sb.booking_id,
       SUM(sc.credit_amount_gbp) AS total_credits_manually_issued
FROM se.data.se_booking sb
    INNER JOIN se.data.se_credit sc ON sb.shiro_user_id = sc.shiro_user_id
WHERE sb.booking_status = 'REFUNDED'
  AND sc.original_se_booking_id IS NULL
  AND sb.cancellation_tstamp < sc.credit_date_created
  AND DATEDIFF(DAY, sb.cancellation_tstamp, sc.credit_date_created) = 1
GROUP BY 1
;



SELECT sb.offer_name, sb.offer_rate_code
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.is_new_model_booking;

SELECT *
FROM se.data.se_booking sb
WHERE sb.offer_id IN ('A25087', 'A25102', 'A25103');

-- A25103 offer id of offer of interest

SELECT *
FROM se.data.se_offer_attributes soa
INNER JOIN se.data.se_cms_mari_link scml ON soa.se_offer_id = scml.se_offer_id
WHERE soa.se_offer_id IN ('A25087', 'A25102', 'A25103');


SELECT * FROM se.data.se_room_rates srr