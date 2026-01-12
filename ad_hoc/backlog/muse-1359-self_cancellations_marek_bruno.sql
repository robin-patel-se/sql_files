/*
On the subject of SELF-CANCELLATIONS, the questions we are looking to answer are:
1. Look at the behaviour of customers who got given credit but then called the call centre and asked for a cash refund.
Do they come back to purchase again at some point?

2. Look at customers who asked for a cash refund when using the self-cancellation tool and see if they came back to purchase later.
 */

------------------------------------------------------------------------------------------------------------------------
--Question 1:
--create a list of users that have had a refund that was refunded to credit, then the credit that was created has been refunded to cash

SELECT sb.booking_id,
       sb.shiro_user_id,
       sb.booking_status,
       sb.cancellation_refund_channel,
       sc.credit_status,
       sc.credit_id,
       sc.original_se_booking_id
FROM se.data.se_booking sb
    INNER JOIN se.data.se_credit sc ON sb.booking_id = sc.original_se_booking_id
WHERE sb.booking_status = 'REFUNDED'            -- cancelled bookings
  AND sb.cancellation_refund_channel = 'CREDIT' -- cancelled bookings where the customer has opted for a refund to credit
  AND sc.credit_status = 'REFUNDED_CASH' --credits that have been refunded to cash
;

-- caveat: ignoring specifics for users that have multiple cancellation credits

WITH users_with_refunded_credits AS (
    SELECT sb.shiro_user_id,
           MAX(sc.credit_last_updated) AS max_credit_updated
    FROM se.data.se_booking sb
        INNER JOIN se.data.se_credit sc ON sb.booking_id = sc.original_se_booking_id
    WHERE sb.booking_status = 'REFUNDED'            -- cancelled bookings
      AND sb.cancellation_refund_channel = 'CREDIT' -- cancelled bookings where the customer has opted for a refund to credit
      AND sc.credit_status = 'REFUNDED_CASH'        --credits that have been refunded to cash
      AND sb.booking_completed_timestamp >= '2020-01-01'
    GROUP BY 1
)
SELECT uwrc.shiro_user_id,
       COUNT(DISTINCT s.booking_id)    AS bookings,
       SUM(s.margin_gross_of_toms_gbp) AS margin
FROM users_with_refunded_credits uwrc
    LEFT JOIN se.data.se_booking s
              ON uwrc.shiro_user_id = s.shiro_user_id--user rebooked
                  AND uwrc.max_credit_updated <= s.booking_completed_timestamp --booking occurred after credit was refunded
                  AND s.booking_status = 'COMPLETE'
GROUP BY 1
;

--copy these figures to a googlesheet: https://docs.google.com/spreadsheets/d/1cuVU_PT6i0dHtEKZjqTW6wqOCko0dMTiVcbNy6tgPrQ/edit#gid=0
------------------------------------------------------------------------------------------------------------------------
-- Question 2:
SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_status = 'REFUNDED'
  AND sb.cancellation_requested_by_domain IS NOT NULL
  AND sb.cancellation_requested_by_domain != '@secretescapes.com'
  AND sb.cancellation_refund_channel LIKE '%PAYMENT_METHOD%'
--there are variations that are not unique for when multiple cancellations occur


-- caveat: ignoring specifics for users that have multiple cancellation events on a single booking

WITH users_self_serve_refunded_to_cash AS (
    SELECT sb.shiro_user_id,
           MAX(sb.cancellation_tstamp) AS max_cancellation_tstamp
    FROM se.data.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
      AND sb.cancellation_requested_by_domain IS NOT NULL -- criteria for self serve cancellation
      AND sb.cancellation_requested_by_domain != '@secretescapes.com' -- criteria for self serve cancellation
      AND sb.cancellation_refund_channel LIKE '%PAYMENT_METHOD%' --refund to payment method, there are variations that are not unique for when multiple cancellations occur
      AND sb.booking_completed_timestamp >= '2020-01-01'
    GROUP BY 1
)
SELECT ussrtc.shiro_user_id,
       COUNT(DISTINCT s.booking_id)    AS bookings,
       SUM(s.margin_gross_of_toms_gbp) AS margin
FROM users_self_serve_refunded_to_cash ussrtc
    LEFT JOIN se.data.se_booking s ON ussrtc.shiro_user_id = s.shiro_user_id
    AND ussrtc.max_cancellation_tstamp <= s.booking_completed_timestamp --booking occurred after cancellation
    AND s.booking_status = 'COMPLETE'
GROUP BY 1

-- caveat: ignoring specifics for users that have multiple cancellations