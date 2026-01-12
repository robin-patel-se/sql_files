-- Of self serve cancellations what is the percentage that opt for cash vs credit
--cancellation_status NOT NULL ---> self serve
--cancellation_refund_channel --> cash vs credit

-- bookings that are cancelled via self serve
SELECT COUNT(*)
FROM se.data.se_booking sb
WHERE sb.cancellation_status IS NOT NULL;
--8741

-- from self serve cancellations what is the percentage of case vs credit
SELECT sb.cancellation_refund_channel, COUNT(*)
FROM se.data.se_booking sb
WHERE sb.cancellation_status IS NOT NULL
GROUP BY 1;
-- payment_method = 5098
-- credit = 3639


-- Of the credit self serve how many have been converted to deleted or refunded as cash.

SELECT COUNT(*)
FROM se.data.se_credit sc
         INNER JOIN se.data.se_booking sb ON sc.original_se_booking_id = sb.booking_id AND sb.cancellation_status IS NOT NULL
--10441 credits generated

SELECT sc.credit_status,
       COUNT(*)
FROM se.data.se_credit sc
         INNER JOIN se.data.se_booking sb ON sc.original_se_booking_id = sb.booking_id AND sb.cancellation_status IS NOT NULL
GROUP BY 1
-- USED_TB 2 - 0.02%
-- ACTIVE 4241 - 40.62%
-- USED 2704 - 25.90%
-- DELETED 3194 - 30.59%
-- REFUNDED_CASH 300 2.87
