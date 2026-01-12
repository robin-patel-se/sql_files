--All credits
WITH list_of_bookings AS (
    SELECT *
    FROM se.data_pii.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
      AND sb.refund_type = 'FULL'
      AND sb.cancellation_refund_channel = 'CREDIT'
      AND sb.cancellation_requested_by IN
          ('jeevitha.govindasamy@secretescapes.com',
           'nelson.gallardo@secretescapes.com',
           'amine.belmokhtar@secretescapes.com')
      AND sb.cancellation_date = '2021-02-23'
)
SELECT *
FROM se.data_pii.se_credit sc
WHERE sc.original_se_booking_id IN (
    SELECT lob.booking_id
    FROM list_of_bookings lob
)
  AND sc.credit_status IS DISTINCT FROM 'ACTIVE'
;

--Aggregate calculations
WITH list_of_bookings AS (
    SELECT *
    FROM se.data_pii.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
      AND sb.refund_type = 'FULL'
      AND sb.cancellation_refund_channel = 'CREDIT'
      AND sb.cancellation_requested_by IN
          ('jeevitha.govindasamy@secretescapes.com',
           'nelson.gallardo@secretescapes.com',
           'amine.belmokhtar@secretescapes.com')
      AND sb.cancellation_date = '2021-02-23'
)
SELECT COUNT(*)                                  AS no_of_credits,
       COUNT(DISTINCT sc.original_se_booking_id) AS credits_refunded
FROM se.data_pii.se_credit sc
WHERE sc.original_se_booking_id IN (
    SELECT lob.booking_id
    FROM list_of_bookings lob
)
  AND sc.credit_status IS DISTINCT FROM 'ACTIVE'
;

