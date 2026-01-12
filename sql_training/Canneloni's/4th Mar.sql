--based on cancellations that have occurred this year, was this the user's first booking (what booking number was it)?
--how many people have rebooked since, those that have, how many bookings have they made and was the margin more or less than the original booking?


SELECT sb.booking_id,
       sb.shiro_user_id,
       sb.booking_status,
       sb.booking_completed_timestamp
FROM se.data.se_booking sb
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')
AND shiro_user_id IN ('3489', '2725');



SELECT booking_id,
       booking_completed_date,
       shiro_user_id,
       margin_gross_of_toms_gbp_constant_currency
FROM se.data.se_booking sb
WHERE booking_status IN ('COMPLETED', 'REFUNDED')
  AND shiro_user_id IN ('3489', '2725');


--        MAX(margin_gross_of_toms_gbp_constant_currency) OVER (PARTITION BY shiro_user_id)             AS maximum,
--        (margin_gross_of_toms_gbp_constant_currency = maximum)                                        AS is_highest,
--        LAG(booking_completed_date) OVER (PARTITION BY shiro_user_id ORDER BY booking_completed_date) AS last_booking_date,
--        DATEDIFF(DAY, last_booking_date, booking_completed_date),
--        ROW_NUMBER() OVER (PARTITION BY shiro_user_id ORDER BY booking_completed_date DESC)           AS booking_index