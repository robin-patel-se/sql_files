WITH canx AS (
    SELECT sb.last_updated AS cancellation_date,
           sb.booking_id,
           sb.shiro_user_id
    FROM data_vault_mvp.dwh.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
)

   , bookings_with_canx AS (

    SELECT sb.booking_completed_date                                          AS booking_date,
           sb.booking_id,
           sb.shiro_user_id,
           COUNT(DISTINCT c.booking_id)                                       AS cancelled_bookings,
           MAX(DATEDIFF(DAY, sb.booking_completed_date, c.cancellation_date)) AS canx_window
    FROM data_vault_mvp.dwh.se_booking sb
             LEFT JOIN canx c ON sb.shiro_user_id = c.shiro_user_id
        AND DATEADD(DAY, -7, sb.booking_completed_date) <= c.cancellation_date
        AND sb.booking_completed_date >= c.cancellation_date
    WHERE sb.booking_status = 'COMPLETE'
      AND sb.booking_completed_date >= '2020-05-01'
    GROUP BY 1, 2, 3
)

SELECT *
FROM bookings_with_canx
WHERE bookings_with_canx.cancelled_bookings > 0;