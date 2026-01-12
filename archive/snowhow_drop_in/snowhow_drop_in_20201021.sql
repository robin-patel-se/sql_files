--KJ

WITH union_table AS (
    SELECT sb.booking_completed_date::DATE AS date,
           'booked'                        AS booking_state,
           count(*)                        AS bookings

    FROM se.data.se_booking sb
    WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')
    GROUP BY 1, 2

    UNION

    SELECT sb.cancellation_date::DATE AS date,
           'canx'                     AS booking_state,
           count(*) * -1              AS bookings

    FROM se.data.se_booking sb
    WHERE sb.booking_status = 'REFUNDED'
    GROUP BY 1, 2
)
SELECT union_table.date,
       sum(union_table.bookings)                                               AS net_bookings,
       sum(iff(union_table.booking_state = 'booked', union_table.bookings, 0)) AS booked_bookings,
       sum(iff(union_table.booking_state = 'canx', union_table.bookings, 0))   AS canx_bookings
FROM union_table
WHERE union_table.date >= '2020-02-01'
GROUP BY 1
ORDER BY 1

