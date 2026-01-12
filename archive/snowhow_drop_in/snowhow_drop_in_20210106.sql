


------------------------------------------------------------------------------------------------------------------------
-- pauline
-- Hello everyone. is there a way to see the repeat purchase rate? and its average timing?

SELECT COUNT(DISTINCT fcb.shiro_user_id)
FROM se.data.fact_complete_booking fcb;

--2,402,129 members have made a live booking

SELECT COUNT(*)
FROM (
         SELECT fcb.shiro_user_id
         FROM se.data.fact_complete_booking fcb
         GROUP BY fcb.shiro_user_id
         HAVING COUNT(*) > 1
     );

--755,002 members have made more than one booking

WITH multiple_bookers AS (
    SELECT fcb.shiro_user_id,
           fcb.booking_completed_date,
           LAG(fcb.booking_completed_date)
               OVER (PARTITION BY fcb.shiro_user_id ORDER BY fcb.booking_completed_date) AS last_booking_completed_date,
           DATEDIFF(DAY, last_booking_completed_date, fcb.booking_completed_date)        AS diff_days
    FROM se.data.fact_complete_booking fcb
        QUALIFY COUNT(*) OVER (PARTITION BY fcb.shiro_user_id) > 1
)

SELECT AVG(multiple_bookers.diff_days)
FROM multiple_bookers;
-- avg of 190 days between repeat bookings
