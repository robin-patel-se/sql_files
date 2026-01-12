-- 17/09/2020

/*
Homework, advancing on the query above:
- what is the total margin and average margin for each lead_time
- what the average holiday length (in days) for each lead time
- how do I filter this to show me the whole of last month with a dynamic filter (so next month it should automatically change)
Bonus (for extra claps)
- how do I tell what percentage of total bookings (in the same time frame) each lead time has? (edited)
*/


WITH t1 AS (
    SELECT datediff(DAY, booking_created_date, check_in_date)               AS lead_time,
           COUNT(DISTINCT booking_id)                                       AS no_of_bookings,
           ROUND(SUM(margin_gross_of_toms_gbp_constant_currency), 0)        AS total_net_margin,
           ROUND(AVG(margin_gross_of_toms_gbp_constant_currency), 0)        AS avg_net_margin,
           ROUND(ABS(AVG(DATEDIFF(DAY, check_out_date, check_in_date))), 0) AS avg_holiday_length
    FROM se.data.se_booking
    WHERE upper(booking_status) = 'COMPLETE'
      AND check_in_date >= CURRENT_DATE - 30
    GROUP BY lead_time
    ORDER BY 2 DESC
),
     t2 AS (
         SELECT sum(no_of_bookings) AS total_bookings
         FROM t1
     )
SELECT t1.lead_time,
       t1.no_of_bookings,
       t1.total_net_margin,
       t1.avg_net_margin,
       t1.avg_holiday_length,
       t2.total_bookings,
       (t1.no_of_bookings / t2.total_bookings) * 100 AS booking_percentage
FROM t1
         LEFT JOIN t2

--join t2
--on ???

WITH cte AS (
    SELECT datediff(DAY, booking_created_date, check_in_date)               AS lead_time,
           COUNT(DISTINCT booking_id)                                       AS no_of_bookings,
           ROUND(SUM(margin_gross_of_toms_gbp_constant_currency), 0)        AS total_net_margin,
           ROUND(AVG(margin_gross_of_toms_gbp_constant_currency), 0)        AS avg_net_margin,
           ROUND(ABS(AVG(DATEDIFF(DAY, check_out_date, check_in_date))), 0) AS avg_holiday_length
    FROM se.data.se_booking
    WHERE upper(booking_status) = 'COMPLETE'
      AND check_in_date >= CURRENT_DATE - 30
    GROUP BY lead_time
    ORDER BY 2 DESC
)
SELECT cte.lead_time,
       cte.no_of_bookings,
       cte.total_net_margin,
       cte.avg_net_margin,
       cte.avg_holiday_length,
       SUM(cte.no_of_bookings) OVER () AS total_bookings
FROM cte



------------------------------------------------------------------------------------------------------------------------
-- 21/09/2020

SELECT TO_DATE(s.start_date)                             AS start_date,
       SUM(IFF(s.product_configuration = 'Hotel', 1, 0)) AS product_config_hotel,
       COUNT(DISTINCT s.se_sale_id)                      AS count_sales,
       product_config_hotel / count_sales                AS percentage
FROM se.data.se_sale_attributes s
WHERE s.start_date >= CURRENT_DATE - 8
  AND s.sale_active = TRUE
GROUP BY 1
ORDER BY 1 ASC;

SELECT DISTINCT product_configuration
FROM se.data.se_sale_attributes ssa;

/*
Homework (optional) advancing on the query above:
Expand out the columns to include a count of all the different product types
Adjust the IFF query to group all the variations of  IHP  (connected, dynamic, static) into one column for IHP sales - adding columns together gets 0 points
Bonus:
Add a new count of bookings to each product configuration for bookings that are over £500 customer_total_price.
 */




------------------------------------------------------------------------------------------------------------------------
--

-- window functions

SELECT sb.shiro_user_id,
       sb.booking_id,
       sb.booking_completed_date,
       sb.margin_gross_of_toms_gbp_constant_currency,
       SUM(sb.margin_gross_of_toms_gbp_constant_currency) OVER (PARTITION BY sb.shiro_user_id)        AS total_margin,
       MAX(sb.margin_gross_of_toms_gbp_constant_currency) OVER (PARTITION BY sb.shiro_user_id)        AS max_booking_margin,
       SUM(sb.margin_gross_of_toms_gbp_constant_currency) OVER (PARTITION BY sb.shiro_user_id
           ORDER BY booking_completed_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)          AS cumulative_margin,
       COUNT(sb.booking_id) OVER (PARTITION BY sb.shiro_user_id)                                      AS total_bookings,
       ROW_NUMBER() OVER (PARTITION BY sb.shiro_user_id ORDER BY sb.booking_completed_date)           AS booking_index,

       LAST_VALUE(sb.sale_id) OVER (PARTITION BY sb.shiro_user_id ORDER BY sb.booking_completed_date) AS last_booked_sale_id

FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.shiro_user_id IN (3489, 2725)
ORDER BY shiro_user_id DESC NULLS LAST, booking_index
;
--     QUALIFY ROW_NUMBER() OVER (PARTITION BY sb.shiro_user_id ORDER BY sb.booking_completed_date DESC) = 1

SELECT * FROM se.data.se_booking WHERE SE_BOOKING.booking_status = 'COMPLETE';




















