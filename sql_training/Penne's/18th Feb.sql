--HOMEWORK
-- 1. From the se.data.se_booking table, for every booking with booking_status = 'COMPLETE' (filter),
-- I would like you to output a new column that computes the margin percentage from the gross revenue
-- (gross revenue is the total value of a booking). Please use fields 'gross_revenue_gbp_constant_currency' and
-- 'margin_gross_of_toms_gbp_constant_currency'. Output should be 4 columns, booking id, margin, gross revenue,
-- and margin percentage
-- HINT: Scalar Numeric

SELECT sb.booking_id,
       sb.gross_revenue_gbp_constant_currency,
       sb.margin_gross_of_toms_gbp_constant_currency,
       sb.margin_gross_of_toms_gbp_constant_currency / sb.gross_revenue_gbp_constant_currency AS margin_percentage
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
LIMIT 50;


------------------------------------------------------------------------------------------------------------------------

-- 2. From the se.data.se_booking table, list the bookings that are booking_status = 'COMPLETE' that have a
-- 'booking_completed_date' within 7 days of today. Output should be all columns for bookings that match the filter
-- HINT: Scalar Date + Context

SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.booking_completed_date >= CURRENT_DATE - 7;

------------------------------------------------------------------------------------------------------------------------

-- 3. From the se.data.se_booking table, output the lead_days for bookings that are booking_status = 'COMPLETE'. Output should
-- be 4 columns, booking id, booking completed date, check in date, and lead days
-- HINT: Scalar Date

SELECT sb.booking_id,
       sb.booking_completed_date,
       sb.check_in_date,
       DATEDIFF(DAY, sb.booking_completed_date, sb.check_in_date) AS lead_days
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE';


------------------------------------------------------------------------------------------------------------------------
SELECT sb.booking_status,
       COUNT(*) AS bookings
FROM se.data.se_booking sb
WHERE DATE_TRUNC(WEEK, sb.booking_created_date) = DATE_TRUNC(WEEK, DATEADD(WEEK, -1, CURRENT_DATE))
  AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
GROUP BY 1;


SELECT DATE_TRUNC(WEEK, sb.booking_completed_date) AS week,
       sb.booking_status,
       COUNT(*)                                    AS bookings
FROM se.data.se_booking sb
WHERE sb.booking_completed_date >= DATE_TRUNC(WEEK, DATEADD(WEEK, -4, CURRENT_DATE))
  AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
GROUP BY 1, 2;


SELECT DATE_TRUNC(WEEK, sb.booking_completed_date)        AS week,
       sb.booking_status,
       AVG(sb.margin_gross_of_toms_gbp_constant_currency) AS avg_margin
FROM se.data.se_booking sb
WHERE sb.booking_completed_date >= DATE_TRUNC(WEEK, DATEADD(WEEK, -4, CURRENT_DATE))
  AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
GROUP BY 1, 2;

SELECT DATE_TRUNC(WEEK, sb.booking_completed_date)                                                       AS week,
       sb.booking_status,
       AVG(CASE WHEN booking_status = 'COMPLETE' THEN sb.margin_gross_of_toms_gbp_constant_currency END) AS avg_complete_margin,
       AVG(CASE WHEN booking_status = 'REFUNDED' THEN sb.margin_gross_of_toms_gbp_constant_currency END) AS avg_refunded_margin
FROM se.data.se_booking sb
WHERE sb.booking_completed_date >= DATE_TRUNC(WEEK, DATEADD(WEEK, -4, CURRENT_DATE))
  AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
GROUP BY 1, 2;


SELECT ds.product_configuration,
       COUNT(*)
FROM se.data.se_booking sb
         LEFT JOIN se.data.dim_sale ds ON sb.se_sale_id = ds.se_sale_id
WHERE sb.booking_completed_date >= DATEADD(MONTH, -6, CURRENT_DATE)
AND sb.booking_status = 'COMPLETE'
GROUP BY 1
