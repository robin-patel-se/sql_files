WITH daily_totals AS (
    SELECT fcb.booking_completed_date,
           SUM(fcb.margin_gross_of_toms_gbp::NUMBER) AS sum_margin
    FROM se.data.fact_complete_booking fcb
    WHERE fcb.booking_completed_date >= CURRENT_DATE - 10
    GROUP BY 1
)
SELECT dt.booking_completed_date,
       dt.sum_margin,
       SUM(dt.sum_margin) OVER (ORDER BY dt.booking_completed_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_margin
FROM daily_totals dt;


WITH bookings_ranked AS (
    SELECT se_year,
           se_week,
           CASE
               WHEN territory IN ('UK') THEN 'UK'
               WHEN territory IN ('DE', 'CH', 'AT') THEN 'DACH'
               ELSE 'ROW' END                                                                AS territory,
           booking_completed_date,
           salesforce_opportunity_id,
           booking_id,
           RANK() OVER (PARTITION BY salesforce_opportunity_id,year ORDER BY booking_id ASC) AS order_rank_year
    FROM se.data.fact_booking fb
        LEFT JOIN se.data.se_calendar se ON fb.booking_completed_date = se.date_value
        LEFT JOIN se.data.dim_sale dm
                  ON fb.se_sale_id = dm.se_sale_id
    WHERE booking_status_type IN ('live')
      AND territory NOT IN ('PL', 'TL')
      AND year >= 2021
)
SELECT se_year,
       se_week,
--territory,
       MAX(booking_completed_date)               AS max_date,
       COUNT(DISTINCT salesforce_opportunity_id) AS deals
FROM bookings_ranked
WHERE se_year IS NOT NULL
  AND order_rank_year = 1
GROUP BY 1, 2
ORDER BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
--q1
WITH weekly_booking_count AS (
    SELECT fcb.se_sale_id,
           se.data.se_week(fcb.booking_completed_date) AS se_week,
           se.data.se_year(fcb.booking_completed_date) AS se_year,
           COUNT(DISTINCT fcb.booking_id)              AS completed_bookings
    FROM se.data.se_sale_attributes ssa
        LEFT JOIN se.data.fact_complete_booking fcb ON ssa.se_sale_id = fcb.se_sale_id
    WHERE se.data.se_year(fcb.booking_completed_date) = 2021 -- NOTE
    GROUP BY 1, 2, 3
),
     booking_threshold AS (
         SELECT wbc.se_sale_id,
                wbc.se_week,
                wbc.se_year,
                wbc.completed_bookings,
                SUM(wbc.completed_bookings) OVER
                    (PARTITION BY wbc.se_sale_id, wbc.se_year ORDER BY wbc.se_week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative,
                IFF(running_cumulative >= 85, TRUE, FALSE)                                                                           AS threshold_flag
         FROM weekly_booking_count wbc
     )
SELECT bt.se_week,
       COUNT(DISTINCT IFF(threshold_flag, bt.se_sale_id, NULL)) AS sales_past_threshold,
       LISTAGG(IFF(threshold_flag, bt.se_sale_id, NULL), ', ')  AS sales_past_threshold_list
FROM booking_threshold bt
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--q2

WITH weekly_booking_count AS (
    SELECT fcb.se_sale_id,
           se.data.se_week(fcb.booking_completed_date)                 AS se_week,
           se.data.se_year(fcb.booking_completed_date)                 AS se_year,
           COUNT(DISTINCT fcb.booking_id)                              AS completed_bookings,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency::NUMBER) AS completed_margin
    FROM se.data.se_sale_attributes ssa
        LEFT JOIN se.data.fact_complete_booking fcb ON ssa.se_sale_id = fcb.se_sale_id
    WHERE se.data.se_year(fcb.booking_completed_date) = 2021 -- NOTE
    GROUP BY 1, 2, 3
),
     margin_threshold AS
         (
             SELECT wbc.se_sale_id,
                    wbc.se_week,
                    wbc.se_year,
                    wbc.completed_bookings,
                    wbc.completed_margin,
                    (wbc.se_week * (10000 / 52))::NUMBER                                                                                 AS margin_target,
                    SUM(wbc.completed_margin) OVER
                        (PARTITION BY wbc.se_sale_id, wbc.se_year ORDER BY wbc.se_week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative,
                    IFF(running_cumulative >= margin_target, TRUE, FALSE)                                                                AS threshold_flag
             FROM weekly_booking_count wbc
         )

SELECT mt.se_week,
       COUNT(DISTINCT IFF(threshold_flag, mt.se_sale_id, NULL)) AS sales_past_threshold,
       LISTAGG(IFF(threshold_flag, mt.se_sale_id, NULL), ', ')  AS sales_past_threshold_list
FROM margin_threshold mt
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.user_segmentation us
WHERE us.gross_bookings > 1
LIMIT 1;

SELECT *
FROM se.data.user_segmentation us
WHERE us.shiro_user_id = 49087121;

------------------------------------------------------------------------------------------------------------------------

SELECT ssa.salesforce_opportunity_id,
       MAX(ssa.product_configuration)
FROM se.data.se_sale_attributes ssa
WHERE ssa.salesforce_opportunity_id = '0066900001QbaKG'
GROUP BY 1;

WITH cte1 AS (
SELECT ssa.salesforce_opportunity_id,
--        ssa.se_sale_id,
--        ssa.product_configuration,
       COUNT(DISTINCT ssa.se_sale_id)                                            AS no_sales,
       SUM(IFF(ssa.product_configuration = 'Hotel Plus', 1, 0)) > 0              AS has_hotel_plus,
       SUM(IFF(ssa.product_configuration = 'Hotel Plus', 1, 0))                  AS no_hotel_plus_sales,
       SUM(IFF(ssa.product_configuration IS DISTINCT FROM 'Hotel Plus', 1, 0))   AS no_non_hotel_plus_sales,
       IFF(no_hotel_plus_sales > no_non_hotel_plus_sales, 'Hotel Plus', 'Hotel') AS grouping
FROM se.data.se_sale_attributes ssa
WHERE ssa.salesforce_opportunity_id = '0066900001QbaKG'
GROUP BY 1;