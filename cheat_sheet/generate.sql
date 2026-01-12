WITH generator_table AS (
    SELECT SEQ4() AS hour
    FROM TABLE (GENERATOR(ROWCOUNT => 24)) v
    ORDER BY 1
)
SELECT DATEADD('hour', hour, sc.date_value) AS hour,
       sc.date_value                        AS date,
       sc.today
FROM se.data.se_calendar sc
CROSS JOIN generator_table gt
WHERE sc.today


------------------------------------------------------------------------------------------------------------------------

SELECT SEQ4() AS hour,
       UNIFORM(1, 2, RANDOM())
FROM TABLE (GENERATOR(ROWCOUNT => 10)) v
ORDER BY 1;

------------------------------------------------------------------------------------------------------------------------

WITH generator_table AS (
    SELECT SEQ4() AS hour
    FROM TABLE (GENERATOR(ROWCOUNT => 24)) v
    ORDER BY 1
)
SELECT DATEADD('hour', hour, sc.date_value) AS hour,
       sc.date_value                        AS date,
       sc.today
FROM se.data.se_calendar sc
    CROSS JOIN generator_table gt
WHERE sc.date_value = sc.today;


SELECT sc.date_value AS date,
       sc.today
FROM se.data.se_calendar sc
WHERE sc.today;

------------------------------------------------------------------------------------------------------------------------


SELECT ssa.product_configuration,
       COUNT(*)
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
GROUP BY 1;


SELECT ssa.product_type,
       COUNT(*)
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------
SELECT ssa.product_type,
       ssa.product_configuration,
       ssa.product_line,
       COUNT(*)
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
GROUP BY GROUPING SETS (1, 2, 3);

SELECT ssa.product_type,
       ssa.product_configuration,
       COUNT(*)
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
GROUP BY 1, 2;

WITH input_data AS (
    SELECT fcb.booking_completed_timestamp::DATE               AS date,
           fcb.se_sale_id,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin,
           COUNT(*)                                            AS bookings
    FROM se.data.fact_complete_booking fcb
    WHERE fcb.booking_completed_timestamp >= CURRENT_DATE - 30
      AND fcb.se_sale_id IN ('A22199',
                             'A14837'
        )
    GROUP BY 1, 2
)
SELECT id.date,
       id.se_sale_id,
       id.margin,
       --window function across all rows within partition
       SUM(id.margin) OVER (PARTITION BY id.se_sale_id)                                                        AS total_margin,
       margin / total_margin                                                                                   AS perc_margin,
       --window function across rows up to current row (based on order by) within partition
       SUM(id.margin) OVER (PARTITION BY id.se_sale_id ORDER BY date)                                          AS cumulative_margin,
       AVG(id.margin) OVER (PARTITION BY id.se_sale_id ORDER BY date)                                          AS avg_cumulative_margin,
       --sliding window function to compute 7 rolling average margin
       AVG(id.margin) OVER (PARTITION BY id.se_sale_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) AS rolling_7_day_avg_margin,
       id.bookings
FROM input_data id
ORDER BY se_sale_id, date;


