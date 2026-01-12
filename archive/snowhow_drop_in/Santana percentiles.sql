SELECT posu_cluster,
       posu_major_region,
       age_weeks,
       CASE
           WHEN percent_rank >= 0.75 THEN '1. Gold'
           WHEN percent_rank >= 0.5 THEN '2. Silver'
           WHEN percent_rank >= 0.25 THEN '3. Bronze'
           ELSE '4. Below Bronze' END     AS sale_band,
       cast(min(margin) AS DECIMAL(8, 2)) AS group_min_margin,
       cast(avg(margin) AS DECIMAL(8, 2)) AS group_avg_margin,
       count(*)
                                          AS group_count,
       cast(sum(margin) AS DECIMAL(8, 2)) AS group_total_margin

FROM (
         SELECT posu_cluster,
                posu_major_region,
                global_sale_id,
                margin,
                age_weeks,
                PERCENT_RANK() OVER (PARTITION BY posu_cluster, age_weeks ORDER BY margin ASC) AS percent_rank
         FROM (
                  SELECT sale.posu_cluster,
                         posu_major_region,
                         sale.salesforce_opportunity_id                                                AS global_sale_id,
                         FLOOR(datediff('day', sale.start_date, booking.booking_created_date) / 7) + 1 AS age_weeks,
                         sum(margin_gross_of_toms_gbp)                                                 AS margin
                  FROM se.data.fact_complete_booking booking
                           INNER JOIN se.data.se_sale_attributes sale ON booking.sale_id = sale.se_sale_id
                  WHERE booking.booking_created_date BETWEEN sale.start_date AND dateadd('day', 27, sale.start_date)
                    AND sale.start_date BETWEEN '2020-05-01' AND '2020-09-30'
                  GROUP BY 1, 2, 3, 4
              )
     )
WHERE posu_major_region = 'UK'
GROUP BY 1, 2, 3, 4
ORDER BY posu_cluster ASC, age_weeks ASC, sale_band ASC;

------------------------------------------------------------------------------------------------------------------------


WITH sales AS (
    SELECT sale.posu_cluster,
           sale.posu_major_region,
           sale.salesforce_opportunity_id                                                                       AS global_sale_id,
           FLOOR(datediff('day', sale.start_date, booking.booking_completed_date) / 7) + 1                      AS age_weeks,
           SUM(COALESCE(booking.margin_gross_of_toms_gbp_constant_currency, 0))                                 AS margin,
           COUNT(DISTINCT sale.se_sale_id)                                                                      AS territory_sales,
           PERCENT_RANK()
                   OVER (PARTITION BY sale.posu_cluster, age_weeks, sale.posu_major_region ORDER BY margin ASC) AS percent_rank,
           CASE
               WHEN percent_rank >= 0.75 THEN '1. Gold'
               WHEN percent_rank >= 0.5 THEN '2. Silver'
               WHEN percent_rank >= 0.25 THEN '3. Bronze'
               ELSE '4. Below Bronze' END                                                                       AS sale_band
    FROM se.data.se_sale_attributes sale
             LEFT JOIN se.data.fact_complete_booking booking ON booking.sale_id = sale.se_sale_id
    WHERE booking.booking_completed_date BETWEEN sale.start_date
        AND dateadd('day', 27, sale.start_date)
      AND sale.start_date BETWEEN '2020-05-01'
        AND '2020-09-30'
    GROUP BY 1, 2, 3, 4
)
SELECT sales.posu_cluster,
       sales.age_weeks,
       sales.sale_band,
       min(sales.margin)
FROM sales
WHERE sales.posu_major_region = 'UK'
GROUP BY 1, 2, 3;

------------------------------------------------------------------------------------------------------------------------

WITH sales AS (
    SELECT sale.posu_cluster,
           sale.posu_major_region,
           sale.salesforce_opportunity_id                                                                       AS global_sale_id,
           FLOOR(datediff('day', sale.start_date, booking.booking_completed_date) / 7) + 1                      AS age_weeks,
           SUM(COALESCE(booking.margin_gross_of_toms_gbp_constant_currency, 0))                                 AS margin,
           COUNT(DISTINCT sale.se_sale_id)                                                                      AS territory_sales,
           PERCENT_RANK()
                   OVER (PARTITION BY sale.posu_cluster, age_weeks, sale.posu_major_region ORDER BY margin ASC) AS percent_rank,
           CASE
               WHEN percent_rank >= 0.75 THEN '1. Gold'
               WHEN percent_rank >= 0.5 THEN '2. Silver'
               WHEN percent_rank >= 0.25 THEN '3. Bronze'
               ELSE '4. Below Bronze' END                                                                       AS sale_band
    FROM se.data.se_sale_attributes sale
             LEFT JOIN se.data.fact_complete_booking booking ON booking.sale_id = sale.se_sale_id
    WHERE booking.booking_completed_date BETWEEN sale.start_date
        AND dateadd('day', 27, sale.start_date)
      AND sale.start_date BETWEEN '2020-05-01'
        AND '2020-09-30'
    GROUP BY 1, 2, 3, 4
)
SELECT sales.posu_cluster,
       sales.posu_major_region,
       sales.age_weeks,
       sales.sale_band,
       min(sales.margin)
FROM sales
WHERE sales.posu_major_region IN ('UK', 'DE')
GROUP BY 1, 2, 3, 4;



WITH sales AS (
    SELECT sale.posu_cluster,
           sale.posu_major_region,
           sale.salesforce_opportunity_id                                                  AS global_sale_id,
           FLOOR(datediff('day', sale.start_date, booking.booking_completed_date) / 7) + 1 AS age_weeks,
           SUM(COALESCE(booking.margin_gross_of_toms_gbp_constant_currency, 0))            AS margin,
           COUNT(DISTINCT sale.se_sale_id)                                                 AS territory_sales,
           PERCENT_RANK()
                   OVER (PARTITION BY sale.posu_cluster, age_weeks ORDER BY margin ASC)    AS percent_rank,
           CASE
               WHEN percent_rank >= 0.75 THEN '1. Gold'
               WHEN percent_rank >= 0.5 THEN '2. Silver'
               WHEN percent_rank >= 0.25 THEN '3. Bronze'
               ELSE '4. Below Bronze' END                                                  AS sale_band
    FROM se.data.se_sale_attributes sale
             LEFT JOIN se.data.fact_complete_booking booking ON booking.sale_id = sale.se_sale_id
    WHERE booking.booking_completed_date BETWEEN sale.start_date
        AND dateadd('day', 27, sale.start_date)
      AND sale.start_date BETWEEN '2020-05-01'
        AND '2020-09-30'
      AND sale.posu_major_region IN ('Germany', 'Austria', 'Switzerland')
    GROUP BY 1, 2, 3, 4
)
SELECT sales.posu_cluster,
       sales.age_weeks,
       sales.sale_band,
       min(sales.margin)
FROM sales

GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


------------------------------------------------------------------------------------------------------------------------


WITH sales AS (
    SELECT sale.posu_cluster_region,
           sale.salesforce_opportunity_id                                                  AS global_sale_id,
           FLOOR(datediff('day', sale.start_date, booking.booking_completed_date) / 7) + 1 AS age_weeks,
           SUM(COALESCE(booking.margin_gross_of_toms_gbp_constant_currency, 0))            AS margin,
           PERCENT_RANK()
                   OVER (PARTITION BY age_weeks ORDER BY margin ASC)                       AS percent_rank,
           CASE
               WHEN percent_rank >= 0.9 THEN '1. Platinum'
               WHEN percent_rank >= 0.75 THEN '2. Gold'
               WHEN percent_rank >= 0.5 THEN '3. Silver'
               WHEN percent_rank >= 0.25 THEN '4. Bronze'
               WHEN percent_rank >= 0.1 THEN '5. Copper'
               ELSE '6. Non-metal aka dirt' END                                            AS sale_band
    FROM se.data.se_sale_attributes sale
             LEFT JOIN se.data.fact_booking booking ON booking.se_sale_id = sale.se_sale_id
    WHERE booking.booking_completed_date BETWEEN cast(sale.start_date AS DATE) AND dateadd('day', 27, sale.start_date)
      AND booking.booking_status_type IN ('live', 'cancelled')
      AND sale.start_date BETWEEN '2020-05-01' AND '2020-09-30'

    GROUP BY 1, 2, 3
)
SELECT sales.age_weeks,
       sales.sale_band,
       round(min(sales.margin), 0)
FROM sales
GROUP BY 1, 2
ORDER BY 1, 2;



SELECT sale.posu_cluster_region,
       sale.salesforce_opportunity_id                                       AS global_sale_id,
       FLOOR(DATEDIFF('day', sale.start_date, calendar.date_value) / 7) + 1 AS age_weeks,
       SUM(COALESCE(booking.margin_gross_of_toms_gbp_constant_currency, 0)) AS margin,
       PERCENT_RANK()
               OVER (PARTITION BY age_weeks ORDER BY margin ASC)            AS percent_rank,
       CASE
           WHEN percent_rank >= 0.9 THEN '1. Platinum'
           WHEN percent_rank >= 0.75 THEN '2. Gold'
           WHEN percent_rank >= 0.5 THEN '3. Silver'
           WHEN percent_rank >= 0.25 THEN '4. Bronze'
           WHEN percent_rank >= 0.1 THEN '5. Copper'
           ELSE '6. Non-metal aka dirt' END                                 AS sale_band
FROM se.data.se_sale_attributes sale
         LEFT JOIN se.data.se_calendar calendar ON sale.start_date <= calendar.date_value AND calendar.date_value <= current_date
         LEFT JOIN se.data.fact_booking booking
                   ON booking.se_sale_id = sale.se_sale_id AND calendar.date_value = booking.booking_completed_date
WHERE booking.booking_completed_date BETWEEN cast(sale.start_date AS DATE) AND dateadd('day', 27, sale.start_date)
  AND booking.booking_status_type IN ('live', 'cancelled')
  AND sale.start_date BETWEEN '2020-05-01' AND '2020-09-30'
  AND global_sale_id = '0061r00001FGFkb'
GROUP BY 1, 2, 3;

------------------------------------------------------------------------------------------------------------------------


WITH sales AS ( --at territory sale level, to compute age and margin
    SELECT s.se_sale_id,
           s.salesforce_opportunity_id                                             AS global_sale_id,
           FLOOR(DATEDIFF('day', s.start_date::DATE, calendar.date_value) / 7) + 1 AS age_weeks,
           s.start_date::DATE                                                      AS sale_start_date,
           s.end_date::DATE                                                        AS sale_end_date,
           SUM(IFF(booking.booking_status_type IN ('live', 'cancelled'),
                   booking.margin_gross_of_toms_gbp_constant_currency, 0))         AS territory_margin
    FROM se.data.se_sale_attributes s
             LEFT JOIN se.data.se_calendar calendar
                       ON s.start_date::DATE <= calendar.date_value
                           AND calendar.date_value <= s.end_date::DATE
                           AND calendar.date_value <= current_date
             LEFT JOIN se.data.fact_booking booking
                       ON booking.se_sale_id = s.se_sale_id AND calendar.date_value = booking.booking_completed_date
    WHERE s.start_date BETWEEN '2020-05-01' AND '2020-09-30'
    GROUP BY 1, 2, 3, 4, 5
),
     agg_to_global AS (
--aggregate up to global sale level and rank
         SELECT sales.global_sale_id,
                sales.age_weeks,
                SUM(territory_margin)                                            AS margin,
                PERCENT_RANK() OVER (PARTITION BY age_weeks ORDER BY margin ASC) AS percent_rank,
                CASE
                    WHEN percent_rank >= 0.9 THEN '1. Platinum'
                    WHEN percent_rank >= 0.75 THEN '2. Gold'
                    WHEN percent_rank >= 0.5 THEN '3. Silver'
                    WHEN percent_rank >= 0.25 THEN '4. Bronze'
                    WHEN percent_rank >= 0.1 THEN '5. Copper'
                    ELSE '6. Non-metal aka dirt' END                             AS sale_band
         FROM sales
         GROUP BY 1, 2
     )

SELECT ag.age_weeks,
       ag.sale_band,
       round(min(ag.margin), 0) AS min_band
FROM agg_to_global ag
GROUP BY 1, 2
ORDER BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------

WITH sales AS ( --at territory sale level, to compute age and margin
    SELECT s.se_sale_id,
           s.salesforce_opportunity_id                                             AS global_sale_id,
           FLOOR(DATEDIFF('day', s.start_date::DATE, calendar.date_value) / 7) + 1 AS age_weeks,
           s.start_date::DATE                                                      AS sale_start_date,
           s.end_date::DATE                                                        AS sale_end_date,
           SUM(IFF(booking.booking_status_type IN ('live', 'cancelled'),
                   booking.margin_gross_of_toms_gbp_constant_currency, 0))         AS territory_margin
    FROM se.data.se_sale_attributes s
             LEFT JOIN se.data.se_calendar calendar
                       ON s.start_date::DATE <= calendar.date_value
                           AND calendar.date_value <= s.end_date::DATE
                           AND calendar.date_value <= current_date
             LEFT JOIN se.data.fact_booking booking
                       ON booking.se_sale_id = s.se_sale_id AND calendar.date_value = booking.booking_completed_date
    WHERE s.start_date BETWEEN '2020-05-01' AND '2020-09-30'
    GROUP BY 1, 2, 3, 4, 5
),
     agg_to_global AS (
--aggregate up to global sale level and rank
         SELECT sales.global_sale_id,
                sales.age_weeks,
                SUM(territory_margin)                                                        AS margin,
                PERCENT_RANK() OVER (PARTITION BY age_weeks ORDER BY margin ASC)             AS percent_rank1,
                CASE
                    WHEN percent_rank1 >= 0.9 THEN '1. Platinum'
                    WHEN percent_rank1 >= 0.75 THEN '2. Gold'
                    WHEN percent_rank1 >= 0.5 THEN '3. Silver'
                    WHEN percent_rank1 >= 0.25 THEN '4. Bronze'
                    WHEN percent_rank1 >= 0.1 THEN '5. Copper'
                    ELSE '6. Non-metal aka dirt' END                                         AS sale_band1,
                PERCENT_RANK() OVER (PARTITION BY age_weeks, margin = 0 ORDER BY margin ASC) AS percent_rank2,
                CASE
                    WHEN percent_rank2 >= 0.9 THEN '1. Platinum'
                    WHEN percent_rank2 >= 0.75 THEN '2. Gold'
                    WHEN percent_rank2 >= 0.5 THEN '3. Silver'
                    WHEN percent_rank2 >= 0.25 THEN '4. Bronze'
                    WHEN percent_rank2 >= 0.1 THEN '5. Copper'
                    ELSE '6. Non-metal aka dirt' END                                         AS sale_band2

         FROM sales
         GROUP BY 1, 2
     )

SELECT 'inc 0 margin sales'                 AS percentile_type,
       ag1.age_weeks,
       ag1.sale_band1 as sale_band,
       round(min(ag1.margin), 0) AS min_band
FROM agg_to_global ag1
GROUP BY 1, 2, 3

UNION ALL

SELECT 'ex 0 margin sales'                 AS percentile_type,
       ag2.age_weeks,
       ag2.sale_band2 as sale_band,
       round(min(ag2.margin), 0) AS min_band
FROM agg_to_global ag2
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
;


------------------------------------------------------------------------------------------------------------------------
WITH sales AS ( --at territory sale level, to compute age and margin
    SELECT s.se_sale_id,
           s.salesforce_opportunity_id                                             AS global_sale_id,
           FLOOR(DATEDIFF('day', s.start_date::DATE, calendar.date_value) / 7) + 1 AS age_weeks,
           s.start_date::DATE                                                      AS sale_start_date,
           s.end_date::DATE                                                        AS sale_end_date,
           SUM(IFF(booking.booking_status_type IN ('live', 'cancelled'),
                   booking.margin_gross_of_toms_gbp_constant_currency, 0))         AS territory_margin
    FROM se.data.se_sale_attributes s
             LEFT JOIN se.data.se_calendar calendar
                       ON s.start_date::DATE <= calendar.date_value
                           AND calendar.date_value <= s.end_date::DATE
                           AND calendar.date_value <= current_date
             LEFT JOIN se.data.fact_booking booking
                       ON booking.se_sale_id = s.se_sale_id AND calendar.date_value = booking.booking_completed_date
    WHERE s.start_date BETWEEN '2020-05-01' AND '2020-09-30'
    GROUP BY 1, 2, 3, 4, 5
),
     agg_to_global AS (
--aggregate up to global sale level and rank
         SELECT sales.global_sale_id,
                sales.age_weeks,
                SUM(territory_margin)                                                        AS margin,
                PERCENT_RANK() OVER (PARTITION BY age_weeks ORDER BY margin ASC)             AS percent_rank1,
                CASE
                    WHEN percent_rank1 >= 0.9 THEN '1. Platinum'
                    WHEN percent_rank1 >= 0.75 THEN '2. Gold'
                    WHEN percent_rank1 >= 0.5 THEN '3. Silver'
                    WHEN percent_rank1 >= 0.25 THEN '4. Bronze'
                    WHEN percent_rank1 >= 0.1 THEN '5. Copper'
                    ELSE '6. Non-metal aka dirt' END                                         AS sale_band1,
                PERCENT_RANK() OVER (PARTITION BY age_weeks, margin = 0 ORDER BY margin ASC) AS percent_rank2,
                CASE
                    WHEN percent_rank2 >= 0.9 THEN '1. Platinum'
                    WHEN percent_rank2 >= 0.75 THEN '2. Gold'
                    WHEN percent_rank2 >= 0.5 THEN '3. Silver'
                    WHEN percent_rank2 >= 0.25 THEN '4. Bronze'
                    WHEN percent_rank2 >= 0.1 THEN '5. Copper'
                    ELSE '6. Non-metal aka dirt' END                                         AS sale_band2

         FROM sales
         GROUP BY 1, 2
     )

SELECT 'inc 0 margin sales'                 AS percentile_type,
       global_sale_id,
       ag1.age_weeks,
       ag1.sale_band1 as sale_band,
       round(min(ag1.margin), 0) AS min_band
FROM agg_to_global ag1
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT 'ex 0 margin sales'                 AS percentile_type,
       global_sale_id,
       ag2.age_weeks,
       ag2.sale_band2 as sale_band,
       round(min(ag2.margin), 0) AS min_band
FROM agg_to_global ag2
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4

SELECT *
FROM data_vault_mvp.dwh.tb_booking b;

