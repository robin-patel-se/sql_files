WITH spvs AS (
    SELECT sts.se_sale_id,
           sts.event_tstamp::date AS event_date,
           COUNT(*)               AS spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba
                        ON sts.touch_id = stba.touch_id
                            AND stba.touch_hostname_territory IS DISTINCT FROM 'SE TECH'
    GROUP BY 1, 2
),
     combine_data AS (
         SELECT sa.se_sale_id,
                sa.salesforce_opportunity_id,
                sa.data_model,
                sa.product_type,
                sa.sale_type,
                sa.product_configuration,
                sa.start_date         AS sale_start_date,
                sa.end_date           AS sale_end_date,
                cal.date_value        AS sale_date,
                COALESCE(spv.spvs, 0) AS spvs,
                spvs > 9              AS sale_live_flag
         FROM se.data.se_sale_attributes sa
                  LEFT JOIN se.data.se_calendar cal ON cal.date_value >= sa.start_date
             AND cal.date_value <= sa.end_date
                  LEFT JOIN spvs spv ON spv.se_sale_id = sa.se_sale_id
             AND spv.event_date = cal.date_value
         WHERE DATE_TRUNC('YEAR', cal.date_value) = '2019-01-01'
     )
SELECT cd.se_sale_id,
       cd.salesforce_opportunity_id,
       cd.data_model,
       cd.product_type,
       cd.sale_type,
       cd.product_configuration,
       cd.sale_start_date,
       cd.sale_end_date,
       DATE_TRUNC('MONTH', sale_date)                  AS sale_month,
       DATE_TRUNC('WEEK', sale_date)                   AS sale_week,
       SUM(CASE WHEN sale_live_flag THEN 1 ELSE 0 END) AS number_of_live_dates
FROM combine_data cd
WHERE DATE_TRUNC('MONTH', sale_date) = '2019-07-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10;


USE WAREHOUSE pipe_xlarge;


WITH spvs AS (
    SELECT sts.se_sale_id,
           sts.event_tstamp::date AS event_date,
           COUNT(*)               AS spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba
                        ON sts.touch_id = stba.touch_id
                            AND stba.touch_hostname_territory IS DISTINCT FROM 'SE TECH'
    GROUP BY 1, 2
    HAVING COUNT(*) > 9
)
SELECT sc.date_value,
       sc.week_start,
       ssa.se_sale_id,
       ssa.salesforce_opportunity_id,
       ssa.start_date,
       ssa.end_date,
       ssa.product_configuration,
       s.spvs
FROM se.data.se_calendar sc
         LEFT JOIN se.data.se_sale_attributes ssa ON sc.date_value BETWEEN ssa.start_date AND ssa.end_date
         INNER JOIN spvs s ON ssa.se_sale_id = s.se_sale_id AND sc.date_value = s.event_date
WHERE sc.date_value BETWEEN '2019-07-01' AND '2019-07-31';


USE WAREHOUSE pipe_xlarge;

WITH spvs AS (
    SELECT sts.se_sale_id,
           sts.event_tstamp::date AS event_date,
           COUNT(*)               AS spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba
                        ON sts.touch_id = stba.touch_id
                            AND stba.touch_hostname_territory IS DISTINCT FROM 'SE TECH'
    GROUP BY 1, 2
    HAVING COUNT(*) > 9
),
     sales_by_dates AS (
         SELECT sc.date_value,
                sc.week_start,
                DATE_TRUNC(MONTH, sc.date_value) AS month,
                ssa.se_sale_id,
                ssa.salesforce_opportunity_id,
                ssa.start_date,
                ssa.end_date,
                ssa.product_configuration,
                s.spvs
         FROM se.data.se_calendar sc
                  LEFT JOIN se.data.se_sale_attributes ssa ON sc.date_value BETWEEN ssa.start_date AND ssa.end_date
                  INNER JOIN spvs s ON ssa.se_sale_id = s.se_sale_id AND sc.date_value = s.event_date
         WHERE sc.date_value BETWEEN '2019-01-01' AND '2019-12-31'
           AND ssa.product_configuration IN ('Hotel', 'Hotel Plus')
     ),
     live_sales_day AS (
         --aggregate up to date,
         SELECT sbd.date_value,
                COUNT(DISTINCT sbd.se_sale_id) AS live_sales
         FROM sales_by_dates sbd
         GROUP BY 1
     )
--aggregate to month
SELECT DATE_TRUNC(MONTH, lsd.date_value) AS month,
       AVG(lsd.live_sales)
FROM live_sales_day lsd
GROUP BY 1
;



SELECT DATE_TRUNC(MONTH, fcb.booking_completed_date) AS month,
       ds.product_configuration,
       COUNT(*)
FROM se.data.fact_complete_booking fcb
         LEFT JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE fcb.booking_completed_date BETWEEN '2019-01-01' AND '2019-12-31'
GROUP BY 1, 2;

SELECT DATE_TRUNC(MONTH, fcb.booking_completed_date) AS month,
       AVG(fcb.booking_lead_time_days)               AS avg_lead_days,
       COUNT(*)                                      AS bookings
FROM se.data.fact_complete_booking fcb
         LEFT JOIN se.data.dim_sale ds ON fcb.se_sale_id = ds.se_sale_id
WHERE fcb.booking_completed_date BETWEEN '2019-01-01' AND '2019-12-31'
  AND ds.product_configuration IN ('Hotel', 'Hotel Plus')
GROUP BY 1;

SELECT *
FROM se.data.fact_complete_booking fcb
WHERE fcb.booking_lead_time_days < 0

airflow backfill --start_date '2021-02-15 07:00:00' --end_date '2021-02-15 07:00:00' --task_regex '.*' se_data_object_creation__daily_at_07h00





SELECT * FROm se.data.athena_email_reporting aer;