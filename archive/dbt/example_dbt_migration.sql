SELECT
    fb.booking_completed_date,
    ds.travel_type,
    sua.email_opt_in,
    COUNT(DISTINCT fb.booking_id)    AS bookings,
    SUM(fb.margin_gross_of_toms_gbp) AS margin
FROM se.data.fact_booking fb
    LEFT JOIN se.data.dim_sale ds ON fb.se_sale_id = ds.se_sale_id
    LEFT JOIN se.data.se_user_attributes sua ON fb.shiro_user_id = sua.shiro_user_id
WHERE fb.booking_completed_date > CURRENT_DATE - 7
  AND fb.booking_status_type = 'live'
GROUP BY 1, 2, 3

-- hard coded refs
--


WITH bookings AS (
    SELECT
        fb.booking_completed_date,
        COUNT(DISTINCT fb.booking_id)    AS bookings,
        SUM(fb.margin_gross_of_toms_gbp) AS margin
    FROM se.data.fact_booking fb
    WHERE fb.booking_status_type = 'live'
      AND fb.booking_completed_date >= CURRENT_DATE - 30
    GROUP BY 1
),

     sales AS (
         SELECT
             dss.view_date,
             COUNT(DISTINCT dss.se_sale_id)                AS active_territory_sales,
             COUNT(DISTINCT dss.salesforce_opportunity_id) AS active_global_sales
         FROM se.data.dim_sale_snapshot dss
         WHERE dss.sale_active
           AND dss.view_date >= CURRENT_DATE - 30
         GROUP BY 1
     ),
     signups AS (
         SELECT
             sua.signup_tstamp::DATE           AS signup_date,
             COUNT(DISTINCT sua.shiro_user_id) AS signups
         FROM se.data.se_user_attributes sua
         WHERE sua.signup_tstamp >= CURRENT_DATE - 30
         GROUP BY 1
     )
SELECT
    sc.date_value,
    sc.day_name,
    b.bookings,
    b.margin,
    s.active_territory_sales,
    s.active_global_sales,
    su.signups
FROM se.data.se_calendar sc
    LEFT JOIN bookings b ON sc.date_value = b.booking_completed_date
    LEFT JOIN sales s ON sc.date_value = s.view_date
    LEFT JOIN signups su ON sc.date_value = su.signup_date
WHERE sc.date_value BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
;

     spvs AS (
         SELECT
             sts.event_tstamp::DATE AS spv_date,
             COUNT(*)
         FROM se.data.scv_touched_spvs sts
         WHERE sts.event_tstamp >= CURRENT_DATE - 30
         GROUP BY 1
     )