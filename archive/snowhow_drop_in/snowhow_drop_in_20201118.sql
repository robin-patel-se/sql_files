WITH sess_bookings AS (
    SELECT stt.touch_id,
           COUNT(*)                                                                                        AS bookings,
           SUM(fcb.margin_gross_of_toms_gbp)                                                               AS margin,
           SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN 1 ELSE 0 END)                     AS ho_bookings,
           SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN fcb.margin_gross_of_toms_gbp END) AS ho_margin,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                        posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN 1
                   ELSE 0 END)                                                                             AS uk_ho_bookings,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                        posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN fcb.margin_gross_of_toms_gbp
                   ELSE 0 END)                                                                             AS uk_ho_margin,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                       THEN 1
                   ELSE 0 END)                                                                             AS dach_ho_bookings,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                       THEN fcb.margin_gross_of_toms_gbp
                   ELSE 0 END)                                                                             AS dach_ho_margin,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN 1
                   ELSE 0 END)                                                                             AS it_ho_bookings,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN fcb.margin_gross_of_toms_gbp
                   ELSE 0 END)                                                                             AS it_ho_margin,
           SUM(CASE WHEN ds.product_type = 'Package' THEN 1 ELSE 0 END)                                    AS p_bookings,
           SUM(CASE WHEN ds.product_type = 'Package' THEN fcb.margin_gross_of_toms_gbp ELSE 0 END)         AS p_margin,
           SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN 1 ELSE 0 END)                               AS "3pp_bookings",
           SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN fcb.margin_gross_of_toms_gbp ELSE 0 END)    AS "3pp_margin"
    FROM se.data.scv_touched_transactions stt
             INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
             LEFT JOIN se.data.dim_sale ds ON fcb.sale_id = ds.se_sale_id
    WHERE stt.event_tstamp >= '2020-01-01'

    GROUP BY 1
)
   , sess_spvs AS (
    SELECT s.touch_id,
           COUNT(*)                                                                    AS spvs,
           COUNT(DISTINCT s.se_sale_id)                                                AS unique_spvs,
           SUM(CASE WHEN ds.product_type IN ('Hotel', 'Hotel Plus') THEN 1 ELSE 0 END) AS ho_spvs,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND
                        posu_country IN ('England', 'Scotland', 'Wales', 'Northern Ireland') THEN 1
                   ELSE 0 END)                                                         AS uk_ho_spvs,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Germany', 'Austria', 'Switzerland')
                       THEN 1
                   ELSE 0 END)                                                         AS dach_ho_spvs,
           SUM(CASE
                   WHEN ds.product_type IN ('Hotel', 'Hotel Plus') AND posu_country IN ('Italy') THEN 1
                   ELSE 0 END)                                                         AS it_ho_spvs,
           SUM(CASE WHEN ds.product_type = 'Package' THEN 1 ELSE 0 END)                AS p_spvs,
           SUM(CASE WHEN ds.product_type = 'WRD - direct' THEN 1 ELSE 0 END)           AS "3pp_spvs"
    FROM se.data.scv_touched_spvs s
             LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.se_sale_id
    WHERE s.event_tstamp >= '2020-01-01'
    GROUP BY 1
)
SELECT stba.touch_start_tstamp::DATE                                 AS day,
       stmc.touch_mkt_channel,
       stba.touch_experience,
       stmc.touch_affiliate_territory                                AS touch_hostname_territory,
       COUNT(DISTINCT stba.touch_id)                                 AS sessions,
       COUNT(DISTINCT stba.attributed_user_id_hash)                  AS users,
       COUNT(DISTINCT CASE
                          WHEN stba.stitched_identity_type = 'se_user_id'
                              THEN stba.attributed_user_id_hash END) AS logged_in_users,
       COALESCE(SUM(b.bookings), 0)                                  AS bookings,
       COALESCE(SUM(b.margin), 0)                                    AS margin,
       COALESCE(SUM(b.ho_bookings), 0)                               AS ho_bookings,
       COALESCE(SUM(b.ho_margin), 0)                                 AS ho_margin,
       COALESCE(SUM(CASE
                        WHEN stmc.touch_affiliate_territory = 'DE' THEN b.dach_ho_bookings
                        WHEN stmc.touch_affiliate_territory = 'UK' THEN uk_ho_bookings
                        WHEN stmc.touch_affiliate_territory = 'IT' THEN it_ho_bookings
                        ELSE 0 END), 0)                              AS dom_ho_bookings,
       COALESCE(SUM(CASE
                        WHEN stmc.touch_affiliate_territory = 'DE' THEN b.dach_ho_margin
                        WHEN stmc.touch_affiliate_territory = 'UK' THEN uk_ho_margin
                        WHEN stmc.touch_affiliate_territory = 'IT' THEN it_ho_margin
                        ELSE 0 END), 0)                              AS dom_ho_margin,
       COALESCE(SUM(b.p_bookings + b."3pp_bookings"), 0)             AS p_bookings,
       COALESCE(SUM(b.p_margin + b."3pp_margin"), 0)                 AS p_margin,
       COALESCE(SUM(s.spvs), 0)                                      AS spvs,
       COALESCE(SUM(s.unique_spvs), 0)                               AS unique_spvs,
       COALESCE(SUM(CASE
                        WHEN stmc.touch_affiliate_territory = 'DE' THEN dach_ho_spvs
                        WHEN stmc.touch_affiliate_territory = 'UK' THEN uk_ho_spvs
                        WHEN stmc.touch_affiliate_territory = 'IT' THEN it_ho_spvs
                        ELSE 0 END), 0)                              AS dom_ho_spvs,
       COALESCE(SUM(s.ho_spvs), 0)                                   AS ho_spvs,
       0                                                             AS hp_spvs,
       COALESCE(SUM(s.p_spvs + s."3pp_spvs"), 0)                     AS p_spvs,
       0                                                             AS "3pp_spvs"
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         LEFT JOIN sess_bookings b ON stba.touch_id = b.touch_id
         LEFT JOIN sess_spvs s ON stba.touch_id = s.touch_id
WHERE stba.touch_start_tstamp >= '2020-01-01'
  AND stmc.touch_affiliate_territory IN ('UK', 'DE', 'IT')
  AND stba.touch_start_tstamp < current_date
GROUP BY 1, 2, 3, 4;


--combined channel metric split with booking

ALTER SESSION SET week_start = 1
SELECT stba.touch_start_tstamp:: DATE             AS date,
       extract(WEEK FROM stba.touch_start_tstamp) AS week,
       extract(YEAR FROM stba.touch_start_tstamp) AS year,
       stmc.touch_mkt_channel                     AS channel,
       touch_affiliate_territory,
       count(DISTINCT attributed_user_id)         AS members,
       count(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN attributed_user_id
                          ELSE NULL END)          AS app_members,
       count(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN attributed_user_id
                          ELSE NULL END)          AS web_members,
       COUNT(DISTINCT stba.touch_id)              AS sessions,
       count(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN stba.touch_id
                          ELSE NULL END)          AS app_sessions,
       count(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN stba.touch_id
                          ELSE NULL END)          AS web_sessions,
       count(DISTINCT sts.event_hash)             AS spvs,
       count(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN sts.event_hash
                          ELSE NULL END)          AS app_spvs,
       count(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN sts.event_hash
                          ELSE NULL END)          AS web_spvs,
       count(DISTINCT stt.booking_id)             AS bookings,
       count(DISTINCT CASE
                          WHEN stba.touch_experience LIKE 'native app%' THEN stt.booking_id
                          ELSE NULL END)          AS app_bookings,
       count(DISTINCT CASE
                          WHEN stba.touch_experience NOT LIKE 'native app%' THEN stt.booking_id
                          ELSE NULL END)          AS web_bookings
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
         LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
         LEFT JOIN se.data.scv_touched_transactions stt ON stmc.touch_id = stt.touch_id
         LEFT JOIN se.data.se_booking sb
                   ON stt.booking_id = sb.booking_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND booking_status = 'COMPLETE'
  AND extract(YEAR FROM stba.touch_start_tstamp) >= 2019
GROUP BY 1, 2, 3, 4, 5;

------------------------------------------------------------------------------------------------------------------------

WITH margin AS (
    SELECT s.salesforce_opportunity_id,
           SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin
    FROM se.data.fact_complete_booking fcb
             LEFT JOIN se.data.se_sale_attributes s ON fcb.sale_id = s.se_sale_id
    GROUP BY 1
)
SELECT ssa.salesforce_opportunity_id                                        AS global_sale_id,
       ssa.data_model,
       ssa.product_configuration,
       ssa.posu_cluster,
       ssa.posu_sub_region,
       ssa.posu_region,
       ssa.company_name,
       ssa.current_contractor_name,
       ssa.original_contractor_name,
       ssa.original_joint_contractor_name,
       m.margin,
       SUM(ssa.active)                                                      AS total_active_sales,
       MAX(ssa.active)                                                      AS max_active_sales,
       COUNT(ssa.se_sale_id)                                                AS total_sales,
       MIN(ssa.start_date::DATE)                                            AS min_start_date,
       MAX(ssa.end_date::DATE)                                              AS max_end_date,
       MAX(CASE WHEN ssa.end_date::DATE < current_date THEN 1 ELSE 0 END)   AS end_date_past,
       MAX(CASE WHEN ssa.end_date::DATE > current_date THEN 1 ELSE 0 END)   AS end_date_future,
       MAX(CASE WHEN ssa.start_date::DATE < current_date THEN 1 ELSE 0 END) AS start_date_past,
       MAX(CASE WHEN ssa.start_date::DATE > current_date THEN 1 ELSE 0 END) AS start_date_future,
       CASE WHEN end_date_past > 0 THEN 'Y' ELSE 'N' END                    AS end_date_in_the_past,
       CASE WHEN end_date_future > 0 THEN 'Y' ELSE 'N' END                  AS end_date_in_the_future,
       CASE WHEN start_date_past > 0 THEN 'Y' ELSE 'N' END                  AS start_date_in_the_past,
       CASE WHEN start_date_future > 0 THEN 'Y' ELSE 'N' END                AS start_date_in_the_future,
       CASE WHEN max_active_sales = 0 THEN 'N' ELSE 'Y' END                 AS any_sale_that_is_active
FROM se.data.se_sale_attributes ssa
         LEFT JOIN margin m ON ssa.salesforce_opportunity_id = m.salesforce_opportunity_id
WHERE ssa.product_configuration = 'Hotel'
  AND ssa.data_model = 'New Data Model'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;

------------------------------------------------------------------------------------------------------------------------
--Santana

WITH sales AS (
    SELECT sale.posu_cluster,
           sale.posu_major_region,
           sale.salesforce_opportunity_id                                                  AS global_sale_id,
           FLOOR(datediff('day', sale.start_date, booking.booking_completed_date) / 7) + 1 AS age_weeks,
           SUM(COALESCE(booking.margin_gross_of_toms_gbp_constant_currency, 0))            AS margin,
           PERCENT_RANK()
                   OVER (PARTITION BY sale.posu_cluster, age_weeks ORDER BY margin ASC)    AS percent_rank,
           CASE
               WHEN percent_rank >= 0.75 THEN '1. Gold'
               WHEN percent_rank >= 0.5 THEN '2. Silver'
               WHEN percent_rank >= 0.25 THEN '3. Bronze'
               ELSE '4. Below Bronze' END                                                  AS sale_band
    FROM se.data.se_sale_attributes sale
             LEFT JOIN se.data.fact_booking booking ON booking.se_sale_id = sale.se_sale_id
    WHERE booking.booking_completed_date BETWEEN sale.start_date AND dateadd('day', 27, sale.start_date)
      AND booking.booking_status_type IN ('live', 'cancelled')
      AND sale.start_date BETWEEN '2020-05-01' AND '2020-09-30'
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
