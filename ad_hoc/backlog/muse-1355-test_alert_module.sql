WITH agg_spvs AS (
    SELECT date,
           DAYNAME(date)   AS day,
           dsb.territory,
           SUM(dsb.spvs)   AS spvs,
           SUM(dsb.margin) AS margin
    FROM data_vault_mvp.bi.daily_spvs_bookings dsb
    WHERE dsb.date BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE
    GROUP BY 1, 2, 3
),
     spvs_alert AS (
         SELECT ags.date,
                ags.day,
                ags.territory,
                ags.spvs,
                ROUND(AVG(ags.spvs) OVER (
                    PARTITION BY ags.territory ORDER BY ags.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING))          AS rolling_7_day_avg_spvs,
                ROUND(AVG(ags.spvs) OVER (
                    PARTITION BY ags.territory, ags.day ORDER BY ags.date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)) AS rolling_4_week_day_of_week_avg_spvs,
                ROUND((rolling_7_day_avg_spvs + rolling_4_week_day_of_week_avg_spvs) / 2)                            AS weighted_average_spvs,
                CASE
                    WHEN weighted_average_spvs < 1000 THEN '✅' -- arbitrary ignore to remove lower level volatile territories
                    WHEN (ags.spvs / NULLIF(weighted_average_spvs, 0)) - 1 > 0.1 THEN '😁'
                    WHEN (ags.spvs / NULLIF(weighted_average_spvs, 0)) - 1 < -0.1 THEN '❌'
                    ELSE '✅'
                    END                                                                                              AS status,
                IFF(status = '❌', TRUE, FALSE)                                                                       AS should_investigate,
                CASE
                    WHEN status = '✅' THEN 'Within Normal Limits'
                    WHEN status = '😁' THEN 'Above Upper Threshold'
                    WHEN status = '❌' THEN 'Below Lower Threshold'
                    END                                                                                              AS status_detail
         FROM agg_spvs ags
     ),
     bookings_alert AS (
         SELECT ags.date,
                ags.day,
                ags.territory,
                ags.margin,
                ROUND(AVG(ags.margin) OVER (
                    PARTITION BY ags.territory ORDER BY ags.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING))          AS rolling_7_day_avg_bookings,
                ROUND(AVG(ags.margin) OVER (
                    PARTITION BY ags.territory, ags.day ORDER BY ags.date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)) AS rolling_4_week_day_of_week_avg_bookings,
                ROUND((rolling_7_day_avg_bookings + rolling_4_week_day_of_week_avg_bookings) / 2)                    AS weighted_average_bookings,
                CASE
                    WHEN weighted_average_bookings < 100 THEN '✅' -- arbitrary ignore to remove lower level volatile territories
                    WHEN (ags.margin / NULLIF(weighted_average_bookings, 0)) - 1 > 0.2 THEN '😁'
                    WHEN (ags.margin / NULLIF(weighted_average_bookings, 0)) - 1 < -0.2 THEN '❌'
                    ELSE '✅'
                    END                                                                                              AS status,
                IFF(status = '❌', TRUE, FALSE)                                                                       AS should_investigate,
                CASE
                    WHEN status = '✅' THEN 'Within Normal Limits'
                    WHEN status = '😁' THEN 'Above Upper Threshold'
                    WHEN status = '❌' THEN 'Below Lower Threshold'
                    END                                                                                              AS status_detail
         FROM agg_spvs ags
     )

SELECT sa.date,
       sa.day,
       sa.territory              AS dim_1,
       NULL                      AS dim_2,
       NULL                      AS dim_3,
       NULL                      AS dim_4,
       'daily_spvs_by_territory' AS alert_type,
       sa.spvs                   AS actual,
       sa.weighted_average_spvs  AS comparator,
       sa.status,
       sa.should_investigate,
       sa.status_detail
FROM spvs_alert sa
WHERE sa.date = CURRENT_DATE - 1
UNION ALL
SELECT ba.date,
       ba.day,
       ba.territory                  AS dim_1,
       NULL                          AS dim_2,
       NULL                          AS dim_3,
       NULL                          AS dim_4,
       'daily_bookings_by_territory' AS alert_type,
       ba.margin                     AS actual,
       ba.weighted_average_bookings  AS comparator,
       ba.status,
       ba.should_investigate,
       ba.status_detail
FROM bookings_alert ba
WHERE ba.date = CURRENT_DATE - 1;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.daily_spvs_bookings CLONE data_vault_mvp.bi.daily_spvs_bookings;

self_describing_task --include 'dv/data_quality/data_quality_checks.py'  --method 'run' --start '2022-03-20 00:00:00' --end '2022-03-20 00:00:00' \
self_describing_task --include 'dv/data_quality/data_quality_checks.py'  --method 'run' --start '2022-03-21 00:00:00' --end '2022-03-21 00:00:00' \
self_describing_task --include 'dv/data_quality/data_quality_checks.py'  --method 'run' --start '2022-03-22 00:00:00' --end '2022-03-22 00:00:00' \
self_describing_task --include 'dv/data_quality/data_quality_checks.py'  --method 'run' --start '2022-03-23 00:00:00' --end '2022-03-23 00:00:00'


DROP TABLE data_vault_mvp_dev_robin.data_quality.data_quality_checks;
SELECT *
FROM data_vault_mvp_dev_robin.data_quality.data_quality_checks;

SELECT DISTINCT toi.flight_validating_airline_id
FROM se.data.tb_order_item toi
WHERE toi.flight_validating_airline_id IS NOT NULL;



-- SELECT ags.date,
--                 ags.day,
--                 ags.territory,
--                 ags.margin,
--                 ROUND(AVG(ags.margin) OVER (
--                     PARTITION BY ags.territory ORDER BY ags.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING))          AS rolling_7_day_avg_bookings,
--                 ROUND(AVG(ags.margin) OVER (
--                     PARTITION BY ags.territory, ags.day ORDER BY ags.date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)) AS rolling_4_week_day_of_week_avg_bookings,
--                 ROUND((rolling_7_day_avg_bookings + rolling_4_week_day_of_week_avg_bookings) / 2)                    AS weighted_average_bookings,
--                 CASE
--                     WHEN weighted_average_bookings < 100 THEN '✅' -- arbitrary ignore to remove lower level volatile territories
--                     WHEN (ags.margin / NULLIF(weighted_average_bookings, 0)) - 1 > 0.2 THEN '😁'
--                     WHEN (ags.margin / NULLIF(weighted_average_bookings, 0)) - 1 < -0.2 THEN '❌'
--                     ELSE '✅'
--                     END                                                                                              AS status,
--                 IFF(status = '❌', TRUE, FALSE)                                                                       AS should_investigate,
--                 CASE
--                     WHEN status = '✅' THEN 'Within Normal Limits'
--                     WHEN status = '😁' THEN 'Above Upper Threshold'
--                     WHEN status = '❌' THEN 'Below Lower Threshold'
--                     END                                                                                              AS status_detail
--          FROM agg_spvs ags

WITH agg_margin AS (
    SELECT fb.booking_completed_date          AS date,
           DAYNAME(fb.booking_completed_date) AS day,
           fb.territory,
           SUM(fb.margin_gross_of_toms_gbp)   AS margin
    FROM data_vault_mvp.dwh.fact_booking fb
    WHERE fb.booking_status_type = 'live'
      AND fb.booking_completed_date BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE - 1
    GROUP BY 1, 2, 3
)
SELECT am.date,
       am.day,
       am.territory,
       am.margin,
       ROUND(AVG(am.margin) OVER (
           PARTITION BY am.territory ORDER BY am.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING))         AS rolling_7_day_avg_margin,
       ROUND(AVG(am.margin) OVER (
           PARTITION BY am.territory, am.day ORDER BY am.date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)) AS rolling_4_week_day_of_week_avg_margin,
       ROUND((rolling_7_day_avg_margin + rolling_4_week_day_of_week_avg_margin) / 2)                     AS weighted_average_margin,
       10000                                                                                             AS low_volume_comparator_threshold,
       CASE
           WHEN weighted_average_margin < low_volume_comparator_threshold THEN '✅' -- arbitrary ignore to remove lower level volatile territories
           WHEN (am.margin / NULLIF(weighted_average_margin, 0)) - 1 > 0.2 THEN '😁'
           WHEN (am.margin / NULLIF(weighted_average_margin, 0)) - 1 < -0.2 THEN '❌'
           ELSE '✅'
           END                                                                                           AS status,
       IFF(status = '❌', TRUE, FALSE)                                                                    AS should_investigate,
       CASE
           WHEN weighted_average_margin < low_volume_comparator_threshold THEN 'Low Volume Comparator'
           WHEN status = '✅' THEN 'Within Normal Limits'
           WHEN status = '😁' THEN 'Above Upper Threshold'
           WHEN status = '❌' THEN 'Below Lower Threshold'
           END                                                                                           AS status_detail
FROM agg_margin am


CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;

DROP TABLE data_vault_mvp_dev_robin.data_quality.data_quality_checks;

SELECT *
FROM data_vault_mvp_dev_robin.data_quality.data_quality_checks;



self_describing_task --include 'dv/data_quality/data_quality_checks.py'  --method 'run' --start '2022-04-03 00:00:00' --end '2022-04-03 00:00:00'


WITH agg_spvs AS (
    SELECT date,
           DAYNAME(date) AS day,
           SUM(dsb.spvs) AS spvs
    FROM data_vault_mvp.bi.daily_spvs_bookings dsb
    WHERE dsb.date BETWEEN CURRENT_DATE - 37 AND CURRENT_DATE
    GROUP BY 1, 2
)
SELECT ass.date,
       ass.day,
       'daily_spvs'                                                                                                AS alert_type,
       ass.spvs,
       ROUND(AVG(ass.spvs) OVER (ORDER BY ass.date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING))                      AS rolling_7_day_avg_margin,
       ROUND(AVG(ass.spvs) OVER (PARTITION BY ass.day ORDER BY ass.date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)) AS rolling_4_week_day_of_week_avg_margin,
       ROUND((rolling_7_day_avg_margin + rolling_4_week_day_of_week_avg_margin) / 2)                               AS weighted_average_margin,
       5000000                                                                                                     AS low_volume_comparator_threshold,
       IFF(weighted_average_margin > low_volume_comparator_threshold, TRUE, FALSE)                                 AS comparator_above_low_volume_comparator_threshold,
       0.1                                                                                                         AS variance_allowance,
       ass.spvs / NULLIF(weighted_average_margin, 0) - 1                                                           AS actual_comparator_diff,
       IFF(ABS(ass.spvs / NULLIF(weighted_average_margin, 0)) - 1 > 0.1, TRUE, FALSE)                              AS actual_comparator_diff_exceeds_allowance,
       CASE
           WHEN weighted_average_margin < low_volume_comparator_threshold THEN '✅' -- arbitrary ignore to remove lower level volatile territories
           WHEN (ass.spvs / NULLIF(weighted_average_margin, 0)) - 1 > 0.1 THEN '😁'
           WHEN (ass.spvs / NULLIF(weighted_average_margin, 0)) - 1 < -0.1 THEN '❌'
           ELSE '✅'
           END                                                                                                     AS status,
       IFF(status = '❌', TRUE, FALSE)                                                                              AS should_investigate,
       CASE
           WHEN weighted_average_margin < low_volume_comparator_threshold THEN 'Low Volume Comparator'
           WHEN status = '✅' THEN 'Within Normal Limits'
           WHEN status = '😁' THEN 'Above Upper Threshold'
           WHEN status = '❌' THEN 'Below Lower Threshold'
           END                                                                                                     AS status_detail
FROM agg_spvs ass;



SELECT cpse.event_date::DATE,
       COUNT(*)
FROM se.bi.crm_performance_segment_events cpse
GROUP BY 1;

SELECT MIN(cpse.schedule_tstamp)
FROM data_vault_mvp.bi.crm_performance_segment_events cpse;

SELECT ss.se_sale_id,
       ss.product_configuration,
       ss.original_contractor_name,
       ss.current_contractor_name,
       ss.company_name,
       ss.supplier_name

FROM data_vault_mvp.dwh.se_sale ss
WHERE ss.product_configuration = 'Catalogue';

SELECT YEAR(booking_completed_date),
       booking_status,
       COUNT(*)
FROM se.data.se_booking sb
GROUP BY 1, 2;


SELECT MIN(bs.date_time_booked) FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs