WITH bookings_table AS (
    SELECT
        fb.*,
        DATE_TRUNC('month', fb.booking_completed_date)                                      AS booking_completed_month,
        DATE_TRUNC('month', fb.cancellation_date)::date                                     AS cancellation_month,
        CASE
            WHEN fb.booking_status_type = 'cancelled' AND booking_completed_month <> cancellation_month
                THEN margin_gross_of_toms_gbp_constant_currency
            ELSE 0 END                                                                      AS margin_actual_canx_post_month,
        IFF(fb.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0) AS margin_actual,
        IFF(fb.booking_status_type = 'live', margin_gross_of_toms_gbp, 0)                   AS margin_actual_reported_rate,
        COALESCE(margin_actual, 0) +
        COALESCE(margin_actual_canx_post_month, 0)                                          AS margin_actual_net_in_month_canx
    FROM se.data.fact_booking fb
),
     bookings AS (
         SELECT
             booking_completed_date::DATE                        AS target_date,
             'margin'                                            AS target_name,
             s.posu_cluster                                      AS dimension_1,
             CASE
                 WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                     THEN '3PP/WRD'
                 --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                 --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                     THEN 'Catalogue' --Temp fix for CA
                 WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                     THEN 'Hotel'
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                     THEN 'IHP' -- Temp fix for CA
                 WHEN s.sale_type IN ('N/A')
                     THEN NULL
                 ELSE s.sale_type
                 END                                             AS dimension_2,
             se.data.posa_category_from_territory(fcb.territory) AS dimension_3,
             SUM(margin_actual)                                  AS margin_actual,
             SUM(margin_actual_reported_rate)                    AS margin_actual_reported_rate,
             SUM(margin_actual_net_in_month_canx)                AS margin_actual_net_in_month_canx
         FROM bookings_table fcb
             LEFT JOIN se.data.dim_sale s ON fcb.se_sale_id = s.se_sale_id
         WHERE booking_completed_date::DATE >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5
     ),
     bookings_v2 AS (
         SELECT
             booking_completed_date::DATE                             AS target_date,
             'margin_v2'                                              AS target_name,
             s.posu_cluster                                           AS dimension_1,
             CASE
                 WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                     THEN '3PP/WRD'
                 --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                 --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                     THEN 'Catalogue' --Temp fix for CA
                 WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                     THEN 'Hotel'
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                     THEN 'IHP' --Temp fix for CA
                 WHEN s.sale_type IN ('N/A')
                     THEN NULL
                 ELSE s.sale_type
                 END                                                  AS dimension_2,
             IFF(se.data.posa_category_from_territory(fcb.territory) = 'Scandi', fcb.territory,
                 se.data.posa_category_from_territory(fcb.territory)) AS dimension_3,
             s.cm_region                                              AS dimension_4,
             SUM(margin_actual)                                       AS margin_actual,
             SUM(margin_actual_reported_rate)                         AS margin_actual_reported_rate,
             SUM(margin_actual_net_in_month_canx)                     AS margin_actual_net_in_month_canx
         FROM bookings_table fcb
             LEFT JOIN se.data.dim_sale s ON fcb.se_sale_id = s.se_sale_id
         WHERE booking_completed_date::DATE >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     new_sales AS (
         SELECT
             CAST(s.sale_start_date AS DATE) AS target_date,
             'new deals'                     AS target_name,
             s.posu_cluster                  AS dimension_1,
             COUNT(*)                        AS new_sales_actual
         FROM se.data.dim_sale s
         WHERE CAST(s.sale_start_date AS DATE) >= '2020-10-01'
         GROUP BY 1, 2, 3
     )
SELECT
    COALESCE(targets.target_date, bookings.target_date, bookings_v2.target_date,
             new_sales.target_date)                                 AS target_date,
    COALESCE(targets.dimension_1, bookings.dimension_1, bookings_v2.dimension_1, new_sales.dimension_1,
             'Other')                                               AS dimension_1,
    COALESCE(targets.dimension_2, bookings.dimension_2, bookings_v2.dimension_2,
             'Other')                                               AS dimension_2,
    COALESCE(targets.dimension_3, bookings.dimension_3, bookings_v2.dimension_3,
             'Other')                                               AS dimension_3,
    COALESCE(targets.dimension_4, bookings_v2.dimension_4, 'Other') AS dimension_4,
    COALESCE(targets.dimension_5, 'Other')                          AS dimension_5,
    COALESCE(targets.target_name, bookings.target_name, bookings_v2.target_name, new_sales.target_name,
             'Other')                                               AS target_name,
    COALESCE(targets.target_value, 0)                               AS target_value,
    COALESCE(bookings.margin_actual, bookings_v2.margin_actual, new_sales.new_sales_actual,
             0)                                                     AS target_actual,
    COALESCE(bookings.margin_actual_reported_rate, bookings_v2.margin_actual_reported_rate,
             0)                                                     AS target_actual_reported_rate,
    COALESCE(bookings.margin_actual_net_in_month_canx, bookings_v2.margin_actual_net_in_month_canx,
             0)                                                     AS target_actual_net_in_month_canx

FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets targets
    FULL OUTER JOIN bookings ON targets.target_date = bookings.target_date
    AND targets.target_name = bookings.target_name
    AND targets.dimension_1 = bookings.dimension_1
    AND targets.dimension_2 = bookings.dimension_2
    AND targets.dimension_3 = bookings.dimension_3
    AND targets.target_name = bookings.target_name
    FULL OUTER JOIN bookings_v2 ON targets.target_date = bookings_v2.target_date
    AND targets.target_name = bookings_v2.target_name
    AND targets.dimension_1 = bookings_v2.dimension_1
    AND targets.dimension_2 = bookings_v2.dimension_2
    AND targets.dimension_3 = bookings_v2.dimension_3
    AND targets.dimension_4 = bookings_v2.dimension_4
    AND targets.target_name = bookings_v2.target_name
    FULL OUTER JOIN new_sales ON new_sales.target_date = targets.target_date
    AND new_sales.target_name = targets.target_name
    AND new_sales.dimension_1 = targets.dimension_1
;
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
WITH bookings_table AS (
    SELECT
        f.*,
        DATE_TRUNC('month', f.booking_completed_date)                                      AS booking_completed_month,
        DATE_TRUNC('month', f.cancellation_date)                                           AS cancellation_month,
        IFF(f.booking_status_type = 'cancelled' AND booking_completed_month <> cancellation_month,
            margin_gross_of_toms_gbp_constant_currency, 0)                                 AS margin_actual_canx_post_month,
        IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0) AS margin_actual,
        IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp, 0)                   AS margin_actual_reported_rate,
        COALESCE(margin_actual, 0) + COALESCE(margin_actual_canx_post_month, 0)            AS margin_actual_net_in_month_canx
    FROM se.data.fact_booking f
),
     bookings AS (
         SELECT
             booking_completed_date::DATE                        AS target_date,
             'margin'                                            AS target_name,
             s.posu_cluster                                      AS dimension_1,
             CASE
                 WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                     THEN '3PP/WRD'
                 --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                 --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                     THEN 'Catalogue' --Temp fix for CA
                 WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                     THEN 'Hotel'
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                     THEN 'IHP' -- Temp fix for CA
                 WHEN s.sale_type IN ('N/A')
                     THEN NULL
                 ELSE s.sale_type
                 END                                             AS dimension_2,
             se.data.posa_category_from_territory(fcb.territory) AS dimension_3,
             SUM(margin_actual)                                  AS margin_actual,
             SUM(margin_actual_reported_rate)                    AS margin_actual_reported_rate,
             SUM(margin_actual_net_in_month_canx)                AS margin_actual_net_in_month_canx
         FROM bookings_table fcb
             LEFT JOIN se.data.dim_sale s ON fcb.se_sale_id = s.se_sale_id
         WHERE booking_completed_date::DATE >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5
     ),
     bookings_v2 AS (
         SELECT
             booking_completed_date::DATE                             AS target_date,
             'margin_v2'                                              AS target_name,
             s.posu_cluster                                           AS dimension_1,
             CASE
                 WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                     THEN '3PP/WRD'
                 --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                 --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                     THEN 'Catalogue' --Temp fix for CA
                 WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                     THEN 'Hotel'
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                     THEN 'IHP' --Temp fix for CA
                 WHEN s.sale_type IN ('N/A')
                     THEN NULL
                 ELSE s.sale_type
                 END                                                  AS dimension_2,
             IFF(se.data.posa_category_from_territory(fcb.territory) = 'Scandi', fcb.territory,
                 se.data.posa_category_from_territory(fcb.territory)) AS dimension_3,
             s.cm_region                                              AS dimension_4,
             SUM(margin_actual)                                       AS margin_actual,
             SUM(margin_actual_reported_rate)                         AS margin_actual_reported_rate,
             SUM(margin_actual_net_in_month_canx)                     AS margin_actual_net_in_month_canx
         FROM bookings_table fcb
             LEFT JOIN se.data.dim_sale s ON fcb.se_sale_id = s.se_sale_id
         WHERE booking_completed_date::DATE >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     new_sales AS (
         SELECT
             CAST(s.sale_start_date AS DATE) AS target_date,
             'new deals'                     AS target_name,
             s.posu_cluster                  AS dimension_1,
             COUNT(*)                        AS new_sales_actual
         FROM se.data.dim_sale s
         WHERE CAST(s.sale_start_date AS DATE) >= '2020-10-01'
         GROUP BY 1, 2, 3
     ),
     cluster_sub_region_raw_actuals AS (
         -- compute actuals for cluster sub region based on 6 dimension granularity
         SELECT
             f.booking_completed_date,
             d.posu_cluster                                                                          AS dimension_1,
             CASE
                 WHEN d.sale_type = 'Hotel' OR d.sale_type = 'Hotel Plus' THEN 'Hotel'
                 WHEN UPPER(d.sale_type) LIKE 'IHP%' AND LOWER(d.supplier_name) NOT LIKE 'secret escapes%' THEN 'T15/T20'
                 WHEN UPPER(d.sale_type) LIKE 'WRD%' OR d.sale_type = '3PP' THEN '3PP/WRD'
                 WHEN UPPER(d.sale_type) LIKE 'IHP%' OR d.sale_type = 'Catalogue' AND LOWER(d.supplier_name) LIKE 'secret escapes%' THEN 'CL Prepackage'
                 ELSE 'Other'
                 END                                                                                 AS dimension_2,
             CASE
                 WHEN d.posa_territory = 'DE' OR d.posa_territory = 'CH' THEN 'DACH'
                 WHEN d.posa_territory = 'UK' THEN d.posa_territory
                 ELSE 'ROW'
                 END                                                                                 AS dimension_3,
             d.cm_region                                                                             AS dimension_4,
             d.posu_cluster_region                                                                   AS dimension_5,
             d.posu_cluster_sub_region                                                               AS dimension_6,
             SUM(IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0)) AS margin_actual,
             SUM(IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp, 0))                   AS margin_actual_reported_rate,
             SUM(IFF(f.booking_status_type = 'cancelled' AND DATE_TRUNC('month', f.booking_completed_date) <> DATE_TRUNC('month', f.cancellation_date),
                     margin_gross_of_toms_gbp_constant_currency, 0))                                 AS margin_actual_canx_post_month,
             margin_actual + margin_actual_canx_post_month                                           AS margin_actual_net_in_month_canx
         FROM se.data.fact_booking f
             LEFT JOIN se.data.dim_sale d ON f.se_sale_id = d.se_sale_id
         WHERE f.territory NOT IN ('TL', 'PL')
         GROUP BY 1, 2, 3, 4, 5, 6, 7
     ),
     cluster_sub_region_grain AS (
         -- compute cluster sub region grain and blow out by calendar
         SELECT DISTINCT
             sc.date_value               AS target_date,
             'cluster_sub_region_target' AS target_name,
             ds.posu_cluster             AS dimension_1,
             CASE
                 WHEN ds.sale_type = 'Hotel' OR ds.sale_type = 'Hotel Plus' THEN 'Hotel'
                 WHEN UPPER(ds.sale_type) LIKE 'IHP%' AND LOWER(ds.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
                 WHEN UPPER(ds.sale_type) LIKE 'WRD%' OR ds.sale_type = '3PP' THEN '3PP/WRD'
                 WHEN UPPER(ds.sale_type) LIKE 'IHP%' OR ds.sale_type = 'Catalogue' AND LOWER(ds.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
                 ELSE 'Other'
                 END                     AS dimension_2,
             CASE
                 WHEN ds.posa_territory = 'DE' OR ds.posa_territory = 'CH' THEN 'DACH'
                 WHEN ds.posa_territory = 'UK' THEN ds.posa_territory
                 ELSE 'ROW'
                 END                     AS dimension_3,
             ds.cm_region                AS dimension_4,
             ds.posu_cluster_region      AS dimension_5,
             ds.posu_cluster_sub_region  AS dimension_6
         FROM data_vault_mvp.dwh.dim_sale ds
             LEFT JOIN data_vault_mvp.dwh.se_calendar sc ON sc.date_value BETWEEN '2022-01-01' AND CURRENT_DATE
         WHERE ds.posa_territory NOT IN ('TL', 'PL')
     ),
     cluster_sub_region_grain_actuals AS (
         -- attach actuals to cluster sub region grain
         SELECT
             g.target_date,
             g.target_name,
             g.dimension_1,
             g.dimension_2,
             g.dimension_3,
             g.dimension_4,
             g.dimension_5,
             g.dimension_6,
             COALESCE(a.margin_actual, 0)                   AS margin_actual,
             COALESCE(a.margin_actual_reported_rate, 0)     AS margin_actual_reported_rate,
             COALESCE(a.margin_actual_net_in_month_canx, 0) AS margin_actual_net_in_month_canx
         FROM cluster_sub_region_grain g
             LEFT JOIN cluster_sub_region_raw_actuals a ON
                     g.target_date = a.booking_completed_date
                 AND g.dimension_1 = a.dimension_1
                 AND g.dimension_2 = a.dimension_2
                 AND g.dimension_3 = a.dimension_3
                 AND g.dimension_4 = a.dimension_4
                 AND g.dimension_5 = a.dimension_5
                 AND g.dimension_6 = a.dimension_6
     )

SELECT
    COALESCE(targets.target_date, bookings.target_date, bookings_v2.target_date, new_sales.target_date, cluster_sub_region_grain_actuals.target_date)          AS target_date,
    COALESCE(targets.dimension_1, bookings.dimension_1, bookings_v2.dimension_1, new_sales.dimension_1, cluster_sub_region_grain_actuals.dimension_1, 'Other') AS dimension_1,
    COALESCE(targets.dimension_2, bookings.dimension_2, bookings_v2.dimension_2, cluster_sub_region_grain_actuals.dimension_2, 'Other')                        AS dimension_2,
    COALESCE(targets.dimension_3, bookings.dimension_3, bookings_v2.dimension_3, cluster_sub_region_grain_actuals.dimension_3, 'Other')                        AS dimension_3,
    COALESCE(targets.dimension_4, bookings_v2.dimension_4, cluster_sub_region_grain_actuals.dimension_4, 'Other')                                              AS dimension_4,
    COALESCE(targets.dimension_5, cluster_sub_region_grain_actuals.dimension_5, 'Other')                                                                       AS dimension_5,
    COALESCE(targets.dimension_6, cluster_sub_region_grain_actuals.dimension_6, 'Other')                                                                       AS dimension_6,
    COALESCE(targets.target_name, bookings.target_name, bookings_v2.target_name, new_sales.target_name, cluster_sub_region_grain_actuals.target_name, 'Other') AS target_name,
    COALESCE(targets.target_value, 0)                                                                                                                          AS target_value,
    COALESCE(bookings.margin_actual, bookings_v2.margin_actual, new_sales.new_sales_actual, cluster_sub_region_grain_actuals.margin_actual, 0)                 AS target_actual,
    COALESCE(bookings.margin_actual_reported_rate, bookings_v2.margin_actual_reported_rate, cluster_sub_region_grain_actuals.margin_actual_reported_rate, 0)   AS target_actual_reported_rate,
    COALESCE(bookings.margin_actual_net_in_month_canx, bookings_v2.margin_actual_net_in_month_canx, cluster_sub_region_grain_actuals.margin_actual_net_in_month_canx,
             0)                                                                                                                                                AS target_actual_net_in_month_canx

FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets targets
    FULL OUTER JOIN bookings ON
            targets.target_date = bookings.target_date
        AND targets.target_name = bookings.target_name
        AND targets.dimension_1 = bookings.dimension_1
        AND targets.dimension_2 = bookings.dimension_2
        AND targets.dimension_3 = bookings.dimension_3
        AND targets.target_name = bookings.target_name
    FULL OUTER JOIN bookings_v2 ON
            targets.target_date = bookings_v2.target_date
        AND targets.target_name = bookings_v2.target_name
        AND targets.dimension_1 = bookings_v2.dimension_1
        AND targets.dimension_2 = bookings_v2.dimension_2
        AND targets.dimension_3 = bookings_v2.dimension_3
        AND targets.dimension_4 = bookings_v2.dimension_4
        AND targets.target_name = bookings_v2.target_name
    FULL OUTER JOIN new_sales ON
            targets.target_date = new_sales.target_date
        AND targets.target_name = new_sales.target_name
        AND targets.dimension_1 = new_sales.dimension_1
    FULL OUTER JOIN cluster_sub_region_grain_actuals ON
            targets.target_date = cluster_sub_region_grain_actuals.target_date
        AND targets.target_name = cluster_sub_region_grain_actuals.target_name
        AND targets.dimension_1 = cluster_sub_region_grain_actuals.dimension_1
        AND targets.dimension_2 = cluster_sub_region_grain_actuals.dimension_2
        AND targets.dimension_3 = cluster_sub_region_grain_actuals.dimension_3
        AND targets.dimension_4 = cluster_sub_region_grain_actuals.dimension_4
        AND targets.dimension_5 = cluster_sub_region_grain_actuals.dimension_5
        AND targets.dimension_6 = cluster_sub_region_grain_actuals.dimension_6


--create a base table of granularity from sale tables
SELECT DISTINCT
    sc.date_value,
    'cluster_sub_region' AS target_name, -- TODO update
    ds.posu_cluster,
    CASE
        WHEN ds.sale_type = 'Hotel' OR ds.sale_type = 'Hotel Plus' THEN 'Hotel'
        WHEN UPPER(ds.sale_type) LIKE 'IHP%' AND LOWER(ds.supplier_name) NOT LIKE 'secret escapes%' THEN 'T15/T20'
        WHEN UPPER(ds.sale_type) LIKE 'WRD%' OR ds.sale_type = '3PP' THEN '3PP/WRD'
        WHEN UPPER(ds.sale_type) LIKE 'IHP%' OR ds.sale_type = 'Catalogue' AND LOWER(ds.supplier_name) LIKE 'secret escapes%' THEN 'CL Prepackage'
        ELSE 'Other'
        END              AS product_sale_grouping,
    CASE
        WHEN ds.posa_territory = 'DE' OR ds.posa_territory = 'CH' THEN 'DACH'
        WHEN ds.posa_territory = 'UK' THEN ds.posa_territory
        ELSE 'ROW'
        END              AS posa_condensed,
    ds.cm_region,
    ds.posu_cluster_region,
    ds.posu_cluster_sub_region
FROM data_vault_mvp.dwh.dim_sale ds
    LEFT JOIN data_vault_mvp.dwh.se_calendar sc ON sc.date_value BETWEEN '2022-01-01' AND CURRENT_DATE



------------------------------------------------------------------------------------------------------------------------


/*
TARGET_NAME
margin_v2
Gross_Adds_Target
margin
BoardPlan
new deals
Net_Adds_Target
margin_v3
*/

SELECT
    gt.dimension_1, -- posu cluster
    gt.dimension_2, -- sale type
    gt.dimension_3, -- territory
    gt.dimension_4, -- cm region
    gt.dimension_5, -- NA
    gt.dimension_6,
    COUNT(*)
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
WHERE gt.target_name = 'margin_v2'
GROUP BY 1, 2, 3, 4, 5, 6;


SELECT
    gt.dimension_1, -- posu cluster
    gt.dimension_2, -- sale type
    gt.dimension_3, -- territory
    gt.dimension_4,
    gt.dimension_5,
    gt.dimension_6,
    COUNT(*)
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
WHERE gt.target_name = 'margin'
GROUP BY 1, 2, 3, 4, 5, 6;


SELECT *
FROM se.data.crm_jobs_list cjl;



SELECT DISTINCT
    ds.posu_cluster,
    CASE
        WHEN ds.sale_type = 'Hotel' OR ds.sale_type = 'Hotel Plus' THEN 'Hotel'
        WHEN UPPER(ds.sale_type) LIKE 'IHP%' AND LOWER(ds.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
        WHEN UPPER(ds.sale_type) LIKE 'WRD%' OR ds.sale_type = '3PP' THEN '3PP/WRD'
        WHEN UPPER(ds.sale_type) LIKE 'IHP%' OR ds.sale_type = 'Catalogue' AND LOWER(ds.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
        ELSE 'Other'
        END AS product_sale_grouping,
    CASE
        WHEN ds.posa_territory = 'DE' OR ds.posa_territory = 'CH' THEN 'DACH'
        WHEN ds.posa_territory = 'UK' THEN ds.posa_territory
        ELSE 'ROW'
        END AS posa_condensed,
    ds.cm_region,
    ds.posu_cluster_region,
    ds.posu_cluster_sub_region
FROM data_vault_mvp.dwh.dim_sale ds;



SELECT DISTINCT
    ds.posu_cluster,
    CASE
        WHEN ds.sale_type = 'Hotel' OR ds.sale_type = 'Hotel Plus' THEN 'Hotel'
        WHEN UPPER(ds.sale_type) LIKE 'IHP%' AND LOWER(ds.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
        WHEN UPPER(ds.sale_type) LIKE 'WRD%' OR ds.sale_type = '3PP' THEN '3PP/WRD'
        WHEN UPPER(ds.sale_type) LIKE 'IHP%' OR ds.sale_type = 'Catalogue' AND LOWER(ds.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
        ELSE 'Other'
        END AS product_sale_grouping,
    CASE
        WHEN ds.posa_territory = 'DE' OR ds.posa_territory = 'CH' THEN 'DACH'
        WHEN ds.posa_territory = 'UK' THEN ds.posa_territory
        ELSE 'ROW'
        END AS posa_condensed,
    ds.cm_region,
    ds.posu_cluster_region,
    ds.posu_cluster_sub_region
FROM data_vault_mvp.dwh.dim_sale ds
WHERE ds.posa_territory NOT IN ('TL', 'PL')
;

SELECT *
FROM se.data.dim_sale ds
WHERE ds.sale_type = 'N/A';

------------------------------------------------------------------------------------------------------------------------

WITH bookings_table AS (
    SELECT
        f.*,
        DATE_TRUNC('month', f.booking_completed_date)                                      AS booking_completed_month,
        DATE_TRUNC('month', f.cancellation_date)                                           AS cancellation_month,
        IFF(f.booking_status_type = 'cancelled' AND booking_completed_month <> cancellation_month,
            margin_gross_of_toms_gbp_constant_currency, 0)                                 AS margin_actual_canx_post_month,
        IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0) AS margin_actual,
        IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp, 0)                   AS margin_actual_reported_rate,
        COALESCE(margin_actual, 0) + COALESCE(margin_actual_canx_post_month, 0)            AS margin_actual_net_in_month_canx
    FROM se.data.fact_booking f
),
     bookings_v2 AS (
         SELECT
             booking_completed_date::DATE                             AS target_date,
             'margin_v2'                                              AS target_name,
             s.posu_cluster                                           AS dimension_1,
             CASE
                 WHEN s.sale_type IN ('3PP', 'WRD', 'WRD - direct')
                     THEN '3PP/WRD'
                 --WHEN s.sale_type IN ('IHP - C', 'IHP - dynamic', 'IHP - static')
                 --  THEN 'IHP' --removed for now whilst we wait for logic from Niro and Kirsten
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) LIKE 'secret escapes%'
                     THEN 'Catalogue' --Temp fix for CA
                 WHEN s.sale_type IN ('Hotel', 'Hotel Plus')
                     THEN 'Hotel'
                 WHEN s.sale_type IN ('IHP - static', 'IHP - dynamic', 'IHP - C', 'Catalogue')
                     AND LOWER(s.supplier_name) NOT LIKE 'secret escapes%'
                     THEN 'IHP' --Temp fix for CA
                 WHEN s.sale_type IN ('N/A')
                     THEN NULL
                 ELSE s.sale_type
                 END                                                  AS dimension_2,
             IFF(se.data.posa_category_from_territory(fcb.territory) = 'Scandi', fcb.territory,
                 se.data.posa_category_from_territory(fcb.territory)) AS dimension_3,
             s.cm_region                                              AS dimension_4,
             SUM(margin_actual)                                       AS margin_actual,
             SUM(margin_actual_reported_rate)                         AS margin_actual_reported_rate,
             SUM(margin_actual_net_in_month_canx)                     AS margin_actual_net_in_month_canx
         FROM bookings_table fcb
             LEFT JOIN se.data.dim_sale s ON fcb.se_sale_id = s.se_sale_id
         WHERE booking_completed_date::DATE >= '2018-01-01'
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     new_sales AS (
         SELECT
             CAST(s.sale_start_date AS DATE) AS target_date,
             'new deals'                     AS target_name,
             s.posu_cluster                  AS dimension_1,
             COUNT(*)                        AS new_sales_actual
         FROM se.data.dim_sale s
         WHERE CAST(s.sale_start_date AS DATE) >= '2020-10-01'
         GROUP BY 1, 2, 3
     ),
     cluster_sub_region_raw_actuals AS (
         -- compute actuals for cluster sub region based on 6 dimension granularity
         SELECT
             f.booking_completed_date,
             d.posu_cluster                                                                          AS dimension_1,
             CASE
                 WHEN d.sale_type = 'Hotel' OR d.sale_type = 'Hotel Plus' THEN 'Hotel'
                 WHEN UPPER(d.sale_type) LIKE 'IHP%' AND LOWER(d.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
                 WHEN UPPER(d.sale_type) LIKE 'WRD%' OR d.sale_type = '3PP' THEN '3PP/WRD'
                 WHEN UPPER(d.sale_type) LIKE 'IHP%' OR d.sale_type = 'Catalogue' AND LOWER(d.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
                 ELSE 'Other'
                 END                                                                                 AS dimension_2,
             CASE
                 WHEN d.posa_territory = 'DE' OR d.posa_territory = 'CH' THEN 'DACH'
                 WHEN d.posa_territory = 'UK' THEN d.posa_territory
                 ELSE 'ROW'
                 END                                                                                 AS dimension_3,
             d.cm_region                                                                             AS dimension_4,
             d.posu_cluster_region                                                                   AS dimension_5,
             d.posu_cluster_sub_region                                                               AS dimension_6,
             SUM(IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp_constant_currency, 0)) AS margin_actual,
             SUM(IFF(f.booking_status_type = 'live', margin_gross_of_toms_gbp, 0))                   AS margin_actual_reported_rate,
             SUM(IFF(f.booking_status_type = 'cancelled' AND DATE_TRUNC('month', f.booking_completed_date) <> DATE_TRUNC('month', f.cancellation_date),
                     margin_gross_of_toms_gbp_constant_currency, 0))                                 AS margin_actual_canx_post_month,
             margin_actual + margin_actual_canx_post_month                                           AS margin_actual_net_in_month_canx
         FROM se.data.fact_booking f
             LEFT JOIN se.data.dim_sale d ON f.se_sale_id = d.se_sale_id
         WHERE f.territory NOT IN ('TL', 'PL', 'IE')
         GROUP BY 1, 2, 3, 4, 5, 6, 7
     ),
     cluster_sub_region_grain AS (
         -- compute cluster sub region grain and blow out by calendar
         SELECT DISTINCT
             sc.date_value               AS target_date,
             'cluster_sub_region_target' AS target_name,
             ds.posu_cluster             AS dimension_1,
             CASE
                 WHEN ds.sale_type = 'Hotel' OR ds.sale_type = 'Hotel Plus' THEN 'Hotel'
                 WHEN UPPER(ds.sale_type) LIKE 'IHP%' AND LOWER(ds.supplier_name) NOT LIKE 'secret escapes%' THEN 'IHP'
                 WHEN UPPER(ds.sale_type) LIKE 'WRD%' OR ds.sale_type = '3PP' THEN '3PP/WRD'
                 WHEN UPPER(ds.sale_type) LIKE 'IHP%' OR ds.sale_type = 'Catalogue' AND LOWER(ds.supplier_name) LIKE 'secret escapes%' THEN 'Catalogue'
                 ELSE 'Other'
                 END                     AS dimension_2,
             CASE
                 WHEN ds.posa_territory = 'DE' OR ds.posa_territory = 'CH' THEN 'DACH'
                 WHEN ds.posa_territory = 'UK' THEN ds.posa_territory
                 ELSE 'ROW'
                 END                     AS dimension_3,
             ds.cm_region                AS dimension_4,
             ds.posu_cluster_region      AS dimension_5,
             ds.posu_cluster_sub_region  AS dimension_6
         FROM data_vault_mvp.dwh.dim_sale ds
             LEFT JOIN data_vault_mvp.dwh.se_calendar sc ON sc.date_value BETWEEN '2022-01-01' AND CURRENT_DATE
         WHERE ds.posa_territory NOT IN ('TL', 'PL', 'IE')
     ),
     cluster_sub_region_grain_actuals AS (
         -- attach actuals to cluster sub region grain
         SELECT
             g.target_date,
             g.target_name,
             g.dimension_1,
             g.dimension_2,
             g.dimension_3,
             g.dimension_4,
             g.dimension_5,
             g.dimension_6,
             COALESCE(a.margin_actual, 0)                   AS margin_actual,
             COALESCE(a.margin_actual_reported_rate, 0)     AS margin_actual_reported_rate,
             COALESCE(a.margin_actual_net_in_month_canx, 0) AS margin_actual_net_in_month_canx
         FROM cluster_sub_region_grain g
             LEFT JOIN cluster_sub_region_raw_actuals a ON
                     g.target_date = a.booking_completed_date
                 AND g.dimension_1 = a.dimension_1
                 AND g.dimension_2 = a.dimension_2
                 AND g.dimension_3 = a.dimension_3
                 AND g.dimension_4 = a.dimension_4
                 AND g.dimension_5 = a.dimension_5
                 AND g.dimension_6 = a.dimension_6
     )

SELECT
    COALESCE(targets.target_date, bookings.target_date, bookings_v2.target_date, new_sales.target_date, cluster_sub_region_grain_actuals.target_date)          AS target_date,
    COALESCE(targets.dimension_1, bookings.dimension_1, bookings_v2.dimension_1, new_sales.dimension_1, cluster_sub_region_grain_actuals.dimension_1, 'Other') AS dimension_1,
    COALESCE(targets.dimension_2, bookings.dimension_2, bookings_v2.dimension_2, cluster_sub_region_grain_actuals.dimension_2, 'Other')                        AS dimension_2,
    COALESCE(targets.dimension_3, bookings.dimension_3, bookings_v2.dimension_3, cluster_sub_region_grain_actuals.dimension_3, 'Other')                        AS dimension_3,
    COALESCE(targets.dimension_4, bookings_v2.dimension_4, cluster_sub_region_grain_actuals.dimension_4, 'Other')                                              AS dimension_4,
    COALESCE(targets.dimension_5, cluster_sub_region_grain_actuals.dimension_5, 'Other')                                                                       AS dimension_5,
    COALESCE(targets.dimension_6, cluster_sub_region_grain_actuals.dimension_6, 'Other')                                                                       AS dimension_6,
    COALESCE(targets.target_name, bookings.target_name, bookings_v2.target_name, new_sales.target_name, cluster_sub_region_grain_actuals.target_name, 'Other') AS target_name,
    COALESCE(targets.target_value, 0)                                                                                                                          AS target_value,
    COALESCE(bookings.margin_actual, bookings_v2.margin_actual, new_sales.new_sales_actual, cluster_sub_region_grain_actuals.margin_actual, 0)                 AS target_actual,
    COALESCE(bookings.margin_actual_reported_rate, bookings_v2.margin_actual_reported_rate, cluster_sub_region_grain_actuals.margin_actual_reported_rate, 0)   AS target_actual_reported_rate,
    COALESCE(bookings.margin_actual_net_in_month_canx, bookings_v2.margin_actual_net_in_month_canx, cluster_sub_region_grain_actuals.margin_actual_net_in_month_canx,
             0)                                                                                                                                                AS target_actual_net_in_month_canx

FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets targets
    FULL OUTER JOIN bookings ON
            targets.target_date = bookings.target_date
        AND targets.target_name = bookings.target_name
        AND targets.dimension_1 = bookings.dimension_1
        AND targets.dimension_2 = bookings.dimension_2
        AND targets.dimension_3 = bookings.dimension_3
        AND targets.target_name = bookings.target_name
    FULL OUTER JOIN bookings_v2 ON
            targets.target_date = bookings_v2.target_date
        AND targets.target_name = bookings_v2.target_name
        AND targets.dimension_1 = bookings_v2.dimension_1
        AND targets.dimension_2 = bookings_v2.dimension_2
        AND targets.dimension_3 = bookings_v2.dimension_3
        AND targets.dimension_4 = bookings_v2.dimension_4
        AND targets.target_name = bookings_v2.target_name
    FULL OUTER JOIN new_sales ON
            targets.target_date = new_sales.target_date
        AND targets.target_name = new_sales.target_name
        AND targets.dimension_1 = new_sales.dimension_1
    FULL OUTER JOIN cluster_sub_region_grain_actuals ON
            targets.target_date = cluster_sub_region_grain_actuals.target_date
        AND targets.target_name = cluster_sub_region_grain_actuals.target_name
        AND targets.dimension_1 = cluster_sub_region_grain_actuals.dimension_1
        AND targets.dimension_2 = cluster_sub_region_grain_actuals.dimension_2
        AND targets.dimension_3 = cluster_sub_region_grain_actuals.dimension_3
        AND targets.dimension_4 = cluster_sub_region_grain_actuals.dimension_4
        AND targets.dimension_5 = cluster_sub_region_grain_actuals.dimension_5
        AND targets.dimension_6 = cluster_sub_region_grain_actuals.dimension_6
