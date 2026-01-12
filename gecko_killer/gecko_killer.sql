--hourly spvs
SELECT
    se.data.posa_category_from_territory(COALESCE(
            se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
            REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
            REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
        ))                            AS territory,
    DATE_TRUNC('hour', event_tstamp)  AS hour,
    event_tstamp::DATE = CURRENT_DATE AS today,
    e.event_tstamp_yesterday,
    e.event_tstamp_today_last_week,
    e.event_tstamp_today_ly,
    e.event_tstamp_today_lly,
    e.event_tstamp_today_2019,
    COUNT(*)                          AS spvs
FROM data_vault_mvp.dwh.trimmed_event_stream e
WHERE (
        ( -- old world native app event data
                    e.collector_tstamp < '2020-02-28 00:00:00'
                AND
                    e.se_sale_id IS NOT NULL
            )
        OR
        ( -- new world native app event data
                    e.collector_tstamp >= '2020-02-28 00:00:00'
                AND
                    e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
            )
    )
  AND DATE_TRUNC('hour', event_tstamp) < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
  AND territory IS DISTINCT FROM 'Poland'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

------------------------------------------------------------------------------------------------------------------------
--hourly bookings + margin
SELECT
    se.data.posa_category_from_territory(bs.territory)          AS territory,
    DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
    ds.product_configuration,
    sc.today,
    sc.yesterday,
    sc.today_last_week,
    sc.today_ly,
    sc.today_lly,
    sc.today_2019,
    ROUND(SUM(IFF(bs.currency = 'GBP',
                  bs.margin_gross_of_toms_cc,
                  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
    ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
    COUNT(DISTINCT bs.booking_id)                               AS bookings
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
    INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
    LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
    LEFT JOIN  hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
            (CURRENT_DATE) >= cc.start_date AND
            (CURRENT_DATE) <= cc.end_date AND
            cc.currency = 'GBP' AND
            cc.category = 'Primary' AND
            bs.currency = cc.base_currency
WHERE (
              sc.today
              OR sc.yesterday
              OR sc.today_last_week
              OR sc.today_ly
              OR sc.today_lly
              OR sc.today_2019
          )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9



------------------------------------------------------------------------------------------------------------------------
--top 10 spvs

WITH spv_counts AS (
    SELECT
        e.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR           AS sale_name,
        e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR AS se_sale_id,
        se.data.posa_category_from_territory(COALESCE(
                se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
                REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
                        'UK'),
                REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
            ))                                                                       AS territory,
        COUNT(*)                                                                     AS spvs
    FROM data_vault_mvp.dwh.trimmed_event_stream e
    WHERE e.event_tstamp::DATE = CURRENT_DATE
      AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
    GROUP BY 1, 2, 3
)
SELECT *
FROM spv_counts sc
QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.spvs DESC) <= 10;

USE WAREHOUSE pipe_large;
WITH spv_counts AS (
    SELECT
        e.contexts_com_secretescapes_content_context_1[0]['name']::VARCHAR           AS sale_name,
        e.contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR AS se_sale_id,
        se.data.posa_category_from_territory(COALESCE(
                se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
                REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
                        'UK'),
                REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
            ))                                                                       AS territory,
        COUNT(*)                                                                     AS spvs
    FROM data_vault_mvp.dwh.trimmed_event_stream e
    WHERE e.event_tstamp::DATE = CURRENT_DATE
      AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
    GROUP BY 1, 2, 3
),
     top_ten_sales_by_territory AS (
         SELECT
             sc.sale_name,
             sc.se_sale_id,
             sc.territory,
             sc.spvs
         FROM spv_counts sc
         QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.spvs DESC) <= 10
     ),
     lifetime_spvs AS (
         SELECT
             sts.se_sale_id,
             se.data.posa_category_from_territory(stmc.touch_affiliate_territory) AS territory,
             COUNT(*)                                                             AS lifetime_spvs
         FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
             INNER JOIN top_ten_sales_by_territory tts ON sts.se_sale_id = tts.se_sale_id AND
                                                          se.data.posa_category_from_territory(stmc.touch_affiliate_territory) =
                                                          tts.territory
         GROUP BY 1, 2
     )
SELECT
    tts.sale_name,
    tts.se_sale_id,
    ds.sale_start_date,
    ssa.company_name,
    tts.territory,
    tts.spvs,
    ls.lifetime_spvs
FROM top_ten_sales_by_territory tts
    LEFT JOIN lifetime_spvs ls ON tts.se_sale_id = ls.se_sale_id AND tts.territory = ls.territory
    LEFT JOIN se.data.se_sale_attributes ssa ON tts.se_sale_id = ssa.se_sale_id
    LEFT JOIN se.data.dim_sale ds ON tts.se_sale_id = ds.se_sale_id
;
------------------------------------------------------------------------------------------------------------------------
--top 10 margin

WITH margin_sums AS (
    SELECT
        bs.record__o:saleName::VARCHAR                     AS sale_name,
        bs.sale_id                                         AS se_sale_id,
        se.data.posa_category_from_territory(bs.territory) AS territory,
        ROUND(SUM(IFF(bs.currency = 'GBP', bs.margin_gross_of_toms_cc, margin_gross_of_toms_cc * cc.multiplier)),
              0)                                           AS margin_gbp_constant_currency,
        ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)         AS margin_gbp,
        COUNT(DISTINCT bs.booking_id)                      AS bookings
    FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
        LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
                (CURRENT_DATE) >= cc.start_date AND
                (CURRENT_DATE) <= cc.end_date AND
                cc.currency = 'GBP' AND
                cc.category = 'Primary' AND
                bs.currency = cc.base_currency
    WHERE date_time_booked::DATE = CURRENT_DATE
    GROUP BY 1, 2, 3
)
SELECT *
FROM margin_sums sc
QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.margin_gbp_constant_currency DESC) <= 10;
;

WITH margin_sums AS (
    SELECT
        bs.record__o:saleName::VARCHAR                     AS sale_name,
        bs.sale_id                                         AS se_sale_id,
        se.data.posa_category_from_territory(bs.territory) AS territory,
        ROUND(SUM(IFF(bs.currency = 'GBP', bs.margin_gross_of_toms_cc, margin_gross_of_toms_cc * cc.multiplier)),
              0)                                           AS margin_gbp_constant_currency,
        ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)         AS margin_gbp,
        COUNT(DISTINCT bs.booking_id)                      AS bookings
    FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
        LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
                (CURRENT_DATE) >= cc.start_date AND
                (CURRENT_DATE) <= cc.end_date AND
                cc.currency = 'GBP' AND
                cc.category = 'Primary' AND
                bs.currency = cc.base_currency
    WHERE date_time_booked::DATE = CURRENT_DATE
    GROUP BY 1, 2, 3
),
     top_ten_sales_by_territory AS (
         SELECT
             sc.sale_name,
             sc.se_sale_id,
             sc.territory,
             sc.margin_gbp_constant_currency,
             sc.margin_gbp,
             sc.bookings
         FROM margin_sums sc
         QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.margin_gbp_constant_currency DESC) <= 10
     ),
     lifetime_margin AS (
         SELECT
             fcb.se_sale_id,
             se.data.posa_category_from_territory(fcb.territory) AS territory,
             SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS lifetime_margin_constant_currency,
             SUM(fcb.margin_gross_of_toms_gbp)                   AS lifetime_margin,
             COUNT(DISTINCT fcb.booking_id)                      AS lifetime_bookings
         FROM se.data.fact_complete_booking fcb
             INNER JOIN top_ten_sales_by_territory tts ON fcb.se_sale_id = tts.se_sale_id AND
                                                          se.data.posa_category_from_territory(fcb.territory) =
                                                          tts.territory
         GROUP BY 1, 2
     )
SELECT
    tts.sale_name,
    tts.se_sale_id,
    tts.territory,
    ds.sale_start_date,
    ssa.company_name,
    tts.margin_gbp_constant_currency,
    tts.margin_gbp,
    tts.bookings,
    ls.lifetime_margin_constant_currency,
    ls.lifetime_margin,
    ls.lifetime_bookings
FROM top_ten_sales_by_territory tts
    LEFT JOIN lifetime_margin ls ON tts.se_sale_id = ls.se_sale_id AND tts.territory = ls.territory
    LEFT JOIN se.data.se_sale_attributes ssa ON tts.se_sale_id = ssa.se_sale_id
    LEFT JOIN se.data.dim_sale ds ON tts.se_sale_id = ds.se_sale_id
;


------------------------------------------------------------------------------------------------------------------------
--search by check in
SELECT
    TRY_TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR) AS check_in_date,
    se.data.posa_category_from_territory(COALESCE(
            se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
            REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
            REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
        ))                                                                                  AS territory,
    COUNT(*)                                                                                AS searches
FROM data_vault_mvp.dwh.trimmed_event_stream e
WHERE e.event_tstamp::DATE = CURRENT_DATE
  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
  AND e.contexts_com_secretescapes_search_context_1[0]['had_results']::BOOLEAN = TRUE
  AND TRY_TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR) >= CURRENT_DATE
  AND TRY_TO_DATE(e.contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR) BETWEEN CURRENT_DATE AND DATEADD(MONTH, 6, CURRENT_DATE)
GROUP BY 1, 2

------------------------------------------------------------------------------------------------------------------------
--search by search term

WITH agg_search_term AS (
    SELECT
        e.contexts_com_secretescapes_search_context_1[0]['location']::VARCHAR AS search_term,
        se.data.posa_category_from_territory(COALESCE(
                se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
                REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB',
                        'UK'),
                REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
            ))                                                                AS territory,
        COUNT(*)                                                              AS fulfilled_searches
    FROM data_vault_mvp.dwh.trimmed_event_stream e
    WHERE e.event_tstamp::DATE = CURRENT_DATE
      AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
      AND e.contexts_com_secretescapes_search_context_1[0]['had_results']::BOOLEAN = TRUE
      AND e.contexts_com_secretescapes_search_context_1[0]['location']::VARCHAR IS DISTINCT FROM ''
      AND e.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR IS NOT NULL
    GROUP BY 1, 2
)
SELECT *
FROM agg_search_term sc
QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.territory ORDER BY sc.fulfilled_searches DESC) <=
        10 airflow backfill --start_date '2022-01-20 00:00:00' --end_date '2022-01-21 00:00:00' --task_regex '.*' yesterday


SELECT
    event_tstamp::DATE,
    tes.event_tstamp_today_2019,
    COUNT(*)
FROM data_vault_mvp.dwh.trimmed_event_stream tes
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------


WITH target AS (
    SELECT
        se.data.posa_category_from_territory(IFF(gt.dimension_3 = 'DACH', 'DE', gt.dimension_3)) AS territory,
        SUM(gt.target_value)                                                                     AS target
    FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
    WHERE gt.target_date = CURRENT_DATE
      AND gt.target_name = 'margin_v2'
    GROUP BY 1
),
     model_bookings AS (
         SELECT
             se.data.posa_category_from_territory(bs.territory)          AS territory,
             DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
             ROUND(SUM(IFF(bs.currency = 'GBP',
                           bs.margin_gross_of_toms_cc,
                           margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency
         FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
             LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
                     (CURRENT_DATE) >= cc.start_date AND
                     (CURRENT_DATE) <= cc.end_date AND
                     cc.currency = 'GBP' AND
                     cc.category = 'Primary' AND
                     bs.currency = cc.base_currency
         WHERE bs.date_time_booked::DATE = CURRENT_DATE - 7
         GROUP BY 1, 2
     )
SELECT
    mb.territory,
    DATEADD('day', 7, mb.hour)                                                            AS hour,
    mb.margin_gbp_constant_currency                                                       AS margin,
    SUM(mb.margin_gbp_constant_currency) OVER (PARTITION BY t.territory ORDER BY mb.hour) AS cumulative_margin,
    SUM(mb.margin_gbp_constant_currency) OVER (PARTITION BY t.territory)                  AS total_margin,
    t.target,
    (cumulative_margin / total_margin) * t.target                                         AS hourly_target
FROM model_bookings mb
    LEFT JOIN target t ON mb.territory = t.territory;


WITH target AS (
    SELECT
        se.data.posa_category_from_territory(IFF(gt.dimension_3 = 'DACH', 'DE', gt.dimension_3)) AS territory,
        SUM(gt.target_value)                                                                     AS target
    FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
    WHERE gt.target_date = CURRENT_DATE
      AND gt.target_name = 'margin_v2'
    GROUP BY 1
),
     model_bookings AS (
         SELECT
             se.data.posa_category_from_territory(bs.territory)          AS territory,
             DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
             sc.today,
             sc.yesterday,
             sc.today_last_week,
             sc.today_ly,
             sc.today_lly,
             sc.today_2019,
             ROUND(SUM(IFF(bs.currency = 'GBP',
                           bs.margin_gross_of_toms_cc,
                           margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
             ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
             COUNT(DISTINCT bs.booking_id)                               AS bookings
         FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
             INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
             LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
             LEFT JOIN  hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
                     (CURRENT_DATE) >= cc.start_date AND
                     (CURRENT_DATE) <= cc.end_date AND
                     cc.currency = 'GBP' AND
                     cc.category = 'Primary' AND
                     bs.currency = cc.base_currency
         WHERE (
                       sc.today
                       OR sc.yesterday
                       OR sc.today_last_week
                       OR sc.today_ly
                       OR sc.today_lly
                       OR sc.today_2019
                   )
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
     ),
     model_targets AS (
         SELECT
             mb.territory,
             DATEADD('day', 7, mb.hour)                                           AS hour,
             mb.margin_gbp_constant_currency                                      AS margin,
             SUM(mb.margin_gbp_constant_currency) OVER (PARTITION BY t.territory) AS total_margin,
             t.target,
             (mb.margin_gbp_constant_currency / total_margin) * t.target          AS hourly_target
         FROM model_bookings mb
             LEFT JOIN target t ON mb.territory = t.territory
         WHERE mb.today_last_week
     )
SELECT
    mb.territory,
    mb.hour,
    mb.today,
    mb.yesterday,
    mb.today_last_week,
    mb.today_ly,
    mb.today_lly,
    mb.today_2019,
    mb.margin_gbp_constant_currency,
    mb.margin_gbp,
    mb.bookings,
    mt.hourly_target,
    mt.target
FROM model_bookings mb
    LEFT JOIN model_targets mt ON mb.territory = mt.territory AND mb.hour = mt.hour;



WITH model_hours AS (
    SELECT
        DATEADD('hour', h.hour, sc.date_value) AS hour,
        sc.date_value                          AS date,
        sc.today,
        sc.yesterday,
        sc.today_last_week,
        sc.today_ly,
        sc.today_lly,
        sc.today_2019
    FROM se.data.se_calendar sc
        LEFT JOIN data_vault_mvp.dwh.hour h
    WHERE (
                  sc.today
                  OR sc.yesterday
                  OR sc.today_last_week
                  OR sc.today_ly
                  OR sc.today_lly
                  OR sc.today_2019
              )
),
     daily_target AS (
         SELECT
             se.data.posa_category_from_territory(IFF(gt.dimension_3 = 'DACH', 'DE', gt.dimension_3)) AS territory,
             SUM(gt.target_value)                                                                     AS target
         FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
         WHERE gt.target_date = CURRENT_DATE
           AND gt.target_name = 'margin_v2'
         GROUP BY 1
     ),

     model_bookings AS (
         SELECT
             se.data.posa_category_from_territory(bs.territory)          AS territory,
             DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
             sc.today,
             sc.yesterday,
             sc.today_last_week,
             sc.today_ly,
             sc.today_lly,
             sc.today_2019,
             ROUND(SUM(IFF(bs.currency = 'GBP',
                           bs.margin_gross_of_toms_cc,
                           margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
             ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
             COUNT(DISTINCT bs.booking_id)                               AS bookings
         FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
             INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
             LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
             LEFT JOIN  hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
                     (CURRENT_DATE) >= cc.start_date AND
                     (CURRENT_DATE) <= cc.end_date AND
                     cc.currency = 'GBP' AND
                     cc.category = 'Primary' AND
                     bs.currency = cc.base_currency
         WHERE (
                       sc.today
                       OR sc.yesterday
                       OR sc.today_last_week
                       OR sc.today_ly
                       OR sc.today_lly
                       OR sc.today_2019
                   )
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
     ),
     distinct_territories AS (
         SELECT DISTINCT
             mb.territory
         FROM model_bookings mb
     ),
     target_grain AS (
         SELECT
             dt.territory,
             dt.target,
             mh.hour
         FROM daily_target dt
             LEFT JOIN model_hours mh
         WHERE mh.today
     ),
     model_target AS (
         -- model target phased on today LW run rate
         SELECT
             tg.territory,
             tg.hour,
             tg.target,
             SUM(COALESCE(mb.margin_gbp_constant_currency, 0))
                 OVER (PARTITION BY tg.territory)                  AS total_margin_gbp_constant_currency,
             (COALESCE(mb.margin_gbp_constant_currency, 0)
                 / total_margin_gbp_constant_currency) * tg.target AS hourly_target_gbp_constant_currency
         FROM target_grain tg
             LEFT JOIN model_bookings mb ON tg.hour = DATEADD('day', 7, mb.hour)
             AND tg.territory = mb.territory
             AND mb.today_last_week
     ),
     grain AS (
         SELECT
             mh.hour,
             mh.date,
             dt.territory,
             mh.today,
             mh.yesterday,
             mh.today_last_week,
             mh.today_ly,
             mh.today_lly,
             mh.today_2019
         FROM model_hours mh
             LEFT JOIN distinct_territories dt
     )
-- SELECT * FROM grain
SELECT
    g.hour,
    g.date,
    g.territory,
    g.today,
    g.yesterday,
    g.today_last_week,
    g.today_ly,
    g.today_lly,
    g.today_2019,
--        mt.target,
--        mt.total_margin_gbp_constant_currency,
    mt.hourly_target_gbp_constant_currency,
    mb.margin_gbp_constant_currency,
    mb.margin_gbp,
    mb.bookings
FROM grain g
    LEFT JOIN model_target mt ON g.hour = mt.hour AND g.territory = mt.territory
    LEFT JOIN model_bookings mb ON g.hour = mb.hour AND g.territory = mb.territory;

--          SELECT mb.territory,
--                 DATEADD('day', 7, mb.hour)                                           AS hour,
--                 mb.margin_gbp_constant_currency                                      AS margin,
--                 SUM(mb.margin_gbp_constant_currency) OVER (PARTITION BY t.territory) AS total_margin,
--                 t.target,
--                 (mb.margin_gbp_constant_currency / total_margin) * t.target          AS hourly_target
--          FROM model_bookings mb
--              LEFT JOIN target t ON mb.territory = t.territory
--          WHERE mb.today_last_week

------------------------------------------------------------------------------------------------------------------------
--hourly margin by product config

SELECT
    se.data.posa_category_from_territory(bs.territory)          AS territory,
    DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
    ds.product_configuration,
    ROUND(SUM(IFF(bs.currency = 'GBP',
                  bs.margin_gross_of_toms_cc,
                  margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
    INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
    LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
    LEFT JOIN  hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
            (CURRENT_DATE) >= cc.start_date AND
            (CURRENT_DATE) <= cc.end_date AND
            cc.currency = 'GBP' AND
            cc.category = 'Primary' AND
            bs.currency = cc.base_currency
WHERE sc.today
GROUP BY 1, 2, 3;


SELECT *
FROM se.data.se_user_attributes sua;


USE WAREHOUSE pipe_large;
SELECT
    se.data.posa_category_from_territory(COALESCE(
            se.data.territory_from_affiliate_url_string(e.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR),
            REPLACE(e.contexts_com_secretescapes_product_display_context_1[0]['posa_territory']::VARCHAR, 'GB', 'UK'),
            REGEXP_REPLACE(e.app_id, '[ios_app |android_app ]')
        ))                            AS territory,
    DATE_TRUNC('hour', event_tstamp)  AS hour,
    event_tstamp::DATE = CURRENT_DATE AS today,
    e.event_tstamp_yesterday,
    e.event_tstamp_today_last_week,
    e.event_tstamp_today_ly,
    e.event_tstamp_today_lly,
    e.event_tstamp_today_2019,
    COUNT(*)                          AS spvs
FROM data_vault_mvp.dwh.trimmed_event_stream e
WHERE (--app spvs
        ( -- old world native app event data
                    e.collector_tstamp < '2020-02-28 00:00:00'
                AND
                    e.contexts_com_secretescapes_sale_page_context_1 IS NOT NULL
            )
        OR
        ( -- new world native app event data
                    e.collector_tstamp >= '2020-02-28 00:00:00'
                AND
                    (
                                e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                            OR
                                e.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
                        )
            )
    )
   OR (--web spvs
        (--client side tracking, prior implementation/validation
                    e.collector_tstamp < '2020-02-28 00:00:00'
                AND (
                                e.page_urlpath LIKE '%/sale'
                            OR
                                e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                        )
                AND e.is_server_side_event = FALSE -- exclude non validated ss events
            )
        OR
        (--server side tracking, post implementation/validation
                    e.collector_tstamp >= '2020-02-28 00:00:00'
                AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                AND PARSE_URL(e.page_url, 1)['path']::VARCHAR NOT LIKE
                    '%/sale-offers' -- remove issue where spv events were firing on offer pages
                AND e.is_server_side_event = TRUE
            )
    )
   OR --wrd spvs
            e.se_category = 'web redirect click'
        AND DATE_TRUNC('hour', event_tstamp) < DATE_TRUNC('hour', CURRENT_TIMESTAMP)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


------------------------------------------------------------------------------------------------------------------------

WITH model_hours AS (
    SELECT
        DATEADD('hour', h.hour, sc.date_value) AS hour,
        sc.date_value                          AS date,
        sc.today,
        sc.yesterday,
        sc.today_last_week,
        sc.today_ly,
        sc.today_lly,
        sc.today_2019
    FROM se.data.se_calendar sc
        LEFT JOIN data_vault_mvp.dwh.hour h
    WHERE (
                  sc.today
                  OR sc.yesterday
                  OR sc.today_last_week
                  OR sc.today_ly
                  OR sc.today_lly
                  OR sc.today_2019
              )
),
     daily_target AS (
         SELECT
             se.data.posa_category_from_territory(IFF(gt.dimension_3 = 'DACH', 'DE', gt.dimension_3)) AS territory,
             SUM(gt.target_value)                                                                     AS target
         FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
         WHERE gt.target_date = CURRENT_DATE
           AND gt.target_name = 'margin_v2'
           AND gt.dimension_2 IS DISTINCT FROM 'Catalogue'
         GROUP BY 1
     ),

     model_bookings AS (
         SELECT
             se.data.posa_category_from_territory(bs.territory)          AS territory,
             DATE_TRUNC('hour', bs.date_time_booked)                     AS hour,
             sc.today,
             sc.yesterday,
             sc.today_last_week,
             sc.today_ly,
             sc.today_lly,
             sc.today_2019,
             ROUND(SUM(IFF(bs.currency = 'GBP',
                           bs.margin_gross_of_toms_cc,
                           margin_gross_of_toms_cc * cc.multiplier)), 0) AS margin_gbp_constant_currency,
             ROUND(SUM(bs.margin_gross_of_toms_gbp), 0)                  AS margin_gbp,
             COUNT(DISTINCT bs.booking_id)                               AS bookings
         FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
             INNER JOIN data_vault_mvp.dwh.se_calendar sc ON bs.date_time_booked::DATE = sc.date_value
             LEFT JOIN  data_vault_mvp.dwh.dim_sale ds ON bs.sale_id = ds.se_sale_id
             LEFT JOIN  hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
                     (CURRENT_DATE) >= cc.start_date AND
                     (CURRENT_DATE) <= cc.end_date AND
                     cc.currency = 'GBP' AND
                     cc.category = 'Primary' AND
                     bs.currency = cc.base_currency
         WHERE (
                       sc.today
                       OR sc.yesterday
                       OR sc.today_last_week
                       OR sc.today_ly
                       OR sc.today_lly
                       OR sc.today_2019
                   )
         GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
     ),
     distinct_territories AS (
         SELECT DISTINCT
             mb.territory
         FROM model_bookings mb
     ),
     target_grain AS (
         SELECT
             dt.territory,
             dt.target,
             mh.hour
         FROM daily_target dt
             LEFT JOIN model_hours mh
         WHERE mh.today
     ),
     model_target AS (
         -- model target phased on today LW run rate
         SELECT
             tg.territory,
             tg.hour,
             tg.target,
             SUM(COALESCE(mb.margin_gbp_constant_currency, 0))
                 OVER (PARTITION BY tg.territory)                  AS total_margin_gbp_constant_currency,
             (COALESCE(mb.margin_gbp_constant_currency, 0)
                 / total_margin_gbp_constant_currency) * tg.target AS hourly_target_gbp_constant_currency
         FROM target_grain tg
             LEFT JOIN model_bookings mb ON tg.hour = DATEADD('day', 7, mb.hour)
             AND tg.territory = mb.territory
             AND mb.today_last_week
     ),
     grain AS (
         SELECT
             mh.hour,
             mh.date,
             dt.territory,
             mh.today,
             mh.yesterday,
             mh.today_last_week,
             mh.today_ly,
             mh.today_lly,
             mh.today_2019
         FROM model_hours mh
             LEFT JOIN distinct_territories dt
     )
-- SELECT * FROM grain
SELECT
    g.hour,
    g.date,
    g.territory,
    g.today,
    g.yesterday,
    g.today_last_week,
    g.today_ly,
    g.today_lly,
    g.today_2019,
--        mt.target,
--        mt.total_margin_gbp_constant_currency,
    mt.hourly_target_gbp_constant_currency,
    mb.margin_gbp_constant_currency,
    mb.margin_gbp,
    mb.bookings
FROM grain g
    LEFT JOIN model_target mt ON g.hour = mt.hour AND g.territory = mt.territory
    LEFT JOIN model_bookings mb ON g.hour = mb.hour AND g.territory = mb.territory;


SELECT
    se.data.posa_category_from_territory(IFF(gt.dimension_3 = 'DACH', 'DE', gt.dimension_3)) AS territory,
    SUM(gt.target_value)                                                                     AS target
FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets gt
WHERE gt.target_date = CURRENT_DATE
  AND gt.target_name = 'margin_v2'
  AND gt.dimension_2 IS DISTINCT FROM 'Catalogue'
GROUP BY 1;


SELECT MAX(target_date)
FROM latest_vault.fpa_gsheets.generic_targets gt
WHERE gt.target_date >= '2023-01-01'