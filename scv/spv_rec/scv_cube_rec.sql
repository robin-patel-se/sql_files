WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
    WHERE sts.event_tstamp >= '2020-01-01'
    GROUP BY 1
),
     cube_spvs AS (
         SELECT DISTINCT
                fspv.key_date_viewed::DATE AS date,
                SUM(fspv.sales_page_views) AS cube_spvs
         FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
                  INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                             ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
         WHERE fspv.key_date_viewed >= '2020-01-01'
         GROUP BY fspv.key_date_viewed
     )
SELECT cs.date,
       cs.cube_spvs,
       ss.scv_spvs,
       ss.scv_app_spvs,
       ss.scv_non_app_spvs,
       ss.scv_spvs - cs.cube_spvs  AS diff,
       scv_spvs / cs.cube_spvs - 1 AS var
FROM cube_spvs cs
         LEFT JOIN scv_spvs ss ON cs.date = ss.date
ORDER BY 1;



WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           ssa.product_configuration,
           ssa.posa_territory,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-01-01'
    GROUP BY 1, 2, 3
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           ssa.product_configuration,
           ssa.posa_territory,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-01-01'
    GROUP BY 1, 2, 3
)
SELECT cs.date,
       cs.product_configuration,
       cs.posa_territory,
       cs.cube_spvs,
       ss.scv_spvs,
       ss.scv_spvs - cs.cube_spvs  AS diff,
       scv_spvs / cs.cube_spvs - 1 AS var
FROM cube_spvs cs
         LEFT JOIN scv_spvs ss ON cs.date = ss.date
    AND cs.product_configuration = ss.product_configuration
    AND cs.posa_territory = ss.posa_territory
ORDER BY 1, 2, 3;


------------------------------------------------------------------------------------------------------------------------

WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-01-01'
      AND ssa.end_date <= '2020-01-01'
    GROUP BY 1, 2
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           dss.sale_id,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-01-01'
      AND ssa.end_date <= '2020-01-01'
    GROUP BY 1, 2
)
SELECT cs.date,
       SUM(cs.cube_spvs),
       SUM(ss.scv_spvs)
FROM cube_spvs cs
         LEFT JOIN scv_spvs ss ON cs.date = ss.date
    AND cs.sale_id = ss.se_sale_id
WHERE ss.scv_spvs IS NULL
GROUP BY 1
ORDER BY 1
;


WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-01-01'
      AND dss.sale_id = '89518'
    GROUP BY 1, 2
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           dss.sale_id,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-01-01'
      AND dss.sale_id = '89518'
    GROUP BY 1, 2
)
SELECT cs.date,
       cs.sale_id,
       cs.cube_spvs,
       ss.scv_spvs
FROM cube_spvs cs
         LEFT JOIN scv_spvs ss ON cs.date = ss.date
    AND cs.sale_id = ss.se_sale_id
ORDER BY scv_spvs - cube_spvs
;

--upcoming
WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-06-09'
      AND ssa.start_date >= sts.event_tstamp
    GROUP BY 1, 2
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           dss.sale_id,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-06-09'
      AND ssa.start_date >= fspv.key_date_viewed
    GROUP BY 1, 2
)
SELECT cs.date,
       cs.sale_id,
       cs.cube_spvs,
       ss.scv_spvs
FROM cube_spvs cs
         LEFT JOIN scv_spvs ss ON cs.date = ss.date
    AND cs.sale_id = ss.se_sale_id
ORDER BY scv_spvs - cube_spvs
;

------------------------------------------------------------------------------------------------------------------------

SELECT es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR,
       es.contexts_com_secretescapes_content_context_1,
       es.contexts_com_secretescapes_secret_escapes_sale_context_1
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.page_urlpath LIKE '%sale-hotel'
  AND es.is_server_side_event;


------------------------------------------------------------------------------------------------------------------------
--spvs for live sales
WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-01-01'
      AND ssa.sale_active
    GROUP BY 1, 2
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           dss.sale_id,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-01-01'
      AND ssa.sale_active
    GROUP BY 1, 2
)
SELECT cs.date,
       SUM(cs.cube_spvs),
       SUM(ss.scv_spvs)
FROM scv_spvs ss
         LEFT JOIN cube_spvs cs ON ss.date = cs.date
    AND cs.sale_id = ss.se_sale_id
GROUP BY 1
ORDER BY 1
;

USE WAREHOUSE pipe_xlarge;

SELECT date,
       SUM(scv_app_spvs),
       sum(scv_ndm_app_spvs)
FROM (

         SELECT sts.event_tstamp::DATE                                          AS date,
                sts.se_sale_id,
                count(*)                                                        AS scv_spvs,
                SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
                SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
                SUM(CASE
                        WHEN left(sts.se_sale_id, 1) = 'A' AND stba.touch_experience = 'native app'
                            THEN 1 END)                                         AS scv_ndm_app_spvs,
                scv_app_spvs / scv_spvs
         FROM se.data.scv_touched_spvs sts
                  INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
                  INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                             ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
                  INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
         WHERE sts.event_tstamp >= '2020-01-01'
--            AND ssa.sale_active
         GROUP BY 1, 2
     )
GROUP BY 1;

SELECT fcb.shiro_user_id,
       COUNT(*)                          AS lifetime_bookings,
       SUM(fcb.margin_gross_of_toms_gbp) AS lifetime_margin,
       SUM(fcb.gross_booking_value_gbp)  AS lifetime_gross_booking_revenue
FROM se.data.fact_complete_booking fcb
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--app percentages

SELECT count(*)                                                        AS scv_spvs,
       SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
       SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
       scv_app_spvs / scv_spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= DATEADD(MONTH, -4, current_date);

SELECT count(*)                                                                                         AS scv_spvs,
       SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)                                   AS scv_app_spvs,
       SUM(CASE WHEN left(sts.se_sale_id, 1) = 'A' AND stba.touch_experience = 'native app' THEN 1 END) AS scv_ndm_app_spvs,
       SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END)                                  AS scv_non_app_spvs,
       scv_app_spvs / scv_spvs,
       scv_ndm_app_spvs / scv_spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= DATEADD(MONTH, -4, current_date)
;

------------------------------------------------------------------------------------------------------------------------
--spvs for expired sales
WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-01-01'
      AND sts.event_tstamp > ssa.end_date
    GROUP BY 1, 2
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           dss.sale_id,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-01-01'
      AND fspv.key_date_viewed > ssa.end_date
    GROUP BY 1, 2
)
SELECT cs.date,
       SUM(cs.cube_spvs),
       SUM(ss.scv_spvs)
FROM scv_spvs ss
         LEFT JOIN cube_spvs cs ON ss.date = cs.date
    AND cs.sale_id = ss.se_sale_id
GROUP BY 1
ORDER BY 1;

-- SELECT cs.date,
--        cs.sale_id,
--        cs.cube_spvs,
--        ss.scv_spvs
-- FROM cube_spvs cs
--          LEFT JOIN scv_spvs ss ON cs.date = ss.date AND cs.sale_id = ss.se_sale_id
-- ORDER BY scv_spvs - cube_spvs
;

------------------------------------------------------------------------------------------------------------------------
--upcoming sales
WITH scv_spvs AS (
    SELECT sts.event_tstamp::DATE                                          AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
    WHERE sts.event_tstamp >= '2020-01-01'
      AND sts.event_tstamp < ssa.start_date
    GROUP BY 1, 2
)
   , cube_spvs AS (
    SELECT fspv.key_date_viewed::DATE AS date,
           dss.sale_id,
           SUM(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
             INNER JOIN se.data.se_sale_attributes ssa ON dss.sale_id = ssa.se_sale_id
    WHERE fspv.key_date_viewed >= '2020-01-01'
      AND fspv.key_date_viewed < ssa.start_date
    GROUP BY 1, 2
)
SELECT cs.date,
       SUM(cs.cube_spvs),
       SUM(ss.scv_spvs)
FROM scv_spvs ss
         LEFT JOIN cube_spvs cs ON ss.date = cs.date
    AND cs.sale_id = ss.se_sale_id
GROUP BY 1
ORDER BY 1;

-- SELECT cs.date,
--        cs.sale_id,
--        cs.cube_spvs,
--        ss.scv_spvs
-- FROM cube_spvs cs
--          LEFT JOIN scv_spvs ss ON cs.date = ss.date AND cs.sale_id = ss.se_sale_id
-- ORDER BY scv_spvs - cube_spvs
;


SELECT sts.event_tstamp::DATE                                                                           AS date,
       count(*)                                                                                         AS scv_spvs,
       SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)                                   AS scv_app_spvs,
       SUM(CASE WHEN left(sts.se_sale_id, 1) = 'A' AND stba.touch_experience = 'native app' THEN 1 END) AS scv_ndm_app_spvs,
       SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END)                                  AS scv_non_app_spvs,
       scv_app_spvs / scv_spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                    ON sts.se_sale_id = dss.sale_id AND LOWER(dss.provider_name) NOT IN ('tvlflash', 'travelbird')
WHERE sts.event_tstamp >= '2020-01-01'
GROUP BY 1;

SELECT COUNT(*)                                               AS sales,
       SUM(CASE WHEN ss.contractor_id IS NOT NULL THEN 1 END) AS contractors
FROM data_vault_mvp.dwh.se_sale ss;


------------------------------------------------------------------------------------------------------------------------
--robot

USE WAREHOUSE pipe_xlarge;

SELECT es.event_tstamp::DATE                                     AS date,
       COUNT(*)                                                  AS page_events,
       SUM(CASE WHEN es.is_robot_spider_event THEN 1 ELSE 0 END) AS robot_events,
       SUM(CASE WHEN es.is_robot_spider_event THEN 0 ELSE 1 END) AS non_robot_events
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-01-01'
  AND es.event_name IN ('page_view', 'screen_view')
GROUP BY 1;

SELECT es.event_tstamp::DATE AS date,
--        COUNT(*)                                                  AS page_events,
--        SUM(CASE WHEN es.is_robot_spider_event THEN 1 ELSE 0 END) AS robot_events,
--        SUM(CASE WHEN es.is_robot_spider_event THEN 0 ELSE 1 END) AS non_robot_events,
       count(*)              AS robot_spvs
FROM hygiene_vault_mvp.snowplow.event_stream es
         INNER JOIN se.data.dim_sale ds ON es.se_sale_id = ds.se_sale_id
WHERE es.event_tstamp >= '2020-02-28'
  AND es.is_server_side_event
  AND es.is_robot_spider_event
  AND es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'sale'
  AND es.event_name IN ('page_view', 'screen_view')
  AND ds.tech_platform != 'TRAVELBIRD'
  AND es.device_platform != 'native app'
GROUP BY 1;

SELECT es.event_tstamp,
       es.is_server_side_event,
       es.is_robot_spider_event,
       es.contexts_com_secretescapes_content_context_1,
       es.event_name,
       ds.tech_platform,
       es.device_platform,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
         INNER JOIN se.data.dim_sale ds ON es.se_sale_id = ds.se_sale_id
WHERE es.event_tstamp >= '2020-02-28'
  AND es.is_server_side_event
  AND es.is_robot_spider_event
  AND es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM 'sale'
  AND es.event_name IN ('page_view', 'screen_view')
  AND ds.tech_platform != 'TRAVELBIRD'
  AND es.device_platform != 'native app';

USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
--se
SELECT sale.sale_dimension,
       spv.saleid,
       SUM(spv.uservisits)
FROM raw_vault_mvp.cms_reports.spvs_by_state_date spv
         LEFT JOIN raw_vault_mvp.cms_reports.sales sale ON sale.id = spv.saleid
WHERE spv.date >= '2020-01-01'
  AND sale.sale_dimension = 'Catalogue'
GROUP BY 1, 2;

--tb
SELECT sale.sale_dimension,
       spv.sale_id,
       SUM(spv.user_visits)
FROM raw_vault_mvp.travelbird_catalogue.sale_visits_by_state_and_date spv
         LEFT JOIN raw_vault_mvp.cms_reports.sales sale ON sale.id = spv.sale_id
WHERE spv.date >= '2020-01-01'
  AND sale.sale_dimension = 'Catalogue'
GROUP BY 1, 2;


--list of sale ids
WITH distinct_sales AS (
    SELECT id AS sale_id,
           sale.sale_dimension
    FROM raw_vault_mvp.cms_reports.sales sale
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY sale.loaded_at DESC) = 1
)
   , se_cat_spvs AS (
    SELECT spv.saleid          AS sale_id,
           SUM(spv.uservisits) AS user_visits
    FROM raw_vault_mvp.cms_reports.spvs_by_state_date spv
             LEFT JOIN distinct_sales ds ON spv.saleid = ds.sale_id
    WHERE TO_DATE(spv.date, 'dd/MM/yyyy') = '2020-04-21'
      AND ds.sale_dimension = 'Catalogue'
    GROUP BY 1
)
   , tb_cat_spvs AS (
    SELECT spv.sale_id,
           SUM(spv.user_visits) AS user_visits
    FROM raw_vault_mvp.travelbird_catalogue.sale_visits_by_state_and_date spv
             LEFT JOIN distinct_sales ds ON spv.sale_id = ds.sale_id
    WHERE spv.date = '2020-04-21'
      AND ds.sale_dimension = 'Catalogue'
    GROUP BY 1
)
   , scv_cat_spvs AS (
    SELECT sts.se_sale_id,
           count(*) AS spvs
    FROM se.data.scv_touched_spvs sts
             LEFT JOIN se.data.dim_sale ds ON sts.se_sale_id = ds.se_sale_id
    WHERE sts.event_tstamp::DATE = '2020-04-21'
      AND ds.sale_type = 'Catalogue'
    GROUP BY 1
)
   , grain AS (
    SELECT DISTINCT
           se.sale_id
    FROM se_cat_spvs se
    UNION
    SELECT DISTINCT
           tb.sale_id
    FROM tb_cat_spvs tb
    UNION
    SELECT DISTINCT
           scv.se_sale_id
    FROM scv_cat_spvs scv
)
SELECT g.sale_id,
       COALESCE(ss.user_visits, 0) AS se_cat_spvs,
       COALESCE(ts.user_visits, 0) AS tb_cat_spvs,
       COALESCE(scv.spvs, 0)       AS scv_cat_spvs,
       CASE
           WHEN se_cat_spvs > 0 AND tb_cat_spvs > 0 THEN 'Both'
           WHEN se_cat_spvs > 0 AND tb_cat_spvs = 0 THEN 'SE only'
           WHEN tb_cat_spvs > 0 AND se_cat_spvs = 0 THEN 'TB only'
           END
                                   AS reporting_platform

FROM grain g
         LEFT JOIN se_cat_spvs ss ON g.sale_id = ss.sale_id
         LEFT JOIN tb_cat_spvs ts ON g.sale_id = ts.sale_id
         LEFT JOIN scv_cat_spvs scv ON g.sale_id = scv.se_sale_id
ORDER BY se_cat_spvs + tb_cat_spvs + scv_cat_spvs DESC;

SELECT *
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = 'A7431';

------------------------------------------------------------------------------------------------------------------------


WITH distinct_sales AS (
    SELECT id AS sale_id,
           sale.sale_dimension
    FROM raw_vault_mvp.cms_reports.sales sale
        QUALIFY row_number() OVER (PARTITION BY id ORDER BY sale.loaded_at DESC) = 1
)
   , se_spvs AS (
    SELECT spv.saleid          AS sale_id,
           SUM(spv.uservisits) AS user_visits
    FROM raw_vault_mvp.cms_reports.spvs_by_state_date spv
    WHERE TO_DATE(spv.date, 'dd/MM/yyyy') = '2020-06-10'
    GROUP BY 1
)
   , tb_spvs AS (
    SELECT spv.sale_id,
           SUM(spv.user_visits) AS user_visits
    FROM raw_vault_mvp.travelbird_catalogue.sale_visits_by_state_and_date spv
    WHERE spv.date = '2020-06-10'
    GROUP BY 1
)
   , scv_spvs AS (
    SELECT sts.se_sale_id,
           count(*) AS spvs
    FROM se.data.scv_touched_spvs sts
    WHERE sts.event_tstamp::DATE = '2020-06-10'
    GROUP BY 1
)
   , cube_spvs AS (
    SELECT dss.sale_id,
           sum(fspv.sales_page_views) AS cube_spvs
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot dss
                        ON fspv.key_sale = dss.key_sale AND LOWER(dss.provider_name) != 'tvlflash' --remove tvl
    WHERE fspv.key_date_viewed = '2020-06-10'
    GROUP BY 1
)
   , grain AS (
    SELECT DISTINCT
           se.sale_id
    FROM se_spvs se
    UNION
    SELECT DISTINCT
           tb.sale_id
    FROM tb_spvs tb
    UNION
    SELECT DISTINCT
           scv.se_sale_id
    FROM scv_spvs scv
    UNION
    SELECT DISTINCT
           cb.sale_id
    FROM cube_spvs cb
)
SELECT ds.sale_id,
       COALESCE(ss.user_visits, 0)  AS se_spvs,
       COALESCE(ts.user_visits, 0)  AS tb_spvs,
       COALESCE(scv.spvs, 0)        AS scv_spvs,
       COALESCE(cspvs.cube_spvs, 0) AS cube_spvs

FROM distinct_sales ds
         LEFT JOIN se_spvs ss ON ds.sale_id = ss.sale_id
         LEFT JOIN tb_spvs ts ON ds.sale_id = ts.sale_id
         LEFT JOIN scv_spvs scv ON ds.sale_id = scv.se_sale_id
         LEFT JOIN cube_spvs cspvs ON ds.sale_id = cspvs.sale_id
ORDER BY se_spvs DESC;

------------------------------------------------------------------------------------------------------------------------

SELECT spv.saleid,
       spv.territory,
       spv.userstate,
       spv.usercountry,
       count(*)
FROM raw_vault_mvp.cms_reports.spvs_by_state_date spv
WHERE TO_DATE(spv.date, 'dd/MM/yyyy') = '2020-06-10'
GROUP BY 1, 2, 3, 4;


SELECT *
FROM raw_vault_mvp.cms_reports.spvs_by_state_date spv
WHERE TO_DATE(spv.date, 'dd/MM/yyyy') = '2020-06-10'
  AND spv.saleid = '52630';
