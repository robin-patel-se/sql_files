--SPVs cut by Date, Company, POSu country, Territory, Marketing Channel, Platform
SELECT sts.event_tstamp::DATE                                        AS date,
       cs.name                                                       AS company,
       ssa.posu_country                                              AS posu_country,
       se.data.posa_category_from_territory(ssa.posa_territory)      AS territory,
       se.data.channel_category(stmc.touch_mkt_channel)              AS channel,
       se.data.platform_from_touch_experience(stba.touch_experience) AS platform,
       count(DISTINCT sts.event_hash)                                AS spvs
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
         LEFT JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
         LEFT JOIN se.data.se_sale_attributes ssa ON sts.se_sale_id = ssa.se_sale_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cs ON ssa.company_id = cs.id
WHERE sts.event_tstamp >= '2020-01-01'
  AND ssa.product_configuration = 'Hotel'
  AND ssa.data_model = 'New Data Model'
GROUP BY 1, 2, 3, 4, 5, 6;


--scv spvs
SELECT --sts.event_tstamp::DATE                                                                           AS date,
       sts.se_sale_id,
       count(*)                                                                                         AS scv_spvs,
       SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)                                   AS scv_app_spvs,
       SUM(CASE WHEN left(sts.se_sale_id, 1) = 'A' AND stba.touch_experience = 'native app' THEN 1 END) AS scv_ndm_app_spvs,
       SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END)                                  AS scv_non_app_spvs,
       scv_app_spvs / scv_spvs,
       scv_ndm_app_spvs / scv_spvs
FROM se.data.scv_touched_spvs sts
         INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE >= '2020-06-01'
GROUP BY 1
;

------------------------------------------------------------------------------------------------------------------------
WITH cube_spvs AS (
--cube spvs
    SELECT --fspv.key_date_viewed,
--        bu.business_unit_code,
           ds.sale_id,
--        os.source_name,
           SUM(fspv.sales_page_views) AS sales_page_views
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds ON fspv.key_sale = ds.key_sale
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bu
                        ON fspv.business_unit_id = bu.business_unit_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot os ON fspv.source_id = os.source_id
    WHERE fspv.key_date_viewed >= '2020-06-01'
      AND LEFT(ds.sale_id, 1) = 'A'
      AND os.source_id != 3 --remove travelist
    GROUP BY ds.sale_id
)
   , cube_margin AS (
--cube margin
    SELECT --fb.key_date_booked,
--        bu.business_unit_code,
           ds.sale_id,
--        os.source_name,
           SUM(fb.margin) AS margin
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_snapshot fb
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds ON fb.key_sale = ds.key_sale
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bu
                        ON fb.key_current_business_unit_id = bu.business_unit_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot db ON fb.key_booking = db.key_booking
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot os ON db.source_id = os.source_id
    WHERE fb.key_date_booked >= '2020-06-01'
      AND LEFT(ds.sale_id, 1) = 'A'
      AND os.source_id != 3 --remove travelist
    GROUP BY ds.sale_id
)
   , scv_spvs AS (
    --scv spvs
    SELECT --sts.event_tstamp::DATE                                                                           AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    WHERE sts.event_tstamp::DATE >= '2020-06-01'
      AND LEFT(sts.se_sale_id, 1) = 'A'

    GROUP BY 1
)
SELECT cs.sale_id,
       cs.sales_page_views,
       cm.margin,
       ss.scv_spvs,
       ss.scv_app_spvs
FROM cube_spvs cs
         LEFT JOIN cube_margin cm ON cs.sale_id = cm.sale_id
         LEFT JOIN scv_spvs ss ON cs.sale_id = ss.se_sale_id
         LEFT JOIN se.data.dim_sale d ON cs.sale_id = d.se_sale_id
WHERE d.tech_platform != 'TRAVELBIRD';

SELECT *
FROM se.data.scv_touched_spvs sts
WHERE sts.se_sale_id = 'A2060';

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-06-01'
  AND es.se_sale_id = 'A2582';

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-06-01'
  AND es.se_sale_id = 'A2060';

SELECT es.is_robot_spider_event,
       es.is_server_side_event,
       es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-06-01'
  AND es.se_sale_id = 'A3734';

SELECT es.is_robot_spider_event,
       es.is_server_side_event,
       es.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR,
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-06-01'
  AND es.se_sale_id = 'A4251';

SELECT e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR,
       e.page_url,
       e.page_urlpath,
       e.is_robot_spider_event,
       e.is_server_side_event,
       e.useragent
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.event_tstamp >= '2020-06-01'
  AND e.se_sale_id = 'A2633'
  AND e.event_name IN ('page_view', 'screen_view');

SELECT e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR,
       e.page_url,
       e.page_urlpath,
       e.is_robot_spider_event,
       e.is_server_side_event
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.event_tstamp >= '2020-06-01'
  AND e.se_sale_id = 'A10246'
  AND e.event_name IN ('page_view', 'screen_view');

SELECT count(*), SUM(CASE WHEN es.is_robot_spider_event THEN 1 END) AS robot
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-06-01'
  AND es.se_sale_id = 'A10246'
  AND es.event_name IN ('page_view', 'screen_view');


------------------------------------------------------------------------------------------------------------------------
WITH cube_spvs AS (
--cube spvs
    SELECT --fspv.key_date_viewed,
--        bu.business_unit_code,
           ds.sale_id,
--        os.source_name,
           SUM(fspv.sales_page_views) AS sales_page_views
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds ON fspv.key_sale = ds.key_sale
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bu
                        ON fspv.business_unit_id = bu.business_unit_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot os ON fspv.source_id = os.source_id
    WHERE fspv.key_date_viewed = '2020-06-10'
      AND LEFT(ds.sale_id, 1) = 'A'
      AND os.source_id != 3
    GROUP BY ds.sale_id
)
   , cube_margin AS (
--cube margin
    SELECT --fb.key_date_booked,
--        bu.business_unit_code,
           ds.sale_id,
--        os.source_name,
           SUM(fb.margin) AS margin
    FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_snapshot fb
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds ON fb.key_sale = ds.key_sale
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bu
                        ON fb.key_current_business_unit_id = bu.business_unit_id
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot db ON fb.key_booking = db.key_booking
             INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot os ON db.source_id = os.source_id
    WHERE fb.key_date_booked = '2020-06-10'
      AND LEFT(ds.sale_id, 1) = 'A'
      AND os.source_id != 3
    GROUP BY ds.sale_id
)
   , scv_spvs AS (
    --scv spvs
    SELECT --sts.event_tstamp::DATE                                                                           AS date,
           sts.se_sale_id,
           count(*)                                                        AS scv_spvs,
           SUM(CASE WHEN stba.touch_experience = 'native app' THEN 1 END)  AS scv_app_spvs,
           SUM(CASE WHEN stba.touch_experience != 'native app' THEN 1 END) AS scv_non_app_spvs,
           scv_app_spvs / scv_spvs
    FROM se.data.scv_touched_spvs sts
             INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
    WHERE sts.event_tstamp::DATE >= '2020-06-10'
      AND LEFT(sts.se_sale_id, 1) = 'A'
    GROUP BY 1
)
SELECT cs.sale_id,
       cs.sales_page_views,
       cm.margin,
       ss.scv_spvs,
       ss.scv_app_spvs
FROM cube_spvs cs
         LEFT JOIN cube_margin cm ON cs.sale_id = cm.sale_id
         LEFT JOIN scv_spvs ss ON cs.sale_id = ss.se_sale_id
ORDER BY sales_page_views - scv_spvs DESC;

SELECT --fb.key_date_booked,
--        bu.business_unit_code,
       ds.sale_id,
--        os.source_name,
       SUM(fb.margin) AS margin
FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_snapshot fb
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds ON fb.key_sale = ds.key_sale
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bu
                    ON fb.key_current_business_unit_id = bu.business_unit_id
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot db ON fb.key_booking = db.key_booking
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot os ON db.source_id = os.source_id
WHERE fb.key_date_booked = '2020-06-10'
  AND ds.sale_id = '4656'
  AND os.source_id != 3
GROUP BY ds.sale_id;

SELECT --fspv.key_date_viewed,
--        bu.business_unit_code,
       ds.sale_id,
--        os.source_name,
       SUM(fspv.sales_page_views) AS sales_page_views
FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_sales_page_views_snapshot fspv
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds ON fspv.key_sale = ds.key_sale
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot bu
                    ON fspv.business_unit_id = bu.business_unit_id
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.original_sources_snapshot os ON fspv.source_id = os.source_id
WHERE fspv.key_date_viewed = '2020-06-10'
  AND ds.sale_id = '4656'
  AND os.source_id != 3
GROUP BY ds.sale_id;

SELECT sts.se_sale_id, count(*)
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp::DATE = '2020-06-10'
GROUP BY 1;

SELECT *
FROM data_vault_mvp.dwh.se_booking sb
WHERE sb.booking_completed_date = '2020-06-10'
  AND sb.sale_id = '4656';
SELECT *
FROM se.data.se_booking_summary_extended sbse
WHERE sbse.datebooked = '2020-06-10'
  AND sbse.saleid = '4656';




USE WAREHOUSE pipe_xlarge;


-- do we have SPVs by hostname anywhere in Snowflake for an ad hoc query
-- and if so is it cuttable by Sale ID and hostname

SELECT stba.touch_hostname,
       sts.se_sale_id,
       COUNT(DISTINCT event_hash) AS spvs
FROM se.data.scv_touched_spvs sts
         LEFT JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp >= '2020-06-01'
GROUP BY 1, 2;