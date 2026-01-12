USE WAREHOUSE pipe_xlarge;
USE SCHEMA data_vault_mvp.single_customer_view_stg;

--format scv spvs into daily spvs by different dimensions
CREATE OR REPLACE TABLE collab.dwh_rec.csv_spvs AS (
    SELECT s.event_tstamp::DATE                                         AS event_date,
           c.touch_affiliate_territory,
           COUNT(*)                                                     AS spvs,
           SUM(CASE WHEN e.se_user_id IS NOT NULL THEN 1 END)           AS scv_spvs_logged_in,
           SUM(CASE WHEN e.se_user_id IS NULL THEN 1 END)               AS scv_spvs_logged_out,
           SUM(CASE WHEN b.touch_experience = 'native app' THEN 1 END)  AS scv_spvs_app,
           SUM(CASE WHEN b.touch_experience != 'native app' THEN 1 END) AS scv_spvs_non_app,
           SUM(CASE
                   WHEN e.se_user_id IS NOT NULL
                       AND b.touch_experience != 'native app'
                       THEN 1 END)                                      AS scv_spvs_non_app_log_in,
           SUM(CASE
                   WHEN e.se_user_id IS NULL
                       AND b.touch_experience != 'native app'
                       THEN 1 END)                                      AS scv_spvs_non_app_log_out,
           SUM(CASE
                   WHEN b.touch_experience != 'native app'
                       AND u.url_medium = 'internal'
                       THEN 1 END)                                      AS scv_spvs_non_app_se_core,
           SUM(CASE
                   WHEN e.se_user_id IS NOT NULL
                       AND b.touch_experience != 'native app'
                       AND u.url_medium = 'internal'
                       THEN 1 END)                                      AS scv_spvs_non_app_log_in_se_core,
           SUM(CASE
                   WHEN e.se_user_id IS NULL
                       AND b.touch_experience != 'native app'
                       AND u.url_medium = 'internal'
                       THEN 1 END)                                      AS scv_spvs_non_app_log_out_se_core
    FROM module_touched_spvs s
             INNER JOIN module_touch_marketing_channel c ON s.touch_id = c.touch_id -- to get affiliate territory
             INNER JOIN module_touch_basic_attributes b ON s.touch_id = b.touch_id --to get experience (for app)
             INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON s.event_hash = e.event_hash -- to get logged in/out
             LEFT JOIN module_url_hostname u ON e.page_url = u.url --to get non app se core, nb app doesn't have a url
    GROUP BY 1, 2
);

GRANT USAGE ON SCHEMA collab.dwh_rec TO ROLE personal_role__carmenmardiros;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.dwh_rec TO ROLE personal_role__carmenmardiros;

-- MONGO NON APP SE CORE
-- MONGO APP SE CORE

SELECT *
FROM collab.data.reconciliation_key_metrics_rec
LIMIT 10;
SELECT *
FROM collab.data.key_metrics_rec_stg_mongo__events_collection
LIMIT 10;
SELECT *
FROM collab.data.key_metrics_rec_stg_snowplow__atomic_events
LIMIT 10;
SELECT *
FROM collab.data.key_metrics_rec_stg_sale_slug_spv_rec
LIMIT 10;
SELECT *
FROM collab.data.key_metrics_rec_stg_sale_id_spv_rec
LIMIT 10;

------------------------------------------------------------------------------------------------------------------------
-- I don't agree that we should be joining on affiliate territory to rec,
-- because based on new information we aren't positive that affiliate mapping is like for like across different systems

WITH carmens_data AS ( --spv data by day
    SELECT k.event_date,
           SUM(k.spvs_cube_se_cms)                                 AS spvs_cube_se_cms,
           SUM(k.spvs_cms_reports)                                 AS spvs_cms_reports,
           SUM(k.spvs_mongo_events)                                AS spvs_mongo_events,
           SUM(k.spvs_mongo_events_less_pigsback)                  AS spvs_mongo_events_less_pigsback,
           SUM(k.mongo_spvs_app)                                   AS mongo_spvs_app,
           SUM(k.snowplow_spvs_app)                                AS snowplow_spvs_app,
           SUM(k.mongo_spvs_non_app_wl)                            AS mongo_spvs_non_app_wl,
           SUM(k.snowplow_spvs_non_app_wl)                         AS snowplow_spvs_non_app_wl,
           SUM(k.mongo_spvs_non_app_se_core)                       AS mongo_spvs_non_app_se_core,
           SUM(k.snowplow_spvs_non_app_se_core)                    AS snowplow_spvs_non_app_se_core,
           SUM(k.dwh_event_stream_spvs_non_robots)                 AS dwh_event_stream_spvs_non_robots,
           SUM(k.dwh_event_stream_spvs_robots)                     AS dwh_event_stream_spvs_robots,
           SUM(k.dwh_event_stream_spvs_non_app_se_core_non_robots) AS dwh_event_stream_spvs_non_app_se_core_non_robots,
           SUM(k.dwh_event_stream_spvs_non_app_se_core_robots)     AS dwh_event_stream_spvs_non_app_se_core_robots,
           SUM(k.dwh_model_spvs_non_app)                           AS dwh_model_spvs_non_app,
           SUM(k.dwh_model_spvs_app)                               AS dwh_model_spvs_app,
           SUM(k.dwh_model_spvs_non_app_se_core)                   AS dwh_model_spvs_non_app_se_core
    FROM collab.data.reconciliation_key_metrics_rec k
    WHERE event_date >= '2018-01-01'
    GROUP BY 1
),
     scv AS ( --scv spv data by day
         SELECT event_date,
                SUM(spvs)                             AS spvs,
                SUM(scv_spvs_logged_in)               AS scv_spvs_logged_in,
                SUM(scv_spvs_logged_out)              AS scv_spvs_logged_out,
                SUM(COALESCE(scv_spvs_app, 0))        AS scv_spvs_app,
                SUM(scv_spvs_non_app)                 AS scv_spvs_non_app,
                SUM(scv_spvs_non_app_log_in)          AS scv_spvs_non_app_log_in,
                SUM(scv_spvs_non_app_log_out)         AS scv_spvs_non_app_log_out,
                SUM(scv_spvs_non_app_se_core)         AS scv_spvs_non_app_se_core,
                SUM(scv_spvs_non_app_log_in_se_core)  AS scv_spvs_non_app_log_in_se_core,
                SUM(scv_spvs_non_app_log_out_se_core) AS scv_spvs_non_app_log_out_se_core
         FROM collab.dwh_rec.csv_spvs
         WHERE event_date >= '2018-01-01'
         GROUP BY 1
     )
SELECT c.event_date,
       date_trunc(WEEK, c.event_date) AS week_of_year,
--        c.spvs_cube_se_cms,
--        c.spvs_cms_reports,
--        c.spvs_mongo_events,
--        c.spvs_mongo_events_less_pigsback,
       c.mongo_spvs_app,
       c.snowplow_spvs_app,
       s.scv_spvs_app,
--        c.mongo_spvs_non_app_wl,
--        c.snowplow_spvs_non_app_wl,
       c.mongo_spvs_non_app_se_core,
       c.snowplow_spvs_non_app_se_core,
       s.scv_spvs_non_app_se_core,
--        c.dwh_event_stream_spvs_non_robots,
--        c.dwh_event_stream_spvs_robots,
--        c.dwh_event_stream_spvs_non_app_se_core_non_robots,
--        c.dwh_event_stream_spvs_non_app_se_core_robots,
--        c.dwh_model_spvs_non_app,
--        c.dwh_model_spvs_app,
--        c.dwh_model_spvs_non_app_se_core,
--        s.spvs,
--        s.scv_spvs_logged_in,
--        s.scv_spvs_logged_out,
--        s.scv_spvs_non_app,
--        s.scv_spvs_non_app_log_in,
--        s.scv_spvs_non_app_log_out,

       s.scv_spvs_non_app_log_in_se_core,
       s.scv_spvs_non_app_log_out_se_core

FROM carmens_data c
         LEFT JOIN scv s ON c.event_date = s.event_date
ORDER BY event_date
;

------------------------------------------------------------------------------------------------------------------------
--revisiting territories

SELECT k.event_date,
       date_trunc(WEEK, k.event_date)                          AS week_of_year,
       k.territory,
--        k.spvs_cube_se_cms,
--        k.spvs_cms_reports,
--        k.spvs_mongo_events,
--        k.spvs_mongo_events_less_pigsback,
       SUM(k.mongo_spvs_app)                                   AS mongo_spvs_app,
       SUM(k.snowplow_spvs_app)                                AS snowplow_spvs_app,
       SUM(COALESCE(s.scv_spvs_app, 0))                        AS scv_spvs_app,
--        k.mongo_spvs_non_app_wl,
--        k.snowplow_spvs_non_app_wl,
       '',
       SUM(k.mongo_spvs_non_app_se_core)                       AS mongo_spvs_non_app_se_core,
       SUM(k.snowplow_spvs_non_app_se_core)                    AS snowplow_spvs_non_app_se_core,
       SUM(s.scv_spvs_non_app_se_core)                         AS scv_spvs_non_app_se_core,
       SUM(k.dwh_model_spvs_non_app_se_core)                   AS dwh_model_spvs_non_app_se_core,

       '',
       SUM(k.dwh_event_stream_spvs_non_app_se_core_non_robots) AS dwh_event_stream_spvs_non_app_se_core_non_robots,
       SUM(k.dwh_event_stream_spvs_non_app_se_core_robots)     AS dwh_event_stream_spvs_non_app_se_core_robots

--        k.dwh_event_stream_spvs_non_robots,
--        k.dwh_event_stream_spvs_robots,
--        k.dwh_event_stream_spvs_non_app_se_core_non_robots,
--        SUM(k.dwh_event_stream_spvs_non_app_se_core_robots)     AS dwh_event_stream_spvs_non_app_se_core_robots
--        k.dwh_model_spvs_non_app,
--        k.dwh_model_spvs_app,
FROM collab.data.reconciliation_key_metrics_rec k
         LEFT JOIN collab.dwh_rec.csv_spvs s
                   ON k.event_date = s.event_date AND k.territory = s.touch_affiliate_territory
WHERE k.event_date >= '2018-01-01'
  AND k.territory IN ('UK', 'DE', 'IT')
GROUP BY 1, 2, 3;
