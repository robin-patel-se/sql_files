USE WAREHOUSE pipe_large;

SELECT *
FROM data_vault_mvp.engagement_stg.user_snapshot;



SELECT u.id                                                 AS user_id,
       u.original_affiliate_id                              AS original_affiliate_id,
       oa.name                                              AS original_affiliate_name,
       oa.territory_id                                      AS original_affiliate_territory_id,
       ot.name                                              AS original_affiliate_territory,
       u.affiliate_id                                       AS current_affiliate_id,
       ca.name                                              AS current_affiliate_name,
       ca.territory_id                                      AS current_affiliate_territory_id,
       ct.name                                              AS current_affiliate_territory,
       DATEDIFF('month', '2011-01-31', u.date_created)::INT AS cohort_id,  -- logic supplied by CRM
       TO_VARCHAR(u.date_created, 'YYYY-MM')                AS cohort_year_month,
       u.date_created                                       AS signup_tstamp,
       CASE
           WHEN p.receive_sales_reminders = 1 THEN 2
           WHEN p.receive_weekly_offers = 1 THEN 1
           ELSE 0
           END::INT                                         AS email_opt_in,
       NULL                                                 AS push_opt_in -- get from sfmc data


FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot oa ON u.original_affiliate_id = oa.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot ot ON oa.territory_id = ot.id

         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot ca ON u.affiliate_id = ca.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot ct ON ca.territory_id = ct.id

         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.profile_snapshot p ON u.profile_id = p.id
;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.profile_snapshot;

SELECT *
FROM raw_vault_mvp.sfmc.push_status
LIMIT 10;

--push opt in status
SELECT DISTINCT user_id,
                LAST_VALUE(opt_in_status) OVER (PARTITION BY user_id ORDER BY loaded_at) AS push_opt_in
FROM raw_vault_mvp.sfmc.push_status;

--first app activity
SELECT attributed_user_id,
       MIN(touch_start_tstamp) AS first_app_activity_tstamp
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
WHERE touch_experience = 'native app'
  AND stitched_identity_type = 'se_user_id'
GROUP BY 1;

--last email open
SELECT subscriber_key,
       MAX(event_date) AS last_email_open_tstamp
FROM raw_vault_mvp.sfmc.events_opens
WHERE TRY_TO_NUMBER(subscriber_key) IS NOT NULL
GROUP BY 1;

--last email click
SELECT subscriber_key,
       MAX(event_date) AS last_email_click_tstamp
FROM raw_vault_mvp.sfmc.events_clicks
WHERE TRY_TO_NUMBER(subscriber_key) IS NOT NULL
GROUP BY 1;

--last pageview tstamp -- need to create incremental table and refactor when touched_pageviews is created
SELECT t.attributed_user_id,
       MAX(t.event_tstamp) AS last_pageview_tstamp
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         LEFT JOIN hygiene_vault_mvp.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE e.event_name = 'page_view'
  AND t.stitched_identity_type = 'se_user_id'
GROUP BY 1;

--last spv tstamp -- need to create incremental table
SELECT t.attributed_user_id,
       MAX(s.event_tstamp) AS last_sale_pageview_tstamp
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touchification t ON s.touch_id = t.touch_id
WHERE t.stitched_identity_type = 'se_user_id'
GROUP BY 1;

--last booking abandon and complete tstamp
SELECT shiro_user_id,
       MAX(CASE WHEN booking_status = 'ABANDONED' THEN booking_created_timestamp END)  AS last_abandoned_booking_tstamp,
       MAX(CASE WHEN booking_status = 'COMPLETE' THEN booking_completed_timestamp END) AS last_complete_booking_tstamp
FROM data_vault_mvp.dwh.se_booking
GROUP BY 1
HAVING last_abandoned_booking_tstamp IS NOT NULL
    OR last_complete_booking_tstamp IS NOT NULL;



CREATE TABLE engagement_stg.user_snapshot
(
    user_id
        CURRENT_AFFILIATE_ID
        CURRENT_AFFILIATE_NAME
        CURRENT_AFFILIATE_TERRITORY_ID
        CURRENT_AFFILIATE_TERRITORY_NAME
        COHORT_ID
        SIGNUP_TSTAMP
        EMAIL_OPT_IN
        PUSH_OPT_IN
        FIRST_APP_ACTIVITY_TSTAMP
        LAST_EMAIL_OPEN_TSTAMP
        LAST_EMAIL_CLICK_TSTAMP
        LAST_PAGEVIEW_TSTAMP
        LAST_SALE_PAGEVIEW_TSTAMP
        LAST_BOOKING_ABANDON_TSTAMP
        LAST_BOOKING_COMPLETE_TSTAMP
        SCHEDULE_TSTAMP
        TRANSFORMED_AT
        UPDATED_AT
        UPDATED_BY_TASK_SCHEDULE_TSTAMP
        UPDATED_BY_TASK_TRANSFORMED_AT
        UPDATED_BY_TASK_ID
);

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.profile_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.profile_snapshot;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_opens CLONE raw_vault_mvp.sfmc.events_opens;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.events_clicks CLONE raw_vault_mvp.sfmc.events_clicks;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfmc.push_status CLONE raw_vault_mvp.sfmc.push_status;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.affiliate_classification CLONE raw_vault_mvp.chiasma_sql_server.affiliate_classification;

self_describing_task --include 'dv/dwh/user_attributes/user_first_activities'  --method 'run' --start '2020-05-11 03:00:00' --end '2020-05-11 03:00:00'

self_describing_task --include 'dv/dwh/user_attributes/user_last_pageview'  --method 'run' --start '2018-01-01 03:00:00' --end '2018-01-01 03:00:00'
self_describing_task --include 'dv/dwh/user_attributes/user_last_spv'  --method 'run' --start '2018-01-01 03:00:00' --end '2018-01-01 03:00:00'

self_describing_task --include 'dv/dwh/user_attributes/user_recent_activities'  --method 'run' --start '2020-05-11 03:00:00' --end '2020-05-11 03:00:00'

self_describing_task --include 'dv/dwh/user_attributes/user_attributes'  --method 'run' --start '2020-05-11 03:00:00' --end '2020-05-11 03:00:00'


SELECT ua.shiro_user_id                 AS user_id,
       ua.current_affiliate_id,
       ua.current_affiliate_name,
       ua.current_affiliate_territory_id,
       ua.current_affiliate_territory,
       ua.cohort_id,
       ua.signup_tstamp,
       ua.email_opt_in,
       ua.push_opt_in,
       uf.first_app_activity_tstamp,
       ur.last_email_open_tstamp,
       ur.last_email_click_tstamp,
       ur.last_pageview_tstamp,
       ur.last_sale_pageview_tstamp,
       ur.last_abandoned_booking_tstamp AS last_booking_abandon_tstamp,
       ur.last_complete_booking_tstamp  AS last_booking_complete_tstamp
FROM data_vault_mvp_dev_robin.dwh.user_attributes ua
         LEFT JOIN data_vault_mvp_dev_robin.dwh.user_first_activities uf ON ua.shiro_user_id = uf.shiro_user_id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.user_recent_activities ur ON ua.shiro_user_id = ur.shiro_user_id;


SELECT user_id,
       current_affiliate_id,
       current_affiliate_name,
       current_affiliate_territory_id,
       current_affiliate_territory_name,
       cohort_id,
       signup_tstamp,
       email_opt_in,
       push_opt_in,
       first_app_activity_tstamp,
       last_email_open_tstamp,
       last_email_click_tstamp,
       last_pageview_tstamp,
       last_sale_pageview_tstamp,
       last_booking_abandon_tstamp,
       last_booking_complete_tstamp,
       schedule_tstamp,
       transformed_at,
       updated_at,
       updated_by_task_schedule_tstamp,
       updated_by_task_transformed_at,
       updated_by_task_id
FROM data_vault_mvp.engagement_stg.user_snapshot;

--run on prod
CREATE OR REPLACE TABLE data_vault_mvp.dwh.user_last_pageview CLONE data_vault_mvp_dev_robin.dwh.user_last_pageview;
CREATE OR REPLACE TABLE data_vault_mvp.dwh.user_last_spv CLONE data_vault_mvp_dev_robin.dwh.user_last_spv;

airflow backfill --start_date '2020-05-11 00:00:00' --end_date '2020-05-11 00:00:00' --task_regex '.*' dwh__user_recent_activites__daily_at_03h00

SELECT *
FROM data_vault_mvp.information_schema.tables
WHERE table_schema = 'ENGAGEMENT_STG';

CREATE SCHEMA data_vault_mvp.engagement_stg_bk CLONE data_vault_mvp.engagement_stg;
DROP TABLE cohort_calendar;
DROP TABLE cohort_calendar_bak;
DROP TABLE current_affiliate;
DROP TABLE current_affiliate_bak;
DROP TABLE email_opt_in;
DROP TABLE email_opt_in_bak;
DROP TABLE first_app_activity_tstamp;
DROP TABLE first_app_activity_tstamp_bak;
DROP TABLE last_booking_abandon_tstamp;
DROP TABLE last_booking_abandon_tstamp_bak;
DROP TABLE last_booking_complete_tstamp;
DROP TABLE last_booking_complete_tstamp_bak;
DROP TABLE last_email_click_tstamp;
DROP TABLE last_email_click_tstamp_bak;
DROP TABLE last_email_open_tstamp;
DROP TABLE last_email_open_tstamp_bak;
DROP TABLE last_pageview_tstamp;
DROP TABLE last_pageview_tstamp_bak;
DROP TABLE last_sale_pageview_tstamp;
DROP TABLE last_sale_pageview_tstamp_bak;
DROP TABLE user_snapshot;
DROP TABLE user_snapshot_bak;
