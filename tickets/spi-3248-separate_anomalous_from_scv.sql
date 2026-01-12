SELECT
    sts.event_tstamp::DATE AS event_date,
    mt.attributed_user_id,
    mt.stitched_identity_type,
    COUNT(*)               AS daily_spvs
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs AS sts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification AS mt
               ON sts.touch_id = mt.touch_id
                   AND
                  mt.event_tstamp >= CURRENT_DATE - 10 -- todo make incremental, make sure its truncated to the date
WHERE sts.event_tstamp::DATE >= CURRENT_DATE - 10 -- todo make incremental, make sure its truncated to the date
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 500
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker mtbaat;

SELECT DISTINCT
    attributed_user_id
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;



SELECT DISTINCT
    attributed_user_id,
    stitched_identity_type
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
WHERE event_date < CURRENT_DATE - 1;


USE ROLE pipelinerunner;
SELECT
    role_name,
    query_text,
    user_name,
    qh.query_tag
FROM snowflake.account_usage.query_history qh
WHERE LOWER(qh.query_text) LIKE '%hygiene_snapshot_vault_mvp.cms_mysql.shiro_user%'
  AND role_name = 'PERSONAL_ROLE__TABLEAU';


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker mtbaat;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/00_anomalous_user_dates.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'

--create backup for comparison
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_20230414 CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

-- drop to empty the table
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/00_anomalous_user_dates.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'


SELECT
    touch_hostname_territory,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;


SELECT
    touch_hostname_territory,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_20230414 mtba
GROUP BY 1;


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.city_translation CLONE latest_vault.cms_mysql.city_translation;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country_translation CLONE latest_vault.cms_mysql.country_translation;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.favorite CLONE latest_vault.cms_mysql.favorite;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list CLONE latest_vault.cms_mysql.wish_list;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list_item CLONE latest_vault.cms_mysql.wish_list_item;

