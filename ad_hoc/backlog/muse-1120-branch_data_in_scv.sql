SELECT DISTINCT
    es.unstruct_event_com_branch_secretescapes_install_1,
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE - 2
  AND es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

self_describing_task --include 'dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2021-11-25 00:00:00' --end '2021-11-25 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_01_utm_or_referrer_hostname_marker.py'  --method 'run' --start '2021-11-25 00:00:00' --end '2021-11-25 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/02_02_time_diff_marker.py'  --method 'run' --start '2021-11-25 00:00:00' --end '2021-11-25 00:00:00'
self_describing_task --include 'dv/dwh/events/03_touchification/03_touchification.py'  --method 'run' --start '2021-11-25 00:00:00' --end '2021-11-25 00:00:00'
self_describing_task --include 'dv/dwh/events/07_events_of_interest/04_module_touched_app_installs.py'  --method 'run' --start '2021-11-25 00:00:00' --end '2021-11-25 00:00:00'

SELECT
    es.event_tstamp,
    es.unstruct_event_com_branch_secretescapes_install_1,
    es.unstruct_event_com_branch_secretescapes_install_1:install_activity:attributed::BOOLEAN                                AS attributed,
    es.unstruct_event_com_branch_secretescapes_install_1:install_activity:event_name::VARCHAR                                AS event_name,
    es.unstruct_event_com_branch_secretescapes_install_1:existing_user::BOOLEAN                                              AS existing_user,
    es.unstruct_event_com_branch_secretescapes_install_1:install_activity:touch_data:tilde_advertising_partner_name::VARCHAR AS partner_name,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~campaign']::VARCHAR                    AS campaign,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~campaign_type']::VARCHAR               AS campaign_type,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~channel']::VARCHAR                     AS channel,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~branch_ad_format']::VARCHAR            AS branch_ad_format,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:brand::VARCHAR                                            AS brand,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:model::VARCHAR                                            AS model,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:os::VARCHAR                                               AS os,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:os_version::VARCHAR                                       AS os_version,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:sdk_version::VARCHAR                                      AS sdk_version
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND es.collector_tstamp::DATE >= CURRENT_DATE - 1;


DROP VIEW data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs;

USE WAREHOUSE pipe_2xlarge;
SELECT
    t.event_hash,
    t.touch_id,
    t.event_tstamp,
    'app install event'                                                                                                      AS event_category,
    'branch app install'                                                                                                     AS event_subcategory,
    es.unstruct_event_com_branch_secretescapes_install_1                                                                     AS app_install_context,
    es.unstruct_event_com_branch_secretescapes_install_1:install_activity:attributed::BOOLEAN                                AS attributed,
    es.unstruct_event_com_branch_secretescapes_install_1:install_activity:event_name::VARCHAR                                AS event_name,
    es.unstruct_event_com_branch_secretescapes_install_1:existing_user::BOOLEAN                                              AS existing_user,
    es.unstruct_event_com_branch_secretescapes_install_1:install_activity:touch_data:tilde_advertising_partner_name::VARCHAR AS partner_name,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~campaign']::VARCHAR                    AS campaign,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~campaign_type']::VARCHAR               AS campaign_type,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~channel']::VARCHAR                     AS channel,
    es.unstruct_event_com_branch_secretescapes_install_1:last_attributed_touch_data['~branch_ad_format']::VARCHAR            AS branch_ad_format,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:brand::VARCHAR                                            AS brand,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:model::VARCHAR                                            AS model,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:os::VARCHAR                                               AS os,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:os_version::VARCHAR                                       AS os_version,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:sdk_version::VARCHAR                                      AS sdk_version,
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:idfa::VARCHAR                                             AS idfa, -- identifier for advertiser
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:idfv::VARCHAR                                             AS idfv, -- identifier for vendor
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:aaid::VARCHAR                                             AS aaid
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream es ON t.event_hash = es.event_hash
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND t.updated_at >= TIMESTAMPADD('day', -1, '2021-11-24 03:00:00'::TIMESTAMP)
;

CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream es ON t.event_hash = es.event_hash
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND t.updated_at >= TIMESTAMPADD('day', -1, '2021-11-24 03:00:00'::TIMESTAMP)
LIMIT 5;


SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt;
SELECT
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_name = 'screen_view'
  AND es.event_tstamp >= CURRENT_DATE - 1;

------------------------------------------------------------------------------------------------------------------------


SELECT
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:idfa::VARCHAR AS idfa, -- identifier for advertiser
    es.unstruct_event_com_branch_secretescapes_install_1:user_data:idfv::VARCHAR AS idfv, -- identifier for vendor
    *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 1;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_name = 'screen_view'
  AND es.event_tstamp >= CURRENT_DATE - 1;

------------------------------------------------------------------------------------------------------------------------
--since donald changed
USE WAREHOUSE pipe_2xlarge;
CREATE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.updated_at BETWEEN '2022-08-23' AND CURRENT_DATE;

drop SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;


self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2022-08-23 00:00:00' --end '2022-08-23 00:00:00'

    airflow dags backfill --start-date '2022-08-23 02:00:00' --end-date '2022-08-23 02:00:00' hygiene__snowplow__event_stream__hourly

    airflow dags backfill --start-date '2022-08-22 00:00:00' --end-date '2022-08-23 00:00:00' single_customer_view__daily_at_03h00

SELECT * FROM data_vault_mvp_dev_robin.dwh.fact_booking fb;
SELECT get_ddl('table', 'data_vault_mvp_dev_robin.dwh.fact_booking');

SELECT * FROM latest_vault.cms_mysql.affiliate a;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate CLONE latest_vault.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;

drop table data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations;


