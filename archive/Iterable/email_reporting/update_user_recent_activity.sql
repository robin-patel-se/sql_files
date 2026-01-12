CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_click_event AS
SELECT *
FROM data_vault_mvp.dwh.email_click_event;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_open_event AS
SELECT *
FROM data_vault_mvp.dwh.email_open_event;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_pageview CLONE data_vault_mvp.dwh.user_last_pageview;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_spv CLONE data_vault_mvp.dwh.user_last_spv;


DROP TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities;

self_describing_task --include 'dv/dwh/user_attributes/user_recent_activities.py'  --method 'run' --start '2021-12-02 00:00:00' --end '2021-12-02 00:00:00'

SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities
WHERE user_recent_activities.last_email_open_tstamp IS NOT NULL;
SELECT COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities
WHERE user_recent_activities.last_email_open_tstamp IS NOT NULL;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_spv CLONE data_vault_mvp.dwh.user_last_spv;

self_describing_task --include 'dv/dwh/user_attributes/user_last_spv.py'  --method 'run' --start '2021-12-02 00:00:00' --end '2021-12-02 00:00:00'

SELECT MIN(loaded_at)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts --2020-02-28 17:08:34.387000000


SELECT MIN(USER_RECENT_ACTIVITIES.created_at)
FROM data_vault_mvp.dwh.user_recent_activities -- 2021-10-14 16:52:42.110000000v