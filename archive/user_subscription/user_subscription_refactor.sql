self_describing_task --include 'dv/dwh/user_attributes/user_subscription_event.py'  --method 'run' --start '2020-12-14 00:00:00' --end '2020-12-14 00:00:00'

SELECT count(*) FROM raw_vault_mvp.cms_mysql.profile p

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mysql.profile clone raw_vault_mvp.cms_mysql.profile;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;

SELECT * FROM data_vault_mvp_dev_robin.dwh.user_subscription_event WHERE USER_SUBSCRIPTION_EVENT.user_id = 62972247;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_subscription clone se.data.user_subscription;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_calendar CLONE data_vault_mvp.dwh.se_calendar;

self_describing_task --include 'dv/dwh/user_attributes/user_subscription.py'  --method 'run' --start '2020-12-14 00:00:00' --end '2020-12-14 00:00:00'


SELECT sua.shiro_user_id,
       sua.original_affiliate_id,
       sua.original_affiliate_name,
       sua.original_affiliate_territory_id,
       se.data.POSA_CATEGORY_FROM_TERRITORY(sua.original_affiliate_territory) AS original_affiliate_territory,
       sua.member_original_affiliate_classification,
       sua.current_affiliate_id,
       sua.current_affiliate_name,
       sua.current_affiliate_territory_id,
       se.data.POSA_CATEGORY_FROM_TERRITORY(sua.current_affiliate_territory) AS current_affiliate_territory,
       sua.cohort_id,
       sua.cohort_year_month,
       sua.signup_tstamp,
       sua.acquisition_platform,
       sua.email_opt_in,
       sua.push_opt_in,
       sua.app_cohort_id,
       sua.app_cohort_year_month,
       sua.first_app_activity_tstamp,
       sua.last_email_open_tstamp,
       sua.last_email_click_tstamp,
       sua.last_pageview_tstamp,
       sua.last_sale_pageview_tstamp,
       sua.last_abandoned_booking_tstamp,
       sua.last_complete_booking_tstamp,
       se.data.member_recency_status(sua.signup_tstamp, current_date) AS member_recency_status
 FROM se.data.se_user_attributes sua;