USE WAREHOUSE pipe_xlarge;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.customer_model_full_uk_de_stg;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking clone data_vault_mvp.customer_model_full_uk_de_stg.stream_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email clone data_vault_mvp.customer_model_full_uk_de_stg.stream_email;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_spv clone data_vault_mvp.customer_model_full_uk_de_stg.stream_spv;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes clone data_vault_mvp.customer_model_full_uk_de_stg.static_member_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar clone data_vault_mvp.customer_model_full_uk_de_stg.static_member_calendar;

CREATE SCHEMA data_vault_mvp.single_customer_view_stg;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs clone data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes clone data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel clone data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_sale clone data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_offer clone data_vault_mvp.dwh.tb_offer;

CREATE OR REPLACE TABLE se_dev_robin.data.user_subscription CLONE se.data.user_subscription;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot clone data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.affiliate_snapshot clone data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot clone data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.user_acquisition_platform clone raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.chiasma_sql_server.affiliate_classification clone raw_vault_mvp.chiasma_sql_server.affiliate_classification;

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp.customer_model_full_uk_de_stg.stream_booking
GROUP BY 1;

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp.customer_model_full_uk_de_stg.stream_booking
GROUP BY 1;

SELECT MAX(calendar_date)
FROM data_vault_mvp.customer_model.customer_model_full_uk_de; --2020-03-31

self_describing_task --include 'dv/customer_model_full_uk_de/001_static_input_members'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'

SELECT MAX(signup_date)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg__20200406t000000__every7days.static_input_members;
SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg__20200406t000000__every7days.static_input_members;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot

SELECT u.id                 AS user_id,
       u.date_created::DATE AS signup_date
FROM data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot u
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON u.original_affiliate_id = a.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON a.territory_id = t.id
WHERE t.name IN ('UK', 'DE') AND u.date_created >= '2020-04-01'


self_describing_task --include 'dv/customer_model_full_uk_de/001_static_input_members'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/010_static_member_attributes'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/020_static_member_calendar'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/030_stream_booking'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/040_stream_email'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/050_stream_spv'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/050_stream_spv'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/050_stream_spv'  --method 'run' --start '2020-04-07 00:00:00' --end '2020-04-07 00:00:00'

--run final merge past the 23rd of March to include all data.
self_describing_task --include 'dv/customer_model_full_uk_de/100_final_customer_model'  --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'
self_describing_task --include 'dv/customer_model_full_uk_de/200_replace_production_final_customer_model'  --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes
GROUP BY 1;

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar
GROUP BY 1;

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email
GROUP BY 1;

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_spv
GROUP BY 1;

SELECT updated_at::DATE, COUNT(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking
GROUP BY 1;

SELECT * FROM data_vault_mvp.dwh.se_booking WHERE shiro_user_id = 62972247;
SELECT * FROM se.data.user_emails WHERE user_id = 62972247;
SELECT * FROM se.data.user_subscription WHERE user_id = 62972247;
SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes WHERE attributed_user_id = '62972247';

--delete from all streams to only keep hero user
DELETE FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg__20200406t000000__every7days.static_input_members
WHERE user_id != 62972247;
DELETE FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes
WHERE user_id != 62972247;
DELETE FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar
WHERE user_id != 62972247;
DELETE FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email
WHERE user_id != 62972247;
DELETE FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_spv
WHERE user_id != 62972247;
DELETE FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking
WHERE user_id != 62972247;


SELECT * FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar;

SELECT MIN(UPDATED_AT) FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar;
SELECT MIN(UPDATED_AT) FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking;
SELECT MIN(UPDATED_AT) FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes;

SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       updated_at,
       member_id,
       calendar_year_month,
       calendar_date,
       member_original_affiliate_territory,
       member_original_affiliate_classification,
       member_original_affiliate_name,
       member_cohort_id,
       member_cohort_year_month,
       member_signup_date,
       member_subscription_status,
       member_acquisition_platform,
       member_acquisition_method,
       member_has_new_app,
       member_first_app_spv,
       member_cumulative_gone_on_holidays_count,
       member_days_since_first_checkout_count,
       member_days_since_previous_checkout_count,
       member_age,
       customer_age,
       booking_margin,
       booking_cumulative_count,
       booking_cumulative_margin_gbp,
       booking_completed_count,
       member_days_since_previous_completed_booking_event,
       hotel_booking_count,
       package_booking_count,
       booking_checkin_date,
       booking_checkout_date,
       booking_nights_stayed_count,
       sessions,
       cumulative_sessions,
       spv_count,
       spv_unique_count,
       spv_hotel_count,
       spv_hotel_unique_count,
       spv_package_count,
       spv_package_unique_count,
       booking_form_views_count,
       unique_booking_form_views_count,
       last_mkt_clickid,
       email_opens_count,
       email_unique_opens_count,
       email_clicks_count,
       email_unique_clicks_count,
       email_unique_sends_count,
       email_sends_count
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.final_customer_model;


SELECT * FROM data_vault_mvp.dwh.user_subscription_event WHERE user_id = 62972247;

CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.final_customer_model clone data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.final_customer_model;

self_describing_task --include 'dv/customer_model_full_uk_de/100_final_customer_model'  --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'

DROP VIEW data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de;

SELECT * FROM data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de;

