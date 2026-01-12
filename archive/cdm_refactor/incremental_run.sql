--static input members

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;

--comment out dependencies
self_describing_task --include 'dv/customer_model_full_uk_de/001_static_input_members'  --method 'run' --start '2020-03-30 00:00:00' --end '2020-03-30 00:00:00'

--static member attributes
CREATE OR REPLACE TABLE raw_vault_dev_robin.chiasma_sql_server.shiro_user_snapshot CLONE raw_vault_mvp.chiasma_sql_server.affiliate_classification;
CREATE OR REPLACE TABLE raw_vault_dev_robin.chiasma_sql_server.user_acquisition_platform CLONE raw_vault_mvp.chiasma_sql_server.user_acquisition_platform;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes CLONE data_vault_mvp.customer_model_full_uk_de_stg.static_member_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

--comment out dependencies
self_describing_task --include 'dv/customer_model_full_uk_de/010_static_member_attributes'  --method 'run' --start '2020-03-30 00:00:00' --end '2020-03-30 00:00:00'

SELECT updated_at, count(*)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes
GROUP BY 1;


--static member calendar
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar CLONE data_vault_mvp.customer_model_full_uk_de_stg.static_member_calendar;
CREATE OR REPLACE TABLE se_dev_robin.data.se_calendar CLONE se.data.se_calendar;

self_describing_task --include 'dv/customer_model_full_uk_de/020_static_member_calendar'  --method 'run' --start '2020-03-30 00:00:00' --end '2020-03-30 00:00:00'

SELECT MAX(calendar_date)
FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar;

--stream booking

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking CLONE data_vault_mvp.customer_model_full_uk_de_stg.stream_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;

self_describing_task --include 'dv/customer_model_full_uk_de/030_stream_booking'  --method 'run' --start '2020-03-30 00:00:00' --end '2020-03-30 00:00:00'


--stream email
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email CLONE data_vault_mvp.customer_model_full_uk_de_stg.stream_email;
CREATE OR REPLACE TABLE se_dev_robin.data.user_emails CLONE se.data.user_emails;

self_describing_task --include 'dv/customer_model_full_uk_de/040_stream_email'  --method 'run' --start '2020-03-30 00:00:00' --end '2020-03-30 00:00:00'

--stream spv
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_spv CLONE data_vault_mvp.customer_model_full_uk_de_stg.stream_spv;
CREATE OR REPLACE VIEW se_dev_robin.data.dim_sale AS
(
SELECT *
FROM se.data.dim_sale
    );


self_describing_task --include 'dv/customer_model_full_uk_de/050_stream_spv'  --method 'run' --start '2020-03-30 00:00:00' --end '2020-03-30 00:00:00'


--final merge
CREATE OR REPLACE TABLE se_dev_robin.data.user_subscription CLONE se.data.user_subscription;
SELECT MIN(updated_at) FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar; --2020-03-30 15:41:05.408000000
SELECT MIN(updated_at) FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking; --2020-03-25 00:00:00.000000000
SELECT MIN(updated_at) FROM data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_spv; --2020-04-15 13:10:32.520000000


self_describing_task --include 'dv/customer_model_full_uk_de/100_final_customer_model' --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'

airflow backfill --start_date '2020-03-30 00:00:00' --end_date '2020-05-03 00:00:00' --task_regex '.*' -m customer_model_full_uk_de__every7days


--run on prod
--make backups
CREATE SCHEMA data_vault_mvp.customer_model_bkup;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.static_member_calendar_bkup CLONE data_vault_mvp.customer_model_full_uk_de_stg.static_member_calendar;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.static_member_attributes_bkup CLONE data_vault_mvp.customer_model_full_uk_de_stg.static_member_attributes;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.stream_booking_bkup CLONE data_vault_mvp.customer_model_full_uk_de_stg.stream_booking;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.stream_email_bkup CLONE data_vault_mvp.customer_model_full_uk_de_stg.stream_email;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.stream_spv_bkup CLONE data_vault_mvp.customer_model_full_uk_de_stg.stream_spv;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.customer_model_full_uk_de_bkup CLONE data_vault_mvp.customer_model.customer_model_full_uk_de;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_bkup.customer_model_last7days_uk_de_bkup CLONE data_vault_mvp.customer_model.customer_model_last7days_uk_de;

--swap in production tables
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.static_member_calendar CLONE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_calendar;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.static_member_attributes CLONE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.static_member_attributes;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.stream_booking CLONE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_booking;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.stream_email CLONE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_email;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model_full_uk_de_stg.stream_spv CLONE data_vault_mvp_dev_robin.customer_model_full_uk_de_stg.stream_spv;
CREATE OR REPLACE TABLE data_vault_mvp.customer_model.customer_model_full_uk_de CLONE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de;

USE WAREHOUSE pipe_large;
SELECT * FROM data_vault_mvp.customer_model.customer_model_full_uk_de WHERE member_id = 62972247;
SELECT * FROM data_vault_mvp.customer_model.customer_model_full_uk_de WHERE member_id = 67970160;
SELECT * FROM data_vault_mvp.customer_model.customer_model_full_uk_de WHERE member_id = 47809201;

GRANT SELECT ON TABLE data_vault_mvp.customer_model.customer_model_full_uk_de TO ROLE personal_role__alexscottsimons;


SELECT * FROM se.data.customer_data_model WHERE member_id = 62972247;

GRANT SELECT ON TABLE se.data.customer_data_model  TO ROLE personal_role__alexscottsimons;


create transient table CUSTOMER_MODEL_BKUP.CUSTOMER_MODEL_FULL_UK_DE_BKUP
(
	MEMBER_ID NUMBER,
	CALENDAR_YEAR_MONTH VARCHAR,
	CALENDAR_DATE DATE,
	MEMBER_ORIGINAL_AFFILIATE_TERRITORY VARCHAR,
	MEMBER_ORIGINAL_AFFILIATE_CLASSIFICATION VARCHAR,
	MEMBER_ORIGINAL_AFFILIATE_NAME VARCHAR,
	MEMBER_COHORT_ID NUMBER,
	MEMBER_COHORT_YEAR_MONTH VARCHAR,
	MEMBER_SIGNUP_DATE DATE,
	MEMBER_SUBSCRIPTION_STATUS NUMBER,
	MEMBER_ACQUISITION_PLATFORM VARCHAR,
	MEMBER_ACQUISITION_METHOD VARCHAR,
	MEMBER_HAS_NEW_APP NUMBER,
	MEMBER_FIRST_APP_SPV DATE,
	MEMBER_AGE NUMBER,
	CUSTOMER_AGE NUMBER,
	BOOKING_MARGIN DOUBLE,
	BOOKING_CUMULATIVE_COUNT NUMBER,
	BOOKING_COMPLETED_COUNT NUMBER,
	SPV_COUNT NUMBER,
	SPV_UNIQUE_COUNT NUMBER,
	LAST_MKT_CLICKID VARCHAR,
	BOOKING_FORM_VIEWS_COUNT NUMBER,
	UNIQUE_BOOKING_FORM_VIEWS_COUNT NUMBER,
	MEMBER_DAYS_SINCE_PREVIOUS_COMPLETED_BOOKING_EVENT NUMBER,
	EMAIL_OPENS_COUNT NUMBER,
	EMAIL_UNIQUE_OPENS_COUNT NUMBER,
	EMAIL_CLICKS_COUNT NUMBER,
	EMAIL_UNIQUE_CLICKS_COUNT NUMBER,
	SPV_HOTEL_COUNT NUMBER,
	SPV_HOTEL_UNIQUE_COUNT NUMBER,
	SPV_PACKAGE_COUNT NUMBER,
	SPV_PACKAGE_UNIQUE_COUNT NUMBER,
	HOTEL_BOOKING_COUNT NUMBER,
	PACKAGE_BOOKING_COUNT NUMBER,
	BOOKING_CHECKIN_DATE DATE,
	BOOKING_CHECKOUT_DATE DATE,
	BOOKING_NIGHTS_STAYED_COUNT NUMBER,
	MEMBER_CUMULATIVE_GONE_ON_HOLIDAYS_COUNT NUMBER,
	MEMBER_DAYS_SINCE_FIRST_CHECKOUT_COUNT NUMBER,
	MEMBER_DAYS_SINCE_PREVIOUS_CHECKOUT_COUNT NUMBER
);

create table CUSTOMER_MODEL.CUSTOMER_MODEL_FULL_UK_DE
(
	SCHEDULE_TSTAMP TIMESTAMPNTZ,
	RUN_TSTAMP TIMESTAMPNTZ,
	OPERATION_ID VARCHAR,
	CREATED_AT TIMESTAMPNTZ,
	UPDATED_AT TIMESTAMPNTZ,
	MEMBER_ID NUMBER,
	CALENDAR_YEAR_MONTH VARCHAR,
	CALENDAR_DATE DATE,
	MEMBER_ORIGINAL_AFFILIATE_TERRITORY VARCHAR,
	MEMBER_ORIGINAL_AFFILIATE_CLASSIFICATION VARCHAR,
	MEMBER_ORIGINAL_AFFILIATE_NAME VARCHAR,
	MEMBER_COHORT_ID NUMBER,
	MEMBER_COHORT_YEAR_MONTH VARCHAR,
	MEMBER_SIGNUP_DATE DATE,
	MEMBER_SUBSCRIPTION_STATUS NUMBER,
	MEMBER_ACQUISITION_PLATFORM VARCHAR,
	MEMBER_ACQUISITION_METHOD VARCHAR,
	MEMBER_HAS_NEW_APP NUMBER,
	MEMBER_FIRST_APP_SPV DATE,
	MEMBER_CUMULATIVE_GONE_ON_HOLIDAYS_COUNT NUMBER,
	MEMBER_DAYS_SINCE_FIRST_CHECKOUT_COUNT NUMBER,
	MEMBER_DAYS_SINCE_PREVIOUS_CHECKOUT_COUNT NUMBER,
	MEMBER_AGE NUMBER,
	CUSTOMER_AGE NUMBER,
	BOOKING_MARGIN DOUBLE,
	BOOKING_CUMULATIVE_COUNT NUMBER,
	BOOKING_CUMULATIVE_MARGIN_GBP DOUBLE,
	BOOKING_COMPLETED_COUNT NUMBER,
	MEMBER_DAYS_SINCE_PREVIOUS_COMPLETED_BOOKING_EVENT NUMBER,
	HOTEL_BOOKING_COUNT NUMBER,
	PACKAGE_BOOKING_COUNT NUMBER,
	BOOKING_CHECKIN_DATE DATE,
	BOOKING_CHECKOUT_DATE DATE,
	BOOKING_NIGHTS_STAYED_COUNT NUMBER,
	SESSIONS NUMBER,
	CUMULATIVE_SESSIONS NUMBER,
	SPV_COUNT NUMBER,
	SPV_UNIQUE_COUNT NUMBER,
	SPV_HOTEL_COUNT NUMBER,
	SPV_HOTEL_UNIQUE_COUNT NUMBER,
	SPV_PACKAGE_COUNT NUMBER,
	SPV_PACKAGE_UNIQUE_COUNT NUMBER,
	BOOKING_FORM_VIEWS_COUNT NUMBER,
	UNIQUE_BOOKING_FORM_VIEWS_COUNT NUMBER,
	LAST_MKT_CLICKID VARCHAR,
	EMAIL_OPENS_COUNT NUMBER,
	EMAIL_UNIQUE_OPENS_COUNT NUMBER,
	EMAIL_CLICKS_COUNT NUMBER,
	EMAIL_UNIQUE_CLICKS_COUNT NUMBER,
	EMAIL_UNIQUE_SENDS_COUNT NUMBER,
	EMAIL_SENDS_COUNT NUMBER
);

--rerun due to missing data 2020-05-05
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de_bkup clone data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de;

DROP TABLE data_vault_mvp_dev_robin.customer_model.customer_model_full_uk_de;
self_describing_task --include 'dv/customer_model_full_uk_de/100_final_customer_model' --method 'run' --start '2020-03-23 00:00:00' --end '2020-03-23 00:00:00'

