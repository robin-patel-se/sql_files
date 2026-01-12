-- event stream

-- run event stream for - 2 weeks to - 1 week
-- run scv end to end on that data
-- archive event stream
-- run event stream for -2 weeks to current date
-- run scv on that data
-- work out how we would combine the two


-- use event tstamp


------------------------------------------------------------------------------------------------------------------------
-- populating synthetic data of 2 weeks from the beginning of june (1st to 14th)
------------------------------------------------------------------------------------------------------------------------

-- adding this filter to the event stream
-- AND event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'

-- event stream

DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

-- checking data

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
; -- 163,957,714 - 164M rows

SELECT
	MIN(event_tstamp),
	MAX(event_tstamp),
	MIN(etl_tstamp),
	MAX(etl_tstamp),
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

/*
MIN(EVENT_TSTAMP)				MAX(EVENT_TSTAMP)				MIN(ETL_TSTAMP)					MAX(ETL_TSTAMP)
2018-07-21 09:25:09.872000000	2025-07-11 17:41:48.132000000	2025-06-01 00:00:00.937000000	2025-06-14 23:59:58.992000000
*/


-- scv
-- 01_artificial_transaction_insert
-- 02_page_screen_enrichment
-- 03_app_push_enhancement
-- 01_module_identity_associations
-- 01_module_unique_urls
-- 02_02_module_url_params
-- 02_01_module_url_hostname
-- 02_module_identity_stitching
-- 03_module_extracted_params
-- 01_touchifiable_events
-- 02_02_time_diff_marker
-- 02_01_utm_or_referrer_hostname_marker
-- 03_touchification
-- 07_module_touched_booking_form_views
-- 09_module_touched_pay_button_clicks
-- 02_module_touched_transactions
-- 01_module_touched_spvs
-- 03_module_touched_searches
-- 08_module_touched_in_app_notification_events
-- 04_module_touched_app_installs
-- 00_anomalous_user_dates
-- 10_module_events_of_interest
-- 01_module_touch_basic_attributes
-- 01_module_touch_utm_referrer
-- 05_module_touched_feature_flags
-- 02_module_touch_marketing_channel
-- 01_module_touch_attribution

DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg
;

---- 01_artificial_transaction_insert

-- module=/biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/01_artificial_transaction_insert.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

self_describing_task --include '01_artificial_transaction_insert'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

---- 02_page_screen_enrichment

-- module=/biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/02_page_screen_enrichment.py make clones

SELECT
	event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
GROUP BY 1
;

self_describing_task --include '02_page_screen_enrichment'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 03_app_push_enhancement

-- biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/03_app_push_enhancement.py

self_describing_task --include '03_app_push_enhancement'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

---- 01_module_identity_associations

-- module=/biapp/task_catalogue/dv/dwh/scv/02_identity_stitching/01_module_identity_associations.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking
	CLONE latest_vault.cms_mysql.external_booking
;

self_describing_task --include '01_module_identity_associations'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

---- 01_module_unique_urls

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/01_module_unique_urls.py make clones

self_describing_task --include '01_module_unique_urls'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls muu
WHERE url IS NULL
;

---- 02_02_module_url_params

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_02_module_url_params.py make clones

-- NOTE: need to adjust pk on this table to remove parameter

self_describing_task --include '02_02_module_url_params'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params mup
;

---- 02_01_module_url_hostname

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_01_module_url_hostname.py make clones

self_describing_task --include '02_01_module_url_hostname'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname muh
;

---- 02_module_identity_stitching

-- module=/biapp/task_catalogue/dv/dwh/scv/02_identity_stitching/02_module_identity_stitching.py make clones

self_describing_task --include '02_module_identity_stitching'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching mis
;

---- 03_module_extracted_params

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/03_module_extracted_params.py make clones

self_describing_task --include '03_module_extracted_params'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

---- 01_touchifiable_events

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/01_touchifiable_events.py make clones

self_describing_task --include '01_touchifiable_events'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events mte
;

---- 02_02_time_diff_marker

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/02_02_time_diff_marker.py make clones

self_describing_task --include '02_02_time_diff_marker'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 02_01_utm_or_referrer_hostname_marker

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py make clones

self_describing_task --include '02_01_utm_or_referrer_hostname_marker'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 03_touchification

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py make clones

-- NOTE: separate out the merge function

self_describing_task --include '03_touchification.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

---- 07_module_touched_booking_form_views

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/07_module_touched_booking_form_views.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

self_describing_task --include '07_module_touched_booking_form_views.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

---- 09_module_touched_pay_button_clicks

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_touched_pay_button_clicks.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

self_describing_task --include '09_module_touched_pay_button_clicks.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 02_module_touched_transactions

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py make clones

self_describing_task --include '02_module_touched_transactions.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 01_module_touched_spvs

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py make clones

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
	CLONE data_vault_mvp.dwh.se_sale
;

self_describing_task --include '01_module_touched_spvs.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 03_module_touched_searches

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py make clones

self_describing_task --include '03_module_touched_searches.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 08_module_touched_in_app_notification_events

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/08_module_touched_in_app_notification_events.py make clones

self_describing_task --include '08_module_touched_in_app_notification_events.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 04_module_touched_app_installs

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py make clones

self_describing_task --include '04_module_touched_app_installs.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 00_anomalous_user_dates

-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py make clones


self_describing_task --include '00_anomalous_user_dates.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 10_module_events_of_interest

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/10_module_events_of_interest.py make clones

self_describing_task --include '10_module_events_of_interest.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 01_module_touch_basic_attributes

-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;


self_describing_task --include '01_module_touch_basic_attributes.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 01_module_touch_utm_referrer

-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py make clones

self_describing_task --include '01_module_touch_utm_referrer.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 05_module_touched_feature_flags

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/05_module_touched_feature_flags.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.feature_toggle
	CLONE latest_vault.cms_mysql.feature_toggle
;

self_describing_task --include '05_module_touched_feature_flags.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 02_module_touch_marketing_channel

-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate
	CLONE latest_vault.cms_mysql.affiliate
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

self_describing_task --include '02_module_touch_marketing_channel.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'


---- 01_module_touch_attribution

-- module=/biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py make clones

self_describing_task --include '01_module_touch_attribution.py'  --method 'run' --start '2025-06-01 00:00:00' --end '2025-06-01 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- archive

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_14 CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream
;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_archive CLONE data_vault_mvp_dev_robin.single_customer_view_stg
;

-- truncate tables
TRUNCATE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
;


SELECT
	'TRUNCATE TABLE ' || table_catalog || '.' || table_schema || '.' || table_name || ';' AS cmd
FROM data_vault_mvp.information_schema.tables
WHERE table_catalog = 'DATA_VAULT_MVP' AND table_schema = 'SINGLE_CUSTOMER_VIEW_STG'
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_pay_button_clicks
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.app_push_send_enhancement
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
;

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
;


------------------------------------------------------------------------------------------------------------------------
-- second run of event stream and scv - just for testing (simulating a 3 year trimmed version of scv)
-- AND event_tstamp::DATE BETWEEN '2025-06-08' AND '2025-06-20'


self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_artificial_transaction_insert'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_page_screen_enrichment'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '03_app_push_enhancement'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_module_identity_associations'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_module_unique_urls'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_02_module_url_params'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_01_module_url_hostname'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_module_identity_stitching'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '03_module_extracted_params'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_touchifiable_events'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_02_time_diff_marker'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_01_utm_or_referrer_hostname_marker'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '03_touchification.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '07_module_touched_booking_form_views.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '09_module_touched_pay_button_clicks.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_module_touched_transactions.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_module_touched_spvs.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '03_module_touched_searches.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '08_module_touched_in_app_notification_events.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '04_module_touched_app_installs.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '00_anomalous_user_dates.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '10_module_events_of_interest.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_module_touch_basic_attributes.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_module_touch_utm_referrer.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '05_module_touched_feature_flags.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '02_module_touch_marketing_channel.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'
self_describing_task --include '01_module_touch_attribution.py'  --method 'run' --start '2025-06-08 00:00:00' --end '2025-06-08 00:00:00'

------------------------------------------------------------------------------------------------------------------------

SELECT
	mtba.touch_start_tstamp::DATE,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_basic_attributes mtba
GROUP BY 1
;



SELECT
	mtba.touch_start_tstamp::DATE,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1
;



------------------------------------------------------------------------------------------------------------------------
-- archive event stream steps

-- clone table to new location with date of archive
-- update comment on archive table to note which dates are included in the archive table

-- create a clone of table for archive with a suffix of the date the archive was created

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_14 CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream
;
;

-- alter table comment to include details of dates
ALTER TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_23
	SET COMMENT = 'Historical event stream data filtered event_stamp from 2025-06-01 00:00:00 to 2025-06-13 23:59:59'
;

-- triple check that auto clustering is off on the archived table
SHOW TABLES IN SCHEMA hygiene_vault_mvp_dev_robin.snowplow
;



------------------------------------------------------------------------------------------------------------------------
-- combining event streams together
------------------------------------------------------------------------------------------------------------------------
-- find the max timestamp of the archive version, we will use this when combining the data together
-- we will remove 3 days from the end of the archive and use the data from the overlapping data set (3 year event stream or second archive)
-- this will accommodate for late arriving events and partial day data (on the day we archive)

SELECT
	MAX(event_tstamp)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_14
;


SELECT
	e.event_tstamp::DATE AS date,
	COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_14 e
GROUP BY 1
;


WITH
	unioned_data AS (
		SELECT *
		FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_14 e
		WHERE e.event_tstamp <= '2025-06-11' -- 3 days less than the max to allow for late arriving events and remove partial day data
		UNION ALL
		SELECT *
		FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
		WHERE es.event_tstamp >= '2025-06-11' -- greater than the filtered archive
	)
-- SELECT * FROM unioned_data;
SELECT
	ud.event_tstamp::DATE AS date,
	COUNT(*)
FROM unioned_data ud
GROUP BY 1
;

-- can create a view in pipeline that would union the datasets together based on their dates.

------------------------------------------------------------------------------------------------------------------------
-- combining scv datasets surfaced in se.data/data_pii
------------------------------------------------------------------------------------------------------------------------
-- scv files in se.data and se.data_pii

-- scv_event_stream.py
-- scv_touched_booking_form_views.py
-- scv_touched_in_app_notification_events.py
-- scv_touched_pay_button_clicks.py
-- scv_touched_transactions.py
-- scv_touched_feature_flags.py
-- scv_touched_searches.py
-- scv_touched_spvs.py
-- scv_touched_app_installs.py
-- scv_authorisation_events.py
-- scv_touch_events_of_interest.py
-- scv_page_screen_enrichment.py
-- scv_session_events_link.py
-- scv_touch_basic_attributes.py
-- scv_touch_marketing_channel.py
-- scv_touch_attribution.py
-- scv_branch_purchase_events.py
;

SHOW TABLES IN SCHEMA hygiene_vault_mvp.snowplow
;

13918231643648


---- scv_touch_basic_attributes

WITH
	unioned_data AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_basic_attributes mtba
		WHERE mtba.touch_start_tstamp <= '2025-06-08' -- filtered to be less than cut off dates of more recent archives OR live scv tables

		UNION ALL

		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes a
		WHERE a.touch_start_tstamp >= '2025-06-08' -- using start date of live data
	)
SELECT
	ud.touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM unioned_data ud
GROUP BY 1
;


SELECT
	mtba.touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp >= '2025-06-01'
GROUP BY 1


---- scv_touch_marketing_channel
WITH
	unioned_data AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel mtmc
		WHERE mtmc.touch_start_tstamp <= '2025-06-08' -- filtered to be less than cut off dates of more recent archives OR live scv tables

		UNION ALL

		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel m
		WHERE m.touch_start_tstamp >= '2025-06-08' -- using start date of live data
	)
SELECT
	ud.touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM unioned_data ud
GROUP BY 1
;


---- scv_touch_attribution
WITH
	unioned_data AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution mta
		WHERE mta.touch_start_tstamp <= '2025-06-08' -- filtered to be less than cut off dates of more recent archives OR live scv tables

		UNION ALL

		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution a
		WHERE a.touch_start_tstamp >= '2025-06-08' -- using start date of live data
	)
SELECT
	ud.touch_start_tstamp::DATE AS date ud.attribution_model, COUNT(*)
FROM unioned_data ud
GROUP BY ALL
;


---- scv_touched_booking_form_views
WITH
	unioned_data AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touched_booking_form_views mtbfv
		WHERE mtbfv.event_tstamp <= '2025-06-08' -- filtered to be less than cut off dates of more recent archives OR live scv tables

		UNION ALL

		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views m
		WHERE m.event_tstamp >= '2025-06-08' -- using start date of live data
	)
SELECT
	ud.event_tstamp::DATE AS date,
	COUNT(*)
FROM unioned_data ud
GROUP BY ALL
;

SELECT
	mtbfv.event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views mtbfv
WHERE mtbfv.event_tstamp BETWEEN '2025-06-01' AND '2025-06-21'
GROUP BY 1


---- scv_touched_in_app_notification_events
WITH
	unioned_data AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touched_in_app_notification_events mtiane
		WHERE mtiane.event_tstamp <= '2025-06-08' -- filtered to be less than cut off dates of more recent archives OR live scv tables

		UNION ALL

		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events m
		WHERE m.event_tstamp >= '2025-06-08' -- using start date of live data
	)
SELECT
	ud.event_tstamp::DATE AS date,
	COUNT(*)
FROM unioned_data ud
GROUP BY ALL
;

SELECT
	mtiane.event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events mtiane
WHERE mtiane.event_tstamp BETWEEN '2025-06-01' AND '2025-06-21'
GROUP BY 1
;

---- scv_touched_pay_button_clicks
WITH
	unioned_data AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touched_pay_button_clicks mtpbc
		WHERE mtpbc.event_tstamp <= '2025-06-08' -- filtered to be less than cut off dates of more recent archives OR live scv tables

		UNION ALL

		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_pay_button_clicks m
		WHERE m.event_tstamp >= '2025-06-08' -- using start date of live data
	)
SELECT
	ud.event_tstamp::DATE AS date,
	COUNT(*)
FROM unioned_data ud
GROUP BY ALL
;

SELECT
	mtpbc.event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_pay_button_clicks mtpbc
WHERE mtpbc.event_tstamp BETWEEN '2025-06-01' AND '2025-06-21'
GROUP BY 1


CREATE SCHEMA single_customer_view_historical.event_stream_2025_06_14 CLONE hygiene_vault_mvp.snowplow;
;

CREATE SCHEMA single_customer_view_historical.single_customer_view_2025_06_14 CLONE data_vault_mvp.single_customer_view_stg;
;

CREATE TABLE single_customer_view_historical.event_stream_2025_06_14.event_stream CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream_2025_06_14
;


CREATE SCHEMA single_customer_view_historical.single_customer_view_2025_06_14 CLONE data_vault_mvp_dev_robin.single_customer_view_archive
;
;

USE ROLE personal_role__robinpatel
;

CREATE DATABASE single_customer_view_historical_dev_robin
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.event_stream_2025_06_14
;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.event_stream_2025_06_14.event_stream
	CLONE single_customer_view_historical.event_stream_2025_06_14.event_stream
;

SELECT *
FROM single_customer_view_historical_dev_robin.unioned_data.event_stream_historical
WHERE source IS DISTINCT FROM 'hygiene_vault_mvp_dev_robin.snowplow.event_stream'
;

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14
;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_spvs
	CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_spvs
;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_spv.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.module_touched_spvs_historical
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14
;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_transactions
	CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_transactions
;

SELECT *
FROM single_customer_view_historical_dev_robin.unioned_data.module_touched_transactions_historical

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_transactions.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.module_touched_transactions_historical
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_booking_form_views
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_booking_form_views;


self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_booking_form_views.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_booking_form_views
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events
CLONE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_in_app_notification_events
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_in_app_notification_events;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_in_app_notification_events.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_in_app_notification_events
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_pay_button_clicks
CLONE data_vault_mvp.single_customer_view_stg.module_touched_pay_button_clicks;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_pay_button_clicks
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_pay_button_clicks;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_pay_button_clicks.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_pay_button_clicks
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
CLONE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_feature_flags
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_feature_flags;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_feature_flags.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_feature_flags
WHERE touch_start_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_searches
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_searches;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touched_searches.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'


SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_searches
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touched_app_installs
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touched_app_installs;


SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_app_installs
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_events_of_interest
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_events_of_interest;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touch_events_of_interest.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'


SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_events_of_interest
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.page_screen_enrichment
CLONE single_customer_view_historical.single_customer_view_2025_06_14.page_screen_enrichment;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_page_screen_enrichment.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_page_screen_enrichment
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touchification
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touchification;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_session_events_link.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	event_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_session_events_link
WHERE event_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touch_basic_attributes
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touch_basic_attributes;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touch_basic_attributes.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_basic_attributes
WHERE touch_start_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touch_marketing_channel
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touch_marketing_channel;

self_describing_task --include 'biapp/task_catalogue/scv_historical/scv_union/historical_touch_marketing_channel.py'  --method 'run' --start '2025-06-29 00:00:00' --end '2025-06-29 00:00:00'

SELECT
	archive_source,
	touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_marketing_channel
WHERE touch_start_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.single_customer_view_2025_06_14;

CREATE OR REPLACE TRANSIENT TABLE single_customer_view_historical_dev_robin.single_customer_view_2025_06_14.module_touch_attribution
CLONE single_customer_view_historical.single_customer_view_2025_06_14.module_touch_attribution;


SELECT
	archive_source,
	attribution_model,
	touch_start_tstamp::DATE AS date,
	COUNT(*)
FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_attribution
WHERE touch_start_tstamp::DATE BETWEEN '2025-06-01' AND '2025-06-14'
GROUP BY ALL
;


module=/biapp/task_catalogue/scv_historical/event_stream_union/historical_event_stream.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_page_screen_enrichment.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_scv_unions.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_session_events_link.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_attribution.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_basic_attributes.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_events_of_interest.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touch_marketing_channel.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_app_installs.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_booking_form_views.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_feature_flags.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_in_app_notification_events.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_pay_button_clicks.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_searches.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_spv.py make clones
module=/biapp/task_catalogue/scv_historical/scv_union/historical_touched_transactions.py make clones




SELECT
	es.event_tstamp,
	es.event_tstamp BETWEEN current_date-1 AND current_date+1
	FROM hygiene_vault_mvp.snowplow.event_stream es WHERE es.event_tstamp::DATE = current_date



SELECT *
FROM snowplow.atomic.events
WHERE collector_tstamp > '2025-06-30 14:15:00'


SELECT * FROM single_customer_view_historical_dev_robin.unioned_data.historical_page_screen_enrichment
DROP VIEW single_customer_view_historical_dev_robin.unioned_data.historical_page_screen_enrichment;