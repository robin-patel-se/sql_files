USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE SCHEMA hygiene_vault_mvp_dev_robin.snowplow CLONE hygiene_vault_mvp.snowplow;
GRANT OWNERSHIP ON TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream TO ROLE personal_role__robinpatel COPY CURRENT GRANTS;
CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg CLONE data_vault_mvp.single_customer_view_stg;
GRANT OWNERSHIP ON ALL TABLES IN SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg TO ROLE personal_role__robinpatel COPY CURRENT GRANTS;

TRUNCATE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_associations;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_pay_button_clicks;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_in_app_notification_events;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.app_push_send_enhancement;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_extracted_params;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs;
TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer;

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
-- running scv 2023 onwards on empty tables
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------
---- event_stream
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_data_fix_historic_events
	CLONE data_vault_mvp.dwh.tvl_data_fix_historic_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_historic_data_fix_ghost_opens
	CLONE data_vault_mvp.dwh.iterable_historic_data_fix_ghost_opens
;

self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- approx 1h46m
-- step01__model_data -- 13m44s 4XL
-- step02__dedupe_modelled_data -- 1h8m33s - 4XL
-- merge -- 12m26s


SELECT
	COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
; -- 9,971,025,460
SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
;

-- 25,842,351,259
------------------------------------------------------------------------------------------------------------------------
---- 01_artificial_transaction_insert
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/01_artificial_transaction_insert.py make clones

USE ROLE personal_role__robinpatel;
CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS SELECT * FROM data_vault_mvp.dwh.fact_booking;
self_describing_task --include '01_artificial_transaction_insert'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- whole file less than 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 02_page_screen_enrichment
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/02_page_screen_enrichment.py make clones

SELECT
	event_tstamp::DATE AS date,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
GROUP BY 1
;

self_describing_task --include '02_page_screen_enrichment'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 15mins

------------------------------------------------------------------------------------------------------------------------
---- 03_app_push_enhancement
------------------------------------------------------------------------------------------------------------------------

-- biapp/task_catalogue/dv/dwh/scv/00_event_stream_modelling/03_app_push_enhancement.py

self_describing_task --include '03_app_push_enhancement'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 01_module_identity_associations
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/02_identity_stitching/01_module_identity_associations.py make clones

USE ROLE personal_role__robinpatel;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.external_booking CLONE latest_vault.cms_mysql.external_booking;
self_describing_task --include '01_module_identity_associations'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 01_module_unique_urls
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/01_module_unique_urls.py make clones

self_describing_task --include '01_module_unique_urls'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_unique_urls muu
WHERE url IS NULL
;

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 02_02_module_url_params
------------------------------------------------------------------------------------------------------------------------
-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_02_module_url_params.py make clones

-- NOTE: need to adjust pk on this table to remove parameter

self_describing_task --include '02_02_module_url_params'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_params mup
;

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 02_01_module_url_hostname
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/02_01_module_url_hostname.py make clones

self_describing_task --include '02_01_module_url_hostname'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_url_hostname muh
;

-- Run times:
-- less than 1 minute

------------------------------------------------------------------------------------------------------------------------
---- 02_module_identity_stitching
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/02_identity_stitching/02_module_identity_stitching.py make clones

self_describing_task --include '02_module_identity_stitching'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching mis
;

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 03_module_extracted_params
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/01_url_manipulation/03_module_extracted_params.py make clones

self_describing_task --include '03_module_extracted_params'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 3 mins

------------------------------------------------------------------------------------------------------------------------
---- 01_touchifiable_events
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/01_touchifiable_events.py make clones

self_describing_task --include '01_touchifiable_events'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events mte
;

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 02_02_time_diff_marker
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/02_02_time_diff_marker.py make clones

self_describing_task --include '02_02_time_diff_marker'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 02_01_utm_or_referrer_hostname_marker
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/02_01_utm_or_referrer_hostname_marker.py make clones

self_describing_task --include '02_01_utm_or_referrer_hostname_marker'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 03_touchification
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/03_touchification/03_touchification.py make clones

-- NOTE: separate out the merge function

self_describing_task --include '03_touchification.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
; -- 2,541,547,927
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
;

-- 6,309,113,660

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 07_module_touched_booking_form_views
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/07_module_touched_booking_form_views.py make clones

USE ROLE personal_role__robinpatel;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty CLONE latest_vault.travelbird_mysql.orders_orderproperty;
self_describing_task --include '07_module_touched_booking_form_views.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 09_module_touched_pay_button_clicks
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/09_module_touched_pay_button_clicks.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelbird_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelbird_mysql.orders_orderproperty
	CLONE latest_vault.travelbird_mysql.orders_orderproperty
;

self_describing_task --include '09_module_touched_pay_button_clicks.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 02_module_touched_transactions
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/02_module_touched_transactions.py make clones

self_describing_task --include '02_module_touched_transactions.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 01_module_touched_spvs
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/01_module_touched_spvs.py make clones

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
self_describing_task --include '01_module_touched_spvs.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 03_module_touched_searches
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/03_module_touched_searches.py make clones

self_describing_task --include '03_module_touched_searches.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 5 mins

------------------------------------------------------------------------------------------------------------------------
---- 08_module_touched_in_app_notification_events
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/08_module_touched_in_app_notification_events.py make clones

self_describing_task --include '08_module_touched_in_app_notification_events.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 04_module_touched_app_installs
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/04_module_touched_app_installs.py make clones

self_describing_task --include '04_module_touched_app_installs.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 00_anomalous_user_dates
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/00_anomalous_user_dates.py make clones

self_describing_task --include '00_anomalous_user_dates.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 1 min

------------------------------------------------------------------------------------------------------------------------
---- 10_module_events_of_interest
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/10_module_events_of_interest.py make clones

self_describing_task --include '10_module_events_of_interest.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 01_module_touch_basic_attributes
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/05_touch_basic_attributes/01_module_touch_basic_attributes.py make clones

USE ROLE personal_role__robinpatel;
CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
self_describing_task --include '01_module_touch_basic_attributes.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 11 mins

------------------------------------------------------------------------------------------------------------------------
---- 01_module_touch_utm_referrer
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/01_module_touch_utm_referrer.py make clones

self_describing_task --include '01_module_touch_utm_referrer.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 17 mins


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer mtur
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer_bkup
;


------------------------------------------------------------------------------------------------------------------------
---- 05_module_touched_feature_flags
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/04_events_of_interest/05_module_touched_feature_flags.py make clones

USE ROLE personal_role__robinpatel;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.feature_toggle CLONE latest_vault.cms_mysql.feature_toggle;
self_describing_task --include '05_module_touched_feature_flags.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 5 mins

------------------------------------------------------------------------------------------------------------------------
---- 02_module_touch_marketing_channel
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py make clones

USE ROLE personal_role__robinpatel;
CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate CLONE latest_vault.cms_mysql.affiliate;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory CLONE latest_vault.cms_mysql.territory;
self_describing_task --include '02_module_touch_marketing_channel.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
;

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
---- 01_module_touch_attribution
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py make clones

self_describing_task --include '01_module_touch_attribution.py'  --method 'run' --start '2023-01-01 00:00:00' --end '2023-01-01 00:00:00'

-- Run times:
-- 2 mins

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

ALTER WAREHOUSE scv_pipe_4xlarge SET STATEMENT_TIMEOUT_IN_SECONDS = 7200
;

ALTER WAREHOUSE scv_pipe_4xlarge SET STATEMENT_TIMEOUT_IN_SECONDS = 3600
;

SHOW PARAMETERS FOR WAREHOUSE scv_pipe_4xlarge
;

SELECT COALESCE(NULL, {})

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer_bkup
WHERE module_touch_utm_referrer_bkup.landing_page_parameters = {}



------------------------------------------------------------------------------------------------------------------------
-- backup to ensure don't overwrite when doing unions

CREATE OR REPLACE SCHEMA hygiene_vault_mvp_dev_robin.snowplow_backup CLONE hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg_backup CLONE data_vault_mvp_dev_robin.single_customer_view_stg
;

------------------------------------------------------------------------------------------------------------------------
-- checking trimmed scv against prod scv with filters
------------------------------------------------------------------------------------------------------------------------

-- touch basic attributes
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touch_basic_attributes mtba

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touch_basic_attributes mtba
		WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.touch_experience,
	stack.touch_start_tstamp::DATE AS date,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL
;


-- touch channel
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touch_marketing_channel mtm

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touch_marketing_channel mtba
		WHERE mtba.touch_start_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.touch_start_tstamp::DATE AS session_date,
	stack.touch_mkt_channel,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL
;


-- touched spvs
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touched_spvs mts

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_spvs m
		WHERE m.event_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.event_category,
	stack.event_tstamp::DATE AS spv_date,
	COUNT(*)                 AS spvs
FROM stack
GROUP BY ALL
;


-- touched searches
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touched_searches s

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_searches s
		WHERE s.event_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.se_brand,
	stack.event_category,
	stack.triggered_by,
	stack.event_tstamp::DATE AS search_date,
	COUNT(*)                 AS searches
FROM stack
GROUP BY ALL
;

-- touched booking form views
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touched_booking_form_views mtbfv

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_booking_form_views m
		WHERE m.event_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.event_category,
	stack.event_tstamp::DATE AS bfv_date,
	COUNT(*)                 AS bfvs
FROM stack
GROUP BY ALL
;

-- touched feature flags
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touched_feature_flags mtff

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_feature_flags f
		WHERE f.touch_start_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.feature_flag,
	stack.touch_start_tstamp::DATE AS session_date,
	COUNT(*)                       AS feature_flags
FROM stack
GROUP BY ALL
;

-- touched transactions
WITH
	stack AS (
		SELECT
			'dev' AS source,
			*
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touched_transactions mtt

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_transactions m
		WHERE m.event_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.event_subcategory,
	stack.event_tstamp::DATE AS event_date,
	COUNT(*)                 AS transactions
FROM stack
GROUP BY ALL
;


-- touch attribution
WITH
	stack AS (
		SELECT
			'dev' AS source,
			mta.*,
			mtmc.touch_mkt_channel
		FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touch_attribution mta
			INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touch_marketing_channel mtmc
					   ON mta.attributed_touch_id = mtmc.touch_id

		UNION ALL

		SELECT
			'prod' AS source,
			a.*,
			mtmc.touch_mkt_channel
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touch_attribution a
			INNER JOIN single_customer_view_historical.single_customer_view_2025_07_01.module_touch_marketing_channel mtmc
					   ON a.attributed_touch_id = mtmc.touch_id
		WHERE a.touch_start_tstamp BETWEEN '2023-01-01' AND '2025-07-01'
	)
SELECT
	stack.source,
	stack.attribution_model,
	stack.touch_mkt_channel,
	stack.touch_start_tstamp::DATE AS session_date,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL
;


------------------------------------------------------------------------------------------------------------------------
-- checking unioned historical scv against prod scv
------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_event_stream esh
		WHERE esh.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.event_stream_2025_07_01.event_stream es
		WHERE es.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_tstamp::DATE AS date,
	COUNT(*)                 AS events
FROM stack
GROUP BY ALL
;


-- touch basic attributes
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_basic_attributes mtba
		WHERE mtba.touch_start_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touch_basic_attributes mtba
		WHERE mtba.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.touch_experience,
	stack.touch_start_tstamp::DATE AS date,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL
;

-- touch channel
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_marketing_channel mtm
		WHERE mtm.touch_start_tstamp >= '2018-01-01'
		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touch_marketing_channel mtba
		WHERE mtba.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.touch_start_tstamp::DATE AS session_date,
	stack.touch_mkt_channel,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL
;


-- touched spvs
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_spvs mts
		WHERE mts.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_spvs m
		WHERE m.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_category,
	stack.event_tstamp::DATE AS spv_date,
	COUNT(*)                 AS spvs
FROM stack
GROUP BY ALL
;


-- touched searches
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_searches s
		WHERE s.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_searches s
		WHERE s.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.se_brand,
	stack.event_category,
	stack.triggered_by,
	stack.event_tstamp::DATE AS search_date,
	COUNT(*)                 AS searches
FROM stack
GROUP BY ALL
;

-- touched booking form views
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_booking_form_views mtbfv
		WHERE mtbfv.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_booking_form_views m
		WHERE m.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_category,
	stack.event_tstamp::DATE AS bfv_date,
	COUNT(*)                 AS bfvs
FROM stack
GROUP BY ALL
;

-- touched feature flags
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_feature_flags mtff
		WHERE mtff.touch_start_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_feature_flags f
		WHERE f.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.feature_flag,
	stack.touch_start_tstamp::DATE AS session_date,
	COUNT(*)                       AS feature_flags
FROM stack
GROUP BY ALL
;

-- touched transactions
WITH
	stack AS (
		SELECT
			'historical' AS source,
			* EXCLUDE archive_source
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_transactions mtt
		WHERE mtt.event_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			*
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touched_transactions m
		WHERE m.event_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.event_subcategory,
	stack.event_tstamp::DATE AS event_date,
	COUNT(*)                 AS transactions
FROM stack
GROUP BY ALL
;


-- touch attribution
WITH
	stack AS (
		SELECT
			'historical' AS source,
			mta.* EXCLUDE archive_source, mtmc.touch_mkt_channel
		FROM single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_attribution mta
			INNER JOIN single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_marketing_channel mtmc
					   ON mta.attributed_touch_id = mtmc.touch_id
						   AND mtmc.touch_start_tstamp >= '2018-01-01'
		WHERE mta.touch_start_tstamp >= '2018-01-01'

		UNION ALL

		SELECT
			'prod' AS source,
			a.*,
			mtmc.touch_mkt_channel
		FROM single_customer_view_historical.single_customer_view_2025_07_01.module_touch_attribution a
			INNER JOIN single_customer_view_historical.single_customer_view_2025_07_01.module_touch_marketing_channel mtmc
					   ON a.attributed_touch_id = mtmc.touch_id
						   AND mtmc.touch_start_tstamp >= '2018-01-01'
		WHERE a.touch_start_tstamp >= '2018-01-01'
	)
SELECT
	stack.source,
	stack.attribution_model,
	stack.touch_mkt_channel,
	stack.touch_start_tstamp::DATE AS session_date,
	COUNT(*)                       AS sessions
FROM stack
GROUP BY ALL
;

------------------------------------------------------------------------------------------------------------------------
self_describing_task --include 'biapp/task_catalogue/staging/hygiene/snowplow/event_stream.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_artificial_transaction_insert'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_page_screen_enrichment'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '03_app_push_enhancement'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_module_identity_associations'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_module_unique_urls'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_02_module_url_params'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_01_module_url_hostname'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_module_identity_stitching'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '03_module_extracted_params'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_touchifiable_events'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_02_time_diff_marker'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_01_utm_or_referrer_hostname_marker'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '03_touchification.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '07_module_touched_booking_form_views.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '09_module_touched_pay_button_clicks.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_module_touched_transactions.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_module_touched_spvs.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '03_module_touched_searches.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '08_module_touched_in_app_notification_events.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '04_module_touched_app_installs.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '00_anomalous_user_dates.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '10_module_events_of_interest.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_module_touch_basic_attributes.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_module_touch_utm_referrer.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '05_module_touched_feature_flags.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '02_module_touch_marketing_channel.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'
self_describing_task --include '01_module_touch_attribution.py'  --method 'run' --start '2025-07-01 00:00:00' --end '2025-07-01 00:00:00'


------------------------------------------------------------------------------------------------------------------------

-- testing adjusting the join logic in 01_module_touch_utm_referrer.py

TRUNCATE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
;

SELECT
	PARSE_URL('https://www.secretescapes.com?test=value'),
	PARSE_URL('https://www.secretescapes.com?test=value')['parameters']::OBJECT,
	PARSE_URL('https://www.secretescapes.com?test=value')['parameters'] IS DISTINCT FROM NULL,
	PARSE_URL('https://www.secretescapes.com')['parameters']::OBJECT,
	PARSE_URL('https://www.secretescapes.com')['parameters']::OBJECT IS DISTINCT FROM NULL,


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
WHERE landing_page_parameters = {}
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg_backup.module_touch_utm_referrer
WHERE landing_page_parameters IS NULL
;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
WHERE landing_page_parameters IS NULL
;


SELECT * FROm data_vault_mvp.single_customer_view_stg.module_url_params mup

SELECT get_ddl('table', 'data_vault_mvp.single_customer_view_stg.module_url_params');



CREATE OR REPLACE SCHEMA  hygiene_vault_mvp_dev_robin.snowplow CLONE  hygiene_vault_mvp_dev_robin.snowplow_backup;
;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg CLONE  data_vault_mvp_dev_robin.single_customer_view_stg_backup;
;