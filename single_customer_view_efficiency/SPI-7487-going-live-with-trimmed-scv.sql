------------------------------------------------------------------------------------------------------------------------
-- Archive scv
------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

-- CREATE SCHEMA single_customer_view_historical.event_stream_2025_07_14_moo CLONE hygiene_vault_mvp.snowplow
-- ;
-- ;

SELECT
	COUNT(*)
FROM single_customer_view_historical.event_stream_2025_07_14.event_stream es
;

SELECT
	COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow_bkup.event_stream
;

SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
;

--
-- CREATE SCHEMA single_customer_view_historical.single_customer_view_2025_07_14_moo CLONE data_vault_mvp.single_customer_view_stg
-- ;
-- ;

------------------------------------------------------------------------------------------------------------------------
-- just incase backing up to dev too
------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE SCHEMA hygiene_vault_mvp_dev_robin.snowplow_bkup CLONE hygiene_vault_mvp.snowplow
;
;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg_bkup CLONE data_vault_mvp.single_customer_view_stg
;
;

------------------------------------------------------------------------------------------------------------------------
-- Truncate prod tables
------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

TRUNCATE TABLE hygiene_vault_mvp.snowplow.event_stream
;

/*
SELECT
	'TRUNCATE TABLE ' || table_catalog || '.' || table_schema || '.' || table_name || ';' AS cmd
FROM data_vault_mvp.information_schema.tables
WHERE table_catalog = 'DATA_VAULT_MVP' AND table_schema = 'SINGLE_CUSTOMER_VIEW_STG'
;
*/

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.app_push_send_enhancement
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touchification
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_unique_urls
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_url_hostname
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_url_params
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_pay_button_clicks
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_in_app_notification_events
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_extracted_params
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_identity_associations
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_time_diff_marker
;

TRUNCATE TABLE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;


------------------------------------------------------------------------------------------------------------------------
-- Rerun event stream and scv
------------------------------------------------------------------------------------------------------------------------

-- https://fabd9732-b4b6-4040-8f54-56c8df8d2eeb.c7.eu-west-1.airflow.amazonaws.com/dags/hygiene__snowplow__event_stream__hourly/grid
-- https://fabd9732-b4b6-4040-8f54-56c8df8d2eeb.c7.eu-west-1.airflow.amazonaws.com/dags/single_customer_view__daily_at_02h00/grid


-- https://secretescapes.atlassian.net/wiki/spaces/DW/pages/2557116440/Backfill+-+How+to

./
scripts/
mwaa-cli production "dags backfill --start-date '2022-12-01 00:00:00' --end-date '2022-12-01 00:00:00' --donot-pickle hygiene__snowplow__event_stream__hourly"
./
scripts/
mwaa-cli production "dags backfill --start-date '2022-12-01 02:00:00' --end-date '2022-12-01 02:00:00' --donot-pickle single_customer_view__daily_at_02h00"

------------------------------------------------------------------------------------------------------------------------
-- Update historical unions
------------------------------------------------------------------------------------------------------------------------

-- https://github.com/secretescapes/one-data-pipeline/pull/4770/files



-- dropping test archives

DROP SCHEMA single_customer_view_historical.event_stream_2025_07_01
;

DROP SCHEMA single_customer_view_historical.single_customer_view_2025_07_01
;

------------------------------------------------------------------------------------------------------------------------
-- row count checks
------------------------------------------------------------------------------------------------------------------------
-- archive module_touch_basic_attributes
SELECT
	COUNT(*)
FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touch_basic_attributes mtba
;

-- prod module_touch_basic_attributes
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
;

-- archive event_stream
SELECT
	COUNT(*)
FROM single_customer_view_historical.event_stream_2025_07_14.event_stream es
;

-- prod event_stream
SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es


-- archive module_touched_spvs
SELECT
	COUNT(*)
FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_spvs
;

-- prod module_touched_spvs
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs
;


-- archive module_touched_transactions
SELECT
	COUNT(*)
FROM single_customer_view_historical.single_customer_view_2025_07_14.module_touched_transactions
;

-- prod module_touched_transactions
SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_time_diff_marker
-- 2,650,554,047

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification
-- 2,650,554,047

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
-- 1,735,466

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
-- 706,126,398