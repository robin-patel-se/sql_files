-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_last_session.py make clones

DROP SCHEMA data_vault_mvp_dev_robin.dwh
;

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.unioned_data
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_basic_attributes AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touch_basic_attributes
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_session
-- 	CLONE data_vault_mvp.dwh.user_last_session
-- ;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_session.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_session.py'  --method 'run' --start '2025-10-06 00:00:00' --end '2025-10-06 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_session
;

------------------------------------------------------------------------------------------------------------------------
-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_last_app_session.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.unioned_data
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_module_touch_basic_attributes AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touch_basic_attributes
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_app_session
-- 	CLONE data_vault_mvp.dwh.user_last_app_session
-- ;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_app_session.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_app_session.py'  --method 'run' --start '2025-10-06 00:00:00' --end '2025-10-06 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_app_session
;


SELECT
	MIN(ura.last_session_end_tstamp)
FROM data_vault_mvp.dwh.user_recent_activities ura
WHERE ura.last_session_end_tstamp IS NOT NULL

------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_last_spv.py make clones


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.unioned_data
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_module_touched_spvs
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_module_touched_spvs
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_session_events_link
AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_session_events_link
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_spv
-- 	CLONE data_vault_mvp.dwh.user_last_spv
-- ;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_spv.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_spv.py'  --method 'run' --start '2025-10-06 00:00:00' --end '2025-10-06 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_spv
;
------------------------------------------------------------------------------------------------------------------------


WITH
	modelling AS (
		SELECT
			prod.shiro_user_id,
			prod.last_sale_pageview_tstamp                                 AS prod_last_sale_pageview_tstamp,
			dev.last_sale_pageview_tstamp                                  AS dev_last_sale_pageview_tstamp,
			prod.last_sale_pageview_tstamp = dev.last_sale_pageview_tstamp AS is_last_sale_pageview_tstamp_the_same
		FROM data_vault_mvp.dwh.user_last_spv prod
		LEFT JOIN data_vault_mvp_dev_robin.dwh.user_last_spv dev
			ON prod.shiro_user_id = dev.shiro_user_id
	)
SELECT *
FROM modelling
WHERE modelling.is_last_sale_pageview_tstamp_the_same = FALSE

SELECT
	CASE
		WHEN dev_last_sale_pageview_tstamp IS NULL THEN 'missing'
		WHEN is_last_sale_pageview_tstamp_the_same THEN 'same'
		WHEN is_last_sale_pageview_tstamp_the_same = FALSE THEN 'not same'
	END AS matching,
	COUNT(*)
FROM modelling
GROUP BY ALL

SELECT
	DATE_TRUNC(YEAR, user_last_spv.last_sale_pageview_tstamp),
	COUNT(*)
FROM data_vault_mvp.dwh.user_last_spv
GROUP BY 1


SELECT
	DATE_TRUNC(YEAR, user_last_spv.last_sale_pageview_tstamp),
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_last_spv
GROUP BY 1


------------------------------------------------------------------------------------------------------------------------
-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_last_pageview.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.unioned_data
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_event_stream AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_event_stream
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_session_events_link AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_session_events_link
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_pageview
-- 	CLONE data_vault_mvp.dwh.user_last_pageview
-- ;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_pageview.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_pageview.py'  --method 'run' --start '2025-10-07 00:00:00' --end '2025-10-07 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_pageview
;
------------------------------------------------------------------------------------------------------------------------

-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_last_screenview.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS single_customer_view_historical_dev_robin.unioned_data
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_event_stream AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_event_stream
;

CREATE OR REPLACE VIEW single_customer_view_historical_dev_robin.unioned_data.historical_session_events_link AS
SELECT *
FROM single_customer_view_historical.unioned_data.historical_session_events_link
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_screenview
-- 	CLONE data_vault_mvp.dwh.user_last_screenview
-- ;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_screenview.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/user_attributes/user_last_screenview.py'  --method 'run' --start '2025-10-07 00:00:00' --end '2025-10-07 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_screenview
;
------------------------------------------------------------------------------------------------------------------------
-- module=/biapp/task_catalogue/dv/dwh/user_attributes/user_recent_activities.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.shiro_user
	CLONE latest_vault.cms_mysql.shiro_user
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_click_event
AS
SELECT *
FROM data_vault_mvp.dwh.email_click_event
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.email_open_event
AS
SELECT *
FROM data_vault_mvp.dwh.email_open_event
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_pageview
-- CLONE data_vault_mvp.dwh.user_last_pageview;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_screenview
-- CLONE data_vault_mvp.dwh.user_last_screenview;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_session
-- CLONE data_vault_mvp.dwh.user_last_session;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_app_session
-- CLONE data_vault_mvp.dwh.user_last_app_session;
--
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_last_spv
-- CLONE data_vault_mvp.dwh.user_last_spv;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.profile
	CLONE latest_vault.cms_mysql.profile
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit
	CLONE data_vault_mvp.dwh.se_credit
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_voucher
	CLONE data_vault_mvp.dwh.se_voucher
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_emails
	CLONE data_vault_mvp.dwh.user_emails
;

-- -- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities
-- 	CLONE data_vault_mvp.dwh.user_recent_activities
;

self_describing_task --include 'biapp.task_catalogue.dv.dwh.user_attributes.user_recent_activities.py'  --method 'run' --start '2025-10-07 00:00:00' --end '2025-10-07 00:00:00'

-- dev

SELECT
	shiro_user_id,
	signup_tstamp,
	user_last_updated_tstamp,
	profile_last_updated_tstamp,
	last_pageview_tstamp,
	last_sale_pageview_tstamp,
	last_session_end_tstamp,
	last_email_open_tstamp,
	last_email_click_tstamp,
	last_abandoned_booking_tstamp,
	last_purchase_tstamp,
	last_complete_booking_tstamp,
	last_voucher_purchase_tstamp,
	latest_cash_credit_expiration_tstamp,
	latest_active_cash_credit_expiration_tstamp
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities
;

-- prod
SELECT
	shiro_user_id,
	signup_tstamp,
	user_last_updated_tstamp,
	profile_last_updated_tstamp,
	last_pageview_tstamp,
	last_sale_pageview_tstamp,
	last_session_end_tstamp,
	last_email_open_tstamp,
	last_email_click_tstamp,
	last_abandoned_booking_tstamp,
	last_purchase_tstamp,
	last_complete_booking_tstamp,
	last_voucher_purchase_tstamp,
	latest_cash_credit_expiration_tstamp,
	latest_active_cash_credit_expiration_tstamp,
FROM data_vault_mvp.dwh.user_recent_activities
;

WITH
	prod AS (
		SELECT
			shiro_user_id,
			HASH(shiro_user_id,
				 signup_tstamp,
				 user_last_updated_tstamp,
				 profile_last_updated_tstamp,
				 last_pageview_tstamp,
				 last_sale_pageview_tstamp,
				 last_session_end_tstamp,
				 last_email_open_tstamp,
				 last_email_click_tstamp,
				 last_abandoned_booking_tstamp,
				 last_purchase_tstamp,
				 last_complete_booking_tstamp,
				 last_voucher_purchase_tstamp,
				 latest_cash_credit_expiration_tstamp,
				 latest_active_cash_credit_expiration_tstamp) AS prod_hash
		FROM data_vault_mvp.dwh.user_recent_activities
	),
	dev AS (

		SELECT
			shiro_user_id,
			HASH(shiro_user_id,
				 signup_tstamp,
				 user_last_updated_tstamp,
				 profile_last_updated_tstamp,
				 last_pageview_tstamp,
				 last_sale_pageview_tstamp,
				 last_session_end_tstamp,
				 last_email_open_tstamp,
				 last_email_click_tstamp,
				 last_abandoned_booking_tstamp,
				 last_purchase_tstamp,
				 last_complete_booking_tstamp,
				 last_voucher_purchase_tstamp,
				 latest_cash_credit_expiration_tstamp,
				 latest_active_cash_credit_expiration_tstamp) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.user_recent_activities
	)
SELECT *
FROM prod
LEFT JOIN dev
	ON prod.shiro_user_id = dev.shiro_user_id
WHERE prod.prod_hash IS DISTINCT FROM dev.dev_hash
;


-- prod
SELECT
	shiro_user_id,
	signup_tstamp,
	user_last_updated_tstamp,
	profile_last_updated_tstamp,
	last_pageview_tstamp,
	last_sale_pageview_tstamp,
	last_session_end_tstamp,
	last_email_open_tstamp,
	last_email_click_tstamp,
	last_abandoned_booking_tstamp,
	last_purchase_tstamp,
	last_complete_booking_tstamp,
	last_voucher_purchase_tstamp,
	latest_cash_credit_expiration_tstamp,
	latest_active_cash_credit_expiration_tstamp,
	HASH(shiro_user_id,
		 signup_tstamp,
		 user_last_updated_tstamp,
		 profile_last_updated_tstamp,
		 last_pageview_tstamp,
		 last_sale_pageview_tstamp,
		 last_session_end_tstamp,
		 last_email_open_tstamp,
		 last_email_click_tstamp,
		 last_abandoned_booking_tstamp,
		 last_purchase_tstamp,
		 last_complete_booking_tstamp,
		 last_voucher_purchase_tstamp,
		 latest_cash_credit_expiration_tstamp,
		 latest_active_cash_credit_expiration_tstamp) AS prod_hash
FROM data_vault_mvp.dwh.user_recent_activities
WHERE user_recent_activities.shiro_user_id = 83536361
;

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities 84,263,928
-- dev
SELECT
	shiro_user_id,
	signup_tstamp,
	user_last_updated_tstamp,
	profile_last_updated_tstamp,
	last_pageview_tstamp,
	last_sale_pageview_tstamp,
	last_session_end_tstamp,
	last_email_open_tstamp,
	last_email_click_tstamp,
	last_abandoned_booking_tstamp,
	last_purchase_tstamp,
	last_complete_booking_tstamp,
	last_voucher_purchase_tstamp,
	latest_cash_credit_expiration_tstamp,
	latest_active_cash_credit_expiration_tstamp,
	HASH(shiro_user_id,
		 signup_tstamp,
		 user_last_updated_tstamp,
		 profile_last_updated_tstamp,
		 last_pageview_tstamp,
		 last_sale_pageview_tstamp,
		 last_session_end_tstamp,
		 last_email_open_tstamp,
		 last_email_click_tstamp,
		 last_abandoned_booking_tstamp,
		 last_purchase_tstamp,
		 last_complete_booking_tstamp,
		 last_voucher_purchase_tstamp,
		 latest_cash_credit_expiration_tstamp,
		 latest_active_cash_credit_expiration_tstamp) AS dev_hash
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities
WHERE user_recent_activities.shiro_user_id = 83536361

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_spv uls
WHERE uls.shiro_user_id = 83223782


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '83223782' AND stba.stitched_identity_type = 'se_user_id'
;

-- 83223782 -- example user where their dates are wrong , session data shows last session on July 5th 2025, where last pageview only shows May 22nd 2025

SELECT
	touchification.attributed_user_id::INT                                         AS shiro_user_id,
	MAX(IFF(events.event_name = 'page_view', touchification.event_tstamp, NULL))   AS last_pageview_tstamp,
	MAX(IFF(events.event_name = 'screen_view', touchification.event_tstamp, NULL)) AS last_screenview_tstamp
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification touchification
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream events
	ON touchification.event_hash = events.event_hash
-- 	AND events.updated_at >= TIMESTAMPADD('DAY', -1, '2025-10-06 03:30:00'::TIMESTAMP)
WHERE events.event_name IN ('page_view', 'screen_view')
  AND touchification.stitched_identity_type = 'se_user_id'
--   AND touchification.updated_at >= TIMESTAMPADD('DAY', -1, '2025-10-06 03:30:00'::TIMESTAMP)
  AND touchification.attributed_user_id = '83223782'
GROUP BY touchification.attributed_user_id::INT
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification touchification
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream events
	ON touchification.event_hash = events.event_hash
WHERE touchification.attributed_user_id = '83223782'
  AND events.event_tstamp::DATE = '2025-07-05'
--   AND events.event_name IN ('page_view', 'screen_view')
  AND touchification.stitched_identity_type = 'se_user_id'
;


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_last_session
(
	shiro_user_id             INT,
	last_session_start_tstamp TIMESTAMP,
	last_session_end_tstamp   TIMESTAMP
)
;

INSERT INTO data_vault_mvp_dev_robin.dwh.user_last_session

SELECT
	1,
	NULL,
	NULL
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_session
;

MERGE INTO data_vault_mvp_dev_robin.dwh.user_last_session AS target
	USING
		(
			SELECT
				1                                   AS shiro_user_id,
				NULL                                AS last_session_start_tstamp,
				TO_TIMESTAMP('2025-10-08 15:47:22') AS last_session_end_tstamp
		)
			AS batch
	ON target.shiro_user_id = batch.shiro_user_id
	WHEN MATCHED
		AND target.last_session_end_tstamp < batch.last_session_end_tstamp
		THEN UPDATE SET
		target.last_session_start_tstamp = batch.last_session_start_tstamp,
		target.last_session_end_tstamp = batch.last_session_end_tstamp
	WHEN MATCHED
		AND target.last_session_end_tstamp IS NULL
		THEN UPDATE SET
		target.last_session_start_tstamp = batch.last_session_start_tstamp,
		target.last_session_end_tstamp = batch.last_session_end_tstamp
	WHEN NOT MATCHED
		THEN INSERT VALUES (batch.shiro_user_id,
							batch.last_session_start_tstamp,
							batch.last_session_end_tstamp)
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_last_session
;

-- prod
SELECT
	YEAR(ura.last_session_end_tstamp),
	COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities ura
GROUP BY 1
;


-- dev
SELECT
	YEAR(ura.last_session_end_tstamp),
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities ura
GROUP BY 1
;


SELECT
	fact_booking.shiro_user_id,
	MAX(IFF(
			fact_booking.booking_status_type = 'abandoned',
			fact_booking.booking_created_date,
			NULL)
	)                                           AS last_abandoned_booking_tstamp,
	MAX(IFF(
			fact_booking.booking_status_type IS DISTINCT FROM 'abandoned',
			fact_booking
			booking_completed_timestamp, NULL)) AS last_purchase_tstamp,
	MAX(IFF(
			fact_booking.booking_status_type = 'live',
			fact_booking.booking_completed_timestamp,
			NULL)
	)                                           AS last_complete_booking_tstamp
FROM data_vault_mvp_dev_robin.dwh.fact_booking fact_booking
GROUP BY fact_booking.shiro_user_id
HAVING last_abandoned_booking_tstamp IS NOT NULL
	OR last_purchase_tstamp IS NOT NULL
	OR last_complete_booking_tstamp IS NOT NULL
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities ura

./scripts/mwaa-cli production "dags backfill --start-date '1969-12-31 04:30:00' --end-date '1970-01-01 00:00:00' --donot-pickle dwh__user_recent_activities__daily_at_03h30"


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities_20251009 CLONE data_vault_mvp.dwh.user_recent_activities;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_spv_20251009 CLONE data_vault_mvp.dwh.user_last_spv;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_pageview_20251009 CLONE data_vault_mvp.dwh.user_last_pageview;