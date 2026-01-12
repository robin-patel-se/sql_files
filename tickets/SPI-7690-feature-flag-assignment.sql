USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.feature_toggle
	CLONE latest_vault.cms_mysql.feature_toggle
;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;


self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.05_module_touched_feature_flags.py' \
    --method 'run' \
    --start '2025-08-28 00:00:00' \
    --end '2025-08-28 00:00:00'

SELECT
	event_name,
	event,
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE() - 1
GROUP BY ALL
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags_clone__union_ff_data
WHERE module_touched_feature_flags_clone__union_ff_data.event_tstamp::DATE = '2024-10-21'
  AND module_touched_feature_flags_clone__union_ff_data.touch_id =
	  '3a642a747fc973db296dbadbdabe4c084c8f8a086919faf2b35bacdc1f063670'
;

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE stff.feature_flag LIKE 'data.test%'
  AND stff.touch_start_tstamp >= CURRENT_DATE() - 1
QUALIFY COUNT(*) OVER (PARTITION BY LEFT(stff.feature_flag, 9), touch_id) > 1

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE stff.feature_flag LIKE 'data.test%'
  AND stff.touch_start_tstamp >= CURRENT_DATE() - 1
  AND stff.touch_id = '2addd7967d23f9609f2494261d38a41eb006d577904ea6414f3109d8fa470970'
;


SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE stff.touch_start_tstamp > CURRENT_DATE - 1


DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
;

-- examples of sessions that has both
SELECT
	touch_id,
	touch_start_tstamp,
	feature_flag,
	num_occurences,
	min_tstamp,
	max_tstamp,
	is_logged_in,
	affiliate_type,
	control_key,
	description,
	platform,
	traffic_split,
	type,
	url_param,
	ff_in_feature_toggle_table,
	ff_is_control,
	num_occurences_client_side,
	num_occurences_server_side
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
WHERE module_touched_feature_flags.touch_id IN (
												'36897226bf5d55e62b517e3ef39d291f625e3f586666d1f656876d68c1eb0e6f', -- example completely logged out with two flags
												'beeb601b1cceb2e68becc253bf2fee94d74bf2e97defd208d723b6966c404404', -- example logged out in test and logged in in control
												'88d13eb80fb3216de0b0399040fc24855da1d352d8983505ea18d6f41bb38399' -- example logged in in test and logged out in control
	)
  AND module_touched_feature_flags.feature_flag LIKE 'data.test%'
  AND module_touched_feature_flags.touch_start_tstamp::DATE = '2025-08-27'
;

WITH
	example_touched_feature_flags AS (
		SELECT *
		FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
		WHERE module_touched_feature_flags.feature_flag LIKE 'data.test%'
		  AND module_touched_feature_flags.touch_start_tstamp::DATE = '2025-08-27'
	)

SELECT
	ff.touch_id,
	ff.feature_flag,
	ff.is_logged_in
FROM example_touched_feature_flags ff
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
WHERE feature_flag LIKE 'data.test%'
  AND touch_start_tstamp >= CURRENT_DATE() - 1
QUALIFY COUNT(*) OVER (PARTITION BY LEFT(feature_flag, 9), touch_id) > 1
;


------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags_20250901 CLONE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;
------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE is_logged_in
;

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE is_logged_in
  AND stff.feature_flag LIKE '%bookingfees%'

SELECT *
FROM latest_vault.cms_mysql.feature_toggle ft
WHERE ft.toggle_key LIKE '%fee%'
;

-- look for duplications
SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE is_logged_in
  AND stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
QUALIFY COUNT(*) OVER (PARTITION BY stff.touch_id) > 1
;

-- Example of a session where the user is logged in is in test and control 0450db65ce7350820d0ec3a77274b68536b5396d988e9eef3870af1a174e9421

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE is_logged_in
  AND stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
  AND stff.touch_start_tstamp::DATE = '2025-05-21'
  AND stff.touch_id = '0450db65ce7350820d0ec3a77274b68536b5396d988e9eef3870af1a174e9421'
;

/*	FEATURE_FLAG
abtest.bookingfees.pricingv2.variant
abtest.bookingfees.pricingv2.control*/

-- both are logged in and variant only has one event on it


-- looking at the events in the session
SELECT
	ses.event_tstamp,
	ses.event_hash,
	ses.event_name,
	ses.page_url,
	ses.page_title,
	ses.contexts_com_secretescapes_user_state_context_1[0]:feature_flags AS feature_flags
FROM se.data_pii.scv_session_events_link ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2025-05-21'
WHERE ssel.event_tstamp::DATE = '2025-05-21'
  AND ssel.touch_id = '0450db65ce7350820d0ec3a77274b68536b5396d988e9eef3870af1a174e9421'


-- Example of a session where the user is logged in is in test and control 55c8728dc21444a3e8e0430ddc8f7da1e113e14959fb6f4c984d7a3a299991d9

SELECT *
FROM se.data.scv_touched_feature_flags stff
WHERE is_logged_in
  AND stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
  AND stff.touch_start_tstamp::DATE = '2025-04-19'
  AND stff.touch_id = '55c8728dc21444a3e8e0430ddc8f7da1e113e14959fb6f4c984d7a3a299991d9'
;

/*	FEATURE_FLAG
abtest.bookingfees.pricingv2.variant
abtest.bookingfees.pricingv2.control*/

-- both are logged in and variant only has one event on it


-- looking at the events in the session
SELECT
	ses.event_tstamp,
	ses.event_hash,
	ses.event_name,
	ses.page_url,
	ses.page_title,
	ARRAY_CONTAINS('abtest.bookingfees.pricingv2.variant'::VARIANT,
				   ses.contexts_com_secretescapes_user_state_context_1[0]:feature_flags) AS contains_variant,
	ARRAY_CONTAINS('abtest.bookingfees.pricingv2.control'::VARIANT,
				   ses.contexts_com_secretescapes_user_state_context_1[0]:feature_flags) AS contains_control,
	ses.contexts_com_secretescapes_user_state_context_1[0]:feature_flags                 AS feature_flags
FROM se.data_pii.scv_session_events_link ssel
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2025-04-19'
WHERE ssel.event_tstamp::DATE = '2025-04-19'
  AND ssel.touch_id = '55c8728dc21444a3e8e0430ddc8f7da1e113e14959fb6f4c984d7a3a299991d9'
;



USE WAREHOUSE pipe_xlarge
;


-- look for duplications

SELECT
	COUNT(DISTINCT stff.touch_id),
	COUNT_IF(stff.is_logged_in)
FROM se.data.scv_touched_feature_flags stff
WHERE stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
;


-- 8,375,329 sessions with the abtest.bookingfees.pricingv2 feature flag

WITH
	dupes AS (
		SELECT *
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
		  AND is_logged_in
		QUALIFY COUNT(*) OVER (PARTITION BY stff.touch_id) > 1
	)
SELECT
	COUNT(DISTINCT dupes.touch_id)
FROM dupes
;


--3,250 -- using a logged in state only

WITH
	dupes AS (
		SELECT *
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
-- 		  AND is_logged_in
		QUALIFY COUNT(*) OVER (PARTITION BY stff.touch_id) > 1
	)
SELECT
	COUNT(DISTINCT dupes.touch_id)
FROM dupes

-- 196,330


WITH
	dupes AS (
		SELECT
			stff.touch_id,
			stff.feature_flag
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
		  AND stff.is_logged_in
		QUALIFY COUNT(*) OVER ( PARTITION BY stff.touch_id) = 1

		UNION ALL

		SELECT DISTINCT
			stff.touch_id,
			'exclude' AS feature_flag
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
		  AND stff.is_logged_in
		QUALIFY COUNT(*) OVER ( PARTITION BY stff.touch_id) > 1
	)
SELECT *
FROM dupes
QUALIFY COUNT(*) OVER (PARTITION BY dupes.touch_id) > 1


WITH
	aggregate_sessions AS
		(
			SELECT
				stff.touch_id,
				ARRAY_AGG(stff.feature_flag) AS feature_flag_array
			FROM se.data.scv_touched_feature_flags stff
			WHERE stff.feature_flag LIKE 'abtest.bookingfees.pricingv2%'
			  AND stff.is_logged_in
			GROUP BY 1
		)
SELECT
	aggregate_sessions.touch_id,
	aggregate_sessions.feature_flag_array,
	IFF(ARRAY_SIZE(aggregate_sessions.feature_flag_array) > 1, 'exclude',
		aggregate_sessions.feature_flag_array[0]) AS feature_flag
FROM aggregate_sessions;