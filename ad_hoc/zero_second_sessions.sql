USE WAREHOUSE pipe_xlarge
;

WITH
	sessions_with_an_spv AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2024-01-01'
	)
		,
	modelling AS (
		SELECT
			DATE_TRUNC(MONTH, stba.touch_start_tstamp)  AS month,
			stba.touch_experience,
			stba.touch_start_tstamp,
			IFF(swas.touch_id IS NOT NULL, TRUE, FALSE) AS session_with_spv
		FROM se.data.scv_touch_basic_attributes stba
			LEFT JOIN sessions_with_an_spv swas ON stba.touch_id = swas.touch_id
		WHERE stba.touch_experience LIKE 'native app%'
		  AND stba.touch_start_tstamp >= '2024-01-01'
	)
SELECT
	month,
	touch_experience,
	COUNT(*)                         AS sessions,
	SUM(IFF(session_with_spv, 1, 0)) AS sessions_with_spv
FROM modelling m
GROUP BY 1, 2
;

WITH
	sessions_with_an_spv AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2024-08-01'
	)
-- SELECT
-- 	stba.touch_duration_seconds,
-- 	COUNT(*)
-- FROM se.data.scv_touch_basic_attributes stba
-- 	LEFT JOIN sessions_with_an_spv swas ON stba.touch_id = swas.touch_id
-- WHERE stba.touch_experience LIKE 'native app%'
--   AND stba.touch_start_tstamp >= '2024-08-01'
--   AND swas.touch_id IS NULL
-- GROUP BY 1

		,
	zero_second_sessions AS (
		SELECT DISTINCT
			stba.touch_id
		FROM se.data.scv_touch_basic_attributes stba
			LEFT JOIN sessions_with_an_spv swas ON stba.touch_id = swas.touch_id
		WHERE stba.touch_experience LIKE 'native app%'
		  AND stba.touch_start_tstamp::DATE = '2024-08-17'
		  AND swas.touch_id IS NULL
		  AND stba.touch_duration_seconds = 0
	),
	modelling AS (
		SELECT
			ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS screen_name,
			*
		FROM se.data_pii.scv_session_events_link ssel
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2024-08-17'
		WHERE ssel.event_tstamp::DATE = '2024-08-17'
		  AND ssel.touch_id IN (
			SELECT *
			FROM zero_second_sessions
		)
	)
SELECT
	modelling.screen_name,
	COUNT(*)
FROM modelling
GROUP BY 1
;



SELECT
	stba.touch_start_tstamp::DATE,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_experience LIKE 'native app%'
  AND stba.touch_start_tstamp::DATE = '2024-08-17'
GROUP BY 1


-- on the 17th of August 0 duration sessions
-- 108693 -- all sessions
-- 59692 -- zero duration sessions on homepage


WITH
	sessions_with_an_spv AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE DATE_TRUNC(MONTH, sts.event_tstamp) = '2024-08-01'
	)
		,
	modelling AS (
		SELECT
			DATE_TRUNC(MONTH, stba.touch_start_tstamp)                   AS month,
			stba.touch_experience,
			stba.touch_start_tstamp,
			stba.touch_duration_seconds = 0                              AS zero_duration_session,
			stmc.touch_mkt_channel,
			IFF(swas.touch_id IS NOT NULL, TRUE, FALSE)                  AS session_with_spv,
			IFF(stba.stitched_identity_type = 'se_user_id', TRUE, FALSE) AS member_session,
			stba.touch_logged_in
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  sessions_with_an_spv swas ON stba.touch_id = swas.touch_id
		WHERE stba.touch_experience LIKE 'native app%'
		  AND DATE_TRUNC(MONTH, stba.touch_start_tstamp) = '2024-08-01'
	)
SELECT
	month,
	touch_experience,
	m.touch_mkt_channel,
	zero_duration_session,
	touch_logged_in,
	COUNT(*)                         AS sessions,
	SUM(IFF(session_with_spv, 1, 0)) AS sessions_with_spv
FROM modelling m
GROUP BY ALL
;


SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_experience LIKE 'native app%'
  AND DATE_TRUNC(MONTH, stba.touch_start_tstamp) = '2024-08-01'
  AND stba.touch_duration_seconds = 0
  AND stmc.touch_mkt_channel = 'Direct'


------------------------------------------------------------------------------------------------------------------------

-- looking at logged in, direct, zero second sessions
WITH
	sessions_with_an_spv AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE DATE_TRUNC(MONTH, sts.event_tstamp) = '2024-08-01'
	)
		,
	modelling AS (
		SELECT
			DATE_TRUNC(MONTH, stba.touch_start_tstamp)                   AS month,
			stba.touch_experience,
			stba.touch_start_tstamp,
			stba.touch_duration_seconds = 0                              AS zero_duration_session,
			stmc.touch_mkt_channel,
			IFF(swas.touch_id IS NOT NULL, TRUE, FALSE)                  AS session_with_spv,
			IFF(stba.stitched_identity_type = 'se_user_id', TRUE, FALSE) AS member_session,
			stba.touch_logged_in
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  sessions_with_an_spv swas ON stba.touch_id = swas.touch_id
		WHERE stba.touch_experience LIKE 'native app%'
		  AND DATE_TRUNC(MONTH, stba.touch_start_tstamp) = '2024-08-01'
	)
SELECT
	DATE_PART(HOUR, m.touch_start_tstamp) AS hour,
	touch_experience,
	COUNT(*)                              AS sessions,
	SUM(IFF(session_with_spv, 1, 0))      AS sessions_with_spv
FROM modelling m
WHERE m.zero_duration_session
  AND m.touch_mkt_channel = 'Direct'
  AND m.touch_logged_in
GROUP BY ALL
;

WITH
	data AS (
		SELECT
			ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen,
			*
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
		WHERE stba.touch_duration_seconds = 0
		  AND stba.touch_experience LIKE 'native app ios'
		  AND stba.touch_start_tstamp >= CURRENT_DATE - 1
	)
SELECT
	d.landing_screen,
-- 	DATE_PART(HOUR, d.touch_start_tstamp) AS hour,
	COUNT(*)
FROM data d
GROUP BY ALL
;


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 30
WHERE stba.attributed_user_id = '79527999'
  AND stba.stitched_identity_type = 'se_user_id'
--   AND stba.touch_start_tstamp >= CURRENT_DATE - 1
;


SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 30
;


SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.email = 'gianni.raftis@gmail.com'
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 2 AND
	  (ses.se_user_id = '72868430'
		  OR ses.email_address = 'gianni.raftis@gmail.com'
		  OR ses.session_userid = 'f2ee2602e4fbb18ae2356301eb76eb06f0048cdc470865d549db767499033ece')


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.collector_tstamp >= CURRENT_DATE - 1 AND
	  (ses.user_id = '72868430'
		  OR ses.email_address = 'gianni.raftis@gmail.com'
		  OR ses.session_userid = 'f2ee2602e4fbb18ae2356301eb76eb06f0048cdc470865d549db767499033ece')



SELECT *
FROM snowplow.atomic.events
WHERE collector_tstamp::DATE = CURRENT_DATE
  AND user_id = '72868430'
;



SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
	COUNT(*)                                   AS sessions,
	COUNT(DISTINCT stba.attributed_user_id)    AS users
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience LIKE 'native app%'
  AND stba.touch_start_tstamp >= '2024-01-01'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_duration_seconds > 0
GROUP BY ALL
;


SELECT
	COUNT(*) AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-08-01'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_duration_seconds = 0
;
-- 2,919,166


SELECT
	stba.attributed_user_id,
	COUNT(*) AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-08-01'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_duration_seconds = 0
GROUP BY 1
ORDER BY 2 DESC
;

-- 344,291 users generated 2,110,278 sessions (6.1 sessions per user)
-- 317,810 users generated 2,110,278 sessions (9.2 sessions per user)

-- userid: 73865217 has 147, 0 second sessions in August 2024

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 30
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-08-01'
  AND stba.stitched_identity_type = 'se_user_id'
--   AND stba.touch_duration_seconds = 0
  AND stba.attributed_user_id = '73865217'
;


SELECT *
FROM se.data_pii.scv_event_stream ses
	LEFT JOIN se.data_pii.scv_session_events_link ssel
			  ON ses.event_hash = ssel.event_hash
				  AND ssel.event_tstamp >= '2024-08-01'
WHERE ses.session_userid = 'e9404194-b9a6-4fe3-9ca9-4aff34dd671d'
  AND ses.event_tstamp >= '2024-08-01'
;

USE WAREHOUSE pipe_xlarge
;


SELECT
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	*
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-09-01'
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-09-01'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_duration_seconds = 0
  AND stba.attributed_user_id = '77985485'
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_hash = '14dc947864834d7fe5a24d0494792308f89741a726db2466d35de67ef2ad109c' AND
	  ses.event_tstamp::DATE = '2024-08-20'

------------------------------------------------------------------------------------------------------------------------
--investigating ben neil user
-- ben neil
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '77985485'
  AND stba.touch_experience = 'native app ios'
;

-- abanounb
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '80215774'
  AND stba.touch_experience = 'native app ios'
;

-- gianni
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '72868430'
  AND stba.touch_experience = 'native app ios'
;

-- alex henshaw
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '76797745'
  AND stba.touch_experience = 'native app ios'
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE AND ses.event_name = 'page_view'
;


-- ben neil
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses ON stba.touch_id = ses.event_hash
WHERE stba.attributed_user_id = '77985485'
  AND stba.touch_experience = 'native app ios'
;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp ::DATE = '2024-09-23'
  AND ses.user_id = '77985485'

SELECT *
FROM latest_vault.iterable.app_users au
;

SELECT *
FROM se.data.se_user_attributes sua
;

------------------------------------------------------------------------------------------------------------------------

SELECT
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	*
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-09-01'
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-10-01'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_duration_seconds = 0
;


SELECT
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR   AS app_state,
	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.device_platform LIKE 'native app %'
  AND ses.event_tstamp >= '2024-10-02'
  AND ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR IS NOT NULL
  AND ses.event_name = 'screen_view'
;



SELECT
-- 	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
COUNT(IFF(ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR IS NOT NULL, event_hash,
		  NULL)) AS app_state,
COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.device_platform LIKE 'native app ios'
  AND ses.event_tstamp >= '2024-10-01'
  AND ses.event_name = 'screen_view'

-- this user had a screen view event where app state was background: 79076858

SELECT
	ses.user_id,
	ses.device_platform,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR   AS app_state,
	ses.event_name,
	ses.event_tstamp,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
	ses.se_sale_id,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.device_platform LIKE 'native app %'
  AND ses.event_tstamp >= '2024-10-01'
--   AND ses.event_name = 'screen_view'
  AND ses.user_id = '79076858'
;


-- Bens user, does broken deep links trigger reoccurring sessions

SELECT
	ses.user_id,
	ses.device_platform,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR   AS app_state,
-- 	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER      AS build,
-- 	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR   AS version,
	ses.event_tstamp,
	ses.event_name,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
	ses.se_sale_id,
	ses.mkt_medium,
	ses.mkt_campaign,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1
-- 	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.device_platform LIKE 'native app %'
  AND ses.event_tstamp >= '2024-10-01'
--   AND ses.event_name = 'screen_view'
  AND ses.user_id = '77985485'
;


-- Gianni user, does broken deep links trigger reoccurring sessions
SELECT
	ses.user_id,
	ses.device_platform,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR   AS app_state,
	ses.event_name,
	ses.event_tstamp,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
	ses.se_sale_id,
	ses.mkt_medium,
	ses.mkt_campaign,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1
-- 	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.device_platform LIKE 'native app %'
  AND ses.event_tstamp >= '2024-10-01'
--   AND ses.event_name = 'screen_view'
  AND ses.user_id = '72868430'
;

------------------------------------------------------------------------------------------------------------------------
-- zero second sessions are they in the background

SELECT
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR   AS app_state,
	*
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-09-01'
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-10-01'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_duration_seconds = 0
;

SELECT *
FROM se.data_pii.scv_event_stream sesb
WHERE ses.device_platform LIKE 'native app %'
  AND ses.event_tstamp >= '2024-10-01'
  AND ses.event_name = 'screen_end'
--   AND ses.event_name = 'screen_view'
;



SELECT
	e.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER    AS build,
	e.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR AS version,
	*
FROM snowplow.atomic.events e
WHERE e.v_tracker LIKE 'ios%'
  AND e.collector_tstamp >= CURRENT_DATE - 1
  AND e.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER = 2
;



SELECT
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.derived_tstamp,
	ses.collector_tstamp,
	ses.event_name,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
-- 	ses.se_sale_id,
	ses.mkt_medium,
	ses.mkt_campaign,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1,
FROM snowplow.atomic.events ses
WHERE ses.collector_tstamp >= '2024-10-01'
--   AND ses.event_name = 'screen_view'
  AND ses.user_id = '80215774'
ORDER BY derived_tstamp
;


------------------------------------------------------------------------------------------------------------------------
SELECT
	COUNT(*) OVER (PARTITION BY stba.attributed_user_id) AS user_sessions_within_period,
	stba.*
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2024-10-01'
  AND stmc.touch_affiliate_territory = 'UK'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_experience = 'native app ios'
QUALIFY COUNT(*) OVER (PARTITION BY stba.attributed_user_id) > 3
;

-- found user with mix of zero and non zero second sessions

SELECT
	COUNT(*) OVER (PARTITION BY stba.attributed_user_id) AS user_sessions_within_period,
	stmc.touch_mkt_channel,
	stmc.utm_campaign,
	stmc.utm_medium,
	stmc.utm_source,
	stba.*
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2024-09-01'
  AND stmc.touch_affiliate_territory = 'UK'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.attributed_user_id = '80981654'
;



SELECT
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.derived_tstamp,
	ses.collector_tstamp,
	ses.event_name,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
-- 	ses.se_sale_id,
	ses.mkt_medium,
	ses.mkt_campaign,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1,
FROM snowplow.atomic.events ses
WHERE ses.collector_tstamp >= '2024-10-01'
--   AND ses.event_name = 'screen_view'
  AND ses.user_id = '80981654'
ORDER BY derived_tstamp
;

SELECT *,
	   ses.contexts_com_secretescapes_app_state_context_1
FROM snowplow.atomic.events ses
WHERE ses.collector_tstamp >= '2024-10-01'
--   AND ses.event_name = 'screen_view'
  AND ses.v_tracker LIKE 'ios%'
  AND ses.user_id = '80981654'
ORDER BY derived_tstamp
;

WITH
	ios_screen_views AS (
		SELECT
			ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL              AS has_app_state,
			ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR AS app_state,
			*
		FROM snowplow.atomic.events ses
		WHERE ses.v_tracker LIKE 'ios%'
		  AND ses.event_name = 'screen_view'
		  AND ses.derived_tstamp >= '2024-10-10'
-- 		  AND ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL -- TODO REMOVE
	)
SELECT
	isv.app_state,
	COUNT(*)
FROM ios_screen_views isv
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------

-- find a user that has a background screen view
SELECT
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                 AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR    AS app_state,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR AS screen_name,
	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.v_tracker LIKE 'ios%'
  AND ses.event_name = 'screen_view'
  AND ses.derived_tstamp >= '2024-10-10'
  AND ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL
-- TODO REMOVE

-- look at all events for that user
SELECT
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.event_tstamp,
	ses.derived_tstamp,
	ses.collector_tstamp,
	ses.event_name,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
-- 	ses.se_sale_id,
	ses.mkt_medium,
	ses.mkt_campaign,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1,
	ses.contexts_com_secretescapes_app_state_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.v_tracker LIKE 'ios%'
--   AND ses.event_name = 'screen_view'
  AND ses.derived_tstamp >= '2024-10-01'
--   AND ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL
  AND ses.user_id = '51834960'

-- this user with a background screen view did have subsequent events however they do look legit
-- 41722104


-- anyone with the new app, filter how many

------------------------------------------------------------------------------------------------------------------------


SELECT
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.event_tstamp,
	ses.derived_tstamp,
	ses.collector_tstamp,
	ses.event_name,
	ses.se_category,
	ses.se_action,
	ses.se_label,
	ses.se_property,
-- 	ses.se_sale_id,
	ses.mkt_medium,
	ses.mkt_campaign,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_screen_context_1,
	ses.contexts_com_secretescapes_user_state_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1,
	ses.contexts_com_secretescapes_content_element_viewed_context_1,
	ses.contexts_com_secretescapes_environment_context_1,
	ses.contexts_com_secretescapes_app_state_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.v_tracker LIKE 'ios%'
--   AND ses.event_name = 'screen_view'
  AND ses.derived_tstamp >= '2024-10-09'
--   AND ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL
  AND ses.user_id = '41722104'

-- sergei user
SELECT
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.event_tstamp,
	ses.event_name,
	ses.se_property,
	ses.se_action,
	ses.se_category,
	ses.se_property,
	ses.contexts_com_secretescapes_app_state_context_1,
	ses.device_platform
-- 	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.user_id = '80279287' AND ses.event_tstamp >= CURRENT_DATE


-- gianni user
SELECT
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.event_tstamp,
	ses.event_name,
	ses.se_property,
	ses.se_action,
	ses.se_category,
	ses.se_property,
	ses.contexts_com_secretescapes_app_state_context_1,
	ses.device_platform
-- 	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.user_id = '72868430' AND ses.event_tstamp >= CURRENT_DATE


-- ben n user
SELECT
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.contexts_com_secretescapes_user_state_context_1[0]['app_state']::VARCHAR            AS app_state,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS landing_screen_name,
	ses.event_tstamp,
	ses.event_name,
	ses.se_property,
	ses.se_action,

	ses.se_category,
	ses.se_property,
	ses.contexts_com_secretescapes_app_state_context_1,
	ses.device_platform
-- 	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.user_id = '77985485' AND ses.event_tstamp >= CURRENT_DATE
;



SELECT *
FROM latest_vault.iterable.app_push_open
WHERE message_id = 'cbb4e9c9059a4ae8a8306b4243d9e9f9'
;


------------------------------------------------------------------------------------------------------------------------
WITH
	new_app_version_sessions AS (
		SELECT
			stba.*,
			ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
			ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
			ses.user_id,
			ses.app_id,
			ses.v_tracker,
			ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS screen_name,
			ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
			ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
			ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
			ses.event_tstamp,
			ses.event_name,
			ses.se_property,
			ses.se_action,
			ses.se_category,
			ses.se_property,
			ses.contexts_com_secretescapes_app_state_context_1,
			ses.device_platform
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-10-10'
		WHERE stba.touch_start_tstamp >= '2024-10-10'
		  AND stba.touch_experience = 'native app ios'
		  AND version = '6.31.1' -- users with current version of app
	),
	user_agg AS (
		SELECT
			navs.attributed_user_id,
			LISTAGG(DISTINCT navs.app_state, ', ') WITHIN GROUP ( ORDER BY navs.app_state) AS app_states,
			COUNT(DISTINCT navs.app_state)                                                 AS num_of_app_states
		FROM new_app_version_sessions navs
		GROUP BY 1
	)

SELECT
	ua.app_states,
	COUNT(DISTINCT ua.attributed_user_id) AS users,
FROM user_agg ua
GROUP BY 1
;

SELECT *
FROM user_agg total users,
	 count OF USERS who have ONLY had background SESSION
-- SELECT
-- 	IFF(navs.touch_duration_seconds = 0, TRUE, FALSE) AS is_zero_second_session,
-- 	navs.app_state,
-- 	COUNT(*)                                          AS session,
-- 	COUNT(DISTINCT navs.attributed_user_id)           AS users
-- FROM new_app_version_sessions navs
-- GROUP BY 1, 2
;

SELECT
	stba.*,
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS screen_name,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.event_tstamp,
	ses.event_name,
	ses.se_property,
	ses.se_action,
	ses.se_category,
	ses.se_property,
	ses.contexts_com_secretescapes_app_state_context_1,
	ses.device_platform
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-10-10'
WHERE stba.touch_start_tstamp >= '2024-10-01'
  AND stba.touch_experience = 'native app ios'
  AND stba.attributed_user_id = '82697059'


WITH
	new_app_version_sessions AS (
		SELECT
			stba.*,
			ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
			ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
			ses.user_id,
			ses.app_id,
			ses.v_tracker,
			ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS screen_name,
			ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
			ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
			ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
			ses.event_tstamp,
			ses.event_name,
			ses.se_property,
			ses.se_action,
			ses.se_category,
			ses.se_property,
			ses.contexts_com_secretescapes_app_state_context_1,
			ses.device_platform,
			IFF(version = '6.31.1', TRUE, FALSE)                                                    AS has_new_app -- users with current version of app
		FROM se.data_pii.scv_touch_basic_attributes stba
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-10-10'
		WHERE stba.touch_start_tstamp >= '2024-10-10'
		  AND stba.touch_experience = 'native app ios'

	),
	user_agg AS (
		SELECT
			navs.attributed_user_id,
			LISTAGG(DISTINCT navs.has_new_app, ', ') WITHIN GROUP ( ORDER BY navs.has_new_app) AS has_new_app,
			COUNT(DISTINCT navs.has_new_app)                                                   AS num_of_app_states
		FROM new_app_version_sessions navs
		GROUP BY 1
	)

SELECT
	ua.has_new_app,
	COUNT(DISTINCT ua.attributed_user_id) AS users
FROM user_agg ua
GROUP BY 1
;

;



SELECT
	stba.*,
	ses.contexts_com_secretescapes_app_state_context_1 IS NOT NULL                          AS has_app_state,
	ses.contexts_com_secretescapes_app_state_context_1[0]['app_state']::VARCHAR             AS app_state,
	ses.user_id,
	ses.app_id,
	ses.v_tracker,
	ses.unstruct_event_com_snowplowanalytics_mobile_screen_view_1['name']::VARCHAR          AS screen_name,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['build']::NUMBER             AS build,
	ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR          AS version,
	ses.contexts_com_snowplowanalytics_snowplow_mobile_context_1[0]['deviceModel']::VARCHAR AS device_model,
	ses.event_tstamp,
	ses.event_name,
	ses.se_property,
	ses.se_action,
	ses.se_category,
	ses.se_property,
	ses.contexts_com_secretescapes_app_state_context_1,
	ses.device_platform,
	IFF(version = '6.31.1', TRUE, FALSE)                                                    AS has_new_app -- users with current version of app
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= '2024-10-10'
WHERE stba.touch_start_tstamp >= '2024-10-10'
  AND stba.touch_experience = 'native app ios'
;



SELECT
	stba.touch_start_tstamp::DATE           AS date,
	app_state_context['app_state']::VARCHAR AS app_state,
	COUNT(*)                                AS sessions
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-10-10'
GROUP BY 1, 2
;


SELECT
	stba.touch_start_tstamp::DATE                       AS date,
	app_state_context['app_state']::VARCHAR IS NOT NULL AS has_app_state,
	COUNT(*)                                            AS sessions
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp >= '2024-10-10'
GROUP BY 1, 2
;


WITH
	new_app_events AS (
		SELECT
			app_state_context['app_state']::VARCHAR IS NOT NULL AS has_app_state,
			ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR =
			'6.31.1'                                            AS new_app_version,
			ses.v_tracker NOT LIKE 'ios%'                       AS landing_page_is_not_native_app,
			ses.contexts_com_secretescapes_app_state_context_1,
			ses.contexts_com_snowplowanalytics_mobile_application_1,
			*
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON stba.touch_id = ses.event_hash AND ses.event_tstamp::DATE = '2024-10-14'
-- AND 						  ses.device_platform = 'native app ios'
		WHERE stba.touch_experience = 'native app ios'
		  AND stba.touch_start_tstamp::DATE = '2024-10-14'

	)

-- SELECT *
-- FROM new_app_events
-- WHERE new_app_events.new_app_version IS NULL
-- ;

SELECT
	new_app_events.new_app_version,
	new_app_events.has_app_state,
	new_app_events.landing_page_is_not_native_app,
	COUNT(*)
FROM new_app_events
GROUP BY 1, 2, 3
;


------------------------------------------------------------------------------------------------------------------------
WITH
	native_app_ios_events AS (
		SELECT
			app_state_context['app_state']::VARCHAR IS NOT NULL AS has_app_state,
			app_state_context['app_state']::VARCHAR             AS app_state,
			ses.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR =
			'6.31.1'                                            AS new_app_version,
			ses.v_tracker NOT LIKE 'ios%'                       AS landing_page_is_not_native_app,
			ses.contexts_com_secretescapes_app_state_context_1,
			ses.contexts_com_snowplowanalytics_mobile_application_1,
			stba.*
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data_pii.scv_event_stream ses
					   ON stba.touch_id = ses.event_hash AND ses.event_tstamp::DATE = '2024-10-14'
-- AND 						  ses.device_platform = 'native app ios'
		WHERE stba.touch_experience = 'native app ios'
		  AND stba.touch_start_tstamp::DATE = '2024-10-14'
	)

-- new app zero second sessions
SELECT
	nais.touch_duration_seconds = 0 AS zero_second_session,
	COUNT(*)
FROM native_app_ios_events nais
WHERE nais.new_app_version
GROUP BY 1

-- --app state aggregate
-- SELECT
-- 	nais.app_state,
-- 	COUNT(*)
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version
-- GROUP BY 1

-- -- investigating background sessions
-- SELECT *
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version
-- AND app_state = 'background'


-- -- how many of these are 0 second sessions
-- SELECT
-- 	nais.touch_duration_seconds = 0 AS zero_second_session,
-- 	COUNT(*)
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version
--   AND app_state = 'background'
-- GROUP BY 1

-- -- investigating background sessions that aren't 0 seconds
-- SELECT *
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version
--   AND app_state = 'background'
--   AND nais.touch_duration_seconds > 0

-- -- background sessions that aren't 0 seconds look legitimate, checking a few touch_ids
-- SELECT *
-- FROM native_app_ios_events nais
-- 	INNER JOIN se.data_pii.scv_session_events_link ssel ON nais.touch_id = ssel.touch_id
-- 	INNER JOIN se.data_pii.scv_event_stream s ON ssel.event_hash = s.event_hash AND s.event_tstamp >= '2024-10-14'
-- WHERE nais.new_app_version
--   AND app_state = 'background'
--   AND nais.touch_duration_seconds > 0
--   AND nais.touch_id IN (
-- 						'57fcdf70db0a4fa907133bda1aede96cbb883b24469c4d4cdf9509901692e17a',
-- 						'f0e05d07ce56718a62570ce9bad3ef34ec4066ad9f84f6052eac51a2be243367',
-- 						'5557dfa8b17d4a115b4c94fc0862bb8d2e5c6438efe52f7169d50be2e759d21c',
-- 						'99f949808a4f31a9fa6b03ea051410b36d4f9f4db53e389d21ac77884e2bef24'
-- 	)
-- looking at count of sessions that we can explictly say were not on the new app
-- SELECT *
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version = FALSE


-- looking at count of zero second sessions that we can explictly say were not on the new app
-- SELECT
-- 	nais.touch_duration_seconds = 0,
-- 	count(*)
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version = FALSE
-- GROUP BY 1

-- looking at sessions that aren't categorised as new app to understand proportions that occurred outside of native app
-- SELECT *
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version IS NULL

-- 71K sessions don't have the app version present, looking at how many of these skipped the native app experience
-- looking at sessions that aren't categorised as new app to understand proportions that occurred outside of native app
-- SELECT nais.landing_page_is_not_native_app,
--        count(*)
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version IS NULL
-- GROUP BY 1

-- -- how many of these sessions that skipped native app experience at 0 second sessions
--
-- SELECT
-- 	nais.landing_page_is_not_native_app,
-- --        nais.touch_duration_seconds = 0 AS zero_second_session,
--        count(*)
-- FROM native_app_ios_events nais
-- WHERE nais.new_app_version IS NULL
-- GROUP BY ALL


-- how many sessions are zero second sessions
SELECT
	nais.touch_duration_seconds = 0 AS zero_second_session,
	COUNT(*)
FROM native_app_ios_events nais
GROUP BY 1
-- ;


-- Check numbers based on the 66K sessions that occurred on the 14th October;
-- APP_STATE	COUNT(*)
-- null		1
-- background	40140
-- foreground	26195


-- of the 66K sessions that happened on the new version of the app, 40K of them occurred in the background.

-- of the 40K, 35K of these are zero second sessions. Great ratio but what are the 5K that have a non 0 second session


-- look at sessions that don't have an app state for the 14th of october because there is a high proportion of sessions
-- that dont have app state but the adoption of new app of active users is at 90% (from app team)

-- 2K sessions we know occurred on an older version of the app
-- 71K sessions do not have an app version present.
-- 62K of these sessions skipped the native app experience;

