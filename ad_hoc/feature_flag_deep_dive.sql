SELECT
	ses.se_user_id,
	ses.se_sale_id,
	ses.is_server_side_event,
	ses.contexts_com_secretescapes_user_state_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_name = 'page_view'
  AND ses.event_tstamp >= CURRENT_DATE - 1
;


SELECT
	stff.touch_start_tstamp::DATE,
	COUNT(DISTINCT stff.touch_id)
FROM se.data.scv_touched_feature_flags stff
WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
GROUP BY 1
;

SELECT
	stba.touch_start_tstamp::DATE,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2023-07-11'
GROUP BY ALL
;



SELECT
	stff.touch_start_tstamp::DATE,
	COUNT(DISTINCT stff.touch_id)
FROM se.data.scv_touched_feature_flags stff
WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
GROUP BY 1
;

WITH
	session_test_data AS (
		SELECT
			stff.touch_id,
			COUNT(*) > 1                                                   AS has_any_test,
			SUM(IFF(stff.feature_flag = 'sale.reviews.enabled', 1, 0)) > 0 AS review_test,
			SUM(IFF(stff.feature_flag = 'sale.reviews.control', 1, 0)) > 0 AS review_control,
			review_test AND review_control                                 AS review_both
		FROM se.data.scv_touched_feature_flags stff
-- 		WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
		GROUP BY 1
	),

	touch_ids_with_spv AS (
		SELECT DISTINCT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= '2023-06-01'
		  -- remove spvs on other domains (eg. tracy)
		  AND PARSE_URL(sts.page_url)['host']::VARCHAR = 'www.secretescapes.com'
	),
	analysis_top_level AS (
		SELECT
			stba.touch_start_tstamp::DATE                                AS date,
			COUNT(DISTINCT stba.touch_id)                                AS total_sessions_with_spv,
			COUNT(DISTINCT IFF(std.has_any_test, stba.touch_id, NULL))   AS total_sessions_with_spv_that_have_ff,
			COUNT(DISTINCT IFF(std.review_test, stba.touch_id, NULL))    AS review_test_sessions,
			COUNT(DISTINCT IFF(std.review_control, stba.touch_id, NULL)) AS review_control_sessions,
			COUNT(DISTINCT IFF(std.review_both, stba.touch_id, NULL))    AS review_control_and_test_sessions
		FROM se.data.scv_touch_basic_attributes stba
			--filter for sessions with an spv
			INNER JOIN touch_ids_with_spv tiws ON stba.touch_id = tiws.touch_id
			LEFT JOIN  session_test_data std ON stba.touch_id = std.touch_id
		WHERE stba.touch_start_tstamp >= '2023-06-01'
		  AND stba.touch_hostname = 'www.secretescapes.com'
		  AND stba.touch_hostname_territory = 'UK'
		  AND stba.touch_logged_in
		GROUP BY 1
	)
		,
	all_non_feature_flag AS (
		SELECT *
		FROM se.data.scv_touch_basic_attributes stba
			LEFT JOIN session_test_data std ON stba.touch_id = std.touch_id
		WHERE stba.touch_start_tstamp >= '2023-07-13'
		  AND stba.touch_hostname = 'www.secretescapes.com'
		  AND std.touch_id IS NULL
	),
	all_feature_flag AS (
		SELECT *
		FROM se.data.scv_touch_basic_attributes stba
			LEFT JOIN session_test_data std ON stba.touch_id = std.touch_id
		WHERE stba.touch_start_tstamp >= '2023-07-13'
		  AND stba.touch_hostname = 'www.secretescapes.com'
		  AND std.touch_id IS NOT NULL
	),
	agg_non_feature_flag AS (
		SELECT
			stmc.touch_mkt_channel,
			COUNT(*)                             AS total,
			SUM(IFF(std.touch_id IS NULL, 1, 0)) AS no_feature_flag,
			no_feature_flag / total
		FROM se.data.scv_touch_basic_attributes stba
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
			LEFT JOIN  session_test_data std ON stba.touch_id = std.touch_id
		WHERE stba.touch_start_tstamp >= '2023-07-13'
		  AND stba.touch_hostname = 'www.secretescapes.com'
		GROUP BY 1
	)
SELECT *
FROM analysis_top_level
;

USE WAREHOUSE pipe_xlarge
;


WITH
	sessions_with_ff AS (
		SELECT
			stff.touch_id
		FROM se.data.scv_touched_feature_flags stff
		WHERE feature_flag IN ('sale.reviews.enabled', 'sale.reviews.control')
		GROUP BY 1
	),
	sessions_with_spv AS (
		SELECT
			sts.touch_id
		FROM se.data.scv_touched_spvs sts
		WHERE sts.event_tstamp >= CURRENT_DATE - 1
		GROUP BY 1
	)
SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN sessions_with_spv s ON stba.touch_id = s.touch_id
	LEFT JOIN  sessions_with_ff sf ON stba.touch_id = sf.touch_id
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND sf.touch_id IS NULL
;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '05c27ed1566b15c3a8a3889f983235aab610ef666ad11b1b2f82953c2d17a0ef'
  AND ses.event_tstamp >= '2023-07-17'
;



SELECT
	sts.page_url,
	PARSE_URL(sts.page_url)['host']::VARCHAR AS hostname
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= '2023-06-01'
;


