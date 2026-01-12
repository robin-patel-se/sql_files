WITH
	test_flags AS (
		SELECT
			stff.touch_id,
-- 			stff.touch_start_tstamp,
-- 			stff.feature_flag
			ARRAY_AGG(stff.feature_flag) AS feature_flags,
			COUNT(*)                     AS num_flags
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag IN
			  ('abtest.price.elasticity.variant', 'abtest.price.elasticity.control')
		  AND stff.touch_start_tstamp::DATE = '2025-10-16'
		  AND is_logged_in = TRUE
		GROUP BY stff.touch_id
	),
	modelling_sessions AS (
		SELECT
			touch_attributes.touch_id,
			touch_attributes.touch_start_tstamp,
			touch_attributes.attributed_user_id,
			touch_channel.touch_mkt_channel,
			touch_channel.touch_affiliate_territory,
			touch_attributes.touch_experience,
			touch_attributes.touch_landing_page_categorisation,
			touch_attributes.touch_landing_screen_categorisation,
			touch_attributes.mobile_application_context['version']::VARCHAR AS app_version,
			test_flags.touch_id IS NULL                                     AS is_missing_feature_flag,
			test_flags.feature_flags,
			test_flags.num_flags,
			touch_attributes.touch_event_count,
			touch_attributes.num_spvs,
			touch_attributes.num_bfvs,
			touch_attributes.num_pay_button_clicks,
			touch_attributes.num_trxs,
		FROM se.data_pii.scv_touch_basic_attributes touch_attributes
		INNER JOIN se.data.scv_touch_marketing_channel touch_channel
			ON touch_attributes.touch_id = touch_channel.touch_id
			AND touch_channel.touch_affiliate_territory IN ('UK', 'DE')
		LEFT JOIN test_flags
			ON touch_attributes.touch_id = test_flags.touch_id
		WHERE touch_attributes.touch_start_tstamp::DATE = '2025-10-16'
		  AND touch_attributes.stitched_identity_type = 'se_user_id'
		  AND touch_attributes.touch_se_brand = 'SE Brand'
	)
SELECT *
FROM modelling_sessions

	-- sessions missing a ff
-- SELECT
-- 	modelling_sessions.feature_flags IS NULL AS is_missing_feature_flag,
-- 	modelling_sessions.touch_experience,
-- 	COUNT(*)
-- FROM modelling_sessions
-- GROUP BY ALL

-- sessions that don't have a ff
-- SELECT *
-- FROM modelling_sessions
-- WHERE is_missing_feature_flag
-- ;

-- sessions without an ff by channel
-- SELECT
-- 	modelling_sessions.touch_mkt_channel,
-- 	COUNT(*)
-- FROM modelling_sessions
-- WHERE is_missing_feature_flag
-- GROUP BY modelling_sessions.touch_mkt_channel
-- ;
-- massively skewed to Direct and email

-- app ios missing by version
-- SELECT
-- 	modelling_sessions.feature_flags IS NULL AS is_missing_feature_flag,
-- 	modelling_sessions.app_version,
-- 	COUNT(*)
-- FROM modelling_sessions
-- WHERE modelling_sessions.touch_experience = 'native app ios'
-- GROUP BY ALL
-- ;

-- bookings for sessions with a ff
-- SELECT count(DISTINCT touched_transactions.booking_id)
-- FROM modelling_sessions
-- LEFT JOIN se.data.scv_touched_transactions touched_transactions
-- 	ON modelling_sessions.touch_id = touched_transactions.touch_id
-- WHERE modelling_sessions.is_missing_feature_flag = FALSE
--   AND touched_transactions.booking_id IS NOT NULL;

-- bookings for sessions without a ff
-- SELECT count(DISTINCT touched_transactions.booking_id)
-- FROM modelling_sessions
-- LEFT JOIN se.data.scv_touched_transactions touched_transactions
-- 	ON modelling_sessions.touch_id = touched_transactions.touch_id
-- WHERE modelling_sessions.is_missing_feature_flag = TRUE
--   AND touched_transactions.booking_id IS NOT NULL;

-- sessions with multiple ff
-- SELECT
-- 	COUNT(DISTINCT IFF(modelling_sessions.num_flags > 1, modelling_sessions.touch_id, NULL))
-- FROM modelling_sessions;
-- 213


-- SELECT
-- 	modelling_sessions.feature_flags[0]::VARCHAR  AS feature_flag,
-- 	modelling_sessions.touch_experience,
-- 	modelling_sessions.app_version,
-- 	COUNT(DISTINCT touch_id)                      AS sessions,
-- 	SUM(modelling_sessions.num_spvs)              AS spvs,
-- 	SUM(modelling_sessions.num_bfvs)              AS bfvs,
-- 	SUM(modelling_sessions.num_pay_button_clicks) AS pay_button_clicks,
-- 	SUM(modelling_sessions.num_trxs)              AS trx,
-- FROM modelling_sessions
-- WHERE modelling_sessions.touch_experience = 'native app ios'
--   AND num_flags = 1 -- remove dupe flag sessions
-- GROUP BY ALL
-- ;


	use warehouse pipe_xlarge
;

/*ff AS (
	SELECT
		touch_id,
		feature_flag,
		CASE
			WHEN feature_flag IN
				 ('abtest.price.elasticity.variant', 'abtest.price.elasticity.control')
				THEN 'price elasticity (v3)'
		END AS ab_test_type,
		CASE
			WHEN feature_flag LIKE '%elasticity.variant%' THEN 'variant' ELSE 'control'
		END AS ab_test_group
	FROM se.data.scv_touched_feature_flags
	WHERE feature_flag IN
		  (
		   'abtest.price.elasticity.variant',
		   'abtest.price.elasticity.control'
			  )
	  AND touch_start_tstamp::DATE >= '2025-10-14' AND touch_start_tstamp::DATE<=CURRENT_DATE()-1
	  AND is_logged_in=TRUE
),

exclude_sessions_multiple_ff_flags AS (
	SELECT
		touch_id,
		ab_test_type,
		COUNT(*) AS number_ff
	FROM ff
	GROUP BY 1, 2
	HAVING number_ff > 1
),

 */

WITH
	ff AS
		(
			SELECT
				stff.touch_id,
				ARRAY_AGG(stff.feature_flag),
				COUNT(*)                          AS flags,
				COUNT(DISTINCT stff.is_logged_in) AS no_login_states
			FROM se.data.scv_touched_feature_flags stff
			WHERE stff.feature_flag LIKE 'abtest.price.elasticity%'
			  AND stff.touch_start_tstamp::DATE = '2025-10-16'
			GROUP BY stff.touch_id
		)
SELECT
	COUNT(*),
	COUNT(IFF(ff.flags > 1, 1, NULL))
FROM ff

;


-- investigating a session that is missing a ff '3d00ccf1cc4bfc7dac5c6977e2d925c5b6e6f028ced4f5797db6e5e441d5a4c6'
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	ssel.stitched_identity_type,
	stba.touch_experience,
	ssel.*,
	ses.*
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON stba.touch_id = ssel.touch_id
	AND ssel.event_tstamp >= '2025-10-10' -- just for pruning
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp >= '2025-10-10' -- just for pruning
WHERE stba.touch_id = '3d00ccf1cc4bfc7dac5c6977e2d925c5b6e6f028ced4f5797db6e5e441d5a4c6'

-- investigating a session that is missing a ff '1b9dfcfe3ddd1ce1af58e1ef4001d9fe87cd4418b89691ed5171854a0f5a1d29'
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	ssel.stitched_identity_type,
	stba.touch_experience,
	ssel.*,
	ses.*
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON stba.touch_id = ssel.touch_id
	AND ssel.event_tstamp >= '2025-10-10' -- just for pruning
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp >= '2025-10-10' -- just for pruning
WHERE stba.touch_id = '1b9dfcfe3ddd1ce1af58e1ef4001d9fe87cd4418b89691ed5171854a0f5a1d29'
;


-- investigating a session that is missing a ff '9cda0c39128a27d60301612a64cce0b0c7f79c185c9f0b248bf4634837b405ee'
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	ssel.stitched_identity_type,
	stba.touch_experience,
	ssel.*,
	ses.*
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON stba.touch_id = ssel.touch_id
	AND ssel.event_tstamp >= '2025-10-10' -- just for pruning
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp >= '2025-10-10' -- just for pruning
WHERE stba.touch_id = '9cda0c39128a27d60301612a64cce0b0c7f79c185c9f0b248bf4634837b405ee'
;

-- travelbird.de


-- investigating a session that is missing a ff 'f06f67ac09e64c0e035d04cdcfb8569e2c4979ffef6cd62842365b845dfe7d2d'
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	ssel.stitched_identity_type,
	stba.touch_experience,
	ssel.*,
	ses.*
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON stba.touch_id = ssel.touch_id
	AND ssel.event_tstamp >= '2025-10-10' -- just for pruning
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp >= '2025-10-10' -- just for pruning
WHERE stba.touch_id = 'f06f67ac09e64c0e035d04cdcfb8569e2c4979ffef6cd62842365b845dfe7d2d'
;

-- investigating a session that is missing a ff '741d42e85de0452129a095c2f5d6c10ab22a404fc7076ac6156c1c92ade62e01'
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	ssel.stitched_identity_type,
	stba.touch_experience,
	ssel.*,
	ses.*
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON stba.touch_id = ssel.touch_id
	AND ssel.event_tstamp >= '2025-10-10' -- just for pruning
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp >= '2025-10-10' -- just for pruning
WHERE stba.touch_id = '741d42e85de0452129a095c2f5d6c10ab22a404fc7076ac6156c1c92ade62e01'
;


-- investigating a session that is missing a ff '0a671a11b5e86b62f46ff7bba747df0e928436a960445ddf30e40c731fc89f34'
SELECT
	stba.touch_id,
	stba.touch_start_tstamp,
	ssel.stitched_identity_type,
	stba.touch_experience,
	ssel.*,
	ses.*
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data_pii.scv_session_events_link ssel
	ON stba.touch_id = ssel.touch_id
	AND ssel.event_tstamp >= '2025-10-10' -- just for pruning
INNER JOIN se.data_pii.scv_event_stream ses
	ON ssel.event_hash = ses.event_hash
	AND ses.event_tstamp >= '2025-10-10' -- just for pruning
WHERE stba.touch_id = '0a671a11b5e86b62f46ff7bba747df0e928436a960445ddf30e40c731fc89f34'
;


------------------------------------------------------------------------------------------------------------------------

WITH
	test_flags AS (
		SELECT
			stff.touch_id,
-- 			stff.touch_start_tstamp,
-- 			stff.feature_flag
			ARRAY_AGG(stff.feature_flag) AS feature_flags,
			COUNT(*)                     AS num_flags
		FROM se.data.scv_touched_feature_flags stff
		WHERE stff.feature_flag IN
			  ('abtest.price.elasticity.variant', 'abtest.price.elasticity.control')
		  AND stff.touch_start_tstamp::DATE = '2025-10-16'
		  AND is_logged_in = TRUE
		GROUP BY stff.touch_id
	),
	modelling_sessions AS (
		SELECT
			touch_attributes.touch_id,
			touch_attributes.touch_start_tstamp,
			touch_attributes.attributed_user_id,
			touch_channel.touch_mkt_channel,
			touch_channel.touch_affiliate_territory,
			touch_attributes.touch_experience,
			touch_attributes.touch_landing_page_categorisation,
			touch_attributes.touch_landing_screen_categorisation,
			touch_attributes.mobile_application_context['version']::VARCHAR AS app_version,
			test_flags.touch_id IS NULL                                     AS is_missing_feature_flag,
			test_flags.feature_flags,
			test_flags.num_flags,
			touch_attributes.touch_event_count,
			touch_attributes.num_spvs,
			touch_attributes.num_bfvs,
			touch_attributes.num_pay_button_clicks,
			touch_attributes.num_trxs,
		FROM se.data_pii.scv_touch_basic_attributes touch_attributes
		INNER JOIN se.data.scv_touch_marketing_channel touch_channel
			ON touch_attributes.touch_id = touch_channel.touch_id
			AND touch_channel.touch_affiliate_territory IN ('UK', 'DE')
		LEFT JOIN test_flags
			ON touch_attributes.touch_id = test_flags.touch_id
		WHERE touch_attributes.touch_start_tstamp::DATE = '2025-10-16'
		  AND touch_attributes.stitched_identity_type = 'se_user_id'
		  AND touch_attributes.touch_se_brand = 'SE Brand'
	),
	users_with_session AS (
		SELECT DISTINCT
			modelling_sessions.attributed_user_id
		FROM modelling_sessions
	),
	sessions_within_30 AS (
		SELECT
			touch_attributes.*
		FROM se.data_pii.scv_touch_basic_attributes touch_attributes
		INNER JOIN users_with_session
			ON touch_attributes.attributed_user_id = users_with_session.attributed_user_id
		WHERE touch_attributes.touch_start_tstamp BETWEEN DATEADD(DAY, -30, '2025-10-16') AND '2025-10-15'
	),
	returning_session_type AS (
		SELECT
			modelling_sessions.*,
			sessions_within_30.touch_start_tstamp                       AS last_touch_start_tstamp,
			sessions_within_30.touch_start_tstamp IS DISTINCT FROM NULL AS is_returning_session,
			IFF(DATEDIFF(DAY, sessions_within_30.touch_start_tstamp, modelling_sessions.touch_start_tstamp) <= 2,
				TRUE, FALSE)                                            AS is_returning_session_within_1d,
			IFF(DATEDIFF(DAY, sessions_within_30.touch_start_tstamp, modelling_sessions.touch_start_tstamp) <= 8,
				TRUE, FALSE)                                            AS is_returning_session_within_7d,
			IFF(DATEDIFF(DAY, sessions_within_30.touch_start_tstamp, modelling_sessions.touch_start_tstamp) <= 15,
				TRUE, FALSE)                                            AS is_returning_session_within_14d,
		FROM modelling_sessions
		ASOF JOIN sessions_within_30
		MATCH_CONDITION (modelling_sessions.touch_start_tstamp >= sessions_within_30.touch_start_tstamp)
			ON modelling_sessions.attributed_user_id = sessions_within_30.attributed_user_id
	)
SELECT
	returning_session_type.feature_flags[0]::VARCHAR  AS test_group,
	returning_session_type.is_returning_session_within_1d,
	returning_session_type.touch_experience,
	COUNT(DISTINCT returning_session_type.touch_id)   AS sessions,
	SUM(returning_session_type.num_spvs)              AS num_spvs,
	SUM(returning_session_type.num_bfvs)              AS num_bfvs,
	SUM(returning_session_type.num_pay_button_clicks) AS num_pay_button_clicks,
	SUM(returning_session_type.num_trxs)              AS num_bookings
FROM returning_session_type
WHERE returning_session_type.num_flags = 1
GROUP BY ALL
ORDER BY returning_session_type.touch_experience,
		 returning_session_type.is_returning_session_within_1d,
		 returning_session_type.feature_flags[0]::VARCHAR
;