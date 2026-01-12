SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
-- 	stba.touch_experience,
	COUNT(*)                                   AS sessions,
	SUM(IFF(stba.touch_logged_in, 1, 0))       AS logged_in_sessions,
	logged_in_sessions / sessions              AS perc_member_sessions_stitched
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2023-01-01'
  AND stba.touch_se_brand = 'SE Brand'
  AND stba.touch_experience NOT IN ('native app android',
									'native app ios')
GROUP BY 1
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios' AND
	  stba.stitched_identity_type = 'se_user_id' AND
	  stba.touch_logged_in = FALSE
;


-- a lot of sessions with 0 duration

SELECT
	stba.touch_start_tstamp::DATE              AS date,
	stba.touch_experience,
	COUNT(*)                                   AS sessions,
	SUM(IFF(stba.touch_event_count = 1, 1, 0)) AS zero_duration_sessions,
	SUM(IFF(stba.touch_event_count > 1, 1, 0)) AS non_zero_duration_sessions,
	zero_duration_sessions / sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience LIKE 'native app%' AND
	  stba.stitched_identity_type = 'se_user_id' AND
	  stba.touch_logged_in = FALSE
GROUP BY 1, 2
;


-- very high proportion on ios with 0 duration or 1 event count session
-- splitting out by hour to see if has high and low points


SELECT
	stba.touch_start_tstamp::DATE              AS date,
	TO_CHAR(stba.touch_start_tstamp, 'HH')     AS hour,
	stba.touch_experience,
	COUNT(*)                                   AS sessions,
	SUM(IFF(stba.touch_event_count = 1, 1, 0)) AS one_event_sessions,
	SUM(IFF(stba.touch_event_count > 1, 1, 0)) AS more_than_one_event_sessions,
	one_event_sessions / sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app ios' AND
	  stba.stitched_identity_type = 'se_user_id' AND
	  stba.touch_start_tstamp >= '2024-01-01' AND
	  stba.touch_logged_in = FALSE
GROUP BY 1, 2, 3


SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp)   AS month,
	CASE
		WHEN stmc.touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
		WHEN stmc.touch_affiliate_territory = 'UK' THEN 'UK'
		WHEN stmc.touch_affiliate_territory IN ('NON_VERIFIED', 'ANOMALOUS', 'SE TECH', 'SE_TEMP') THEN 'SE TECH'
		ELSE 'ROW'
	END                                          AS territory_group,
	stba.touch_logged_in,
	COUNT(*)                                     AS sessions,
	COUNT(DISTINCT stba.attributed_user_id_hash) AS users
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
GROUP BY 1, 2, 3
;

USE WAREHOUSE pipe_xlarge
;


SELECT
	DATE_TRUNC(MONTH, stba.touch_start_tstamp)   AS month,
	CASE
		WHEN stmc.touch_affiliate_territory IN ('DE', 'CH', 'AT') THEN 'DACH'
		WHEN stmc.touch_affiliate_territory = 'UK' THEN 'UK'
		WHEN stmc.touch_affiliate_territory IN ('NON_VERIFIED', 'ANOMALOUS', 'SE TECH', 'SE_TEMP') THEN 'SE TECH'
		ELSE 'ROW'
	END                                          AS territory_group,
	CASE
		WHEN stba.touch_logged_in THEN 'logged in'
		WHEN stba.touch_logged_in = FALSE AND stba.stitched_identity_type = 'se_user_id' THEN 'logged out member'
		WHEN stba.touch_logged_in = FALSE AND stba.stitched_identity_type IS DISTINCT FROM 'se_user_id'
			THEN 'logged out non member'
	END                                          AS session_member_status,
	COUNT(*)                                     AS sessions,
	COUNT(DISTINCT stba.attributed_user_id_hash) AS users
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2022-01-01'
GROUP BY 1, 2, 3
;


------------------------------------------------------------------------------------------------------------------------
-- look at app sessions that have an se_user_id stitched identity type but aren't logged in to try to understand why
-- there are so many

SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_experience LIKE 'native app%' AND stba.touch_start_tstamp >= CURRENT_DATE - 1
;

-- found a session which is logged out but stitched and has 25 events

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp >= '2024-04-28'
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= '2024-04-28'
WHERE stba.touch_start_tstamp::DATE = '2024-04-28'
  AND stba.touch_id = '8b438fcb7c156adf91b85826e4bb88c6bdc8f62fbb246ca36cb8323a72993cdf'
;



SELECT
	ses.event_tstamp::DATE                 AS date,
	stba.touch_experience,
	COUNT(*)                               AS sale_page_events,
	SUM(IFF(ses.se_user_id IS NULL, 1, 0)) AS logged_out_sale_page_events
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp >= '2024-01-01'
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= '2024-01-01'
WHERE ses.event_tstamp >= '2024-01-01'
  AND stba.touch_experience LIKE 'native app%'
  AND ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
GROUP BY 1, 2
;


SELECT *
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = 'A44064'
;



SELECT
	ses.event_tstamp::DATE                                                     AS date,
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
	COUNT(*)                                                                   AS logged_out_session
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp >= '2024-01-01'
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= '2024-01-01'
WHERE ses.event_tstamp >= '2024-04-01'
  AND stba.touch_experience = 'native app ios'
  AND ssel.stitched_identity_type = 'se_user_id'
  AND stba.touch_logged_in = FALSE
GROUP BY 1, 2
;


SELECT
	DATE_TRUNC(HOUR, fcb.booking_completed_timestamp) AS hour,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency)
FROM se.data.fact_complete_booking fcb
WHERE fcb.se_brand = 'SE Brand'
  AND fcb.booking_completed_date > '2024-04-15'
  AND fcb.territory = 'UK'
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------
-- looking at sessions with only 1 event
SELECT
	stba.touch_start_tstamp::DATE              AS date,
	COUNT(*)                                   AS sessions,
	SUM(IFF(stba.touch_event_count = 1, 1, 0)) AS single_event_sessions,
	single_event_sessions / sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 10
  AND stba.touch_experience LIKE 'native app%'
GROUP BY 1
;

-- 60% of sessions only have 1 event

SELECT
	stba.touch_experience AS experience,
	COUNT(*)              AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_experience LIKE 'native app%'
  AND stba.touch_event_count = 1
GROUP BY 1
;
-- vast majority are on ios 98%

SELECT
	stba.touch_logged_in AS logged_in_state,
	COUNT(*)             AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_experience LIKE 'native app ios'
  AND stba.touch_event_count = 1
GROUP BY 1
;
-- 62% are in a logged out state.


SELECT
	stba.stitched_identity_type,
	COUNT(*) AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_experience LIKE 'native app ios'
  AND stba.touch_event_count = 1
GROUP BY 1
;
--98% are arttributed to a user id (we know who they are)

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_experience LIKE 'native app ios'
  AND stba.touch_event_count = 1
;
-- eda on app single event sessions


SELECT
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
	COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_experience LIKE 'native app ios'
  AND stba.touch_event_count = 1
GROUP BY 1
;

-- 60% of sessions are in instant access (40k).

-- looking into if users sessions are being separated

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON stba.touch_id = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_experience LIKE 'native app ios'
  AND stba.attributed_user_id = '6473460'
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1 AND ses.idfv = '6BAABDD9-D38D-46FC-9EFA-605044BB688A'

