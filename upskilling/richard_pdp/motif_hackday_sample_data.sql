WITH
	input_sessions AS (
		SELECT *
		FROM se.data_pii.scv_touch_basic_attributes stba -- session summary table
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
		WHERE stba.touch_start_tstamp >= CURRENT_DATE - 28 -- limit to 4 weeks of data
		  AND stba.stitched_identity_type = 'se_user_id'
		  AND stba.touch_event_count > 3
		  AND stmc.touch_affiliate_territory = 'UK'

	)
SELECT
	COUNT(DISTINCT ins.attributed_user_id)
FROM input_sessions ins
;


-- Limited to 4 weeks of session data
-- only looking at sessions we can attribute to a user
-- with at least 3 sessioned events
-- UK traffic only

-- based on the above we have 1.5M sessions over 436,855 users


-- checking that screen name of events is something that we can utilise
SELECT
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 28
  AND ses.device_platform LIKE 'native app %'
  AND ses.event_name = 'screen_view'
;


-- getting the event level data that we will then filter using the distinct list of input users
SELECT
	pc.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name,
	pc.page_classification,
	pc.event_tstamp,
	ssel.attributed_user_id,
	pc.device_platform,
	*
FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification pc
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON pc.event_hash = ssel.event_hash
				   AND ssel.event_tstamp >= CURRENT_DATE - 28
				   AND ssel.stitched_identity_type = 'se_user_id'
WHERE pc.event_tstamp >= CURRENT_DATE - 28
  AND pc.event_name IN ('page_view', 'screen_view')
;

------------------------------------------------------------------------------------------------------------------------
-- getting a 8th of the list of users in the hope that we produce <2M events
WITH
	input_sessions AS (
		SELECT *
		FROM se.data_pii.scv_touch_basic_attributes stba -- session summary table
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
		WHERE stba.touch_start_tstamp >= CURRENT_DATE - 28 -- limit to 4 weeks of data
		  AND stba.stitched_identity_type = 'se_user_id'
		  AND stba.touch_event_count > 3
		  AND stmc.touch_affiliate_territory = 'UK'
	),
	distinct_list_of_users AS (
		SELECT DISTINCT
			ins.attributed_user_id
		FROM input_sessions ins
	)
SELECT *
FROM distinct_list_of_users dlou sample (13)
;

------------------------------------------------------------------------------------------------------------------------
-- combining events with distinct list of users

WITH
	input_sessions AS (
		SELECT *
		FROM se.data_pii.scv_touch_basic_attributes stba -- session summary table
			INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
		WHERE stba.touch_start_tstamp >= CURRENT_DATE - 28 -- limit to 4 weeks of data
		  AND stba.stitched_identity_type = 'se_user_id'
		  AND stba.touch_event_count > 3
		  AND stmc.touch_affiliate_territory = 'UK'
	),
	distinct_list_of_users AS (
		SELECT DISTINCT
			ins.attributed_user_id
		FROM input_sessions ins
	),
	sample_users AS (
		SELECT *
		FROM distinct_list_of_users dlou sample (11)
	),
	event_data AS (
		SELECT
			COALESCE(
					IFF(pc.contexts_com_secretescapes_search_context_1 IS NOT NULL, 'search event',
						NULL),
					pc.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR,
					pc.page_classification
			)                                                                          AS event_name,
			pc.event_tstamp,
			ssel.attributed_user_id,
			pc.device_platform,
			pc.page_urlpath,
			pc.contexts_com_secretescapes_search_context_1[0]['num_results']           AS num_search_results,
			pc.contexts_com_secretescapes_search_context_1[0]['triggered_by']::VARCHAR AS search_triggered_by
		FROM dbt.bi_customer_insight__intermediate.ci_event_stream_page_classification pc
			INNER JOIN se.data_pii.scv_session_events_link ssel
					   ON pc.event_hash = ssel.event_hash
						   AND ssel.event_tstamp >= CURRENT_DATE - 28 -- reduce table scan to last 4 weeks
						   AND ssel.stitched_identity_type = 'se_user_id'
		WHERE pc.event_tstamp >= CURRENT_DATE - 28 -- reduce table scan to last 4 weeks
				AND
			  (
				  pc.event_name IN ('page_view', 'screen_view') -- only care about these types of events for funnels
					  OR
				  pc.contexts_com_secretescapes_search_context_1 IS NOT NULL -- search event
				  )
	)
SELECT
	ed.*
FROM sample_users su
	INNER JOIN event_data ed ON su.attributed_user_id = ed.attributed_user_id
;

-- Limited to 4 weeks of session data (web and app)
-- only looking at sessions we can attribute to a user
-- with at least 3 sessioned events
-- UK traffic only
-- based on the above we have 1.5M sessions over 436,855 users

-- sample of 13% of these users (57K), we adjusted this to attempt to
-- return events within the limits of the local environment for motif

-- this produces 1.8M events
