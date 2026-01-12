-- Element Category = product category, then element_sub_category will show for expample stay types (the tiles at the top of the app)
-- Element Category = product sub category, then element_sub_category, this would be adult for example.
-- Element Category = hompage, element_sub_category handpicked for you etc
-- Element Category = hompage = current sales and element_sub_category = homepage panel


WITH
	data AS (
		SELECT DISTINCT
			ses.event_hash,
			ses.posa_territory,
			ses.page_url,
			ses.event_tstamp,
			ses.page_urlpath,
			ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR                    AS snowplow_session_id,
			ses.contexts_com_secretescapes_content_element_interaction_context_1                                     AS context,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
		FROM se.data_pii.scv_event_stream AS ses
		WHERE ses.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
		  AND ses.device_platform LIKE 'native app%'
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR IN
			  ('homepage panel', 'product sub category', 'product category', 'current sales')
		  AND interaction_type = 'click'
	)
SELECT
	element_category,
	element_sub_category,
	COUNT(*) AS clicks
FROM data
WHERE posa_territory = 'UK'
GROUP BY ALL
;

USE WAREHOUSE pipe_xlarge
;

SELECT DISTINCT
	ses.event_hash,
	ses.posa_territory,
	ses.page_url,
	ses.event_tstamp,
	ses.page_urlpath,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR                    AS snowplow_session_id,
	ses.contexts_com_secretescapes_content_element_interaction_context_1                                     AS context,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type
FROM se.data_pii.scv_event_stream AS ses
WHERE ses.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
  AND ses.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
  AND ses.device_platform LIKE 'native app%'
  AND ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR IN
	  ('homepage panel', 'product sub category', 'product category', 'current sales')
  AND interaction_type = 'click'
;

-- list of session ids
SELECT DISTINCT
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR AS snowplow_session_id,
	ssel.touch_id
FROM se.data_pii.scv_event_stream AS ses
	INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
WHERE ses.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
  AND ses.event_name = 'screen_view'
;


-- list of bookings and margin
SELECT
	stt.touch_id,
	COUNT(DISTINCT stt.booking_id)                      AS bookings,
	SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
WHERE stt.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
GROUP BY 1
;

WITH
	touch_id AS (
		-- list of session ids and touch id
		SELECT DISTINCT
			ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR AS snowplow_session_id,
			ssel.touch_id
		FROM se.data_pii.scv_event_stream AS ses
			INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
		WHERE ses.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
		  AND ses.event_name = 'screen_view'
	),
	booking_info AS (
		-- list of bookings and margin at session level
		SELECT
			stt.touch_id,
			COUNT(DISTINCT stt.booking_id)                      AS bookings,
			SUM(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.fact_complete_booking fcb ON stt.booking_id = fcb.booking_id
		WHERE stt.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
		GROUP BY 1
	),
	modelling AS (
-- currently bookings will be overstated at a click level, this could be adjusted to only use one
-- of the clicks within a session
		SELECT DISTINCT
			ses.event_hash,
			ses.posa_territory,
			ses.page_url,
			ses.event_tstamp,
			ses.page_urlpath,
			ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR                    AS snowplow_session_id,
			ses.contexts_com_secretescapes_content_element_interaction_context_1                                     AS context,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
			ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type,
			bi.bookings,
			bi.margin_gbp
		FROM se.data_pii.scv_event_stream AS ses
			LEFT JOIN touch_id ti
					  ON ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR =
						 ti.snowplow_session_id
			LEFT JOIN booking_info bi ON ti.touch_id = bi.touch_id
		WHERE ses.event_tstamp BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
		  AND ses.device_platform LIKE 'native app%'
		  AND ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR IN
			  ('homepage panel', 'product sub category', 'product category', 'current sales')
		  AND interaction_type = 'click'
	)
SELECT
	element_category,
	element_sub_category,
	COUNT(*)        AS clicks,
	SUM(bookings)   AS bookings,
	SUM(margin_gbp) AS margin_gbp
FROM modelling
WHERE posa_territory IN ('UK', 'DE')
GROUP BY 1, 2
;