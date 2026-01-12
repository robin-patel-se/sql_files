--9th November 5%
--15th November 50%

USE WAREHOUSE pipe_xlarge
;

WITH
	search_data AS (
		SELECT
			GET(GET(es.contexts_com_secretescapes_user_context_1, 0), 'unique_browser_id') AS ubid,
			GET_PATH(GET(es.contexts_com_secretescapes_filter_context_1, 0),
					 'options[0].code')::VARCHAR                                           AS filter_context_options_code,
			GET_PATH(GET(es.contexts_com_secretescapes_filter_context_1, 0),
					 'options[0].option_name')::VARCHAR                                    AS filter_context_options_name,
			es.contexts_com_secretescapes_search_context_1[0]:change_filters::VARCHAR      AS search_change_filters,
			es.contexts_com_secretescapes_search_context_1[0]:had_results::VARCHAR         AS search_had_results,
			es.contexts_com_secretescapes_search_context_1[0]:num_results::VARCHAR         AS search_num_results,
			es.*,
			sel.touch_id,
			sel.attributed_user_id
		FROM se.data_pii.scv_event_stream es
			INNER JOIN se.data_pii.scv_session_events_link sel ON sel.event_hash = es.event_hash
		WHERE es.se_brand = 'Travelist'
		  --AND es.event_tstamp::DATE >= '2023-11-16'
		  AND es.event_tstamp::DATE = CURRENT_DATE - 1
	),
	booking_data AS (
		SELECT
			touch_id,
			COUNT(DISTINCT booking_id) AS bookings
		FROM se.data.scv_touched_transactions
		GROUP BY 1
	)
SELECT
	CASE
		WHEN sd.filter_context_options_name <> 'legacy' THEN 'vision'
		WHEN sd.filter_context_options_name IS DISTINCT FROM 'legacy' AND sd.search_change_filters = FALSE THEN 'vision'
		WHEN sd.filter_context_options_name = 'legacy' THEN 'legacy'
		ELSE NULL
	END                                   AS search_type,
	COUNT(DISTINCT sd.ubid::VARCHAR)      AS events,
	COUNT(DISTINCT sd.attributed_user_id) AS users,
	SUM(bd.bookings)                      AS total_bookings
FROM search_data sd
	LEFT JOIN booking_data bd ON bd.touch_id = sd.touch_id
WHERE search_type IS NOT NULL
  AND sd.attributed_user_id IS NOT NULL
GROUP BY 1
;


SELECT
	DATE(o.created_at_dts) AS created,
	op.value,
	COUNT(1)
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_order_snapshot o
	LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_orderproperty_snapshot op
			  ON (op.name = 'abtest_search' AND op.order_id = o.id)
	LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_orderproperty_snapshot oop
			  ON (oop.order_id = o.id AND oop.name = 'utm_medium')
WHERE DATE(o.created_at_dts) = '2023-11-26'
  AND o.order_status = 'COMPLETE'
  AND (oop.value IS NULL OR oop.value != 'newsletter')
GROUP BY op.value, DATE(o.created_at_dts)
ORDER BY DATE(o.created_at_dts), op.value
;


-- session that has a search and a booking
-- touch id: 50a625d4da173c3ba79bc38645591f239e3067417f9ec868c4021d93e6e05fba


SELECT
	GET(GET(es.contexts_com_secretescapes_user_context_1, 0), 'unique_browser_id') AS ubid,
	GET_PATH(GET(es.contexts_com_secretescapes_filter_context_1, 0),
			 'options[0].code')::VARCHAR                                           AS filter_context_options_code,
	GET_PATH(GET(es.contexts_com_secretescapes_filter_context_1, 0),
			 'options[0].option_name')::VARCHAR                                    AS filter_context_options_name,
	es.contexts_com_secretescapes_search_context_1[0]:change_filters::VARCHAR      AS search_change_filters,
	es.contexts_com_secretescapes_search_context_1[0]:had_results::VARCHAR         AS search_had_results,
	es.contexts_com_secretescapes_search_context_1[0]:num_results::VARCHAR         AS search_num_results,
	es.*,
	sel.touch_id,
	sel.attributed_user_id
FROM se.data_pii.scv_event_stream es
	INNER JOIN se.data_pii.scv_session_events_link sel ON sel.event_hash = es.event_hash
WHERE es.se_brand = 'Travelist'
  --AND es.event_tstamp::DATE >= '2023-11-16'
  AND es.event_tstamp::DATE = CURRENT_DATE - 1


SELECT
	COUNT(DISTINCT es.event_hash) AS events,
	COUNT(DISTINCT sel.touch_id)  AS sessions
FROM se.data_pii.scv_event_stream es
	INNER JOIN se.data_pii.scv_session_events_link sel
			   ON sel.event_hash = es.event_hash AND es.event_tstamp::DATE = CURRENT_DATE - 1
WHERE es.se_brand = 'Travelist'
  AND es.event_tstamp::DATE = CURRENT_DATE - 1
;

WITH
	input_events AS (
		-- filter for search events within the event stream and classify them
		SELECT
			sel.touch_id,
			es.event_tstamp,
			es.event_hash,
			sel.attributed_user_id,
			contexts_com_secretescapes_search_context_1 IS NOT NULL                                 AS has_search_context,
			contexts_com_secretescapes_filter_context_1 IS NOT NULL                                 AS has_filter_context,
			es.contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR           AS ubid,
			contexts_com_secretescapes_search_context_1,
			es.contexts_com_secretescapes_search_context_1[0]:change_filters::VARCHAR               AS search_change_filters,
			es.contexts_com_secretescapes_search_context_1[0]:had_results::VARCHAR                  AS search_had_results,
			es.contexts_com_secretescapes_search_context_1[0]:num_results::VARCHAR                  AS search_num_results,
			es.contexts_com_secretescapes_filter_context_1,
			es.contexts_com_secretescapes_filter_context_1[0]['options'][0]['code']::VARCHAR        AS filter_context_options_code,
			es.contexts_com_secretescapes_filter_context_1[0]['options'][0]['option_name']::VARCHAR AS filter_context_options_name,
			CASE
				WHEN filter_context_options_name IS DISTINCT FROM 'legacy'
					THEN 'vision' -- there are nulls in filter context options name, currently we call this 'vision'
				WHEN filter_context_options_name IS DISTINCT FROM 'legacy' AND search_change_filters = FALSE
					THEN 'vision'
				WHEN filter_context_options_name = 'legacy' THEN 'legacy'
			END                                                                                     AS search_type
		FROM se.data_pii.scv_event_stream es
			INNER JOIN se.data_pii.scv_session_events_link sel
					   ON sel.event_hash = es.event_hash AND sel.event_tstamp::DATE = CURRENT_DATE - 1
		WHERE es.se_brand = 'Travelist'
		  AND es.event_tstamp::DATE = CURRENT_DATE - 1
	),
	booking_data AS (
		-- aggregate booking data up to session
		SELECT
			stt.touch_id,
			COUNT(DISTINCT stt.booking_id)       AS bookings,
			SUM(IFF(op.value = 'control', 1, 0)) AS control_search_bookings,
			SUM(IFF(op.value = 'test', 1, 0))    AS test_search_bookings
		FROM se.data.scv_touched_transactions stt
			INNER JOIN se.data.tb_booking tb ON stt.booking_id = tb.booking_id
			INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty op
					   ON tb.id = op.order_id AND op.name = 'abtest_search'
		GROUP BY 1
	),
	search_session_data AS (
		-- aggregate search data up to session
		SELECT
			touch_id,
			attributed_user_id,
			IFF(COUNT(DISTINCT search_type) > 1, 'both', ANY_VALUE(search_type)) AS session_search_type, --if there are two types of searches in a session we call this both
			COUNT(DISTINCT ie.ubid)                                              AS unique_ubids,
			COUNT(*)                                                             AS searches
		FROM input_events ie
		WHERE has_search_context OR has_filter_context
		GROUP BY 1, 2
	),
	model_data AS (
		SELECT
			ssd.touch_id,
			attributed_user_id,
			session_search_type,
			unique_ubids,
			searches,
			COALESCE(bookings, 0)                AS bookings,
			COALESCE(control_search_bookings, 0) AS control_search_bookings,
			COALESCE(test_search_bookings, 0)    AS test_search_bookings
		FROM search_session_data ssd
			LEFT JOIN booking_data bd ON ssd.touch_id = bd.touch_id
	)
-- SELECT
-- 	session_search_type,
-- 	SUM(unique_ubids)                  AS unique_ubids,
-- 	COUNT(DISTINCT attributed_user_id) AS users,
-- 	COUNT(DISTINCT touch_id)           AS sessions,
-- 	SUM(searches)                      AS searches,
-- 	SUM(bookings)                      AS bookings,
-- 	SUM(control_search_bookings)       AS control_search_bookings,
-- 	SUM(test_search_bookings)          AS test_search_bookings
-- FROM model_data md
-- GROUP BY 1
-- ;


SELECT
	session_search_type,
	ssd.touch_id,
	stt.booking_id,
	op.value AS back_end_search_type
FROM search_session_data ssd
	INNER JOIN se.data.scv_touched_transactions stt ON ssd.touch_id = stt.touch_id
	INNER JOIN se.data.tb_booking tb ON stt.booking_id = tb.booking_id
	LEFT JOIN  hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderproperty op
			   ON tb.id = op.order_id AND op.name = 'abtest_search'
;


-- SELECT
-- 	ie.touch_id,
-- 	ie.event_tstamp,
-- 	ie.event_hash,
-- 	ie.has_search_context,
-- 	ie.has_filter_context,
-- 	ie.ubid,
-- 	ie.contexts_com_secretescapes_search_context_1,
-- 	ie.search_change_filters,
-- 	ie.search_had_results,
-- 	ie.search_num_results,
-- 	ie.contexts_com_secretescapes_filter_context_1,
-- 	ie.filter_context_options_code,
-- 	ie.filter_context_options_name,
-- 	search_type
-- FROM input_events ie
-- -- 	INNER JOIN booking_data bd ON ie.touch_id = bd.touch_id
-- WHERE has_search_context

;


SELECT *
FROM se.data.scv_touched_transactions stt
WHERE booking_id = 'TB-22299118'
;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp >= CURRENT_DATE - 1
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1
  AND stba.touch_id = 'a88fd45c151dfc4b2cdec5f84613beb648a973ac97ed80f000de7b7bec208e27'
;


SELECT
	ssel.*,
	ses.*
FROM se.data_pii.scv_event_stream ses
	LEFT JOIN se.data_pii.scv_session_events_link ssel
			  ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp >= CURRENT_DATE - 1
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.unique_browser_id = '84420348-5a25-4553-9af9-f128340e0f5e'
;

------------------------------------------------------------------------------------------------------------------------


-- TB-22298641

SELECT *
FROM se.data.scv_touched_transactions stt
WHERE booking_id = 'TB-22298641'
;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp::DATE = '2023-11-26'
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2023-11-26'
WHERE stba.touch_start_tstamp::DATE = '2023-11-26'
  AND stba.touch_id = '6d4ccda0ff39df66f61439055a6b870ee2f7132a30bbd7b3479fd5682008e754'
;


SELECT
	ssel.*,
	ses.*
FROM se.data_pii.scv_event_stream ses
	LEFT JOIN se.data_pii.scv_session_events_link ssel
			  ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp >= CURRENT_DATE - 1
WHERE ses.event_tstamp >= CURRENT_DATE - 1 AND
	  (ses.unique_browser_id = '5a50f755-5b3d-45ad-8052-8199bb97ecb7'
		  OR tvl_user_id IN (4797314, 6005098))

;


SELECT
	ses.event_tstamp,
	ses.tvl_user_id,
	ses.device_platform,
	ssel.touch_id,
	ssel.stitched_identity_type,
	ssel.attributed_user_id,
	ses.event_name,
	ses.contexts_com_secretescapes_search_context_1
FROM se.data_pii.scv_event_stream ses
	LEFT JOIN se.data_pii.scv_session_events_link ssel
			  ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp >= CURRENT_DATE - 1
WHERE ses.event_tstamp >= CURRENT_DATE - 1 AND
	  (ses.unique_browser_id = '5a50f755-5b3d-45ad-8052-8199bb97ecb7'
		  OR tvl_user_id IN (4797314, 6005098))

;


------------------------------------------------------------------------------------------------------------------------


-- TB-22298732

SELECT *
FROM se.data.scv_touched_transactions stt
WHERE booking_id = 'TB-22299091'
;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON stba.touch_id = ssel.touch_id AND ssel.event_tstamp::DATE = '2023-11-26'
	INNER JOIN se.data_pii.scv_event_stream ses
			   ON ssel.event_hash = ses.event_hash AND ses.event_tstamp::DATE = '2023-11-26'
WHERE stba.touch_start_tstamp::DATE = '2023-11-26'
  AND stba.touch_id = '59c9adec01be6e2c04c5e6681865cfb0d0a43891bce3ae23fbbc7a9c8c33d5a7'
;


SELECT
	ssel.*,
	ses.*
FROM se.data_pii.scv_event_stream ses
	LEFT JOIN se.data_pii.scv_session_events_link ssel
			  ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp >= CURRENT_DATE - 1
WHERE ses.event_tstamp ::DATE = '2023-11-26'
  AND ses.unique_browser_id = 'bd2531c9-c318-480a-acbf-b8b6f33f027f'

;

USE WAREHOUSE pipe_large
;

SELECT
	ses.event_tstamp,
	ses.tvl_user_id,
	ses.device_platform,
	ssel.touch_id,
	ssel.stitched_identity_type,
	ssel.attributed_user_id,
	ses.event_name,
	ses.page_url,
	PARSE_URL(page_url)['parameters']['utm_medium']::VARCHAR                                 AS utm_medium,
	PARSE_URL(page_url)['parameters']['utm_source']::VARCHAR                                 AS utm_source,
	PARSE_URL(page_url)['parameters']['utm_campaign']::VARCHAR                               AS utm_campaign,
	PARSE_URL(page_url)['parameters']['utm_term']::VARCHAR                                   AS utm_term,
	PARSE_URL(page_url)['parameters']['utm_content']::VARCHAR                                AS utm_content,
	ses.contexts_com_secretescapes_search_context_1,
	ses.contexts_com_secretescapes_filter_context_1,
	ses.contexts_com_secretescapes_filter_context_1[0]['options'][0]['code']::VARCHAR        AS filter_context_options_code,
	ses.contexts_com_secretescapes_search_context_1[0]:change_filters::VARCHAR               AS search_change_filters,
	ses.contexts_com_secretescapes_filter_context_1[0]['options'][0]['option_name']::VARCHAR AS filter_context_options_name,
	CASE
		WHEN filter_context_options_name IS DISTINCT FROM 'legacy'
			THEN 'vision' -- there are nulls in filter context options name, currently we call this 'vision'
		WHEN filter_context_options_name IS DISTINCT FROM 'legacy' AND search_change_filters = FALSE
			THEN 'vision'
		WHEN filter_context_options_name = 'legacy' THEN 'legacy'
	END                                                                                      AS search_type
FROM se.data_pii.scv_event_stream ses
	INNER JOIN se.data_pii.scv_session_events_link ssel
			   ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp::DATE >= '2023-11-01'
WHERE ses.event_tstamp::DATE >= '2023-11-01'
  AND ses.unique_browser_id = '532cec62-51d3-4f74-9369-07b2cf3d1136'
;;




