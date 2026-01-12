-- investigate interactions on app
-- https://docs.google.com/spreadsheets/d/1m3FUHdEZ9_-I7fO3OUFB6luYxtntREk533gFEEC5tSQ/edit#gid=1683184390

SELECT
	ses.event_hash,
	ses.posa_territory,
	ses.event_tstamp,
	ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR                               AS screen_name,
	ses.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR                    AS snowplow_session_id,
	ses.contexts_com_secretescapes_content_element_interaction_context_1                                     AS interaction_context,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
	ses.contexts_com_secretescapes_content_element_interaction_context_1[0]['interaction_type']::VARCHAR     AS interaction_type,
	ses.*
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.v_tracker LIKE ANY ('andr%', 'ios%')
  AND ses.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
  AND interaction_type = 'click'