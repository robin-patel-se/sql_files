SELECT e.contexts_com_secretescapes_all_pages_session_login_type_context_1[0]:session_login_type::VARCHAR AS login_type,
       COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1
GROUP BY 1;

SELECT login_type, COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE event_tstamp >= CURRENT_DATE - 1
GROUP BY 1;

SELECT COUNT(*)
FROM se.data.se_user_attributes sua
WHERE sua.signup_tstamp >= CURRENT_DATE - 1

