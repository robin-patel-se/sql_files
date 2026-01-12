USE WAREHOUSE pipe_xlarge;
ALTER SESSION SET QUERY_TAG = 'google login investigation';

-- login info based on session
SELECT
    ses.event_hash,
    ses.contexts_com_secretescapes_all_pages_session_login_type_context_1[0]['session_login_type']::VARCHAR,
    ssel.touch_id,
    stba.*
FROM se.data_pii.scv_event_stream ses
    INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON ssel.touch_id = stba.touch_id
WHERE ses.collector_tstamp >= CURRENT_DATE - 1 --TODO remove
  AND ses.contexts_com_secretescapes_all_pages_session_login_type_context_1[0]['session_login_type']::VARCHAR = 'GOOGLE_LOGIN';


WITH distinct_list_sessions_with_glogin AS (
    SELECT DISTINCT
        ssel.touch_id
    FROM se.data_pii.scv_event_stream ses
        INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
    WHERE ses.event_tstamp BETWEEN '2022-06-01' AND CURRENT_DATE
      AND ses.contexts_com_secretescapes_all_pages_session_login_type_context_1[0]['session_login_type']::VARCHAR = 'GOOGLE_LOGIN'
)
SELECT
    stba.touch_start_tstamp::DATE           AS date,
    COUNT(DISTINCT stba.touch_id)           AS sessions,
    SUM(IFF(stba.touch_logged_in, 1, 0))    AS logged_in_sessions,
    SUM(IFF(gl.touch_id IS NOT NULL, 1, 0)) AS google_login_sessions
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN distinct_list_sessions_with_glogin gl
              ON stba.touch_id = gl.touch_id
WHERE stba.touch_start_tstamp BETWEEN '2022-06-01' AND CURRENT_DATE
GROUP BY 1;


SELECT *
FROM se.data.se_sale_attributes_snapshot ssas;



