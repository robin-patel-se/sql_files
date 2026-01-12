SELECT *
FROM snowplow.atomic.events e;

SELECT ses.event_tstamp,
       ses.event_name
FROM se.data_pii.scv_event_stream ses;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.event_name IN ('page_view', 'screen_view')
  AND ses.is_robot_spider_event = FALSE;

SELECT *
FROM se.data_pii.scv_session_events_link ssel;

USE WAREHOUSE pipe_xlarge;

SELECT stba.touch_experience,
       ses.event_tstamp,
       ses.page_url
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN se.data_pii.scv_session_events_link ssel ON stba.touch_id = ssel.touch_id
         INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE stba.touch_id = '17e9d8918154e37c2ac7be8673d728b7217ed7f0aa8f463be671c8ad39826c4a';

------------------------------------------------------------------------------------------------------------------------
--events belonging to a session that have at least one event that meet a criteria
WITH sessions_meet_criteria AS (
    SELECT DISTINCT
           ssel.touch_id
    FROM se.data_pii.scv_event_stream ses
             INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
    WHERE ses.event_tstamp >= CURRENT_DATE - 1
      AND ses.page_urlpath LIKE '%search' --this is the criteria I care about (search page)
)
SELECT ses2.*
FROM se.data.scv_touch_basic_attributes stba
         INNER JOIN sessions_meet_criteria smc ON stba.touch_id = smc.touch_id
    -- change to event grain
         INNER JOIN se.data_pii.scv_session_events_link s ON smc.touch_id = s.touch_id
         INNER JOIN se.data_pii.scv_event_stream ses2 ON s.event_hash = ses2.event_hash;


------------------------------------------------------------------------------------------------------------------------

--counts of sessions that met a criteria in proportion to all sessions
WITH sessions_meet_criteria AS (
    SELECT DISTINCT
           ssel.touch_id
    FROM se.data_pii.scv_event_stream ses
             INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
    WHERE ses.event_tstamp >= CURRENT_DATE - 1
      AND ses.page_urlpath LIKE '%search' --this is the criteria I care about (search page)
)

SELECT COUNT(*)                                 AS all_sessions,
       SUM(IFF(smc.touch_id IS NOT NULL, 1, 0)) AS search_sessions,
       search_sessions / all_sessions
FROM se.data.scv_touch_basic_attributes stba
         LEFT JOIN sessions_meet_criteria smc ON stba.touch_id = smc.touch_id
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1;


--how to parse a url
SELECT PARSE_URL(
               'https://www.secretescapes.de/elegantes-5-star-hotel-im-kroatischen-rovinj-kostenfrei-stornierbar-grand-park-hotel-rovinj-leading-hotels-of-the-world-istrien-kroatien/sale-hotel?source=swp&checkin=2021-08-23&checkout=2021-08-30',
               1)                       AS parsed_url,

       parsed_url:host,
       parsed_url: PARAMETERS:affiliate AS check_in_date;


--parsing the event_stream page url
SELECT PARSE_URL(ses.page_url, 1):parameters:affiliate
FROM se.data_pii.scv_event_stream ses
WHERE ses.page_url IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1