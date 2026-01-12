-- We are looking at two types of pages, a filter page and a search query page (examples below).
--
-- Filter page: https://www.secretescapes.com/great-british-breaks/filter
-- Search query page: https://www.secretescapes.com/search/search?query=Maldives
--
-- What we are looking for is to be able to track a user's activity (sessions, SPVs, bookings, avg time of site etc)
-- after they have landed on one of the page types above for a specific time period (7 days, 30 days etc);

USE WAREHOUSE pipe_xlarge;

SELECT ses.event_tstamp,
       ses.page_url,
       ssel.touch_id,
       ssel.attributed_user_id
FROM se.data_pii.scv_event_stream ses
         INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash
WHERE (

    -- Filter page: https://www.secretescapes.com/great-british-breaks/filter
        ses.page_urlpath = '/great-british-breaks/filter'
        OR
        -- Search query page: https://www.secretescapes.com/search/search?query=Maldives
        LOWER(ses.page_urlquery) LIKE '%query=maldives%'
    )
  AND ses.event_name = 'page_view'
  AND ssel.stitched_identity_type = 'se_user_id'
  AND ses.event_tstamp >= CURRENT_DATE - 7 --TODO adjust as necessary
;


