SELECT
    unstruct_event_com_iterable_system_webhook_1,
    unstruct_event_com_iterable_system_webhook_1['userId']::VARCHAR,
    unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'],
    SHA2(COALESCE(unstruct_event_com_iterable_system_webhook_1['userId'], '') ||
         COALESCE(unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'], '')) AS session_user_id,

    *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE = CURRENT_DATE
  AND event_vendor = 'com.iterable';


USE WAREHOUSE pipe_xlarge;

-- check missing user id
SELECT
    unstruct_event_com_iterable_system_webhook_1,
    unstruct_event_com_iterable_system_webhook_1['userId']::VARCHAR,
    unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'],
    SHA2(COALESCE(unstruct_event_com_iterable_system_webhook_1['userId']::VARCHAR, '') ||
         COALESCE(unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'], '')) AS session_user_id,

    *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE >= '2023-01-01'
  AND unstruct_event_com_iterable_system_webhook_1 IS NOT NULL
  AND unstruct_event_com_iterable_system_webhook_1['userId']::VARCHAR IS NULL;
-- 12 missing, and one as close as 1 day ago.

-- checking missing campaign id
SELECT
    unstruct_event_com_iterable_system_webhook_1,
    unstruct_event_com_iterable_system_webhook_1['userId']::VARCHAR,
    unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'],
    SHA2(COALESCE(unstruct_event_com_iterable_system_webhook_1['userId'], '') ||
         COALESCE(unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'], '') ||
         COALESCE(e.collector_tstamp::VARCHAR, '')
        ) AS session_user_id,

    *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE >= '2023-01-01'
  AND unstruct_event_com_iterable_system_webhook_1 IS NOT NULL
  AND unstruct_event_com_iterable_system_webhook_1['dataFields']['campaignId'] IS NULL;

-- no missing campaign id


------------------------------------------------------------------------------------------------------------------------
-- checking presence of multiple known identifiers on one event
SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses
               ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= CURRENT_DATE - 1
WHERE ssel.event_tstamp >= CURRENT_DATE - 1


------------------------------------------------------------------------------------------------------------------------
-- checking channelling information

SELECT unstruct_event_com_iterable_system_webhook_1
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::date = CURRENT_DATE  AND event_vendor = 'com.iterable';


