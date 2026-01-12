SELECT COUNT(*)
FROM se.data_pii.scv_event_stream ses
-- WHERE ses.event_tstamp::DATE = '2021-04-23'
WHERE ses.se_action = 'sign up';


self_describing_task --include 'dv/dwh/events/03_touchification/01_touchifiable_events.py'  --method 'run' --start '2021-04-29 00:00:00' --end '2021-04-29 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;


SELECT COUNT(*)
--        e.event_hash,
--        e.event_tstamp,
--        e.derived_tstamp,
--        e.event_name,
--        e.page_url,
--        e.page_referrer,
--        e.device_platform,
--        e.unique_browser_id,
--        e.cookie_id,
--        e.session_userid
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE event_name IN ('page_view', 'screen_view', 'transaction_item', 'transaction',
                     'booking_update_event')                                   -- explicitly define the events we want to touchify
  AND COALESCE(e.unique_browser_id, e.cookie_id, e.session_userid) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
  AND e.is_robot_spider_event = FALSE -- remove extra computation required to resessionise robot events
;
--2,149,940,422


SELECT COUNT(*)
--        e.event_hash,
--        e.event_tstamp,
--        e.derived_tstamp,
--        e.event_name,
--        e.page_url,
--        e.page_referrer,
--        e.device_platform,
--        e.unique_browser_id,
--        e.cookie_id,
--        e.session_userid
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE COALESCE(e.unique_browser_id, e.cookie_id, e.session_userid) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
  AND e.is_robot_spider_event = FALSE                                          -- remove extra computation required to resessionise robot events
  AND (e.event_name IN ('page_view', 'screen_view', 'transaction_item', 'transaction',
                        'booking_update_event') -- explicitly define the events we want to touchify

    OR e.se_action = 'sign up' -- sign up events
    OR e.unstruct_event_com_secretescapes_searched_with_refinement_event_1 IS NOT NULL --search events

    );
--2,153,441,496 -- with just sign ups
-- --with sign ups and search events

USE WAREHOUSE pipe_xlarge;

SELECT e.dvce_type,
       e.contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR
FROM snowplow.atomic.events e
WHERE e.event_name = 'booking_update_event'
  AND e.collector_tstamp >= CURRENT_DATE - 1;


SELECT *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1
  AND e.unstruct_event_com_snowplowanalytics_snowplow_link_click_1 IS NOT NULL;



WITH events AS (
    SELECT unstruct_event_com_snowplowanalytics_snowplow_link_click_1,
           f.key,
           f.value
    FROM snowplow.atomic.events e,
         LATERAL FLATTEN(INPUT => e.unstruct_event_com_snowplowanalytics_snowplow_link_click_1) f
    WHERE e.collector_tstamp::DATE >= '2021-02-19 09:00:00'
      AND app_id = 'DE'
)
SELECT *
FROM events
WHERE events.key = 'targetUrl'
  AND events.value ILIKE '%ncwrd%';


SELECT unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::VARCHAR AS target_url,
       PARSE_URL(unstruct_event_com_snowplowanalytics_snowplow_link_click_1:targetUrl::VARCHAR, 1)parsed_url:path::VARCHAR                                                      AS parsed_url
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE >= '2021-02-19 09:00:00'
  AND e.unstruct_event_com_snowplowanalytics_snowplow_link_click_1 IS NOT NULL
AND parsed_url:path::VARCHAR LIKE '%ncwrd';