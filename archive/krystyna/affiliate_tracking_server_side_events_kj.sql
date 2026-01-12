SELECT stmc.touch_mkt_channel,
       COUNT(*)
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
WHERE stmc.utm_medium = 'affiliateprogramme'
  AND stt.event_tstamp >= CURRENT_DATE - 1
GROUP BY 1;


USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.scv_touched_transactions stt
         INNER JOIN se.data.scv_touch_marketing_channel stmc ON stt.touch_id = stmc.touch_id
         INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE stmc.utm_medium = 'affiliateprogramme'
  AND stmc.touch_mkt_channel = 'Direct'
  AND stt.event_tstamp >= CURRENT_DATE - 1

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '51580440';

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_user_id = 51580440
  AND ses.event_tstamp >= '2021-03-25'
  AND ses.event_name IN ('page_view', 'screen_view', 'transaction_item', 'transaction', 'booking_update_event');

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.v_tracker LIKE 'java%'
  AND ses.event_name = 'booking_update_event';
SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.v_tracker LIKE 'py%'
  AND ses.event_name = 'booking_update_event';


SELECT *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1;


SELECT *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE = '2021-01-01'
  AND e.event_name = 'event'
  AND e.se_action = 'locationSearch=europa';

SELECT e.collector_tstamp::DATE,
       COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE >= '2021-01-01'
  AND e.contexts_com_secretescapes_search_context_1 IS NOT NULL
GROUP BY 1;



