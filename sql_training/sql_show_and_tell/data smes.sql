SELECT sts.search_context
FROM se.data.scv_touched_searches sts;

SELECT ses.contexts_com_secretescapes_sale_page_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.contexts_com_secretescapes_sale_page_context_1 IS NOT NULL
  AND ses.event_tstamp::DATE = CURRENT_DATE - 1;


{
  "check_in_date": "2021-08-30",
  "check_out_date": "2021-09-08",
  "flexible_search": FALSE,
  "had_results": TRUE,
  "location": "",
  "location_search": FALSE,
  "months": [],
  "months_search": FALSE,
  "num_results": 152,
  "refine_by_travel_type_search": FALSE,
  "refine_by_trip_type_search": TRUE,
  "specific_dates_search": TRUE,
  "travel_types": [],
  "trip_types": [
    "zz_allinclusive",
    "beach"
  ],
  "weekend_only_search": FALSE
}
------------------------------------------------------------------------------------------------------------------------
SELECT sts.search_context,
       sts.trip_types,
       sts.search_context:trip_types,
       tipe_types.value::VARCHAR AS trip_type,
       tipe_types.index
FROM se.data.scv_touched_searches sts,
     LATERAL FLATTEN(INPUT => search_context:trip_types, OUTER => TRUE) tipe_types,
     LATERAL FLATTEN(INPUT => search_context: MONTHS, OUTER => TRUE) months_ex -- if you wanted to expand out
WHERE sts.event_hash = '9453335551ad72e24425f2ccaad8c76af4bd639809c067c504e5dcba0c020ac2';

WITH json_e AS (
    SELECT
--plug your own json here
PARSE_JSON('{
  "check_in_date": "2021-08-30",
  "check_out_date": "2021-09-08",
  "flexible_search": false,
  "had_results": true,
  "location": "",
  "location_search": false,
  "months": [],
  "months_search": false,
  "num_results": 152,
  "refine_by_travel_type_search": false,
  "refine_by_trip_type_search": true,
  "specific_dates_search": true,
  "travel_types": [],
  "trip_types": [
    "zz_allinclusive",
    "beach",
    "robins cool tag"
  ],
  "weekend_only_search": false
}') AS example_json
)
SELECT json_e.example_json,
       element_sids.value AS trip_type,
       element_sids.index
FROM json_e,
     LATERAL FLATTEN(INPUT => example_json:trip_types, OUTER => TRUE) element_sids;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_2xlarge;

--sessions that started as a result of an spv
--session touch ids are actually the event hash of the first event within the session, we can exploit this by checking event hashes in touched
--spvs table by checking if the session id = event hash.
SELECT DISTINCT
       sts.touch_id
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1 --TODO remove
  AND sts.touch_id = sts.event_hash;


--lifted 10 example sessions that started with an spv
--lowered the grain by filtering session link table for that session id, this will return all events associated to a session
--filtering for only page view events to ensure we don't show second events that weren't page views
--created a transient table to aid quicker downstream modelling
CREATE TRANSIENT TABLE scratch.robinpatel.examplesessions AS (
    SELECT ses.event_tstamp,
           ses.event_hash,
           ssel.touch_id,
           ses.page_url,
           ses.page_urlpath
    FROM se.data_pii.scv_session_events_link ssel
        INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
    WHERE ssel.touch_id IN (
                            '2ac898c4086a67b7ce31e4f4ebcdf68f0f26c1ff9fc2e5c6351b1b9571fc20d8',
                            '6d71d283677b92b253d4cc6215a6c03190bd11d0235999d6f50f61ee566b65bf',
                            '8eaa513393e7d96a2f9d24ed65b79abe2f30fac188b903398d850b3022782392',
                            '44096c05c80a888e8b093dff62c3482dbe145440baff8b751036df3e0f41fadc',
                            '5861f8785b88caa832fe0c08afd235d154b0aef39cee92adb6c77022f42dc41b',
                            '0cc1e88c218656af2a59c11e4578ff816ddbd8966960c8a244fdfb110e95141c',
                            '27c730b2c0e388a059eec153f479580060a4a1def867d44a41d8b6409b50000c',
                            '54b539ea731cfc19d628722cca5a9d51b181d9ed5454f9c331e01a3c9b5f740c',
                            '88f69a4dba137503cf05905bc180153da130889a6d20a6952b641367297ff183',
                            '229f5eb475172bc06a05f9fb4b9deea152e7e01d21a8664c6f4dcfdbcb023745'
        )
      AND ssel.event_tstamp::DATE = CURRENT_DATE - 1
      AND ses.event_name = 'page_view'
);


--added index to identify what was the second event
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY es.touch_id ORDER BY es.event_tstamp) AS index
FROM scratch.robinpatel.examplesessions es
    QUALIFY index = 2;


------------------------------------------------------------------------------------------------------------------------
--this can all then be squashed into
WITH input_sessions AS (
    --sessions that start with an spv
    SELECT DISTINCT
           sts.touch_id
    FROM se.data.scv_touched_spvs sts
    WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1 --TODO remove
      AND sts.touch_id = sts.event_hash --exploit
),
     lower_grain AS (
         --get all events for input sessions, then filter for only page views
         SELECT ses.event_tstamp,
                ses.event_hash,
                ssel.touch_id,
                ses.page_url,
                ses.page_urlpath
         FROM input_sessions ins
             INNER JOIN se.data_pii.scv_session_events_link ssel ON ins.touch_id = ssel.touch_id
             INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
         WHERE ssel.event_tstamp::DATE = CURRENT_DATE - 1
           AND ses.event_name = 'page_view' --remove non page view events
     )
--create index and filter on second event
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY lg.touch_id ORDER BY lg.event_tstamp) AS index
FROM lower_grain lg
    QUALIFY index = 2;
