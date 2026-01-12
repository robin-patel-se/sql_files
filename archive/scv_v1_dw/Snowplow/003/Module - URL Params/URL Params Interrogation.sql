------------------------------------------------------------------------------------------------------------------------
--Unique URLS from subset of 380,183,377 events

--Bets: Carmen 1M , Robin 30M
USE WAREHOUSE PIPE_LARGE;
USE SCHEMA SCRATCH.ROBINPATEL;


SELECT COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE PAGE_URL IS NOT NULL
; -- 367908541 events with pageviews


SELECT COUNT(*)
FROM (
         SELECT PAGE_URL
         from SNOWPLOW_SAMPLE_EVENTS_UID
         WHERE PAGE_URL IS NOT NULL
         GROUP BY 1)
; -- 22,412,766 unique urls

CREATE OR REPLACE TABLE SNOWPLOW_ATOMIC_EVENTS_URLS AS (
    SELECT URL
    FROM (SELECT PAGE_URL AS URL
          from SNOWPLOW_SAMPLE_EVENTS_UID
          WHERE PAGE_URL IS NOT NULL

          UNION

          SELECT PAGE_REFERRER AS URL
          from SNOWPLOW_SAMPLE_EVENTS_UID
          WHERE PAGE_URL IS NOT NULL
         )
    GROUP BY 1
);-- Combine page and referrer url and create distinct list.

SELECT COUNT(*)
FROM SNOWPLOW_ATOMIC_EVENTS_URLS; -- 24549550 Distinct urls

SELECT SPLIT(PARSE_URL(URL, 1)['query']::varchar, '&')
FROM SNOWPLOW_ATOMIC_EVENTS_URLS
WHERE URL =
      'https://it.secretescapes.com/indimenticabile-tour-della-turchia-istanbul-ankara-e-cappadocia-italia/sale?jl_uid=64043298&jl_cmpn=1000672&utm_source=newsletter&utm_content=segment_core_it_act_libero_group3&utm_campaign=1000672&noPasswordSignIn=true&utm_medium=email&clickid=32YVsPTTtxyJRirwUx0Mo3EzUknzjayWizH%3AUc0&irgwc=1&utm_medium=affiliateprogramme&utm_source=impactit&utm_campaign=YieldKit%20GmbH&utm_content=Online%20Tracking%20Link';

SELECT *
FROM SNOWPLOW_ATOMIC_EVENTS_URLS,
     LATERAL FLATTEN(INPUT => SPLIT(PARSE_URL(URL, 1)['query']::varchar, '&'), outer => true) params;

CREATE OR REPLACE TABLE SNOWPLOW_ATOMIC_EVENTS_URL_PARAMS AS (
    SELECT URL,
--        params.VALUE::varchar,
           params.INDEX::varchar                                                     AS utm_index,
           LEFT(REGEXP_SUBSTR(params.VALUE::varchar, '.*=', 1, 1, 'e'),
                LENGTH(REGEXP_SUBSTR(params.VALUE::varchar, '.*=', 1, 1, 'e')) - 1)  as parameter,
           RIGHT(REGEXP_SUBSTR(params.VALUE::varchar, '=.*', 1, 1, 'e'),
                 LENGTH(REGEXP_SUBSTR(params.VALUE::varchar, '=.*', 1, 1, 'e')) - 1) as parameter_value
    FROM SNOWPLOW_ATOMIC_EVENTS_URLS,
         LATERAL FLATTEN(INPUT => SPLIT(PARSE_URL(URL, 1)['query']::varchar, '&'), outer => true) params
);

-- SELECT
--        *
-- FROM SNOWPLOW_ATOMIC_EVENTS_URLS e,
--      LATERAL FLATTEN(INPUT => PARSE_URL(URL, 1)['parameters'], outer => true) params;


SELECT e.*,
--        PARSE_URL(URL, 1)['parameters'], --extract parameters from url into array
       params.KEY::VARCHAR   AS PARAMETER,
       params.VALUE::VARCHAR AS PARAMETER_VALUE
FROM SNOWPLOW_ATOMIC_EVENTS_URLS e,
     LATERAL FLATTEN(INPUT => PARSE_URL(URL, 1)['parameters'], outer => true) params
;



WITH params AS (
    SELECT e.*,
--        PARSE_URL(URL, 1)['parameters'], --extract parameters from url into array
           params.KEY::VARCHAR   AS PARAMETER,
           params.VALUE::VARCHAR AS PARAMETER_VALUE
    FROM SNOWPLOW_ATOMIC_EVENTS_URLS e,
         LATERAL FLATTEN(INPUT => PARSE_URL(URL, 1)['parameters'], outer => true) params
);

------------------------------------------------------------------------------------------------------------------------
--dupe utm params
SELECT *
FROM SAT_URL_PARAMS
WHERE URL =
      'https://www.secretescapes.com/tranquil-jersey-waterside-break-with-car-hire-radisson-blu-waterfront-hotel-channel-islands2/sale?utm_source=SE_media&utm_medium=Jersey&utm_campaign=September2019&noPasswordSignIn=true&utm_medium=email&utm_source=media-campaign&utm_campaign=media_uk_20191003_jersey&utm_content=987069#travelDetails-content'

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM SAT_URL_PARAMS;


SELECT u.url
FROM SAT_UNIQUE_URLS u
         LEFT JOIN SAT_URL_PARAMS params;

--what common params do we have?
SELECT parameter,
       COUNT(*)
FROM SAT_URL_PARAMS
GROUP BY 1
ORDER BY 2 DESC;

------------------------------------------------------------------------------------------------------------------------


SELECT REFR_SOURCE,
       REFR_MEDIUM,
       REFR_URLHOST,
       REFR_TERM,
       REFR_URLPATH,
       REFR_URLFRAGMENT,

       PAGE_REFERRER
FROm SNOWPLOW_SAMPLE_EVENTS
WHERE REFR_MEDIUM = 'email';

SELECT REFR_MEDIUM, COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS
GROUP BY 1
ORDER BY 2 DESC;


