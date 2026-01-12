USE WAREHOUSE pipe_xlarge;

SELECT REGEXP_SUBSTR(page_urlpath, '/magazine-(..)/', 1, 1, 'e')
                          AS territory,
       event_tstamp::DATE AS date,
       count(*)
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE event_name = 'page_view'
  AND page_url LIKE '%magazine%'
  AND event_tstamp::DATE >= '2020-03-24'
GROUP BY 1, 2;


SELECT se_user_id,
       app_id,
       event_tstamp::DATE AS date
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE event_name = 'page_view'
  AND page_url LIKE '%/magazine-%'
  AND event_tstamp::DATE >= '2020-03-01';



SELECT REGEXP_SUBSTR(page_urlpath, '/magazine-(..)/', 1, 1, 'e')
                              AS magazine_territory,
       collector_tstamp::DATE AS date,
       count(*)
FROM snowplow.atomic.events
WHERE event_name = 'page_view'
  AND page_url LIKE '%/magazine-%'
  AND collector_tstamp::DATE >= '2020-03-25'
GROUP BY 1, 2;


SELECT REGEXP_SUBSTR(page_urlpath, '/magazine-(..)/', 1, 1, 'e')
                                                                              AS magazine_territory,
       collector_tstamp::DATE                                                 AS date,
       sum(CASE WHEN user_id IS NOT NULL AND user_id != '""' THEN 1 ELSE 0 END) AS logged_in,
       sum(CASE WHEN user_id IS NULL OR user_id = '""' THEN 1 ELSE 0 END)       AS logged_out
FROM snowplow.atomic.events
WHERE event_name = 'page_view'
  AND page_url LIKE '%/magazine-%'
  AND collector_tstamp::DATE >= '2020-03-25'
GROUP BY 1, 2;

SELECT user_id,
       page_urlpath
FROM snowplow.atomic.events
WHERE event_name = 'page_view'
  AND page_url LIKE '%/magazine-%'
  AND collector_tstamp::DATE >= '2020-03-25'
GROUP BY 1, 2;