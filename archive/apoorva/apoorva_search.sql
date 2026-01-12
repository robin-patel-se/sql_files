WITH t AS (
    WITH location_table AS (
        -- Location taxonomy
        SELECT DISTINCT
               upper(sct.name) AS continent_name
             , upper(syt.name) AS country_name
             , upper(sdt.name) AS division_name
             , upper(sit.name) AS city_name
        FROM se.data.se_location_info sli
                 JOIN se.data.se_continent_translation sct
                      ON sli.continent_id = sct.continent_id
                 JOIN se.data.se_country_translation syt
                      ON sli.country_id = syt.country_id
                 JOIN se.data.se_country_division_translation sdt
                      ON sli.division_id = sdt.division_id
                 JOIN se.data.se_city_translation sit
                      ON sli.city_id = sit.city_id
        WHERE sct.locale = 'de_DE'
          AND syt.locale = 'de_DE'
          AND sdt.locale = 'de_DE'
          AND sit.locale = 'de_DE'
    ),
         search_table AS (
             -- search counts by search term
             SELECT trim(upper(contexts_com_secretescapes_search_context_1[0]['location'])) AS search_location,
                    COUNT(se_category)                                                      AS search_count
             FROM snowplow.atomic.events
             WHERE collector_tstamp >= to_date('2020.07.29', 'YYYY.MM.DD')
               AND se_category LIKE '%search event%'
               AND contexts_com_secretescapes_search_context_1 IS NOT NULL
               AND search_location IS NOT NULL
               AND br_name NOT LIKE 'Robot/Spider'
               AND useragent NOT REGEXP
                   '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
               AND app_id = 'DE'
             GROUP BY search_location
         )
         -- city searches
    SELECT l.continent_name
         , l.country_name
         , l.division_name
         , l.city_name         AS city_name
         , l.city_name         AS search_location
         , sum(s.search_count) AS search_count
    FROM location_table l
             JOIN search_table s
                  ON s.search_location LIKE '%' || l.city_name || '%'
    GROUP BY 1, 2, 3, 4, 5
    UNION ALL
    -- division searches
    SELECT l.continent_name
         , l.country_name
         , l.division_name
         , NULL                 AS city_name
         , l.division_name      AS search_location
         , sum(s1.search_count) AS search_count
    FROM location_table l
             JOIN search_table s1
                  ON s1.search_location LIKE '%' || l.division_name || '%'
             LEFT JOIN search_table s2
                       ON s2.search_location LIKE '%' || l.city_name || '%'
    WHERE s2.search_count IS NULL
    GROUP BY 1, 2, 3, 4, 5
    UNION ALL
    -- country searches
    SELECT l.continent_name
         , l.country_name
         , NULL                 AS division_name
         , NULL                 AS city_name
         , l.country_name       AS search_location
         , sum(s1.search_count) AS search_count
    FROM location_table l
             JOIN search_table s1
                  ON s1.search_location LIKE '%' || l.country_name || '%'
             LEFT JOIN search_table s2
                       ON s2.search_location LIKE '%' || l.division_name || '%'
             LEFT JOIN search_table s3
                       ON s3.search_location LIKE '%' || l.city_name || '%'
    WHERE s2.search_count IS NULL
      AND s3.search_count IS NULL
    GROUP BY 1, 2, 3, 4, 5
    UNION ALL
    -- continent searches
    SELECT l.continent_name
         , NULL                 AS country_name
         , NULL                 AS division_name
         , NULL                 AS city_name
         , l.continent_name     AS search_location
         , sum(s1.search_count) AS search_count
    FROM location_table l
             JOIN search_table s1
                  ON s1.search_location LIKE '%' || l.continent_name || '%'
             LEFT JOIN search_table s2
                       ON s2.search_location LIKE '%' || l.country_name || '%'
             LEFT JOIN search_table s3
                       ON s3.search_location LIKE '%' || l.division_name || '%'
             LEFT JOIN search_table s4
                       ON s4.search_location LIKE '%' || l.city_name || '%'
    WHERE s2.search_count IS NULL
      AND s3.search_count IS NULL
      AND s4.search_count IS NULL
    GROUP BY 1, 2, 3, 4, 5
    UNION ALL
    -- remaining searches
    SELECT NULL           AS continent_name
         , NULL           AS country_name
         , NULL           AS division_name
         , NULL           AS city_name
         , s.search_location
         , s.search_count AS search_count
    FROM search_table s
             LEFT JOIN location_table l1
                       ON s.search_location LIKE '%' || l1.continent_name || '%'
             LEFT JOIN location_table l2
                       ON s.search_location LIKE '%' || l2.country_name || '%'
             LEFT JOIN location_table l3
                       ON s.search_location LIKE '%' || l3.division_name || '%'
             LEFT JOIN location_table l4
                       ON s.search_location LIKE '%' || l4.city_name || '%'
    WHERE l1.continent_name IS NULL
      AND l2.country_name IS NULL
      AND l3.division_name IS NULL
      AND l4.city_name IS NULL
)
SELECT t.continent_name
     , t.country_name
     , LISTAGG(t.division_name, ', ')
     , LISTAGG(t.city_name, ', ')
     , t.search_location
     , t.search_count
FROM t
GROUP BY 1, 2, 5, 6
ORDER BY 6 DESC;

USE WAREHOUSE pipe_xlarge;

SELECT e.event_name,
       contexts_com_secretescapes_search_context_1,
       contexts_com_secretescapes_search_context_1[0]['check_in_date']::VARCHAR                 AS check_in_date,
       contexts_com_secretescapes_search_context_1[0]['check_out_date']::VARCHAR                AS check_out_date,
       contexts_com_secretescapes_search_context_1[0]['had_results']::BOOLEAN                   AS had_results,
       contexts_com_secretescapes_search_context_1[0]['location']::VARCHAR                      AS location,
       contexts_com_secretescapes_search_context_1[0]['location_search']::BOOLEAN               AS location_search,
       contexts_com_secretescapes_search_context_1[0]['months']::VARCHAR                        AS months,
       contexts_com_secretescapes_search_context_1[0]['months_search']::BOOLEAN                 AS months_search,
       contexts_com_secretescapes_search_context_1[0]['num_results']::VARCHAR                   AS num_results,
       contexts_com_secretescapes_search_context_1[0]['refine_by_travel_types_search']::BOOLEAN AS refine_by_travel_types_search,
       contexts_com_secretescapes_search_context_1[0]['refine_by_trip_types_search']::BOOLEAN   AS refine_by_trip_types_search,
       contexts_com_secretescapes_search_context_1[0]['travel_types']::VARCHAR                  AS travel_types,
       contexts_com_secretescapes_search_context_1[0]['trip_types']::VARCHAR                    AS trip_types
FROM snowplow.atomic.events e
WHERE se_category LIKE '%search event%'
  --remove bots
  AND br_name NOT LIKE 'Robot/Spider'
  AND useragent NOT REGEXP
      '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
  AND e.etl_tstamp >= current_date - 1;


SELECT e.collector_tstamp::DATE                                                      AS date,
       count(DISTINCT e.domain_sessionid)                                            AS all_sessions,
       count(DISTINCT IFF(e.se_category = 'search event', e.domain_sessionid, NULL)) AS search_sessions,
       search_sessions / NULLIF(all_sessions, 0)                                     AS search_sess_perc
FROM snowplow.atomic.events e
WHERE
  --remove bots
    br_name NOT LIKE 'Robot/Spider'
  AND e.app_id NOT LIKE 'app_id%'
  AND useragent NOT REGEXP
      '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
  AND e.etl_tstamp >= '2020-01-01'
GROUP BY 1;

SELECT e.collector_tstamp::DATE                                                      AS date,
       REGEXP_REPLACE(REGEXP_REPLACE(app_id, 'ios_app '), 'android_app ')            AS territory,
       count(DISTINCT e.domain_sessionid)                                            AS all_sessions,
       count(DISTINCT IFF(e.se_category = 'search event', e.domain_sessionid, NULL)) AS search_sessions,
       search_sessions / NULLIF(all_sessions, 0)
FROM snowplow.atomic.events e
WHERE
  --remove bots
    br_name NOT LIKE 'Robot/Spider'
  AND e.app_id NOT LIKE 'app_id%'
  AND useragent NOT REGEXP
      '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
  AND e.etl_tstamp >= '2020-01-01'
  --remove ss events
  AND v_tracker NOT LIKE 'py-%'   --TB ss events
  AND v_tracker NOT LIKE 'java-%' --SE core ss events
GROUP BY 1, 2;



SELECT COUNT(*)
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= current_date - 10
  AND e.se_category LIKE 'search event';