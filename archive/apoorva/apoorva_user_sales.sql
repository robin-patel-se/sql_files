SELECT DISTINCT
       e.se_category,
       e.se_action,
       se_label
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= '2021-01-20'
  AND e.se_category = 'content viewed';


SELECT DISTINCT
       e.se_category,
       e.se_action,
       se_label
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= '2021-01-20'
  AND e.se_category = 'content viewed'
  AND e.se_action IN ('collection results panel', 'homepage panel', 'search results panel');


SELECT *
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= '2021-01-20'
  AND e.se_category = 'content viewed'
  AND e.se_action IN ('collection results panel', 'homepage panel', 'search results panel')
  AND e.se_label IN ('collection', 'current sales', 'search results');

USE WAREHOUSE pipe_xlarge;

SELECT e.user_id,
       e.se_category,
       e.se_action,
       e.se_label,
       e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['sale_id']::VARCHAR       AS sale_id,
       e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['list_position']::VARCHAR AS list_position
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= '2021-01-20'
  AND e.se_category = 'content viewed'
  AND e.se_action IN ('collection results panel', 'homepage panel', 'search results panel')
  AND e.se_label IN ('collection', 'current sales', 'search results')
  AND e.user_id IS NOT NULL;


WITH sale_card_views AS (
    SELECT e.user_id,
           e.se_category,
           e.se_action,
           e.se_label,
           e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['sale_id']::VARCHAR      AS sale_id,
           e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['list_position']::NUMBER AS list_position
    FROM snowplow.atomic.events e
    WHERE e.etl_tstamp >= '2021-01-20'
      AND e.se_category = 'content viewed'
--       AND e.se_action IN ('collection results panel', 'homepage panel', 'search results panel')
      AND e.se_action = 'homepage panel'
--       AND e.se_label IN ('collection', 'current sales', 'search results')
      AND e.se_label = 'current sales'
      AND e.user_id IS NOT NULL
)
SELECT sc.user_id,
       COUNT(DISTINCT sc.sale_id) AS no_of_sales,
       COUNT(*)                   AS content_viewed_events,
       MAX(sc.list_position)      AS max_list_position
FROM sale_card_views sc
GROUP BY 1
;



SELECT e.user_id,
       e.se_category,
       e.se_action,
       e.se_label,
       e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['sale_id']::VARCHAR       AS sale_id,
       e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['list_position']::VARCHAR AS list_position,
       *
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= '2021-01-20'
  AND e.se_category = 'content viewed'
--       AND e.se_action IN ('collection results panel', 'homepage panel', 'search results panel')
  AND e.se_action = 'homepage panel'
--       AND e.se_label IN ('collection', 'current sales', 'search results')
  AND e.se_label = 'current sales'
  AND e.user_id = 26551027


WITH sale_card_views AS (
    --event level query
    SELECT e.user_id,
           e.se_category,
           e.se_action,
           e.se_label,
           e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['sale_id']::VARCHAR      AS sale_id,
           e.contexts_com_secretescapes_content_element_viewed_context_1[0]['sales'][0]['list_position']::NUMBER AS list_position
    FROM snowplow.atomic.events e
    WHERE e.etl_tstamp >= '2021-01-01'
      AND e.se_category = 'content viewed'
      AND e.se_action IN ('collection results panel', 'homepage panel')
--       AND e.se_action = 'homepage panel'
      AND e.se_label IN ('collection', 'current sales')
--       AND e.se_label = 'current sales'
      AND e.user_id IS NOT NULL
      AND br_family IS DISTINCT FROM 'Robot/Spider'
      AND useragent NOT REGEXP
          '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
),
     user_counts AS (
         --user level query, aggregated events to user level
         SELECT sc.user_id,
                COUNT(DISTINCT sc.sale_id) AS no_of_sales,
                COUNT(*)                   AS content_viewed_events,
                MAX(sc.list_position)      AS max_list_position
         FROM sale_card_views sc
         GROUP BY 1
     )

--aggregate max list position level
SELECT uc.max_list_position,
       COUNT(DISTINCT uc.user_id) AS users
FROM user_counts uc
GROUP BY 1
;

