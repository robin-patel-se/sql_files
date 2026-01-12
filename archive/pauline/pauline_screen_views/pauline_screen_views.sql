USE WAREHOUSE pipe_xlarge;

SELECT es.event_tstamp,
       es.device_platform,
       es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_name
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_name = 'screen_view'
AND es.event_tstamp >= '2020-12-03'

------------------------------------------------------------------------------------------------------------------------


SELECT e.contexts_com_secretescapes_content_element_interaction_context_1,
       e.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS section,
       e.contexts_com_secretescapes_content_element_interaction_context_1[0]['sale_id']::VARCHAR              AS sale_id,
       e.se_category,
       e.se_action,
       e.se_label
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= current_date - 1
  AND e.se_category IS DISTINCT FROM 'content rendered'
  AND e.se_label = 'click'
  AND (e.app_id LIKE 'ios%' OR e.app_id LIKE 'android%');

SELECT DISTINCT
       e.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS section

FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= current_date - 1
  AND e.se_category IS DISTINCT FROM 'content rendered'
  AND e.se_label = 'click'
  AND (e.app_id LIKE 'ios%' OR e.app_id LIKE 'android%');


SELECT distinct
       e.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS section
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= current_date - 90
  AND e.se_category IS DISTINCT FROM 'content rendered'
  AND e.se_label = 'click'
  AND section NOT LIKE 'filter%'
  AND (e.app_id LIKE 'ios%' OR e.app_id LIKE 'android%')
GROUP BY 1
ORDER BY 1;


SELECT e.etl_tstamp::date                                                                                     AS date,
       e.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS section,
       count(*)                                                                                               AS clicks
FROM snowplow.atomic.events e
WHERE e.etl_tstamp >= current_date - 90
  AND e.se_category IS DISTINCT FROM 'content rendered'
  AND e.se_label = 'click'
  AND (e.app_id LIKE 'ios%' OR e.app_id LIKE 'android%')
  AND section NOT LIKE 'filter%'
GROUP BY 1, 2
ORDER BY 1;