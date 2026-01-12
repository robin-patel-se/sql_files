USE WAREHOUSE pipe_large;

SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       e.is_server_side_event,                                         --remove
       e.se_user_id,--remove
       REGEXP_SUBSTR(e.page_url, '.*bookingId=(\\\\d*)') AS booking_id e.page_url,
       'page views'                                      AS event_category,
       'BFV'                                             AS event_subcategory,
       CURRENT_TIMESTAMP                                 AS updated_at --TODO: replace with '{schedule_tstamp}'

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e
                    ON e.event_hash = t.event_hash
         LEFT JOIN module_extracted_params p ON e.page_url = p.url AND p.from_app = 'true'
WHERE e.event_name = 'page_view'
  AND e.page_url LIKE '%/sale/book%'
  AND e.event_tstamp::DATE > '2020-04-10';


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE se_user_id = 69566901
  AND event_tstamp::DATE = '2020-04-12';


SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       e.is_server_side_event,         --remove
       e.se_user_id,--remove
       e.page_url,
       e.unstruct_event_com_secretescapes_booking_update_event_1,
       e.unstruct_event_com_secretescapes_booking_update_event_1['category'],
       'page views'      AS event_category,
       'BFV'             AS event_subcategory,
       CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e
                    ON e.event_hash = t.event_hash
         LEFT JOIN module_extracted_params p ON e.page_url = p.url AND p.from_app = 'true'
WHERE e.event_name = 'booking_update_event'
  AND e.se_user_id = 69566901
  AND e.event_tstamp::DATE = '2020-04-12';
------------------------------------------------------------------------------------------------------------------------


USE WAREHOUSE pipe_large;

SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       e.is_server_side_event,                                                --remove
       e.se_user_id,                                                          --remove
       --TODO investigate how these sale ids appear between new and old model
       REGEXP_SUBSTR(e.page_url, 'saleId=(\\d*)', 1, 1, 'e')    AS sale_id,
       --TODO investigate how these booking ids appear between new and old model
       REGEXP_SUBSTR(e.page_url, 'bookingId=(\\d*)', 1, 1, 'e') AS booking_id,
       e.page_url,
       'page views'                                             AS event_category,
       'BFV'                                                    AS event_subcategory,
       CURRENT_TIMESTAMP                                        AS updated_at --TODO: replace with '{schedule_tstamp}'

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e
                    ON e.event_hash = t.event_hash
         LEFT JOIN module_extracted_params p ON e.page_url = p.url AND p.from_app = 'true'
WHERE e.event_name = 'page_view'
  AND e.page_url LIKE '%/sale/book%'
  AND e.event_tstamp::DATE >= '2020-04-01'
  AND e.is_server_side_event = FALSE;
