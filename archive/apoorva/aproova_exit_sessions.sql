--exit pages for sessions

WITH sessions_for_each_page AS (
    SELECT CASE
               WHEN es.page_urlpath LIKE '%/sale' OR es.page_urlpath LIKE '%/sale-%' THEN '/sale-page'
               WHEN es.page_urlpath REGEXP '\\\/\\\d.*' THEN '/sale-page-catalogue'
               ELSE es.page_urlpath
               END                     AS page_urlpath,
           count(DISTINCT mt.touch_id) AS sessions
    FROM hygiene_vault_mvp.snowplow.event_stream es
             LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
    WHERE es.event_tstamp >= current_date - 10
    GROUP BY 1
),
     session_exits AS (

         SELECT CASE
                    WHEN stba.touch_exit_pagepath LIKE '%/sale' OR stba.touch_exit_pagepath LIKE '%/sale-%' THEN '/sale-page'
                    WHEN stba.touch_exit_pagepath REGEXP '\\\/\\\d.*' THEN '/sale-page-catalogue'
                    ELSE stba.touch_exit_pagepath
                    END                     AS exit_page,
                count(*)                    AS sessions,
                avg(stba.touch_event_count) AS avg_events_in_touch
         FROM se.data.scv_touch_basic_attributes stba
         WHERE stba.touch_start_tstamp >= current_date - 10
         GROUP BY 1
     )
SELECT se.exit_page,
       se.sessions                           AS exit_sessions,
       sp.sessions                           AS sessions_include_path, --sessions that include path
       exit_sessions / sessions_include_path AS exit_perc,
       se.avg_events_in_touch
FROM session_exits se
         LEFT JOIN sessions_for_each_page sp ON se.exit_page = sp.page_urlpath
ORDER BY 2 DESC;
------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM se.data.scv_touched_spvs sts
WHERE sts.page_url REGEXP '.*(swp|swd).*';


SELECT *
FROM collab.product.credit_by_user cbu
         LEFT JOIN se.data.master_se_booking_list bl ON cbu.from_refunded_se_booking_id = bl.booking_id




;



