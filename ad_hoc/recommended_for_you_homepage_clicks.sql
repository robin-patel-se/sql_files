-- people on homepage that click recommended for you -- g can do this
-- people who's landing page is homepage and then click recommended for you


SELECT
    tes.event_tstamp,
    tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
    tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
    tes.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR

FROM se.data_pii.trimmed_event_stream tes
WHERE tes.event_tstamp >= CURRENT_DATE - 1
  AND tes.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
  AND tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR = 'recommended for you'
  AND tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR = 'homepage panel'
;

SELECT
    tes.event_tstamp,
    tes.event_name,
    tes.event,
    tes.event_hash,
    tes.contexts_com_secretescapes_content_element_interaction_context_1,
    tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR     AS element_category,
    tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR AS element_sub_category,
    tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['sale_id']::VARCHAR              AS element_clicked_spv,
    tes.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR,
    tes.page_url,
    CASE
        WHEN RLIKE(tes.page_urlpath, '.*/current-sales.?', 'i') THEN 'UK'
        WHEN RLIKE(tes.page_urlpath, '.*/aktuelle-angebote.?', 'i') THEN 'DE'
        WHEN RLIKE(tes.page_urlpath, '.*/offerte-in-corso.?', 'i') THEN 'IT'
        WHEN RLIKE(tes.page_urlpath, '.*/aktuella-kampanjer.?', 'i') THEN 'SWEDEN'
        WHEN RLIKE(tes.page_urlpath, '.*/aanbiedingen.?', 'i') THEN 'NL_BE'
        WHEN RLIKE(tes.page_urlpath, '.*/nuvaerende-salg.?', 'i') THEN 'DK_CS' -- DK (current sales)
        WHEN RLIKE(tes.page_urlpath, '.*/aktuelle-tilbud.?', 'i') THEN 'DK_CO' -- DK (current offers)
        WHEN RLIKE(tes.page_urlpath, '.*/ventas-actuales.?', 'i') THEN 'ES_CS' -- ES (current sales)
        END                                                                                                  AS homepage
FROM se.data_pii.trimmed_event_stream tes
WHERE tes.event_tstamp >= CURRENT_DATE - 1
  AND tes.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = '1abc5168-fffc-4a29-889d-2071fe722dd8' -- todo remove
  AND (tes.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
    OR
       tes.event_name IN ('page_view'))
;

WITH interaction_aggregation AS (
    SELECT
        tes.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                             AS web_page_id,
        MAX(IFF(tes.event_name = 'page_view', tes.event_hash, NULL))                                                         AS event_hash,
        SUM(IFF(tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_category']::VARCHAR = 'recommended for you'
                    AND tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['element_sub_category']::VARCHAR = 'homepage panel'
            , 1, 0))                                                                                                         AS count_clicked_homepage_rfy,
        LISTAGG(DISTINCT tes.contexts_com_secretescapes_content_element_interaction_context_1[0]['sale_id']::VARCHAR, ' | ') AS sales_clicked
    FROM se.data_pii.trimmed_event_stream tes
    WHERE tes.event_tstamp >= CURRENT_DATE - 1 --TODO remove
      AND (tes.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
        OR tes.event_name IN ('page_view'))
    GROUP BY 1
),
     enrich_touch_id AS (
         SELECT
             ia.event_hash,
             ssel.touch_id,
             ia.count_clicked_homepage_rfy,
             ia.sales_clicked
         FROM interaction_aggregation ia
             INNER JOIN se.data_pii.scv_session_events_link ssel ON ia.event_hash = ssel.event_hash
             AND ia.count_clicked_homepage_rfy > 0
     )
SELECT
    stmc.touch_affiliate_territory,
    COUNT(DISTINCT stba.touch_id)            AS sessions,
    COUNT(DISTINCT IFF(LOWER(stba.touch_landing_pagepath)
                           LIKE ANY ('%current-sales%',
                                     '%aktuelle-angebote%',
                                     '%offerte-in-corso%',
                                     '%aktuella-kampanjer%',
                                     '%aanbiedingen%',
                                     '%nuvaerende-salg%',
                                     '%aktuelle-tilbud%',
                                     '%ventas-actuales%'),
                       stba.touch_id, NULL)) AS landing_on_homepage_session,
    SUM(IFF(eti.touch_id IS NOT NULL, 1, 0)) AS clicked_on_rfy,
    SUM(eti.count_clicked_homepage_rfy)      AS rfy_clicks
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    LEFT JOIN  enrich_touch_id eti ON stba.touch_id = eti.event_hash
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 1 --TODO remove
GROUP BY 1
;

USE WAREHOUSE pipe_xlarge;