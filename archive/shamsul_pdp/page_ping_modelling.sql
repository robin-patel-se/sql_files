SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND es.event_hash = 'f26376b4a79fb745fccfe4065c4b1375d62d809f65b2aeeac02c1ac7323ddc5b';

-- a sample of page pings clearly associated to one page view
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND es.se_user_id = '33672413'
  AND es.event IS DISTINCT FROM 'struct'
  AND es.event_hash IN (
                        'dbe267f123890ed99ce7eed20d06399cf8d7e575c72ffa052fb1c094327ccdd8',
                        '62b66f9bc1ba8298da46a520ce17f192a47e62433dc494565869403acffbe334',
                        '3ed6c6f55a7a56e1d2c6012fe73079a2e1f60194efe1ecd7d7a3dad6b98eb032'
    )
-- TODO remove


-- limit to just page pings
SELECT
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
    *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND es.se_user_id = '33672413'      -- TODO remove
  AND es.event_name = 'page_ping'
  AND es.event_hash IN (
                        'dbe267f123890ed99ce7eed20d06399cf8d7e575c72ffa052fb1c094327ccdd8',
                        '62b66f9bc1ba8298da46a520ce17f192a47e62433dc494565869403acffbe334',
                        '3ed6c6f55a7a56e1d2c6012fe73079a2e1f60194efe1ecd7d7a3dad6b98eb032'
    )
-- TODO remove

--begin aggregating
SELECT
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
    MAX(es.event_tstamp)                                                    AS max_event_tstamp,
    COUNT(DISTINCT es.event_hash)                                           AS page_pings,
    MAX(es.pp_yoffset_max)                                                  AS max_yoffset,
    MAX(es.doc_height)                                                      AS max_docheight,
    IFF(max_docheight = 0, 0, max_yoffset / max_docheight)                  AS scroll_depth,
    CASE
        WHEN scroll_depth <= 0.1 THEN '<=10%'
        WHEN scroll_depth <= 0.25 THEN '11-25%'
        WHEN scroll_depth <= 0.50 THEN '26-50%'
        WHEN scroll_depth <= 0.75 THEN '51-75%'
        ELSE '>=75%'
        END                                                                 AS scroll_depth_bucket
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND es.se_user_id = '33672413'      -- TODO remove
  AND es.event_name = 'page_ping'
  AND es.event_hash IN (
                        'dbe267f123890ed99ce7eed20d06399cf8d7e575c72ffa052fb1c094327ccdd8',
                        '62b66f9bc1ba8298da46a520ce17f192a47e62433dc494565869403acffbe334',
                        '3ed6c6f55a7a56e1d2c6012fe73079a2e1f60194efe1ecd7d7a3dad6b98eb032'
    )                                 -- TODO remove
GROUP BY 1;


--remove filters
SELECT
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
    MAX(es.event_tstamp)                                                    AS max_event_tstamp,
    COUNT(DISTINCT es.event_hash)                                           AS page_pings,
    MAX(es.pp_yoffset_max)                                                  AS max_yoffset,
    MAX(es.doc_height)                                                      AS max_docheight,
    IFF(max_docheight = 0, 0, max_yoffset / max_docheight)                  AS scroll_depth,
    CASE
        WHEN scroll_depth <= 0.1 THEN '<=10%'
        WHEN scroll_depth <= 0.25 THEN '11-25%'
        WHEN scroll_depth <= 0.50 THEN '26-50%'
        WHEN scroll_depth <= 0.75 THEN '51-75%'
        ELSE '>=75%'
        END                                                                 AS scroll_depth_bucket
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND es.se_user_id = '33672413'      -- TODO remove
  AND es.event_name = 'page_ping'
GROUP BY 1;


-- model data to page view
WITH agg_page_pings AS (
    SELECT
        es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
        MAX(es.event_tstamp)                                                    AS max_event_tstamp,
        COUNT(DISTINCT es.event_hash)                                           AS page_pings,
        MAX(es.pp_yoffset_max)                                                  AS max_yoffset,
        MAX(es.doc_height)                                                      AS max_docheight
    FROM hygiene_vault_mvp.snowplow.event_stream es
    WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
      AND es.se_user_id = '33672413'      -- TODO remove
      AND es.event_name = 'page_ping'
    GROUP BY 1
)
SELECT
    es.event_hash,
    es.event_name,
    es.event_tstamp,
    es.page_url,
    es.v_tracker,
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR    AS web_page_id,
    es.doc_height,
    COALESCE(app.max_event_tstamp, es.event_tstamp)                            AS max_event_tstamp,
    COALESCE(app.page_pings, 0)                                                AS page_pings,
    COALESCE(app.max_yoffset, 0)                                               AS event_max_yoffset,
    COALESCE(app.max_docheight, es.doc_height)                                 AS event_max_doc_height,
    IFF(event_max_doc_height = 0, 0, event_max_yoffset / event_max_doc_height) AS scroll_depth,
    CASE
        WHEN scroll_depth = 0 THEN '0%'
        WHEN scroll_depth <= 0.25 THEN '<25%'
        WHEN scroll_depth <= 0.50 THEN '26-50%'
        WHEN scroll_depth <= 0.75 THEN '51-75%'
        WHEN scroll_depth > 0.75 THEN '>75%'
        ELSE 'NA'
        END                                                                    AS scroll_depth_bucket
FROM hygiene_vault_mvp.snowplow.event_stream es
    LEFT JOIN agg_page_pings app ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = app.web_page_id
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND es.se_user_id = '33672413'      -- TODO remove
  AND es.event_name = 'page_view'
;


-- remove user filter
WITH agg_page_pings AS (
    SELECT
        es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
        MAX(es.event_tstamp)                                                    AS max_event_tstamp,
        COUNT(DISTINCT es.event_hash)                                           AS page_pings,
        MAX(es.pp_yoffset_max)                                                  AS max_yoffset,
        MAX(es.doc_height)                                                      AS max_docheight
    FROM hygiene_vault_mvp.snowplow.event_stream es
    WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
      AND es.event_name = 'page_ping'
    GROUP BY 1
)
SELECT
    es.event_hash,
    es.event_name,
    es.event_tstamp,
    es.page_url,
    es.v_tracker,
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                                        AS web_page_id,
    es.doc_height,
    IFF(event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(app.max_event_tstamp, es.event_tstamp))                            AS event_max_event_tstamp,
    DATEDIFF('second', es.event_tstamp, event_max_event_tstamp) < 30                                                               AS is_bounced_page,
    IFF(event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(app.page_pings, 0))                                                AS page_pings,
    IFF(event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(app.max_yoffset, 0))                                               AS event_max_yoffset,
    IFF(event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(app.max_docheight, es.doc_height))                                 AS event_max_doc_height,
    IFF(event_name IS DISTINCT FROM 'page_view', NULL, IFF(event_max_doc_height = 0, 0, event_max_yoffset / event_max_doc_height)) AS scroll_depth,
    CASE
        WHEN scroll_depth = 0 THEN '0%'
        WHEN scroll_depth <= 0.25 THEN '<25%'
        WHEN scroll_depth <= 0.50 THEN '26-50%'
        WHEN scroll_depth <= 0.75 THEN '51-75%'
        WHEN scroll_depth > 0.75 THEN '>75%'
        END                                                                                                                        AS scroll_depth_bucket
FROM hygiene_vault_mvp.snowplow.event_stream es
    LEFT JOIN agg_page_pings app ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = app.web_page_id
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
;



-- Next steps:
-- Can we model screen 'pings' up to a screen view?


SELECT *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1
  AND e.event_name = 'screen_view';

SELECT
    e.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR AS session_id,
    e.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR            AS screen_name,
    e.event_name,
    e.collector_tstamp,
    *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1 --TODO remove later
  AND e.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR = 'dd3d7bdf-d7fb-4ee9-9c37-f379b7a1afdf' -- TODO remove later

;


-- screen view events time max tstamp
SELECT
    e.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR AS session_id,
    e.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR            AS screen_name,
    e.event_name,
    e.collector_tstamp,
    LEAD(e.collector_tstamp) OVER (PARTITION BY session_id ORDER BY e.collector_tstamp) AS next_tstamp
FROM snowplow.atomic.events e
WHERE e.collector_tstamp >= CURRENT_DATE - 1                                                                                       --TODO remove later
  AND e.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR = 'dd3d7bdf-d7fb-4ee9-9c37-f379b7a1afdf' -- TODO remove later
  AND e.event_name = 'screen_view' -- TODO investigate what we should do when the app is put in background
;


--next steps harmonise these queries

WITH model_page_views AS (
    SELECT
        es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR           AS web_page_id,
        MAX(es.event_tstamp)                                                              AS max_event_tstamp,
        COUNT(DISTINCT es.event_hash)                                                     AS page_pings,
        MAX(es.pp_yoffset_max)                                                            AS max_yoffset,
        MAX(es.doc_height)                                                                AS max_docheight,
        ARRAY_AGG(es.contexts_com_secretescapes_content_element_interaction_context_1[0]) AS content_interaction_array,
        content_interaction_array[0] IS NOT NULL                                          AS has_interaction,
        ARRAY_AGG(es.contexts_com_secretescapes_content_element_viewed_context_1[0])      AS content_viewed_array
    FROM hygiene_vault_mvp.snowplow.event_stream es
    WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
      AND (
            (-- page ping events
                es.event_name = 'page_ping')
            OR ( -- content interaction events
                    es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
                    AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
                )
            OR (-- content viewed events
                    es.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
                    AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
                )
        )
    GROUP BY 1
),
     model_screen_views AS (
         SELECT
             es.event_hash,
             es.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR  AS session_id,
             LEAD(es.collector_tstamp) OVER (PARTITION BY session_id ORDER BY es.collector_tstamp) AS max_event_tstamp
         FROM hygiene_vault_mvp.snowplow.event_stream es
         WHERE es.collector_tstamp >= CURRENT_DATE -- TODO remove
           AND es.event_name = 'screen_view' -- TODO investigate what we should do when the app is put in background
     )
SELECT
    es.event_hash,
    es.event_name,
    es.event_tstamp,
    es.page_url,
    es.v_tracker,
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                                           AS web_page_id,
    es.doc_height,
    IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_event_tstamp, es.event_tstamp))                            AS event_max_event_tstamp,
    app.max_event_tstamp,
    DATEDIFF('second', es.event_tstamp, event_max_event_tstamp)                                                                       AS page_duration_seconds,
    DATEDIFF('second', es.event_tstamp, event_max_event_tstamp) < 30                                                                  AS is_bounced_page,
    IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.page_pings, 0))                                                AS page_pings,
    IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_yoffset, 0))                                               AS event_max_yoffset,
    IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_docheight, es.doc_height))                                 AS event_max_doc_height,
    IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, IFF(event_max_doc_height = 0, 0, event_max_yoffset / event_max_doc_height)) AS scroll_depth,
    CASE
        WHEN scroll_depth = 0 THEN '0%'
        WHEN scroll_depth <= 0.25 THEN '<25%'
        WHEN scroll_depth <= 0.50 THEN '26-50%'
        WHEN scroll_depth <= 0.75 THEN '51-75%'
        WHEN scroll_depth > 0.75 THEN '>75%'
        END                                                                                                                           AS scroll_depth_bucket,
    web.content_interaction_array,
    web.has_interaction,
    web.content_viewed_array
FROM hygiene_vault_mvp.snowplow.event_stream es
    LEFT JOIN model_page_views web ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = web.web_page_id
    LEFT JOIN model_screen_views app ON es.event_hash = app.event_hash
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
;


-- need to tidy the max event tstamp
-- look at content interaction
-- for web
-- for app

-- content interaction on web
SELECT
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
    es.event_tstamp,
    es.event_hash,
    es.pp_yoffset_max,
    es.doc_height,
    es.event_name,
    es.contexts_com_secretescapes_content_element_interaction_context_1

FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND (-- page ping events
            es.event_name = 'page_ping'
        OR ( -- content interaction events
                    es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
                    AND es.device_platform NOT IN ('native app ios', 'native app android') -- remove app content interaction
                ))
  AND web_page_id = '079c568b-c040-4ec1-b2f0-d241327535b7' --TODO remove
;

SELECT
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR           AS web_page_id,
    MAX(es.event_tstamp)                                                              AS max_event_tstamp,
    COUNT(DISTINCT es.event_hash)                                                     AS page_pings,
    MAX(es.pp_yoffset_max)                                                            AS max_yoffset,
    MAX(es.doc_height)                                                                AS max_docheight,
    ARRAY_AGG(es.contexts_com_secretescapes_content_element_interaction_context_1[0]) AS content_interaction_array,
    content_interaction_array[0] IS NOT NULL                                          AS has_interaction,
    ARRAY_AGG(es.contexts_com_secretescapes_content_element_viewed_context_1[0])      AS content_viewed_array
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
  AND (
        (-- page ping events
            es.event_name = 'page_ping')
        OR ( -- content interaction events
                es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
                AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
            )
        OR (-- content viewed events
                es.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
                AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction

            )
    )
GROUP BY 1;


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
  AND es.event_tstamp >= CURRENT_DATE - 1;

--screen view foreground and background


------------------------------------------------------------------------------------------------------------------------
-- to find foreground and background events (se_category 'foreground')
SELECT
    es.event_hash,
    es.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR  AS session_id,
    LEAD(es.collector_tstamp) OVER (PARTITION BY session_id ORDER BY es.collector_tstamp) AS max_event_tstamp,
    *
FROM se.data_pii.scv_event_stream es
WHERE es.collector_tstamp >= CURRENT_DATE -- TODO remove
  AND es.v_tracker LIKE ANY ('ios%', 'andr%')
--   AND es.event_name = 'screen_view' -- TODO investigate what we should do when the app is put in background


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE scratch.robinpatel.page_ping AS (
    WITH model_page_views AS (
        SELECT
            es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR           AS web_page_id,
            MAX(es.event_tstamp)                                                              AS max_event_tstamp,
            COUNT(DISTINCT es.event_hash)                                                     AS page_pings,
            MAX(es.pp_yoffset_max)                                                            AS max_yoffset,
            MAX(es.doc_height)                                                                AS max_docheight,
            ARRAY_AGG(es.contexts_com_secretescapes_content_element_interaction_context_1[0]) AS content_interaction_array,
            content_interaction_array[0] IS NOT NULL                                          AS has_interaction,
            ARRAY_AGG(es.contexts_com_secretescapes_content_element_viewed_context_1[0])      AS content_viewed_array
        FROM se.data_pii.scv_event_stream es
        WHERE es.event_tstamp >= CURRENT_DATE -- TODO adjust for incremental
          AND (
                (-- page ping events
                    es.event_name = 'page_ping')
                OR ( -- content interaction events
                        es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
                        AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
                    )
                OR (-- content viewed events
                        es.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
                        AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
                    )
            )
        GROUP BY 1
    ),
         model_screen_views AS (
             SELECT
                 es.event_hash,
                 es.contexts_com_snowplowanalytics_snowplow_client_session_1[0]['sessionId']::VARCHAR  AS session_id,
                 LEAD(es.collector_tstamp) OVER (PARTITION BY session_id ORDER BY es.collector_tstamp) AS max_event_tstamp
             FROM se.data_pii.scv_event_stream es
             WHERE es.collector_tstamp >= CURRENT_DATE -- TODO remove
               AND es.event_name = 'screen_view' -- TODO investigate what we should do when the app is put in background
         )
    SELECT
        es.event_hash,
        es.event_name,
        es.event_tstamp,
        es.page_url,
        es.v_tracker,
        es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                                           AS web_page_id,
        es.doc_height,
        IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_event_tstamp, es.event_tstamp))                            AS event_max_event_tstamp,
        app.max_event_tstamp,
        DATEDIFF('second', es.event_tstamp, event_max_event_tstamp)                                                                       AS page_duration_seconds,
        IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.page_pings, 0))                                                AS page_pings,
        IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_yoffset, 0))                                               AS event_max_yoffset,
        IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_docheight, es.doc_height))                                 AS event_max_doc_height,
        IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, IFF(event_max_doc_height = 0, 0, event_max_yoffset / event_max_doc_height)) AS scroll_depth,
        CASE
            WHEN scroll_depth = 0 THEN '0%'
            WHEN scroll_depth <= 0.25 THEN '<25%'
            WHEN scroll_depth <= 0.50 THEN '26-50%'
            WHEN scroll_depth <= 0.75 THEN '51-75%'
            WHEN scroll_depth > 0.75 THEN '>75%'
            END                                                                                                                           AS scroll_depth_bucket,
        web.content_viewed_array,
        web.content_interaction_array,
        web.has_interaction
    FROM se.data_pii.scv_event_stream es
        LEFT JOIN model_page_views web ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = web.web_page_id
        LEFT JOIN model_screen_views app ON es.event_hash = app.event_hash
    WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
      AND es.event_name IN ('page_view', 'screen_view')
)
;

-- identify the gaps in app tracking so that we can itemise them for tracking inconsistencies
-- -- screen view page ping - doesn't exist
-- -- common identifier for content viewed/rendered/interaction to enable us to link them to a screen view
-- -- common identifier for structured foreground and background events to enable us to link them to a screen view (nice to have)

-- architectural decisions behind how we surface this

USE WAREHOUSE pipe_large;

-- we are going to create a downstream (from event stream hygiene) self describing that will process the enrichment data, we can then separately decide how and when we use this data.

SELECT GET_DDL('table', 'scratch.robinpatel.page_ping');



CREATE OR REPLACE TABLE page_ping
(
    event_hash                VARCHAR,
    event_name                VARCHAR,
    event_tstamp              TIMESTAMP,
    page_url                  VARCHAR,
    v_tracker                 VARCHAR,
    web_page_id               VARCHAR,
    doc_height                NUMBER,
    event_max_event_tstamp    TIMESTAMP,
    max_event_tstamp          TIMESTAMP,
    page_duration_seconds     NUMBER,
    page_pings                NUMBER,
    event_max_yoffset         NUMBER,
    event_max_doc_height      NUMBER,
    scroll_depth              NUMBER,
    scroll_depth_bucket       VARCHAR,
    content_viewed_array      ARRAY,
    content_interaction_array ARRAY,
    has_interaction           BOOLEAN
);


SELECT *
FROM se.data.user_booking_review ubr;



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment__step03__model_data AS (
    SELECT
        es.event_hash,
        es.event_name,
        es.event_tstamp,
        es.page_url,
        es.v_tracker,
        es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR                                AS web_page_id,
        es.doc_height,
        IFF(es.event_name IS DISTINCT FROM 'page_view', NULL, COALESCE(web.max_event_tstamp, es.event_tstamp)) AS event_max_event_tstamp,
        app.max_event_tstamp,
        DATEDIFF('second', es.event_tstamp, event_max_event_tstamp)                                            AS page_duration_seconds,
        IFF(es.event_name IS DISTINCT FROM 'page_view',
            NULL, COALESCE(web.page_pings, 0))                                                                 AS page_pings,
        IFF(es.event_name IS DISTINCT FROM 'page_view',
            NULL, COALESCE(web.max_yoffset, 0))                                                                AS event_max_yoffset,
        IFF(es.event_name IS DISTINCT FROM 'page_view',
            NULL, COALESCE(web.max_docheight, es.doc_height))                                                  AS event_max_doc_height,
        IFF(es.event_name IS DISTINCT FROM 'page_view',
            NULL, IFF(event_max_doc_height = 0, 0, event_max_yoffset / event_max_doc_height))                  AS scroll_depth,
        CASE
            WHEN scroll_depth = 0 THEN '0%'
            WHEN scroll_depth <= 0.25 THEN '<25%'
            WHEN scroll_depth <= 0.50 THEN '26-50%'
            WHEN scroll_depth <= 0.75 THEN '51-75%'
            WHEN scroll_depth > 0.75 THEN '>75%'
            END                                                                                                AS scroll_depth_bucket,
        web.content_viewed_array,
        web.content_interaction_array,
        web.has_interaction
    FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
        LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment__step01__model_page_views web
                  ON es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR = web.web_page_id
        LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment__step02__model_screen_views app
                  ON es.event_hash = app.event_hash
    WHERE es.event_tstamp >= CURRENT_DATE -- TODO remove
      AND es.event_name IN ('page_view', 'screen_view')
);


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment;
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
WHERE event_name = 'page_view';

CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;


SELECT
    es.contexts_com_snowplowanalytics_snowplow_web_page_1[0]['id']::VARCHAR AS web_page_id,
    MAX(es.event_tstamp)                                                    AS max_event_tstamp,
    COUNT(DISTINCT es.event_hash)                                           AS page_pings,
    MAX(es.pp_yoffset_max)                                                  AS max_yoffset,
    MAX(es.doc_height)                                                      AS max_docheight,
    ARRAY_AGG(DISTINCT es.contexts_com_secretescapes_content_element_interaction_context_1[0])
              WITHIN GROUP (es.event_tstamp)                                AS content_interaction_array,
    content_interaction_array[0] IS NOT NULL                                AS has_interaction,
    ARRAY_AGG(DISTINCT es.contexts_com_secretescapes_content_element_viewed_context_1[0])
              WITHIN GROUP (es.event_tstamp)                                AS content_viewed_array
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.event_tstamp >= TIMESTAMPADD('day', -1, '2020-12-31 03:00:00'::TIMESTAMP)
  AND (
        (-- page ping events
            es.event_name = 'page_ping')
        OR ( -- content interaction events
                es.contexts_com_secretescapes_content_element_interaction_context_1 IS NOT NULL
                AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
            )
        OR (-- content viewed events
                es.contexts_com_secretescapes_content_element_viewed_context_1 IS NOT NULL
                AND es.device_platform NOT IN ('native app ios', 'native app android') -- to remove app content interaction
            )
    )
GROUP BY 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment;


SELECT *
FROM data_vault_mvp.single_customer_view_stg.page_screen_enrichment pse;
SELECT *
FROM se.data_pii.scv_page_screen_enrichment
WHERE event_name = 'page_view';

SELECT
    event_name,
    COUNT(*),
    MAX(event_tstamp)
FROM se.data_pii.scv_page_screen_enrichment
GROUP BY 1;

