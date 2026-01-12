USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_TOUCHED_SPVS
(
    EVENT_HASH        VARCHAR,
    TOUCH_ID          VARCHAR,
    EVENT_TSTAMP      TIMESTAMPNTZ,
    SE_SALE_ID        VARCHAR,
    EVENT_CATEGORY    VARCHAR,
    EVENT_SUBCATEGORY VARCHAR,
    UPDATED_AT        TIMESTAMP_LTZ
);

--TODO: need to de dupe spvs for client side and server side
MERGE INTO MODULE_TOUCHED_SPVS AS TARGET
    USING (
--SPVs from page views
        SELECT e.EVENT_HASH,
               t.TOUCH_ID,
               e.EVENT_TSTAMP,
               e.SE_SALE_ID,
               'page views'      AS event_category,
               'SPV'             AS event_subcategory,
               CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'

        FROM MODULE_TOUCHIFICATION t
                 INNER JOIN EVENT_STREAM e
                            ON e.EVENT_HASH = t.EVENT_HASH
                 LEFT JOIN MODULE_EXTRACTED_PARAMS P ON e.PAGE_URL = p.URL AND p.FROM_APP = 'true'
        WHERE e.EVENT_NAME = 'page_view'
          AND e.SE_SALE_ID IS NOT NULL
          AND (
                e.PAGE_URLPATH LIKE '%/sale'
                OR
                e.PAGE_URLPATH LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
            -- need to adjust for new definitions of spv e.g. travel bird booking flow
            )
          AND p.FROM_APP IS NULL -- remove from native app offer spvs
          AND e.IS_SERVER_SIDE_EVENT = FALSE -- currently excluding until data is validated.
--           AND t.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.EVENT_HASH = BATCH.EVENT_HASH
    WHEN NOT MATCHED
        THEN INSERT (
                     EVENT_HASH,
                     TOUCH_ID,
                     EVENT_TSTAMP,
                     SE_SALE_ID,
                     EVENT_CATEGORY,
                     EVENT_SUBCATEGORY,
                     UPDATED_AT)
        VALUES (BATCH.EVENT_HASH,
                BATCH.TOUCH_ID,
                BATCH.EVENT_TSTAMP,
                BATCH.SE_SALE_ID,
                BATCH.EVENT_CATEGORY,
                BATCH.EVENT_SUBCATEGORY,
                BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.TOUCH_ID != BATCH.TOUCH_ID
        THEN UPDATE SET
        TARGET.TOUCH_ID = BATCH.TOUCH_ID,
        TARGET.EVENT_TSTAMP = BATCH.EVENT_TSTAMP,
        TARGET.SE_SALE_ID = BATCH.SE_SALE_ID,
        TARGET.EVENT_CATEGORY = BATCH.EVENT_CATEGORY,
        TARGET.EVENT_SUBCATEGORY = BATCH.EVENT_SUBCATEGORY,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT;


--SPVs from screen views
MERGE INTO MODULE_TOUCHED_SPVS AS TARGET
    USING (
        SELECT e.EVENT_HASH,
               t.TOUCH_ID,
               e.EVENT_TSTAMP,
               e.SE_SALE_ID,
               'screen views'    AS event_category,
               'SPV'             AS event_subcategory,
               CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'

        FROM MODULE_TOUCHIFICATION t
                 INNER JOIN EVENT_STREAM e
                            ON e.EVENT_HASH = t.EVENT_HASH
        WHERE e.EVENT_NAME = 'screen_view'
          AND SE_SALE_ID IS NOT NULL --
          AND e.IS_SERVER_SIDE_EVENT = FALSE -- currently excluding until data is validated.


--           AND t.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.EVENT_HASH = BATCH.EVENT_HASH
    WHEN NOT MATCHED
        THEN INSERT (
                     EVENT_HASH,
                     TOUCH_ID,
                     EVENT_TSTAMP,
                     SE_SALE_ID,
                     EVENT_CATEGORY,
                     EVENT_SUBCATEGORY,
                     UPDATED_AT)
        VALUES (BATCH.EVENT_HASH,
                BATCH.TOUCH_ID,
                BATCH.EVENT_TSTAMP,
                BATCH.SE_SALE_ID,
                BATCH.EVENT_CATEGORY,
                BATCH.EVENT_SUBCATEGORY,
                BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.TOUCH_ID != BATCH.TOUCH_ID
        THEN UPDATE SET
        TARGET.TOUCH_ID = BATCH.TOUCH_ID,
        TARGET.EVENT_TSTAMP = BATCH.EVENT_TSTAMP,
        TARGET.SE_SALE_ID = BATCH.SE_SALE_ID,
        TARGET.EVENT_CATEGORY = BATCH.EVENT_CATEGORY,
        TARGET.EVENT_SUBCATEGORY = BATCH.EVENT_SUBCATEGORY,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT;


------------------------------------------------------------------------------------------------------------------------
--assertions
--all spvs have a se sale id
SELECT CASE
           WHEN (SELECT COUNT(*) FROM MODULE_TOUCHED_SPVS WHERE SE_SALE_ID IS NULL)
               > 0 THEN FALSE
           ELSE TRUE END AS ALL_SPVS_HAVE_SALE_ID;

--unique spv per event hash
SELECT CASE
           WHEN COUNT(*) > 0 THEN FALSE
           ELSE TRUE END AS UNIQUE_EVENT_HASH
FROM (
         SELECT EVENT_HASH,
                COUNT(*)
         FROM MODULE_TOUCHED_SPVS
         GROUP BY 1
         HAVING COUNT(*) > 1);

------------------------------------------------------------------------------------------------------------------------
--check how many spvs have sale id

SELECT count(*)                                                                   as spvs,
       SUM(CASE WHEN SE_SALE_ID IS NOT NULL THEN 1 ELSE 0 END)                    as spvs_with_sale_id,
       (SUM(CASE WHEN SE_SALE_ID IS NOT NULL THEN 1 ELSE 0 END) / count(*)) * 100 as percent_spvs_with_sale_id

FROM EVENT_STREAM e
WHERE ( -- web spvs
        e.EVENT_NAME = 'page_view'
        AND (
                e.PAGE_URLPATH LIKE '%/sale'
                OR
                e.PAGE_URLPATH LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
            -- need to adjust for new definitions of spv e.g. travel bird booking flow
            )
        AND e.PAGE_URL NOT LIKE '%fromApp=true%' -- remove from native app offer spvs
        AND e.IS_SERVER_SIDE_EVENT = FALSE -- currently excluding until data is validated.
    )
   OR ( -- app spvs
        e.EVENT_NAME = 'screen_view'
        AND e.SE_SALE_ID IS NOT NULL
        AND e.IS_SERVER_SIDE_EVENT = FALSE -- currently excluding until data is validated.
    );

------------------------------------------------------------------------------------------------------------------------
--new version with line in sand

USE WAREHOUSE PIPE_LARGE;
USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;

--SPVs from page views
SELECT e.EVENT_HASH,
       t.TOUCH_ID,
       e.EVENT_TSTAMP,
       e.SE_SALE_ID,
       'page views' AS event_category,
       'SPV'        AS event_subcategory
FROM MODULE_TOUCHIFICATION t
         INNER JOIN HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
                    ON e.EVENT_HASH = t.EVENT_HASH
WHERE e.EVENT_NAME = 'page_view'
  AND e.SE_SALE_ID IS NOT NULL
  AND (
        (--client side tracking, prior implementation/validation
                e.COLLECTOR_TSTAMP < '2020-02-28 00:00:00'
                AND (
                        e.PAGE_URLPATH LIKE '%/sale'
                        OR
                        e.PAGE_URLPATH LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                    -- need to adjust for new definitions of spv e.g. travel bird booking flow
                    )
                AND
                e.DEVICE_PLATFORM != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                AND e.IS_SERVER_SIDE_EVENT = FALSE -- exclude non validated ss events
            )
        OR
        (--server side tracking, post implementation/validation
                e.COLLECTOR_TSTAMP >= '2020-02-28 00:00:00'
                AND e.CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['sub_category']::VARCHAR = 'sale'
                AND
                e.DEVICE_PLATFORM != 'native app' --explicitly remove native app (as app offer pages appear like web SPVs)
                AND e.IS_SERVER_SIDE_EVENT = TRUE -- exclude non validated ss events
            )
    )

--SPVs from native app
SELECT e.EVENT_HASH,
       t.TOUCH_ID,
       e.EVENT_TSTAMP,
       e.SE_SALE_ID,
       'screen views'    AS event_category,
       'SPV'             AS event_subcategory,
       CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'

FROM MODULE_TOUCHIFICATION t
         INNER JOIN HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
                    ON e.EVENT_HASH = t.EVENT_HASH
WHERE e.EVENT_NAME = 'screen_view'
  AND e.DEVICE_PLATFORM = 'native app'

--           AND t.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

  AND (
        ( -- old world native app event data
         e.COLLECTOR_TSTAMP < '2020-02-28 00:00:00'
         AND
         SE_SALE_ID IS NOT NULL
        )
    OR
       ( -- new world native app event data
        e.COLLECTOR_TSTAMP >= '2020-02-28 00:00:00'
           AND
        e.CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['sub_category']::VARCHAR = 'sale'
       )
    )


