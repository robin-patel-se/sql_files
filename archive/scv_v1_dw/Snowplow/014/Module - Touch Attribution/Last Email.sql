USE WAREHOUSE PIPE_LARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

--retract touches that have been re-touchified

MERGE INTO MODULE_TOUCH_ATTRIBUTION AS TARGET
    USING (
        SELECT EVENT_HASH
        FROM MODULE_TOUCHIFICATION
--      WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load, test thoroughly the time gap between this module and touchification 'could' be quite different.
    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.EVENT_HASH AND TARGET.ATTRIBUTION_MODEL = 'last email'
    WHEN MATCHED THEN DELETE;

-- insert new or retouched last email attribution touches


MERGE INTO MODULE_TOUCH_ATTRIBUTION AS TARGET
    USING (
        WITH nullify AS (
            --nullify non email channels as these are going to be re-attributed
            SELECT t.TOUCH_ID,
                   t.ATTRIBUTED_USER_ID,
                   t.TOUCH_START_TSTAMP,
                   c.TOUCH_MKT_CHANNEL,
                   CASE
                       WHEN
                               c.TOUCH_MKT_CHANNEL != 'Email'
                               AND (
                                           DATEDIFF(days, LAG(t.TOUCH_START_TSTAMP)
                                                              OVER (PARTITION BY t.ATTRIBUTED_USER_ID ORDER BY t.TOUCH_START_TSTAMP),
                                                    t.TOUCH_START_TSTAMP) <
                                           180 -- TODO: confirm time parameter, eanother touch within 6 months
                                       OR
                                           LAG(t.TOUCH_ID)
                                               OVER (PARTITION BY t.ATTRIBUTED_USER_ID ORDER BY t.TOUCH_START_TSTAMP) IS NULL -- first touch
                                   )
                           THEN NULL
                       ELSE c.TOUCH_ID END AS nullify
            FROM MODULE_TOUCH_BASIC_ATTRIBUTES t
                     INNER JOIN MODULE_TOUCH_MARKETING_CHANNEL c ON t.TOUCH_ID = c.TOUCH_ID
            --      WHERE c.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

        )
                ,
             attribute as (
                 -- attribute non email touches back to last email
                 SELECT TOUCH_ID,
                        ATTRIBUTED_USER_ID,
                        TOUCH_START_TSTAMP,
                        TOUCH_MKT_CHANNEL,
                        LAST_VALUE(NULLIFY) IGNORE NULLS
                            OVER (PARTITION BY ATTRIBUTED_USER_ID
                            ORDER BY TOUCH_START_TSTAMP rows between unbounded preceding and current row) AS LAST_ATTRIBUTED_TOUCH_ID
                 FROM nullify
             )
--
        SELECT TOUCH_ID,
               ATTRIBUTED_USER_ID,
               TOUCH_MKT_CHANNEL,
               TOUCH_START_TSTAMP,
               CASE
                   WHEN LAST_ATTRIBUTED_TOUCH_ID IS NULL
                       THEN TOUCH_ID
                   ELSE LAST_ATTRIBUTED_TOUCH_ID END
                                 AS ATTRIBUTED_TOUCH_ID,
               'last email'      AS ATTRIBUTION_MODEL,
               1                 AS ATTRIBUTED_WEIGHT,
               CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'

        FROM attribute
    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.TOUCH_ID AND TARGET.ATTRIBUTED_TOUCH_ID = BATCH.ATTRIBUTED_TOUCH_ID AND
                  TARGET.ATTRIBUTION_MODEL = BATCH.ATTRIBUTION_MODEL
    WHEN NOT MATCHED
        THEN INSERT (
                     TOUCH_ID,
                     ATTRIBUTED_TOUCH_ID,
                     ATTRIBUTION_MODEL,
                     ATTRIBUTED_WEIGHT,
                     UPDATED_AT
        )
        VALUES (BATCH.TOUCH_ID,
                BATCH.ATTRIBUTED_TOUCH_ID,
                BATCH.ATTRIBUTION_MODEL,
                BATCH.ATTRIBUTED_WEIGHT,
                BATCH.UPDATED_AT)
;

------------------------------------------------------------------------------------------------------------------------
--assertions
--all touches have a last email attribution touch.
SELECT CASE
           WHEN (
                   (SELECT COUNT(*)
                    FROM MODULE_TOUCH_UTM_REFERRER) =
                   (SELECT COUNT(*)
                    FROM MODULE_TOUCH_ATTRIBUTION
                    WHERE ATTRIBUTION_MODEL = 'last email')
               ) THEN TRUE
           ELSE FALSE END AS ALL_TOUCHES_ATTRIBUTED_LAST_EMAIL