USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

--retract touches that have been re-touchified

MERGE INTO MODULE_TOUCH_ATTRIBUTION AS TARGET
    USING (
        SELECT EVENT_HASH
        FROM MODULE_TOUCHIFICATION
--      WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load, test thoroughly the time gap between this module and touchification 'could' be quite different.
    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.EVENT_HASH AND TARGET.ATTRIBUTION_MODEL = 'last paid'
    WHEN MATCHED THEN DELETE;

-- insert new or retouched last paid attribution touches

MERGE INTO MODULE_TOUCH_ATTRIBUTION AS TARGET
    USING (
        WITH nullify AS (
            --nullify non paid channels as these are going to be re-attributed
            SELECT t.TOUCH_ID,
                   t.ATTRIBUTED_USER_ID,
                   t.TOUCH_START_TSTAMP,
                   c.TOUCH_MKT_CHANNEL,
                   CASE
                       WHEN
                                   c.TOUCH_MKT_CHANNEL NOT IN
                                   ('PPC - Brand', 'PPC - Non Brand', 'Display', 'Paid Social')
                               AND (
                                               DATEDIFF(days, LAG(t.TOUCH_START_TSTAMP)
                                                                  OVER (PARTITION BY t.ATTRIBUTED_USER_ID ORDER BY t.TOUCH_START_TSTAMP),
                                                        t.TOUCH_START_TSTAMP) <
                                               180 -- TODO: confirm time parameter, another touch within 6 months
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
                 -- attribute non paid touches back to last paid
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
               'last paid'       AS ATTRIBUTION_MODEL,
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
--all touches have a last paid attribution touch.
SELECT CASE
           WHEN (
                   (SELECT COUNT(*)
                    FROM MODULE_TOUCH_UTM_REFERRER) =
                   (SELECT COUNT(*)
                    FROM MODULE_TOUCH_ATTRIBUTION
                    WHERE ATTRIBUTION_MODEL = 'last paid')
               ) THEN TRUE
           ELSE FALSE END AS ALL_TOUCHES_ATTRIBUTED_LAST_PAID

------------------------------------------------------------------------------------------------------------------------

-- New version

WITH users_with_new_touches AS (
    --get users who've had a new touch
    SELECT DISTINCT ATTRIBUTED_USER_ID
    FROM MODULE_TOUCH_MARKETING_CHANNEL
--      WHERE c.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
),
     all_touches_from_users AS (
         --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel direct
         SELECT c.TOUCH_ID,
                b.TOUCH_START_TSTAMP,
                c.TOUCH_MKT_CHANNEL,
                c.ATTRIBUTED_USER_ID,
                CASE
                    --don't nullify if first touch
                    WHEN LAG(c.TOUCH_MKT_CHANNEL)
                             OVER (PARTITION BY c.ATTRIBUTED_USER_ID ORDER BY b.TOUCH_START_TSTAMP) IS NULL
                        THEN c.TOUCH_ID
                    --nullify if is a direct channel
                    WHEN c.TOUCH_MKT_CHANNEL NOT IN ('PPC - Brand', 'PPC - Non Brand CPA', 'PPC - Non Brand CPL', 'Display', 'Paid Social')
                        THEN NULL
                    ELSE c.TOUCH_ID
                    END AS NULLIFY_TOUCH_ID,
                CASE
                    --don't nullify if first touch
                    WHEN LAG(c.TOUCH_MKT_CHANNEL)
                             OVER (PARTITION BY c.ATTRIBUTED_USER_ID ORDER BY b.TOUCH_START_TSTAMP) IS NULL
                        THEN b.TOUCH_START_TSTAMP
                    --nullify if is a direct channel
                    WHEN c.TOUCH_MKT_CHANNEL NOT IN ('PPC - Brand', 'PPC - Non Brand CPA', 'PPC - Non Brand CPL', 'Display', 'Paid Social')
                        THEN NULL
                    ELSE b.TOUCH_START_TSTAMP
                    END AS NULLIFY_TOUCH_START_TSTAMP
         FROM MODULE_TOUCH_MARKETING_CHANNEL c
                  INNER JOIN MODULE_TOUCH_BASIC_ATTRIBUTES b ON c.TOUCH_ID = b.TOUCH_ID
              -- get all touches from users who have had a new touch
         WHERE c.ATTRIBUTED_USER_ID IN (SELECT ATTRIBUTED_USER_ID FROM users_with_new_touches)
     ),
     last_value AS (
         --use proxy touch id and touch tstamp to back fill nulls
         SELECT TOUCH_ID,
                TOUCH_START_TSTAMP,
                TOUCH_MKT_CHANNEL,
                ATTRIBUTED_USER_ID,
                LAST_VALUE(NULLIFY_TOUCH_ID) IGNORE NULLS OVER
                    (PARTITION BY ATTRIBUTED_USER_ID ORDER BY TOUCH_START_TSTAMP
                    rows between unbounded preceding and current row) AS PERSISTED_TOUCH_ID,
                LAST_VALUE(NULLIFY_TOUCH_START_TSTAMP) IGNORE NULLS OVER
                    (PARTITION BY ATTRIBUTED_USER_ID ORDER BY TOUCH_START_TSTAMP
                    rows between unbounded preceding and current row) AS PERSISTED_TOUCH_START_TSTAMP
         FROM all_touches_from_users
     )
--check that the back fills don't persist longer than 6months
SELECT TOUCH_ID,
--        TOUCH_START_TSTAMP,
--        TOUCH_MKT_CHANNEL,
--        ATTRIBUTED_USER_ID,
--        PERSISTED_TOUCH_ID,
--        PERSISTED_TOUCH_START_TSTAMP,
       CASE
           WHEN TOUCH_ID != PERSISTED_TOUCH_ID AND
               -- if a different non direct touch id exists AND its within 6 months then use it
                DATEDIFF(day, PERSISTED_TOUCH_START_TSTAMP, TOUCH_START_TSTAMP) <= 60
               THEN PERSISTED_TOUCH_ID
           ELSE TOUCH_ID END AS ATTRIBUTED_TOUCH_ID,
       'last non direct'     AS ATTRIBUTION_MODEL,
       1                     AS ATTRIBUTED_WEIGHT
FROM last_value