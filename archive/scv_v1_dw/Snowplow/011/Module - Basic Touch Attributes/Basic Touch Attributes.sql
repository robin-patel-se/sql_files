USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_TOUCH_BASIC_ATTRIBUTES
(
    TOUCH_ID               VARCHAR,
    ATTRIBUTED_USER_ID     VARCHAR,
    STITCHED_IDENTITY_TYPE VARCHAR,
    TOUCH_START_TSTAMP     TIMESTAMP_NTZ,
    TOUCH_END_TSTAMP       TIMESTAMP_NTZ,
    TOUCH_DURATION_SECONDS INT,
    TOUCH_POSA_TERRITORY   VARCHAR,
    TOUCH_EXPERIENCE       VARCHAR,
    TOUCH_LANDING_PAGE     VARCHAR,
    TOUCH_LANDING_PAGEPATH VARCHAR,
    TOUCH_HOSTNAME         VARCHAR,
    TOUCH_EXIT_PAGEPATH    VARCHAR,
    TOUCH_REFERRER_URL     VARCHAR,
    TOUCH_EVENT_COUNT      INT,
    TOUCH_HAS_BOOKING      BOOLEAN,
    UPDATED_AT             TIMESTAMP_LTZ
);
--retract touches that have been re-touchified

MERGE INTO MODULE_TOUCH_BASIC_ATTRIBUTES AS TARGET
    USING (
        SELECT EVENT_HASH
        FROM MODULE_TOUCHIFICATION

--      WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.EVENT_HASH
    WHEN MATCHED THEN DELETE;


--insert new or retouched touch attributes

MERGE INTO MODULE_TOUCH_BASIC_ATTRIBUTES AS TARGET
    USING (
        WITH agg_values AS (
            SELECT t.TOUCH_ID,
                   t.ATTRIBUTED_USER_ID                                        as attributed_user_id,
                   t.STITCHED_IDENTITY_TYPE                                    as stitched_identity_type,
                   MIN(e.EVENT_TSTAMP)                                         AS touch_start_tstamp,
                   MAX(e.EVENT_TSTAMP)                                         AS touch_end_tstamp,
                   TIMEDIFF(seconds, MIN(e.EVENT_TSTAMP), MAX(e.EVENT_TSTAMP)) AS touch_duration_seconds,
                   COUNT(*)                                                    AS touch_event_count,
                   CASE
                       WHEN SUM(CASE WHEN e.EVENT_NAME = 'transaction_item' THEN 1 ELSE 0 END) > 0 THEN TRUE
                       ELSE FALSE END                                          AS touch_has_booking
            FROM MODULE_TOUCHIFICATION t
                     INNER JOIN EVENT_STREAM e ON t.EVENT_HASH = e.EVENT_HASH
--             WHERE t.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
            GROUP BY 1, 2, 3
        )

        SELECT DISTINCT t.TOUCH_ID,
                        a.ATTRIBUTED_USER_ID,
                        a.STITCHED_IDENTITY_TYPE,
                        a.TOUCH_START_TSTAMP,
                        a.TOUCH_END_TSTAMP,
                        a.TOUCH_DURATION_SECONDS,
                        FIRST_VALUE(e.POSA_TERRITORY)
                                    OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH) AS touch_posa_territory,
                        FIRST_VALUE(e.DEVICE_PLATFORM)
                                    OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH) AS touch_experience,
                        FIRST_VALUE(e.PAGE_URL)
                                    OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH) as touch_landing_page,
                        FIRST_VALUE(e.PAGE_URLPATH)
                                    OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH) as touch_landing_pagepath,
                        FIRST_VALUE(e.PAGE_URLHOST)
                                    OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH) as touch_hostname,
                        LAST_VALUE(e.PAGE_URLPATH)
                                   OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH)  as touch_exit_pagepath,
                        FIRST_VALUE(e.PAGE_REFERRER)
                                    OVER (PARTITION BY t.TOUCH_ID ORDER BY t.EVENT_INDEX_WITHIN_TOUCH) as touch_referrer_url,
                        a.TOUCH_EVENT_COUNT,
                        a.TOUCH_HAS_BOOKING,
                        CURRENT_TIMESTAMP                                                              AS updated_at --TODO: replace with '{schedule_tstamp}'
        FROM MODULE_TOUCHIFICATION t
                 INNER JOIN EVENT_STREAM e ON t.EVENT_HASH = e.EVENT_HASH
                 INNER JOIN agg_values a ON t.TOUCH_ID = a.TOUCH_ID
--         WHERE t.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
    ) AS BATCH ON TARGET.TOUCH_ID = BATCH.TOUCH_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     TOUCH_ID,
                     ATTRIBUTED_USER_ID,
                     STITCHED_IDENTITY_TYPE,
                     TOUCH_START_TSTAMP,
                     TOUCH_END_TSTAMP,
                     TOUCH_DURATION_SECONDS,
                     TOUCH_POSA_TERRITORY,
                     TOUCH_EXPERIENCE,
                     TOUCH_LANDING_PAGE,
                     TOUCH_LANDING_PAGEPATH,
                     TOUCH_HOSTNAME,
                     TOUCH_EXIT_PAGEPATH,
                     TOUCH_REFERRER_URL,
                     TOUCH_EVENT_COUNT,
                     TOUCH_HAS_BOOKING,
                     UPDATED_AT
        ) VALUES (BATCH.TOUCH_ID,
                  BATCH.ATTRIBUTED_USER_ID,
                  BATCH.STITCHED_IDENTITY_TYPE,
                  BATCH.TOUCH_START_TSTAMP,
                  BATCH.TOUCH_END_TSTAMP,
                  BATCH.TOUCH_DURATION_SECONDS,
                  BATCH.TOUCH_POSA_TERRITORY,
                  BATCH.TOUCH_EXPERIENCE,
                  BATCH.TOUCH_LANDING_PAGE,
                  BATCH.TOUCH_LANDING_PAGEPATH,
                  BATCH.TOUCH_HOSTNAME,
                  BATCH.TOUCH_EXIT_PAGEPATH,
                  BATCH.TOUCH_REFERRER_URL,
                  BATCH.TOUCH_EVENT_COUNT,
                  BATCH.TOUCH_HAS_BOOKING,
                  BATCH.UPDATED_AT)
    WHEN MATCHED
        THEN UPDATE SET
        TARGET.TOUCH_ID = BATCH.TOUCH_ID,
        TARGET.ATTRIBUTED_USER_ID = BATCH.ATTRIBUTED_USER_ID,
        TARGET.STITCHED_IDENTITY_TYPE = BATCH.STITCHED_IDENTITY_TYPE,
        TARGET.TOUCH_START_TSTAMP = BATCH.TOUCH_START_TSTAMP,
        TARGET.TOUCH_END_TSTAMP = BATCH.TOUCH_END_TSTAMP,
        TARGET.TOUCH_DURATION_SECONDS = BATCH.TOUCH_DURATION_SECONDS,
        TARGET.TOUCH_POSA_TERRITORY = BATCH.TOUCH_POSA_TERRITORY,
        TARGET.TOUCH_EXPERIENCE = BATCH.TOUCH_EXPERIENCE,
        TARGET.TOUCH_LANDING_PAGE = BATCH.TOUCH_LANDING_PAGE,
        TARGET.TOUCH_LANDING_PAGEPATH = BATCH.TOUCH_LANDING_PAGEPATH,
        TARGET.TOUCH_HOSTNAME = BATCH.TOUCH_HOSTNAME,
        TARGET.TOUCH_EXIT_PAGEPATH = BATCH.TOUCH_EXIT_PAGEPATH,
        TARGET.TOUCH_REFERRER_URL = BATCH.TOUCH_REFERRER_URL,
        TARGET.TOUCH_EVENT_COUNT = BATCH.TOUCH_EVENT_COUNT,
        TARGET.TOUCH_HAS_BOOKING = BATCH.TOUCH_HAS_BOOKING,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT;

------------------------------------------------------------------------------------------------------------------------
--assertions
--check all touches have attributes
SELECT CASE
           WHEN
                       (SELECT COUNT(DISTINCT TOUCH_ID) FROM MODULE_TOUCHIFICATION)
                   =
                       (SELECT COUNT(*) FROM MODULE_TOUCH_BASIC_ATTRIBUTES)
               THEN TRUE
           ELSE FALSE END AS ALL_TOUCHES_HAVE_ATTRIBUTES;

--unique touch ids
SELECT CASE WHEN COUNT(*) > 1 THEN FALSE ELSE TRUE END AS UNIQUE_TOUCH_ID
FROM (
         SELECT TOUCH_ID
         FROM MODULE_TOUCH_BASIC_ATTRIBUTES
         GROUP BY 1
         HAVING COUNT(*) > 1
     );