USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;
------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------TOUCHIFICATION------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--touchification, grouping of events based on utm/referrer change, time lag change, platform change
--first run


CREATE OR REPLACE TABLE MODULE_TOUCHIFICATION
(
    EVENT_HASH               VARCHAR,
    ATTRIBUTED_USER_ID       VARCHAR,
    STITCHED_IDENTITY_TYPE   VARCHAR,
    EVENT_TSTAMP             TIMESTAMP,
    TOUCH_ID                 VARCHAR,
    EVENT_INDEX_WITHIN_TOUCH INT,
    UPDATED_AT               TIMESTAMP_LTZ
)
;

MERGE INTO MODULE_TOUCHIFICATION AS TARGET
    USING (
        SELECT e.event_hash,
               i.ATTRIBUTED_USER_ID,
               i.STITCHED_IDENTITY_TYPE,
               e.EVENT_TSTAMP,
               FIRST_VALUE(e.EVENT_HASH)
                           OVER (PARTITION BY i.ATTRIBUTED_USER_ID, d.TIME_DIFF_PARTITION, u.UTM_REF_PARTITION, e.DEVICE_PLATFORM
                               ORDER BY e.EVENT_TSTAMP) AS touch_id,
               ROW_NUMBER()
                       OVER (PARTITION BY i.ATTRIBUTED_USER_ID, d.TIME_DIFF_PARTITION, u.UTM_REF_PARTITION, e.DEVICE_PLATFORM
                           ORDER BY e.EVENT_TSTAMP)     AS event_index_within_touch,
               CURRENT_TIMESTAMP                        AS updated_at --TODO: replace with '{schedule_tstamp}'
        FROM MODULE_TOUCHIFIABLE_EVENTS e
                 INNER JOIN MODULE_IDENTITY_STITCHING i
                            ON e.IDENTITY_FRAGMENT = i.IDENTITY_FRAGMENT
                 INNER JOIN MODULE_TIME_DIFF_MARKER d ON e.EVENT_HASH = d.EVENT_HASH
                 INNER JOIN MODULE_UTM_REFERRER_MARKER u ON e.EVENT_HASH = u.EVENT_HASH
    ) AS BATCH ON TARGET.EVENT_HASH = BATCH.EVENT_HASH
    WHEN NOT MATCHED
        THEN INSERT (
                     EVENT_HASH,
                     ATTRIBUTED_USER_ID,
                     STITCHED_IDENTITY_TYPE,
                     EVENT_TSTAMP,
                     TOUCH_ID,
                     EVENT_INDEX_WITHIN_TOUCH,
                     UPDATED_AT
        ) VALUES (BATCH.EVENT_HASH,
                  BATCH.ATTRIBUTED_USER_ID,
                  BATCH.STITCHED_IDENTITY_TYPE,
                  BATCH.EVENT_TSTAMP,
                  BATCH.TOUCH_ID,
                  BATCH.EVENT_INDEX_WITHIN_TOUCH,
                  BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.TOUCH_ID != BATCH.TOUCH_ID OR TARGET.ATTRIBUTED_USER_ID != BATCH.ATTRIBUTED_USER_ID
        THEN UPDATE SET
        TARGET.EVENT_HASH = BATCH.EVENT_HASH,
        TARGET.ATTRIBUTED_USER_ID = BATCH.ATTRIBUTED_USER_ID,
        TARGET.STITCHED_IDENTITY_TYPE = BATCH.STITCHED_IDENTITY_TYPE,
        TARGET.EVENT_TSTAMP = BATCH.EVENT_TSTAMP,
        TARGET.TOUCH_ID = BATCH.TOUCH_ID,
        TARGET.EVENT_INDEX_WITHIN_TOUCH = BATCH.EVENT_INDEX_WITHIN_TOUCH,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
;
------------------------------------------------------------------------------------------------------------------------
--assertions
--unique event hash occurrence
SELECT CASE WHEN COUNT(*) > 1 THEN FALSE ELSE TRUE END AS UNIQUE_HASH_FOR_TOUCHIFICATION
FROM (
         SELECT EVENT_HASH
         FROM MODULE_TOUCHIFICATION
         GROUP BY 1
         HAVING COUNT(*) > 1
     );

--check that all the events that should be touchified have done and the number of rows match.
SELECT CASE
           WHEN
                       (SELECT COUNT(*) FROM MODULE_TOUCHIFICATION) != --541548135
                       (SELECT COUNT(*)
                        FROM EVENT_STREAM e
                        WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction')
                          AND e.IS_ROBOT_SPIDER_EVENT = FALSE --note the filters on this are the same as those that go into touchifiable events
                          AND e.IDENTITY_FRAGMENT IS NOT NULL
                       ) THEN FALSE
           ELSE TRUE END AS ALL_TOUCHIFIABLE_EVENTS_TOUCHIFIED;

