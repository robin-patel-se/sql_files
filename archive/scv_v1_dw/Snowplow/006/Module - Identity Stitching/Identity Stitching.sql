USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_IDENTITY_STITCHING
(
    ATTRIBUTED_USER_ID     VARCHAR,
    STITCHED_IDENTITY_TYPE VARCHAR,
    IDENTITY_FRAGMENT      VARCHAR,
    UPDATED_AT             TIMESTAMP_LTZ
);

MERGE INTO MODULE_IDENTITY_STITCHING AS TARGET
    USING (
        WITH last_value AS
                 (SELECT DISTINCT LAST_VALUE(SE_USER_ID)
                                             IGNORE NULLS OVER (PARTITION BY IDENTITY_FRAGMENT
                                                 ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT)
                                                                                                                  AS ATTRIBUTED_SE_USER_ID,
                                  LAST_VALUE(EMAIL_ADDRESS)
                                             IGNORE NULLS OVER (PARTITION BY IDENTITY_FRAGMENT
                                                 ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_EMAIL_ADDRESS,

                                  LAST_VALUE(BOOKING_ID)
                                             IGNORE NULLS OVER (PARTITION BY IDENTITY_FRAGMENT
                                                 ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_BOOKING_ID,
                                  IDENTITY_FRAGMENT

                  FROM MODULE_IDENTITY_ASSOCIATIONS
                     -- WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
                 )

        SELECT coalesce(ATTRIBUTED_SE_USER_ID, ATTRIBUTED_EMAIL_ADDRESS, ATTRIBUTED_BOOKING_ID,
                        IDENTITY_FRAGMENT) AS ATTRIBUTED_USER_ID, --enforce hierarchy of identifiers to associate with the most recent of a certain type
               CASE
                   WHEN ATTRIBUTED_SE_USER_ID IS NOT NULL THEN 'se_user_id'
                   WHEN ATTRIBUTED_EMAIL_ADDRESS IS NOT NULL THEN 'email_address'
                   WHEN ATTRIBUTED_BOOKING_ID IS NOT NULL THEN 'booking_id'
                   WHEN IDENTITY_FRAGMENT IS NOT NULL THEN 'snowplow_domain_userid'
                   END
                                           AS stitched_identity_type,
               IDENTITY_FRAGMENT,
               CURRENT_TIMESTAMP           AS updated_at          --TODO: replace with '{schedule_tstamp}'

        FROM last_value
    ) AS BATCH ON TARGET.IDENTITY_FRAGMENT = BATCH.IDENTITY_FRAGMENT
    WHEN NOT MATCHED
        THEN INSERT (ATTRIBUTED_USER_ID,
                     STITCHED_IDENTITY_TYPE,
                     IDENTITY_FRAGMENT,
                     UPDATED_AT)
        VALUES (BATCH.ATTRIBUTED_USER_ID,
                BATCH.STITCHED_IDENTITY_TYPE,
                BATCH.IDENTITY_FRAGMENT,
                BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.ATTRIBUTED_USER_ID != BATCH.ATTRIBUTED_USER_ID
        THEN UPDATE SET
        TARGET.ATTRIBUTED_USER_ID = BATCH.ATTRIBUTED_USER_ID,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
;


------------------------------------------------------------------------------------------------------------------------
--assertions
--unique event user identifier
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_IDENTITY_FRAGMENT
FROM (
         SELECT IDENTITY_FRAGMENT,
                COUNT(*)
         FROM MODULE_IDENTITY_STITCHING
         GROUP BY 1
         HAVING COUNT(*) > 1
     );


--test whether all the event user identifiers have been stitched
SELECT CASE
           WHEN
                       (SELECT COUNT(DISTINCT IDENTITY_FRAGMENT) FROM EVENT_STREAM)
                   =
                       (SELECT COUNT(*) FROM MODULE_IDENTITY_STITCHING) THEN TRUE
           ELSE FALSE END AS ALL_IDENTITY_FRAGMENTS_STITCHED;


--
SELECT CASE
           WHEN
                   (SELECT COUNT(*)
                    FROM MODULE_IDENTITY_STITCHING
                    WHERE ATTRIBUTED_USER_ID IS NULL)
                   > 0 THEN FALSE
           ELSE TRUE END
           AS ALL_EVENT_IDENTIFIERS_HAVE_ATTRIBUTED_USER_ID;