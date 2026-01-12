USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_IDENTITY_ASSOCIATIONS
(
    IDENTITY_FRAGMENT     VARCHAR,
    SE_USER_ID            VARCHAR,
    EMAIL_ADDRESS         VARCHAR,
    BOOKING_ID            VARCHAR,
    EARLIEST_EVENT_TSTAMP TIMESTAMP_NTZ,
    LATEST_EVENT_TSTAMP   TIMESTAMP_NTZ,
    UPDATED_AT            TIMESTAMP_LTZ
)
    CLUSTER BY (IDENTITY_FRAGMENT)
;

MERGE INTO MODULE_IDENTITY_ASSOCIATIONS AS TARGET
    USING (
        SELECT IDENTITY_FRAGMENT,                                                                               -- _every_ event will contain an identity fragment that can be used to backfill associations to
               SE_USER_ID,
               CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::varchar AS EMAIL_ADDRESS,         -- TODO: need to adjust when we can get email address from event level data.
               TI_ORDERID                                                             AS BOOKING_ID,
               MIN(EVENT_TSTAMP)                                                      AS EARLIEST_EVENT_TSTAMP, --needed to handle duplicate event user identifiers matching to secret escapes user identifier
               MAX(EVENT_TSTAMP)                                                      AS LATEST_EVENT_TSTAMP,
               CURRENT_TIMESTAMP                                                      AS UPDATED_AT             --TODO: replace with '{schedule_tstamp}'
        FROM EVENT_STREAM
        WHERE IDENTITY_FRAGMENT IS NOT NULL
          AND (
                SE_USER_ID IS NOT NULL
                OR CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address'] IS NOT NULL
                OR TI_ORDERID IS NOT NULL
            )
          -- AND UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
        GROUP BY 1, 2, 3, 4
    ) AS BATCH ON
                COALESCE(TARGET.IDENTITY_FRAGMENT::VARCHAR, '') =
                COALESCE(BATCH.IDENTITY_FRAGMENT::VARCHAR, '') AND
                COALESCE(TARGET.SE_USER_ID::VARCHAR, '') = COALESCE(BATCH.SE_USER_ID::VARCHAR, '') AND
                COALESCE(TARGET.EMAIL_ADDRESS::VARCHAR, '') = COALESCE(BATCH.EMAIL_ADDRESS::VARCHAR, '') AND
                COALESCE(TARGET.BOOKING_ID::VARCHAR, '') = COALESCE(BATCH.BOOKING_ID::VARCHAR, '')
    WHEN NOT MATCHED
        THEN INSERT (
                     IDENTITY_FRAGMENT,
                     SE_USER_ID,
                     EMAIL_ADDRESS,
                     BOOKING_ID,
                     EARLIEST_EVENT_TSTAMP,
                     LATEST_EVENT_TSTAMP,
                     UPDATED_AT
        )
        VALUES (BATCH.IDENTITY_FRAGMENT,
                BATCH.SE_USER_ID,
                BATCH.EMAIL_ADDRESS,
                BATCH.BOOKING_ID,
                BATCH.EARLIEST_EVENT_TSTAMP,
                BATCH.LATEST_EVENT_TSTAMP,
                BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.EARLIEST_EVENT_TSTAMP > BATCH.EARLIEST_EVENT_TSTAMP
        THEN UPDATE SET
        TARGET.EARLIEST_EVENT_TSTAMP = BATCH.EARLIEST_EVENT_TSTAMP,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
    WHEN MATCHED AND TARGET.LATEST_EVENT_TSTAMP < BATCH.LATEST_EVENT_TSTAMP
        THEN UPDATE SET
        TARGET.LATEST_EVENT_TSTAMP = BATCH.LATEST_EVENT_TSTAMP;

------------------------------------------------------------------------------------------------------------------------
--assertions
--unique list of identity associations
SELECT CASE
           WHEN
                       (SELECT COUNT(*) FROM MODULE_IDENTITY_ASSOCIATIONS)
                   =
                       (SELECT COUNT(*)
                        FROM (
                                 SELECT DISTINCT SE_USER_ID,
                                                 BOOKING_ID,
                                                 EMAIL_ADDRESS,
                                                 IDENTITY_FRAGMENT,
                                                 EARLIEST_EVENT_TSTAMP,
                                                 UPDATED_AT
                                 FROM MODULE_IDENTITY_ASSOCIATIONS)
                       ) THEN TRUE
           ELSE FALSE END AS UNQUE_IDENTITY_ASSOCIATIONS;

--correct amount of identity associations
SELECT CASE
           WHEN
                   (SELECT COUNT(*)
                    FROM MODULE_IDENTITY_ASSOCIATIONS) --75583194
                   =
                   (SELECT COUNT(*)
                    FROM (
                             SELECT IDENTITY_FRAGMENT,
                                    CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::varchar,
                                    SE_USER_ID,
                                    TI_ORDERID
                             FROM EVENT_STREAM
                             WHERE IDENTITY_FRAGMENT IS NOT NULL
                             GROUP BY 1, 2, 3, 4
                         )) --75348547
               THEN TRUE
           ELSE FALSE END AS CORRECT_NUMBER_OF_IDENTITY_ASSOCIATIONS
;