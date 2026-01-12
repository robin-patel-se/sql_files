USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;

CREATE OR REPLACE TABLE MODULE_IDENTITY_ASSOCIATIONS
(

    CREATED_AT            TIMESTAMP,
    UPDATED_AT            TIMESTAMP,

    --known identities - identities that can resolve to an actual person
    SE_USER_ID            VARCHAR,
    EMAIL_ADDRESS         VARCHAR,
    BOOKING_ID            VARCHAR,

    --unknown identities - identities that anonymously identify a person
    UNIQUE_BROWSER_ID     VARCHAR,
    COOKIE_ID             VARCHAR,
    SESSION_USERID        VARCHAR,


    EARLIEST_EVENT_TSTAMP TIMESTAMP,
    LATEST_EVENT_TSTAMP   TIMESTAMP
)
    CLUSTER BY (UNIQUE_BROWSER_ID)
;

MERGE INTO MODULE_IDENTITY_ASSOCIATIONS AS TARGET
    USING (
        SELECT CURRENT_TIMESTAMP()::TIMESTAMP AS CREATED_AT,
               CURRENT_TIMESTAMP()::TIMESTAMP AS UPDATED_AT,

               SE_USER_ID,
               EMAIL_ADDRESS,
               BOOKING_ID,

               UNIQUE_BROWSER_ID,
               COOKIE_ID,
               SESSION_USERID,

               MIN(EVENT_TSTAMP)              AS EARLIEST_EVENT_TSTAMP, --needed to handle duplicate event user identifiers matching to secret escapes user identifier
               MAX(EVENT_TSTAMP)              AS LATEST_EVENT_TSTAMP
        FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM
--         WHERE SCHEDULE_TSTAMP >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP)

        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ) AS BATCH ON
        --merge in new distinct associations
            TARGET.SE_USER_ID IS NOT DISTINCT FROM BATCH.SE_USER_ID AND
            TARGET.EMAIL_ADDRESS IS NOT DISTINCT FROM BATCH.EMAIL_ADDRESS AND
            TARGET.BOOKING_ID IS NOT DISTINCT FROM BATCH.BOOKING_ID AND
            TARGET.UNIQUE_BROWSER_ID IS NOT DISTINCT FROM BATCH.UNIQUE_BROWSER_ID AND
            TARGET.COOKIE_ID IS NOT DISTINCT FROM BATCH.COOKIE_ID AND
            TARGET.SESSION_USERID IS NOT DISTINCT FROM BATCH.SESSION_USERID
    WHEN NOT MATCHED
        THEN INSERT (CREATED_AT,
                     UPDATED_AT,
                     SE_USER_ID,
                     EMAIL_ADDRESS,
                     BOOKING_ID,
                     UNIQUE_BROWSER_ID,
                     COOKIE_ID,
                     SESSION_USERID,
                     EARLIEST_EVENT_TSTAMP,
                     LATEST_EVENT_TSTAMP
        )
        VALUES (BATCH.CREATED_AT,
                BATCH.UPDATED_AT,
                BATCH.SE_USER_ID,
                BATCH.EMAIL_ADDRESS,
                BATCH.BOOKING_ID,
                BATCH.UNIQUE_BROWSER_ID,
                BATCH.COOKIE_ID,
                BATCH.SESSION_USERID,
                BATCH.EARLIEST_EVENT_TSTAMP,
                BATCH.LATEST_EVENT_TSTAMP)
    --When a late arriving event has come in that updates the earliest time we have seen this association
    WHEN MATCHED AND TARGET.EARLIEST_EVENT_TSTAMP > BATCH.EARLIEST_EVENT_TSTAMP
        THEN UPDATE SET
        TARGET.EARLIEST_EVENT_TSTAMP = BATCH.EARLIEST_EVENT_TSTAMP,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
    --When another association has come in that updates the latest timestamp we have seen this association
    WHEN MATCHED AND TARGET.LATEST_EVENT_TSTAMP < BATCH.LATEST_EVENT_TSTAMP
        THEN UPDATE SET
        TARGET.LATEST_EVENT_TSTAMP = BATCH.LATEST_EVENT_TSTAMP,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
;
