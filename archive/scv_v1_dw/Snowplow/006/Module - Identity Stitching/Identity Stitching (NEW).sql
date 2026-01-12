USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;

CREATE OR REPLACE TABLE MODULE_IDENTITY_STITCHING
(

    CREATED_AT             TIMESTAMP,
    UPDATED_AT             TIMESTAMP,

    ATTRIBUTED_USER_ID     VARCHAR,
    STITCHED_IDENTITY_TYPE VARCHAR,

    UNIQUE_BROWSER_ID      VARCHAR,
    COOKIE_ID              VARCHAR,
    SESSION_USERID         VARCHAR


);

MERGE INTO MODULE_IDENTITY_STITCHING AS TARGET
    USING (
        WITH last_value AS (
            --for each distinct combination of known identifiers get the last (non null) version of known identifiers
            --Cian confirmed that we should associate single unknown identities to multiple known identities to the most
            --the recent association.
            SELECT DISTINCT LAST_VALUE(SE_USER_ID)
                                       IGNORE NULLS OVER (PARTITION BY UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID
                                           ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT)
                                                                                                            AS ATTRIBUTED_SE_USER_ID,
                            LAST_VALUE(EMAIL_ADDRESS)
                                       IGNORE NULLS OVER (PARTITION BY UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID
                                           ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_EMAIL_ADDRESS,

                            LAST_VALUE(BOOKING_ID)
                                       IGNORE NULLS OVER (PARTITION BY UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID
                                           ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_BOOKING_ID,
                            UNIQUE_BROWSER_ID,
                            COOKIE_ID,
                            SESSION_USERID

            FROM MODULE_IDENTITY_ASSOCIATIONS
            -- WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
        )

        SELECT CURRENT_TIMESTAMP()::TIMESTAMP AS CREATED_AT,
               CURRENT_TIMESTAMP()::TIMESTAMP AS UPDATED_AT,


               COALESCE(ATTRIBUTED_SE_USER_ID,
                        ATTRIBUTED_EMAIL_ADDRESS,
                        ATTRIBUTED_BOOKING_ID,
                        UNIQUE_BROWSER_ID,
                        COOKIE_ID,
                        SESSION_USERID)       AS ATTRIBUTED_USER_ID, --enforce hierarchy of identifiers to associate with the most recent of a certain type
               CASE
                   WHEN ATTRIBUTED_SE_USER_ID IS NOT NULL THEN 'se_user_id'
                   WHEN ATTRIBUTED_EMAIL_ADDRESS IS NOT NULL THEN 'email_address'
                   WHEN ATTRIBUTED_BOOKING_ID IS NOT NULL THEN 'booking_id'
                   WHEN UNIQUE_BROWSER_ID IS NOT NULL THEN 'unique_browser_id'
                   WHEN COOKIE_ID IS NOT NULL THEN 'cookie_id'
                   WHEN SESSION_USERID IS NOT NULL THEN 'session_userid'
                   END
                                              AS STITCHED_IDENTITY_TYPE,
               UNIQUE_BROWSER_ID,
               COOKIE_ID,
               SESSION_USERID


        FROM last_value
    ) AS BATCH ON TARGET.UNIQUE_BROWSER_ID IS NOT DISTINCT FROM BATCH.UNIQUE_BROWSER_ID AND
                  TARGET.COOKIE_ID IS NOT DISTINCT FROM BATCH.COOKIE_ID AND
                  TARGET.SESSION_USERID IS NOT DISTINCT FROM BATCH.SESSION_USERID
    WHEN NOT MATCHED
        THEN INSERT (CREATED_AT,
                     UPDATED_AT,
                     ATTRIBUTED_USER_ID,
                     STITCHED_IDENTITY_TYPE,
                     UNIQUE_BROWSER_ID,
                     COOKIE_ID,
                     SESSION_USERID
        )
        VALUES (BATCH.CREATED_AT,
                BATCH.UPDATED_AT,
                BATCH.ATTRIBUTED_USER_ID,
                BATCH.STITCHED_IDENTITY_TYPE,
                BATCH.UNIQUE_BROWSER_ID,
                BATCH.COOKIE_ID,
                BATCH.SESSION_USERID)
    WHEN MATCHED AND TARGET.ATTRIBUTED_USER_ID != BATCH.ATTRIBUTED_USER_ID
        THEN UPDATE SET
        TARGET.ATTRIBUTED_USER_ID = BATCH.ATTRIBUTED_USER_ID,
        TARGET.STITCHED_IDENTITY_TYPE = BATCH.STITCHED_IDENTITY_TYPE,
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

DROP TABLE cus