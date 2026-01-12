USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_TOUCHED_SIGNUPS
(
    EVENT_HASH        VARCHAR,
    TOUCH_ID          VARCHAR,
    EVENT_TSTAMP      TIMESTAMPNTZ,
    BOOKING_ID        VARCHAR,
    EVENT_CATEGORY    VARCHAR,
    EVENT_SUBCATEGORY VARCHAR,
    UPDATED_AT        TIMESTAMP_LTZ
);

WITH first_signup_event AS (
    --identify each user's first event with login type = 'REGISTERED'
    SELECT DISTINCT FIRST_VALUE(e.EVENT_HASH)
                                OVER (PARTITION BY i.ATTRIBUTED_USER_ID ORDER BY e.EVENT_TSTAMP) AS FIRST_SIGN_UP_EVENT_HASH
    FROM MODULE_EXTRACTED_PARAMS p
             INNER JOIN EVENT_STREAM e ON e.PAGE_URL = p.URL
             INNER JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
    WHERE p.ACCOUNT_VERIFIED = 'true'
)
SELECT f.FIRST_SIGN_UP_EVENT_HASH AS event_hash,
       t.TOUCH_ID                 as touch_id,
       t.ATTRIBUTED_USER_ID,
       'page views'               AS event_category,
       'sign up'                  AS event_subcategory,
       CURRENT_TIMESTAMP          AS updated_at --TODO: replace with '{schedule_tstamp}'

FROM first_signup_event f
         INNER JOIN MODULE_TOUCHIFICATION t ON t.EVENT_HASH = f.FIRST_SIGN_UP_EVENT_HASH;
