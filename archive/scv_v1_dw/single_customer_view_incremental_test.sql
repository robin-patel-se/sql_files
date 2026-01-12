DROP SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;
DROP SCHEMA HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_IDENTITY_STITCHING;
UNDROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_IDENTITY_STITCHING;
DROP TABLE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.module_touched_transactions;
DROP TABLE HYGIENE_VAULT_MVP_DEV_ROBIN.CMS_MYSQL.SALE;

USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;

SELECT COUNT(*)
FROM (
         SELECT EVENT_HASH
         FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM
         GROUP BY 1
         HAVING COUNT(*) > 1
     );

SELECT COUNT(*)
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE ETL_TSTAMP::DATE >= '2018-01-01';

SELECT e.EVENT_HASH,
       e.UNIQUE_BROWSER_ID,
       e.COOKIE_ID,
       e.SESSION_USERID
FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
WHERE (UNIQUE_BROWSER_ID IS NOT NULL
    OR COOKIE_ID IS NOT NULL
    OR SESSION_USERID IS NOT NULL);

SELECT COUNT(*)
FROM (
         SELECT e.EVENT_HASH,
                e.UNIQUE_BROWSER_ID,
                e.COOKIE_ID,
                e.SESSION_USERID
         FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
                  LEFT JOIN DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_IDENTITY_ASSOCIATIONS ia
                            ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                               COALESCE(ia.UNIQUE_BROWSER_ID, ia.COOKIE_ID, ia.SESSION_USERID)
         WHERE ia.SCHEDULE_TSTAMP IS NULL
           AND (e.UNIQUE_BROWSER_ID IS NOT NULL
             OR e.COOKIE_ID IS NOT NULL
             OR e.SESSION_USERID IS NOT NULL));
;

USE WAREHOUSE PIPE_LARGE;
USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;

SELECT e.EVENT_HASH,
       e.EVENT_TSTAMP,
       e.DERIVED_TSTAMP,
       e.EVENT_NAME,
       e.PAGE_URL,
       e.PAGE_REFERRER,
       e.DEVICE_PLATFORM,
       e.UNIQUE_BROWSER_ID,
       e.COOKIE_ID,
       e.SESSION_USERID,
       i.ATTRIBUTED_USER_ID
FROM SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFIABLE_EVENTS e
         INNER JOIN MODULE_IDENTITY_STITCHING i ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                                   COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
WHERE i.ATTRIBUTED_USER_ID IS NULL;


SELECT EVENT_NAME,
       EVENT,
       COUNT(*)
FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID) IS NOT NULL
GROUP BY 1, 2;

select *
from RAW_VAULT_MVP.CMS_MYSQL.RESERVATION
where id = 495418;
select *
from RAW_VAULT_MVP.CMS_MYSQL.BOOKING
where id = 38771883;



SELECT ATTRIBUTED_USER_ID,
       COUNT(*)
FROM SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
GROUP BY ATTRIBUTED_USER_ID
HAVING COUNT(*) > 1
ORDER BY 2 DESC;

SELECT e.event_tstamp,
       e.derived_tstamp,
       e.device_platform,
       e.dvce_created_tstamp,
       e.dvce_sent_tstamp,
       e.collector_tstamp,
       e.v_tracker,
       e.app_id,
       e.event_hash,
       t.*
FROM SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION t
         LEFT JOIN HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.event_stream e ON t.EVENT_HASH = e.event_hash
WHERE ATTRIBUTED_USER_ID = '40749369'
ORDER BY t.EVENT_TSTAMP;

USE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;

SELECT EVENT_HASH, COUNT(*)
FROM MODULE_TIME_DIFF_MARKER
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT EVENT_HASH, COUNT(*)
FROM MODULE_TOUCHIFICATION
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT EVENT_HASH, COUNT(*)
FROM MODULE_UTM_REFERRER_MARKER
GROUP BY 1
HAVING COUNT(*) > 1;

TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker;



SELECT e.EVENT_HASH

FROM MODULE_TOUCHIFIABLE_EVENTS e
         INNER JOIN MODULE_IDENTITY_STITCHING i
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
group by 1
having count(*) > 1;

SElect COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID),
       count(*)
FROM MODULE_IDENTITY_STITCHING
group by 1
having count(*) > 1;


SELECT DISTINCT LAST_VALUE(SE_USER_ID)
                           IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                               ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT)
                                                                                                AS ATTRIBUTED_SE_USER_ID,
                LAST_VALUE(EMAIL_ADDRESS)
                           IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                               ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_EMAIL_ADDRESS,

                LAST_VALUE(BOOKING_ID)
                           IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                               ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_BOOKING_ID,

                LAST_VALUE(UNIQUE_BROWSER_ID)
                           IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                               ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_UNIQUE_BROWSER_ID,

                LAST_VALUE(COOKIE_ID)
                           IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                               ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_COOKIE_ID,

                LAST_VALUE(SESSION_USERID)
                           IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                               ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_SESSION_USERID

FROM MODULE_IDENTITY_ASSOCIATIONS;

SELECT *
FROM MODULE_IDENTITY_STITCHING
WHERE COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID) = '958d6526-e8b9-4df2-b0fb-9cfec825c10f'



WITH new_associations AS (
    -- get a distinct list of the unknown identifiers coalesced by importance (identity fragment) that have had a new association
    SELECT DISTINCT COALESCE(UNIQUE_BROWSER_ID,
                             COOKIE_ID,
                             SESSION_USERID) AS identity_fragment
    FROM MODULE_IDENTITY_ASSOCIATIONS
    WHERE CREATED_AT = '2020-02-07 14:30:38.493000000'
)
   --reprocess all associations for any association that match the coalesced identity fragment
   , last_value AS (
    SELECT DISTINCT LAST_VALUE(SE_USER_ID)
                               IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                                   ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT)
                                                                                                    AS ATTRIBUTED_SE_USER_ID,
                    LAST_VALUE(EMAIL_ADDRESS)
                               IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                                   ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_EMAIL_ADDRESS,

                    LAST_VALUE(BOOKING_ID)
                               IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                                   ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_BOOKING_ID,

                    LAST_VALUE(UNIQUE_BROWSER_ID)
                               IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                                   ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_UNIQUE_BROWSER_ID,

                    LAST_VALUE(COOKIE_ID)
                               IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                                   ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_COOKIE_ID,

                    LAST_VALUE(SESSION_USERID)
                               IGNORE NULLS OVER (PARTITION BY COALESCE(UNIQUE_BROWSER_ID, COOKIE_ID, SESSION_USERID)
                                   ORDER BY EARLIEST_EVENT_TSTAMP, LATEST_EVENT_TSTAMP, UPDATED_AT) AS ATTRIBUTED_SESSION_USERID
    FROM MODULE_IDENTITY_ASSOCIATIONS
    WHERE COALESCE(UNIQUE_BROWSER_ID,
                   COOKIE_ID,
                   SESSION_USERID) IN
          (SELECT identity_fragment FROM new_associations)
)
SELECT
    --enforce hierarchy of identifiers to associate with the most recent of a certain type
    COALESCE(ATTRIBUTED_SE_USER_ID,
             ATTRIBUTED_EMAIL_ADDRESS,
             ATTRIBUTED_BOOKING_ID,
             ATTRIBUTED_UNIQUE_BROWSER_ID,
             ATTRIBUTED_COOKIE_ID,
             ATTRIBUTED_SESSION_USERID) AS ATTRIBUTED_USER_ID,
    CASE
        WHEN ATTRIBUTED_SE_USER_ID IS NOT NULL THEN 'se_user_id'
        WHEN ATTRIBUTED_EMAIL_ADDRESS IS NOT NULL THEN 'email_address'
        WHEN ATTRIBUTED_BOOKING_ID IS NOT NULL THEN 'booking_id'
        WHEN ATTRIBUTED_UNIQUE_BROWSER_ID IS NOT NULL THEN 'unique_browser_id'
        WHEN ATTRIBUTED_COOKIE_ID IS NOT NULL THEN 'cookie_id'
        WHEN ATTRIBUTED_SESSION_USERID IS NOT NULL THEN 'session_userid'
        END
                                        AS STITCHED_IDENTITY_TYPE,
    ATTRIBUTED_UNIQUE_BROWSER_ID        AS UNIQUE_BROWSER_ID,
    ATTRIBUTED_COOKIE_ID                AS COOKIE_ID,
    ATTRIBUTED_SESSION_USERID           AS SESSION_USERID

FROM last_value
;


-- 2020-02-07 14:39:54.507000000


SELECT CREATED_AT, count(*)
FROM MODULE_IDENTITY_ASSOCIATIONS
group by 1;

DELETE
FROM MODULE_IDENTITY_ASSOCIATIONS
WHERE CREATED_AT = '2020-02-07 14:39:54.507000000';

-- for any recent associations that have appeared we want to coalesce the unknown identifiers and then process all

SELECT CASE
           WHEN
                       (SELECT COUNT(*) FROM MODULE_TOUCHIFICATION) != --541548135
                       (SELECT COUNT(*)
                        FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
                        WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction')
                          AND e.IS_ROBOT_SPIDER_EVENT = FALSE --note the filters on this are the same as those that go into touchifiable events
                          AND COALESCE(e.UNIQUE_BROWSER_ID,
                                       e.COOKIE_ID,
                                       e.SESSION_USERID) IS NOT NULL
                       ) THEN FALSE
           ELSE TRUE END AS ALL_TOUCHIFIABLE_EVENTS_TOUCHIFIED;


SELECT *
FROM MODULE_TOUCH_UTM_REFERRER;

SELECT

--        CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['sale_id']::VARCHAR,
--        LENGTH(CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['sale_id']::VARCHAR)
CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['id']::VARCHAR,
LENGTH(CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['id']::VARCHAR)
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE ETL_TSTAMP::DATE = '2020-02-03'
  AND CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1 IS NOT NULL
-- CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['sale_id']::VARCHAR IS NOT NULL
ORDER BY 2 DESC;


WITH users_with_events_in_30m AS (
    SELECT i.attributed_user_id
    FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
             INNER JOIN MODULE_IDENTITY_STITCHING i ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
    WHERE e.EVENT_TSTAMP < TIMEADD(min, 35, CURRENT_TIMESTAMP()) -- schedule tstamp
--     AND e.SCHEDULE_TSTAMP >= TIMESTAMPADD('day', -1, '{schedule_tstamp}'::TIMESTAMP)
),
     last_touch_id as (
         SELECT DISTINCT LAST_VALUE(TOUCH_ID)
                                    OVER (PARTITION BY ATTRIBUTED_USER_ID ORDER BY EVENT_TSTAMP) as last_touch_id
         FROM MODULE_TOUCHIFICATION
         WHERE ATTRIBUTED_USER_ID IN (SELECT * FROM users_with_events_in_30m)
           AND EVENT_TSTAMP > DATEADD(day, -1, CURRENT_TIMESTAMP()) -- schedule tstamp, only reprocess 1 days worth of data
     )

SELECT e.EVENT_HASH,
       e.EVENT_TSTAMP,
       e.DERIVED_TSTAMP,
       e.EVENT_NAME,
       e.PAGE_URL,
       e.PAGE_REFERRER,
       e.DEVICE_PLATFORM,
       e.UPDATED_AT
FROM MODULE_TOUCHIFICATION t
         INNER JOIN HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e ON t.EVENT_HASH = e.EVENT_HASH
WHERE t.TOUCH_ID IN (SELECT * FROM last_touch_id)
;


SELECT TOUCH_MKT_CHANNEL,
       COUNT(*)
FROM MODULE_TOUCH_MARKETING_CHANNEL
GROUP BY 1;

SELECT TOUCH_MKT_CHANNEL,
       COUNT(*)
FROM MODULE_TOUCH_MARKETING_CHANNEL
-- WHERE TOUCH_MKT_CHANNEL = 'PPC - Undefined'
GROUP BY 1;

SELECT AFFILIATE,
       COUNT(*)
FROM MODULE_TOUCH_MARKETING_CHANNEL
WHERE TOUCH_MKT_CHANNEL = 'PPC - Undefined'
GROUP BY 1

SELECT TOUCH_ID,
       TOUCH_MKT_CHANNEL,
       TOUCH_LANDING_PAGE,
       ATTRIBUTED_USER_ID,
       UTM_CAMPAIGN,
       UTM_MEDIUM,
       UTM_SOURCE,
       UTM_TERM,
       UTM_CONTENT,
       CLICK_ID,
       SUB_AFFILIATE_NAME,
       AFFILIATE,
       AWADGROUPID,
       AWCAMPAIGNID,
       REFERRER_HOSTNAME,
       REFERRER_MEDIUM,
       SCHEDULE_TSTAMP
FROM MODULE_TOUCH_MARKETING_CHANNEL
WHERE TOUCH_MKT_CHANNEL = 'PPC - Undefined';


SELECT DISTINCT ETL_TSTAMP::DATE
FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM;

USE WAREHOUSE PIPE_XLARGE;

CREATE OR REPLACE TRANSIENT TABLE SCRATCH.ROBINPATEL.feb_03_atomic_events AS
    (
        SELECT *
        FROM SNOWPLOW.ATOMIC.EVENTS
        WHERE ETL_TSTAMP::DATE = '2020-02-03'
    );
--   AND LENGTH(SE_LABEL) > 999;

SELECT *
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE LENGTH(SE_LABEL) > 1000;

DROP TABLE SCRATCH.ROBINPATEL.feb_03_atomic_events;

SELECT ETL_TSTAMP::DATE,
       COUNT(*)
FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM
GROUP BY 1;


SELECT ETL_TSTAMP::DATE,
       COUNT(*)
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE ETL_TSTAMP::DATE >= '2020-02-11'
GROUP BY 1;

SELECT COUNT(distinct touch_id),
       count(*)
FROM MODULE_TOUCHIFICATION;

SELECT SE_LABEL
FROM SCRATCH.ROBINPATEL.feb_03_atomic_events
WHERE LENGTH(SE_LABEL) > 1000;


CREATE OR REPLACE TABLE SCRATCH.ROBINPATELDEV35777.SPVS_BY_CUTTABLE_DIMENSIONS AS (
    SELECT b.TOUCH_START_TSTAMP::DATE                   AS TOUCH_START_DATE,
           b.TOUCH_POSA_TERRITORY,
           b.TOUCH_EXPERIENCE,
           b.TOUCH_HOSTNAME,
           c.TOUCH_MKT_CHANNEL                          AS LAST_NON_DIRECT_MKT_CHANNEL,
           sa.PRODUCT_TYPE,
           sa.PRODUCT_CONFIGURATION,
           sa.PRODUCT_LINE,
           COUNT(distinct b.ATTRIBUTED_USER_ID)         AS USERS,
           COUNT(distinct spv.TOUCH_ID)                 AS TOUCHES,
           COUNT(spv.EVENT_HASH)                        AS SPVs,
           COUNT(distinct spv.SE_SALE_ID, spv.TOUCH_ID) AS UNIQUE_SPVS
    FROM MODULE_TOUCH_BASIC_ATTRIBUTES b
             INNER JOIN MODULE_TOUCH_ATTRIBUTION a
                        ON b.TOUCH_ID = a.TOUCH_ID AND a.ATTRIBUTION_MODEL = 'last non direct'
             INNER JOIN MODULE_TOUCH_MARKETING_CHANNEL c ON a.ATTRIBUTED_TOUCH_ID = c.TOUCH_ID
             INNER JOIN MODULE_EVENTS_OF_INTEREST spv ON b.TOUCH_ID = spv.TOUCH_ID AND spv.EVENT_SUBCATEGORY = 'SPV'
             INNER JOIN MODULE_CURRENT_SALE_ATTRIBUTES sa ON spv.SE_SALE_ID = sa.SALE_ID
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ORDER BY 1
);

ALTER TABLE SCRATCH.ROBINPATEL.feb_03_atomic_events
    ALTER SE_LABEL SET DATA TYPE VARCHAR;

SELECT UPDATED_AT, COUNT(*)
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_IDENTITY_STITCHING
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--checking ubid in stitching

SELECT COUNT(*),
       COUNT(DISTINCT ATTRIBUTED_USER_ID)

FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_IDENTITY_STITCHING
WHERE UNIQUE_BROWSER_ID IS NOT NULL;

------------------------------------------------------------------------------------------------------------------------
-- touchifiable events

--new events
SELECT COUNT(*)

FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
WHERE EVENT_NAME IN
      ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
  AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
  AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
  AND e.SCHEDULE_TSTAMP >= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP);
--5203412

--restitched
SELECT COUNT(*)

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
WHERE e.EVENT_NAME IN
      ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
  AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
  AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
  AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP);
--1691860809

--interval users
WITH users_with_events_in_35m AS (
    -- first identify members that have had events within the first 35 mins of the schedule interval
    SELECT i.attributed_user_id
    FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
             INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                        ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                           COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
    WHERE e.EVENT_TSTAMP < TIMESTAMPADD('min', 35, '2020-02-20 00:00:00'::TIMESTAMP)
      AND e.SCHEDULE_TSTAMP >= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP)
),
     last_touch_id as (
         --query these users for their most recent touch id.
         SELECT DISTINCT LAST_VALUE(TOUCH_ID)
                                    OVER (PARTITION BY ATTRIBUTED_USER_ID ORDER BY EVENT_TSTAMP) as last_touch_id
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
         WHERE ATTRIBUTED_USER_ID IN (SELECT * FROM users_with_events_in_35m)
           AND EVENT_TSTAMP > TIMESTAMPADD('min', -35, '2020-02-20 00:00:00'::TIMESTAMP)
     )
     --select events that belong to the user's most recent touch.
SELECT COUNT(*)

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
WHERE t.TOUCH_ID IN (SELECT * FROM last_touch_id)
  AND e.EVENT_NAME IN
      ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
  AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
;
--480944

--late arriving users
WITH late_arriving_users AS ( -- identify event user identifiers that have late arriving records
    SELECT DISTINCT e.UNIQUE_BROWSER_ID,
                    e.COOKIE_ID,
                    e.SESSION_USERID
    FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream e
    WHERE e.EVENT_NAME IN
          ('page_view', 'screen_view', 'transaction_item', 'transaction')                -- explicitly define the events we want to touchify
      AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL       -- we only want to sessionise events that can be attributed to a user
      AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                                -- remove extra computation required to resessionise robot events
      AND e.SCHEDULE_TSTAMP >= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP) -- newly processed event
      AND e.EVENT_TSTAMP <= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP) -- late arriving event
)
SELECT COUNT(*)
FROM late_arriving_users l
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(l.UNIQUE_BROWSER_ID, l.COOKIE_ID, l.SESSION_USERID)
WHERE e.EVENT_NAME IN
      ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
  AND e.IS_ROBOT_SPIDER_EVENT = FALSE;
--28183342

------------------------------------------------------------------------------------------------------------------------
-- looking at restitched query
SELECT COUNT(*)

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
WHERE e.EVENT_NAME IN
      ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
  AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
  AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
  AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP);


SELECT i.UPDATED_AT,
       TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP),
       COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
WHERE i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-20 00:00:00'::TIMESTAMP)
GROUP BY 1, 2;



------------------------------------------------------------------------------------------------------------------------
--failed assertion on 22nd of Feb
--BACKUP SCHEMA
CREATE OR REPLACE SCHEMA DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG_BK CLONE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG;
CREATE OR REPLACE SCHEMA HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW_BK CLONE HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW;

/*        Transform Assertion (kind=exception) :: AssertNoDuplicates FAILED: 433987851 = 433987632 (diff 219). Case detail: We have duplicates for key ('TOUCH_ID')in table 'data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone'
        baseline_query:
        SELECT
            COUNT(*)
        FROM
            data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone
        ;

	comparative_query:
        SELECT
            COUNT(*)
        FROM
            (
        SELECT
        DISTINCT TOUCH_ID AS TOUCH_ID
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone
            )
        ;

	-------
	baseline: 433987851
	comparative: 433987632*/

--aggregate values
SELECT TOUCH_ID

FROM (
         SELECT t.TOUCH_ID,
                t.ATTRIBUTED_USER_ID                                        as ATTRIBUTED_USER_ID,
                t.STITCHED_IDENTITY_TYPE                                    as STITCHED_IDENTITY_TYPE,
                MIN(e.EVENT_TSTAMP)                                         AS TOUCH_START_TSTAMP,
                MAX(e.EVENT_TSTAMP)                                         AS TOUCH_END_TSTAMP,
                TIMEDIFF(seconds, MIN(e.EVENT_TSTAMP), MAX(e.EVENT_TSTAMP)) AS TOUCH_DURATION_SECONDS,
                COUNT(*)                                                    AS TOUCH_EVENT_COUNT,
                CASE
                    WHEN SUM(CASE WHEN e.EVENT_NAME = 'transaction_item' THEN 1 ELSE 0 END) > 0
                        THEN TRUE
                    ELSE FALSE END                                          AS TOUCH_HAS_BOOKING
         FROM data_vault_mvp_dev_robin.single_customer_view_stg_bk.module_touchification t
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
         WHERE t.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
         GROUP BY 1, 2, 3
     )
GROUP BY 1
HAVING COUNT(*) > 1
;


SELECT e.EVENT_HASH,
       t.TOUCH_ID,
       t.ATTRIBUTED_USER_ID     as ATTRIBUTED_USER_ID,
       t.STITCHED_IDENTITY_TYPE as STITCHED_IDENTITY_TYPE,
       e.UNIQUE_BROWSER_ID,
       e.COOKIE_ID,
       e.SESSION_USERID,
       t.UPDATED_AT,
       t.CREATED_AT,
       t.SCHEDULE_TSTAMP
FROM data_vault_mvp_dev_robin.single_customer_view_stg_bk.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow_bk.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
WHERE t.TOUCH_ID IN (
                     '5a040dde6315963f1c82177012eca59626589cb2b7c7149ecd4b42370f87efe2',
                     '04f4b8ef7628045c583650b6e2cc3cba121e0ef5e9a768db99cee64edde94f5e',
                     'daec9c45cc666b942c6f6fe53c8a0071e8a1f7c338863feb60609ea7a6fc42fa',
                     '87ee80a2c6a87214eb816bf701fb6faf227ab2d3b2f41451b808df8f4579c316',
                     '74e3d7ee0961c98cbdbb31838bb28efa89052e488db282a00c0cd024c0ef0bf4',
                     'fdcf942c88aea1d948bc361c71e3bb7b7edd399442bb5c4ef146e38ed58ad925',
                     'c7760bf019da4c543c7310222c4a12a4b01b564f009397cfd8d66f6bda68559c',
                     '7a07f5871e4b74ed5c82cde2543adc87f808fb8ae8e64123a061daa759a35b34',
                     'f812512c1cd8b3f1d0fe416f8e93743fb6b8854544c204362aeaac712f066be0',
                     '7d460671357b7b83eb4c30cb1634e643801226e7dad6f7ebe73dc2a8ce22014c',
                     '0f606412efbfbfaa7c241d0cce420068b0a528f11f77654999474b4efc0e6f47',
                     'e51f1c13bf912fa2656a396251df893b2b2376f707eec38553fa5e1d8afc6c91',
                     '56d9e18d3c70e0e2ebedfec186a8feb8409a289d6f1bb6f12aa43531fe052f65',
                     '7da01a49218413b3e5f3d57126eff05419de3bcea5ae681495b3443cb3c4faf2',
                     'd446b0c42da3617b9a826505332da1cfd5577d3b133e7b7be6952f5696437f6e',
                     '9f39f9dbb822689ff876579cbdfcf10c9e4f8825080982c1833e2d5681b1171b',
                     'c5259574dd8eee3c9305d53c5b9eaebded3f2e668607ac1c87580fd8e9f301af',
                     'fc56fe007eeea33127be3636366a2b4f5cb8794d6aa7bc250c02decfadfe00c1',
                     'bd70e4a8666a2df68b8e0e14a8b49f48f178f1fd004f22ea48c51f2760f673bf',
                     '3e06812ffd28d924025adec34d094298535683a2a32b8bc6965045f004306d95',
                     '5c8fab391db31f46959202bf37016c2e5d46dc4ce6b0f3f1aac9c50016e8048e',
                     '681c0efe80c1a917ef1f2a1e5f6ff30e4d9f1ceab749603be3ade11999773728',
                     '6c30038da440c7e86aca882326992c03a72f9dfad91ba555f02f517c1b498356',
                     'adff9cd9aaf785d730966396e59f56179ab7171149006119178ff7594bbe9b79',
                     '2e576315bb7de599c76940647c4ed72c6d612958b92a1d6e421ce76294f3b7f3',
                     'cf04fef75c116579c26db25b37e40b3b83d5f586f20df9ecebe4aafd7faa8188',
                     '609d1feac03adc0350bf939430a465b84488568c8378554a5303a3fbedb18e97',
                     'a5a625df594039192bd74bc8f9ce3da7fa25c1556ea38a60e6d1c0c8be8d3440',
                     '299f445e7b5d228b97c32b1261e36462fd2e0dd79f63e8840fb6cb2add75d901',
                     '9ef5b72db734b51f59aa9db89ad332f352132eb89a666d2bd807cd4a841cafe3',
                     'bf27154abaddcb78c9ea156b1f8608260a642b36b2d13f8065aef7d21f387892',
                     'c4a52f97e2da30abe987710b0f1ac083192e1c7a419b4e8c87475cbaa61b4f98',
                     'c7cb00bfc429b275ff0611dd1f5faaf3f02b2809c947be356a6ef44735a73853',
                     '16a17dd2eb881c1fabda7a67ce9abe3920e576992dc9267636e72fb52a78c3ad',
                     'a1c9e43dece30b93c118e8e6110370cc57998fa0067cec7e06fd236b890c35f4',
                     '662f2a68ee0cb668f1d9a09e723d2c766146d56e9d602b3f77c18f1a7295d348',
                     'e5fd1b65356be6d22ce126b98e4efd01b460a053f6c014f57392fd4a1fb8619a',
                     '68a2d3b790ab1460c2e4f1a82525a8f885d97a60c0cadeeac2cca0bf2eb9d06a',
                     'bf3b28fd8e5fe89dc90ed086e44ff29b72ecf8da7a63bb95b1edd66d4d6b5dbb',
                     '8d1efb3fdf45b2498d06a50a7cd09bac756a62aeddaf33b3d25d4c42223073bc',
                     'a5e80b0bc24580d6bc0c58c90f3a0a249733e0cc27c31e26dac6c9a32c4160f4',
                     '1f2fdb8368f14b6a8d29fe83e84bc99dbcf4f2c13d70de69e11e17fb797fe9b3',
                     '29ea1ae66ec1f3e3bc13e645f58629b16dc719c88915f5ee142ba2a13274a0ff',
                     'd1ae66c8aa387d7cf7fe7eebbfa46446de1041c689461a808bafe189f4348419',
                     '6db8a1d7d6ad16c61e60a1388f0f14ee9460683cfe619cb11ef56e7af1f63d8f',
                     '6e522cf52eb490be885c27180b596e54d831d3e177cb7cbe08cf6dd95d9a8524',
                     '307e3e03e8d1348c9cc0ebfe2410ac27812eca11a95b2a142961e55119c7cdb9',
                     '12ad382154bd5f86a85b650dfa65fb1946b28d799f0cfb6eaa5fe104733772bb',
                     '260eda52b67f9c0a82f39c0aefed3b5fc7c16780d09450b2e730e3ca6da12686',
                     '57caff56cb6f13745592e160a74c53ac6cf12b4269bf01afcae86c06ad6432a8',
                     'cf8a3a1e17430d4dd60e5799e3c8ccca68c41181a87db3e91be88d4df158c76e',
                     '75047825fad1892fca728e0255267350e17e8588ae3ce5df1388570154c85b54',
                     '30ed57b03662192181e02eb2038ebc59840a1bb3327c807b9217e6d057c1e0e7',
                     '872f573c96eec7aa04e7c4017fe91f3930c0ea5b1cb6ada7e267bec8e17dac90',
                     '5d7841246afd0e6a0cab729ef072972529aa056aeca38a451909f2f55dcde864',
                     'c30a8a0f52425148420ee172d2f194c47f56d3ee6bac0f62dfee7a92720f5a2d',
                     '93f7fba8372deb2277dd58aa2b9e41049bcb54da0fca9651d490e88f2989be4b',
                     'd80b24a53df6431c40eeae82885bb3db29f47e996186a3365b6669b5cf2daa03',
                     '22514a8fcb0b431debd9e7cc329ff3619f69a9d5d871cf764cbd52a1af1a7957',
                     'ae7683afec15fc7cfc4a9b1abaac7c80c6ca6b04910c00e567e59dc189d16029',
                     '7624e9ec4adfe420bc7cec82aef944b5a2e2323bf6067c3276efedaddb16aef5',
                     '9442e51291b08fc1f8fc1ec9d19aa12927d256f22b265e1ac24d7fe6fcb1e50e',
                     'f7dda426cda3c6f5c1b92375efe0380f0758c8b7c1f62c675afb1ecf1cfbfb5b',
                     '4480c51c9061707a27e48828721553a0bb832cf8bbeda6cca8c5fb96dbe1cf63',
                     '7762b86de702c990b25dc57c3a5b81a3c0327df476dcaeb6d997aa7e81bd8b99',
                     'fe28247f5d8336840d72da0544d98792c10be9f5df73e257076dc5792bd1fc53',
                     'b748036240ed8a02000fea00f46980f9a7f479bad528000959cbc516d054f124',
                     '5e07336ad5c08a164ed0f5e3c9807fb0fdf60ebd384ea45a2073d9bd11060616',
                     '2dc254ab7498a76e47ad7ae82541a764f65350d0dc99b4bdf4ca10051b019bfb',
                     '7a2438d61f2b06601b672914768510d70a01fc8a185412bc83ce948a0c44ce14',
                     '13bb45485ed69da20c6d8600ba29ec911796ea1046554d973b0b8169710ba487',
                     '4ade8bf5dc145445db4552ee4d3e3a44f22f9750d2f5e5f51ae4f51038485382',
                     '9b08d236076e32590c443904099c3fce2fd0b030c8e3812718871797a805384b',
                     '96e4427d67c2949e98f83aa31a87b2c3226d2262618695a20d7ecdbd90fcee9b',
                     'fd7a6896bb270caa7fac6db5c7fbfc3565afe342cdf0f8963e63444707601b81',
                     '6033e839b3e8817d3fdb7da1cb6e6af044462d2fc6b03579755eb9630a2fe388',
                     '47992dd1451202c47c2a2aa3c4bc00c48ee98e6e22025418c6a8f36d6267e4c3',
                     '5d3f010053fdf7d69cf29e3475498811820ed3a1477af10c73c47810a3a0764b',
                     'b94116f58a188ecf7546d1e62988f6c3b5107a5d97f8c17484c84fde2e581508',
                     'b5fb50f26d825b3a000b3808a3a61e2179afa1f5320dac43345fb9508c5ced10',
                     '9b491d957426b02aedcc0e76f6a1cc9ff2863b9ee015d786ae74746556250584',
                     'c219f0329b5db13c21fdfae777d1ae801be88a1fbf1be86ec4a295d716b5e7f3',
                     '03533a76ccbe7e53d641a7e592c16147cd02d8c8fe67ce5e956fb52bc3a7c18d',
                     '3d4c228646a864cdd81bafd2bb2bbfb5ff810d26523870f39ac54ed6639f3389',
                     'a4a3f0d445168e4baf016c0de6f12bfb5a7f2de27b01aa627f2bf68994655d6a',
                     '5bc06520a482fd00e556903ea9685f6f01300bd2b4eec15a2eae19214f99618e',
                     'f5e89229cfe3e6280da6e065fdbaf0ebfeed6ac66db23d28cd1a89a0174d301c',
                     '201d6e26f7a0f4d323adbdbfe654b1182ccf426fb81742a5b5d62d3f923df8f4',
                     '228345c8df2e97f752c63d167ae89e5ca4dd63d9f65ea185162d678bcf24ac8a',
                     '45bb8f63072636ac70aa62d826034fabaf77212b0d49982de7113159664ea1c7',
                     'e97202a87137bab16bf612fb7ab192ca2727a4a1ec51494eeb63d803fe05e1d8',
                     '05fe5a4ccfc2963a4f7f7ed473b68a66a5d574d22f2a033f9459f1d4962a279c',
                     'e7769f751c5ba50c6fd093b58f3a6ae03757f957b9be0a3196f811828838a4d0',
                     '00cf608a1bf04bfa6b45df9e1c2591502799636c87b8697fab77161eae4e11c1',
                     '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                     'e0c2213c5e357edebacbb154b613c697fcca2a6aa0f9f734ee554a49eb20a572',
                     '51f9db1eb22f0cbe2016cf6a2763e092b757e910e13513d4ae0d6b8eafc51424',
                     '34b6ce3dce188f08a95244f9fee8b86b294a63dfaa88cd7518f9c220d1e4895a',
                     'c38cac55e1567d6f2d7a7a64a7aa55d936b7c6bf6f2551d9d4ccb802fb626a95',
                     '0c84154969d38495ae481aa06f2c95bff0a8fc486e705f128e14681baddcc6b9',
                     '508b7916908caaafdb781fd40051eed369ac30efbc6a0640afa1a18728f41916',
                     '2a010b786e3113002050961f46626c1acdb24ac8dbe105658c114528ddc3d4cc',
                     'b6ab2f07fccc0dbcf251546e7556122c77343665154a9fdcddefe9fc32edfb87',
                     '4027da6d6495fe3aeb5e30b90a90ddc669333f2d1aeb07c610fcc884d933651e',
                     '42c532bfa6d89120332e982f69923c58ac8c9cd7e6f5b6f0c7495c790b3828f7',
                     'cc87b6921832367825683ce89bfbd1e7f3d17a2261a1048b675e0ee0f101acf8',
                     'd32eab5bb52988d669af321ee04cc2a09789986b2cae70d135ad118da2902a8b',
                     'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                     '906310720212df86a9eadf6e8f68cf5f1a71d74379a0790393160d84f127a334',
                     '9c18621057df3c8314e7139fcb4bcf377397952eb6976a6c5d9cc7d3b8ad0db6',
                     'b30ab7e16c17b50d6aa9e3054e980b554fe3c914144c6c90c02e1b69e8d2395f',
                     '2951ee2929c6fbbfb11d2c960e4b8367722cd1a657b74def08469dc2070fdca3',
                     '7f6b032bed2b018f3dacba90e01338b93017bab801fbbe1dc4807de5ce9353ae',
                     'ac07924e0f367432ad0dd5b70eb7db01ab39d7a63c8fa6202abb08f9b2fd8538',
                     '2a702f954c0892fb96ec74ab3f8725c2b3b2a758e42aa3bf4c37cf94505970c9',
                     'b5755fb2133e4904a06beaeac2c7c403dcd7111d3ddd39b05fc01bd223a38bc8',
                     '05a61d57612d67d61ad23e6950ad16698c61c12c897a3c257877987b97d336bd',
                     '2f439e5077561d308b57c8b1bb45488d0821cf5224f4a2b6d472a201c4b2fb4e',
                     'a3cb6d7e0fbe71acb7fbea302871b11194595d878c0391bb4469252ca8d588af',
                     '0bf90c575d681099e25b889398ceb94ced9bd98d2fdf530386c4936901a6f15d',
                     '1c425e3f6af030288c47b97e4e735ee1a25d0bf668f51d2b4d782991a0f16231',
                     'f42a405c357fa71f260b0921951bf55fb520de08f34e82e4b6d1c1719a887cef',
                     '9aee160d14595e60e3315a4d0e910ba6106e3d821854d975b103b15d16c053c1',
                     '5d10997a2bc2ddab273b9954a5dd112afad0993bef782b671cdd102bdbf91808',
                     '876d61d332485ada99dbdbb95c4a5f4f2178f9c3cd7f943844bfc0d4f43c445a',
                     '609008b1003d544f13523067e3cec7878ec0619c26b31230926ad64d1a5e7e1c',
                     '2aabb779ed645b6b6939805360e6e89cc3eec80d640e39a667e23c080487dd99',
                     'e3294c106ebeddba8c49443136eb78501ec7e92220a864242ea32c1f3da0520c',
                     'f338ed2db939f4707f5b9d7cd49c473796b4ac8230b8864b8e7117d5bf6ce2fb',
                     '485e6264b78db278e5e86e4ca278afa75be3de96a6420e5b99aedac1a3fbaa26',
                     'ae3ae344e6f452d4f00b35fb72d81a574deda783c5c358d742dcf828fcb8a873',
                     '15f76f754c27c7d3982d1920287ea66aa052f35326d2ea6ba942d9eec6f101fa',
                     '9f8948f0087c611bd8945cdf50bc6648d88ad4d2fa674f72ba4d34391b338b93',
                     '9822177c3fb57cce5f0f76f612bc021f58f1aad73bf7727cd4d03a2769030165',
                     'acf0eceab7bf8d4266d61cfce9b73f3d46690fdff538c44f5585c7c11f153b79',
                     '8e271a3f880bc65a9c02c7835aa1dd6a1d2d2b87d541e7209df81c74a49d15b1',
                     'f89c1564cabe4c701b75ba987aae8caaa1f2d74ce44d5ac383d38daba79fe94e',
                     '4eca56bdc816df40811bda60a86aeb03466e22a3d21df9a36ff03ba3160a2034',
                     '22a35961bb8e1ee3932f2eef60bd2cabcd1f43c2ca18aece407b1f19a664785d',
                     '08ea522b3ee7a6b6ba52c99e46ce803de28234cbf5b391cbe1f899c24db309fa',
                     'bc81986f51b291d9e337fae7b08c0c78abef7890c66a77a59980d5af99ab085f',
                     '83451f145492331f460d26d0d2b88ff6bef56d4a2a23f64bfd28fbf99cb59554',
                     '7e37f64da61f9ee821b40be8de1e72d4c99df16fa884f4541e12a5a75aad9bfb',
                     '983518fe1b9dc8777d7275d3a40d1644b73609fc7fbd9cf1112a45ff4c2a9e3b',
                     '27561a661c2f2606c016488d9081c6772147bdc50b77196da5a9dbfdd2baa076',
                     'a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                     '3e68655b3ee17144ccc907f1700dfb9f969e00b230197a0b113cba129f3d42bb',
                     '715d83e7b670bb04e3239d49a117fa134240e40d65a8b210662c8978f652307c',
                     'ee5383342c02906c27f9b5cca50af548e806e343a394e55929aba2f787c6a774',
                     '7e12dbe9eb184a1ae1d0fe66f44f74c8f0da0da34a3dfa3963bf7f5ee0a514e8',
                     'c211a9bf7910430befd886c4a14d5deef62cd6c018bf0a784fbda92580f3dd97',
                     '08f12dffa57caf364607f30dfe64e4397e65eacfc624a651face1529e907cb19',
                     '1b6b51e98af9dc7d190c88837cc8dab2201f1e87af5886a2c674d1e0ffb51d4b',
                     'd12af0991ee29f3e7eb0760a5e2876ba7f841f622ac4fcf144f10a1d73642a3f',
                     '2aad94c31fd35ad8fe4b546cb3151ab23a87f353330189dbd501b6081d522279',
                     'b69476286d4465ca0a6b60479a622ca615c0a5d930e174692a3464f8aaa5a3a1',
                     'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f',
                     'c558f81ad5ac247be12e80b3386a230beeb941f61eee35ea1f2d5e47cefcb1ee',
                     '5107cafc28776208b80cf4fb0694828f42c6e8739799b2461eab9afa4ba8e911',
                     'a8a0b0e478f4bf14889838bb5ce95a468ebb102489a7fd92babd089595ca2c52',
                     '7d6fc2d25de2afe3ea46ba035ec3f68f7b0875488ddf74e017242d1db198a892',
                     'fbdbabdb5e2a66a3e0d720feef61378f489290d4bbd7684a1fcb86fba279d4ee',
                     '4479829dfd0f89b1241d3e1025004a97a89756c9e3075d01bd8340365ab322f2',
                     '3c51e71a7c265f32a7f37bd33c146705a03cd5e220ce339929da37413703443a',
                     '8118f646c7f68a2bbdc92b8ca9d1d540bd822c36a823aa9d0a40f9151669e89a',
                     '48827d7d5c84e1279b92605d1bb0f3687b0db12ad5700afa1f5e89aa02957387',
                     '6da21c0af5da679007a21ce8d0a1e62dedace4e6a0acefd6b1aea788c442573f',
                     'd8df1e499637585aa8414069ffeaf81613e9370f7831eeebcd57d31138846450',
                     '11e2ec2eb607e95aa267253936f93132d69342b24bd417f9d6fbacd9600ca4f7',
                     '0b1632d9f074c550d93c81aa6628db3d032078a5b831a07ad340fdf188116916',
                     '4682b6d4e99659e3f82d6102ab5d5c7b1f2e46b4fb1a7b21062e42165de124a3',
                     '2158089ba7af85fc29130961af27cf4fc1506ea1d84abb3b62f64b702b719182',
                     '0d3d8844290ed333550936f61c9c1727bf60b4aa403cc49234130bc3f3d98b75',
                     'f1a714678981738fdd4b901b048bf6e7cf63272413f879410f4bef94624a1cdb',
                     '86bbbbd29d943f0de3ddd8c5582be922433a93ef8c54dc6e79671b8a3acff561',
                     'f9df2fcfc709f66ad7aece0d2bd0cb6580c4a558c1ae7acb3202b6cab6469274',
                     'b194d74d3af7e4b4d06108d569388aa3f60248cb9a3c9841ec35cb587e2fe851',
                     '6114ec6e723d27ce2f5b4b60f791a64fd29eced8bb4f45117c6775918a861229',
                     '363fdb8ac8c23afce2b5e7508c492c400548675f58e0660b83251cb1d50ef57d',
                     'ebb34ca5c7de004b5131b3ea70bf8f25dc71577abb96805160f2fe06a4917eca',
                     '8790c648408f47f2de3812e1f9a23e852ef5d0c0d33d3036943e0425541a563a',
                     '6167e5a152ed5977f7c4bca75d3d58c9927f5c77f9a98c13da0b3446c3719c6b',
                     'c15bde00d40b0670171d1f18c9d6ddf545773a9534f8a58389f06131e92eb933',
                     'cd20950f20d3b7ad60ea0070b0413634c0ad23d079228251986d6d050b32cac7',
                     '61e62f04e0075c3ecd32eb71e0a4db06819234bb253a46f3269716574311c259',
                     '61e9fea5a4acbd7150b77bebea60de9d3f4a27df3bca7fd12023ce8329a6b828',
                     '90f0d13f0dd9a7ab9d98afa582a4b33a5df25515f04430f4f456d8578e19a8d6',
                     '5fcf726033d59cd956767c25d758527be699e648149368574dae56d389beeee0',
                     '10533ae935cc041e36b861304c4ee86aa516289ee8d0f8b672e2a727cceb5bab',
                     '0312299c7a165c42e3469e9144f5e363572642f2e76a772f196c430598d8a06d',
                     '17eb56f9f535e65a414b4a74d04a303f03a44e245bbe4f5e8fc27b23b11d8e14',
                     '7dc8a99591b8ad0faa2d8b598b22a7b9dcb0b459d090959e71d825e2c64ca04a',
                     '9b83a9edcbfe1205c800c6e2e1e5b56bb497fb29bf1affd2a43d8066e0be9bb0',
                     '7b88d37ade3d942aade2a5e604d6c4718cf0e29f47a9d7e859cb356fc2b18b5b',
                     '77e0bca4a12b905c98d04c937b2f46d1f09a3ddb849a381c7bc0a11829fdafdc',
                     '0d63b183557ca1b28bc7ae47ca3ebdcdde5674c789eb9022d028d7ca7e2e813c',
                     'f53a4ec2830d52fb7121f353d896ce10132e58f8a57abcb40c512f319a7a63f8',
                     'dc1b6362798c1eb605ab9e7286cc34ce6f2e34ef595129c8eaa75eb1eb1734ef',
                     '8b6a3a9a095c1642c4e1a9417ae847e46ecefe37c17a8997bc41fd9692baed44',
                     'ee324a8874702c738492e8ff5f0bd6d5611d0b5eb6a383d0f0441ce969aaa859',
                     '18efda30349c4735527b837130df59d4dfc6148aedab5a21923965b92cff629b',
                     'a0db774a3a3fc250c90c3b9bbeca37a0411fe16269f4b2dd682dabb5d535ffb3',
                     '9d72157e67c0040db7c7ab6e1968ef83931d69934b8d187527f037753f172b61',
                     '0220699d569ba3800e311bce8d13caec1f3b983d14e5c2584d9c34f74183f980',
                     '5d6c686f06ea1add96000462dffffc31005e7cb8a691a2c4269b057b56ec42fa',
                     '59339a5babbf051485026bee8a638a8690e3ec4b1cf0a24933a347d9dd05843d',
                     'f6d8856258e62ee1bb5b6f4afbdaaa59236e80f0895a4badb3bf51d01c7b7c3f',
                     '69e1397f1ac3477d96bd79cad4d5d01b931a83d7388f935fe294e08541fb716f',
                     '603635016280242024eeeab7f106d35d92cb7a4059b7bb71cd9f183eb87e5781',
                     'e237e55f6c8f2e1b2bb4d21e44c631b10e2cc24d764742e6406cd11b61c0f19e',
                     '84c59e31d1ea487995dd12fe4a1e994d279152c6ceaab66e39d659c608b7a5b1',
                     '28302ce0905e8d0cc449c9a49afb5693c87ec8cd994b99a8c27845443391b83b',
                     'e0ce0fcab64239b34b649791bb6f46cc892f1a6ce9367005cbf4fe3e9cd37f6c',
                     'c2920a4e02cf8e18e83203af01e542e7fdfcd63d8391849116f53781cb0d368e',
                     'a430e4db6e470d297b2c74f2affeec6eccb28895f978ace3eca4ccc0aea7cd32',
                     '84944f30bff1142cfc84ba8b81d690848ceabde8e585019b220e4e90a7853bce',
                     '4aeb09c83dfcc762d8bb05f9524d09046edf20884cd6014c09f880cb8aebd227',
                     'eb9f393159f0e85c066911df5e3ba05e909509aafb42db97f854808798d0e8d5',
                     'b86a1e876fb81dc866b52b64d2022af9b80e16e681ed1d303634b6b978d6548b'
    );

SELECT e.EVENT_HASH,
       t.TOUCH_ID,
       t.ATTRIBUTED_USER_ID     as ATTRIBUTED_USER_ID,
       t.STITCHED_IDENTITY_TYPE as STITCHED_IDENTITY_TYPE,
       e.UNIQUE_BROWSER_ID,
       e.COOKIE_ID,
       e.SESSION_USERID
FROM data_vault_mvp_dev_robin.single_customer_view_stg_bk.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow_bk.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
WHERE e.EVENT_HASH IN (
                       '5a040dde6315963f1c82177012eca59626589cb2b7c7149ecd4b42370f87efe2',
                       '04f4b8ef7628045c583650b6e2cc3cba121e0ef5e9a768db99cee64edde94f5e',
                       'daec9c45cc666b942c6f6fe53c8a0071e8a1f7c338863feb60609ea7a6fc42fa',
                       '87ee80a2c6a87214eb816bf701fb6faf227ab2d3b2f41451b808df8f4579c316',
                       '74e3d7ee0961c98cbdbb31838bb28efa89052e488db282a00c0cd024c0ef0bf4',
                       'fdcf942c88aea1d948bc361c71e3bb7b7edd399442bb5c4ef146e38ed58ad925',
                       'c7760bf019da4c543c7310222c4a12a4b01b564f009397cfd8d66f6bda68559c',
                       '7a07f5871e4b74ed5c82cde2543adc87f808fb8ae8e64123a061daa759a35b34',
                       'f812512c1cd8b3f1d0fe416f8e93743fb6b8854544c204362aeaac712f066be0',
                       '7d460671357b7b83eb4c30cb1634e643801226e7dad6f7ebe73dc2a8ce22014c',
                       '0f606412efbfbfaa7c241d0cce420068b0a528f11f77654999474b4efc0e6f47',
                       'e51f1c13bf912fa2656a396251df893b2b2376f707eec38553fa5e1d8afc6c91',
                       '56d9e18d3c70e0e2ebedfec186a8feb8409a289d6f1bb6f12aa43531fe052f65',
                       '7da01a49218413b3e5f3d57126eff05419de3bcea5ae681495b3443cb3c4faf2',
                       'd446b0c42da3617b9a826505332da1cfd5577d3b133e7b7be6952f5696437f6e',
                       '9f39f9dbb822689ff876579cbdfcf10c9e4f8825080982c1833e2d5681b1171b',
                       'c5259574dd8eee3c9305d53c5b9eaebded3f2e668607ac1c87580fd8e9f301af',
                       'fc56fe007eeea33127be3636366a2b4f5cb8794d6aa7bc250c02decfadfe00c1',
                       'bd70e4a8666a2df68b8e0e14a8b49f48f178f1fd004f22ea48c51f2760f673bf',
                       '3e06812ffd28d924025adec34d094298535683a2a32b8bc6965045f004306d95',
                       '5c8fab391db31f46959202bf37016c2e5d46dc4ce6b0f3f1aac9c50016e8048e',
                       '681c0efe80c1a917ef1f2a1e5f6ff30e4d9f1ceab749603be3ade11999773728',
                       '6c30038da440c7e86aca882326992c03a72f9dfad91ba555f02f517c1b498356',
                       'adff9cd9aaf785d730966396e59f56179ab7171149006119178ff7594bbe9b79',
                       '2e576315bb7de599c76940647c4ed72c6d612958b92a1d6e421ce76294f3b7f3',
                       'cf04fef75c116579c26db25b37e40b3b83d5f586f20df9ecebe4aafd7faa8188',
                       '609d1feac03adc0350bf939430a465b84488568c8378554a5303a3fbedb18e97',
                       'a5a625df594039192bd74bc8f9ce3da7fa25c1556ea38a60e6d1c0c8be8d3440',
                       '299f445e7b5d228b97c32b1261e36462fd2e0dd79f63e8840fb6cb2add75d901',
                       '9ef5b72db734b51f59aa9db89ad332f352132eb89a666d2bd807cd4a841cafe3',
                       'bf27154abaddcb78c9ea156b1f8608260a642b36b2d13f8065aef7d21f387892',
                       'c4a52f97e2da30abe987710b0f1ac083192e1c7a419b4e8c87475cbaa61b4f98',
                       'c7cb00bfc429b275ff0611dd1f5faaf3f02b2809c947be356a6ef44735a73853',
                       '16a17dd2eb881c1fabda7a67ce9abe3920e576992dc9267636e72fb52a78c3ad',
                       'a1c9e43dece30b93c118e8e6110370cc57998fa0067cec7e06fd236b890c35f4',
                       '662f2a68ee0cb668f1d9a09e723d2c766146d56e9d602b3f77c18f1a7295d348',
                       'e5fd1b65356be6d22ce126b98e4efd01b460a053f6c014f57392fd4a1fb8619a',
                       '68a2d3b790ab1460c2e4f1a82525a8f885d97a60c0cadeeac2cca0bf2eb9d06a',
                       'bf3b28fd8e5fe89dc90ed086e44ff29b72ecf8da7a63bb95b1edd66d4d6b5dbb',
                       '8d1efb3fdf45b2498d06a50a7cd09bac756a62aeddaf33b3d25d4c42223073bc',
                       'a5e80b0bc24580d6bc0c58c90f3a0a249733e0cc27c31e26dac6c9a32c4160f4',
                       '1f2fdb8368f14b6a8d29fe83e84bc99dbcf4f2c13d70de69e11e17fb797fe9b3',
                       '29ea1ae66ec1f3e3bc13e645f58629b16dc719c88915f5ee142ba2a13274a0ff',
                       'd1ae66c8aa387d7cf7fe7eebbfa46446de1041c689461a808bafe189f4348419',
                       '6db8a1d7d6ad16c61e60a1388f0f14ee9460683cfe619cb11ef56e7af1f63d8f',
                       '6e522cf52eb490be885c27180b596e54d831d3e177cb7cbe08cf6dd95d9a8524',
                       '307e3e03e8d1348c9cc0ebfe2410ac27812eca11a95b2a142961e55119c7cdb9',
                       '12ad382154bd5f86a85b650dfa65fb1946b28d799f0cfb6eaa5fe104733772bb',
                       '260eda52b67f9c0a82f39c0aefed3b5fc7c16780d09450b2e730e3ca6da12686',
                       '57caff56cb6f13745592e160a74c53ac6cf12b4269bf01afcae86c06ad6432a8',
                       'cf8a3a1e17430d4dd60e5799e3c8ccca68c41181a87db3e91be88d4df158c76e',
                       '75047825fad1892fca728e0255267350e17e8588ae3ce5df1388570154c85b54',
                       '30ed57b03662192181e02eb2038ebc59840a1bb3327c807b9217e6d057c1e0e7',
                       '872f573c96eec7aa04e7c4017fe91f3930c0ea5b1cb6ada7e267bec8e17dac90',
                       '5d7841246afd0e6a0cab729ef072972529aa056aeca38a451909f2f55dcde864',
                       'c30a8a0f52425148420ee172d2f194c47f56d3ee6bac0f62dfee7a92720f5a2d',
                       '93f7fba8372deb2277dd58aa2b9e41049bcb54da0fca9651d490e88f2989be4b',
                       'd80b24a53df6431c40eeae82885bb3db29f47e996186a3365b6669b5cf2daa03',
                       '22514a8fcb0b431debd9e7cc329ff3619f69a9d5d871cf764cbd52a1af1a7957',
                       'ae7683afec15fc7cfc4a9b1abaac7c80c6ca6b04910c00e567e59dc189d16029',
                       '7624e9ec4adfe420bc7cec82aef944b5a2e2323bf6067c3276efedaddb16aef5',
                       '9442e51291b08fc1f8fc1ec9d19aa12927d256f22b265e1ac24d7fe6fcb1e50e',
                       'f7dda426cda3c6f5c1b92375efe0380f0758c8b7c1f62c675afb1ecf1cfbfb5b',
                       '4480c51c9061707a27e48828721553a0bb832cf8bbeda6cca8c5fb96dbe1cf63',
                       '7762b86de702c990b25dc57c3a5b81a3c0327df476dcaeb6d997aa7e81bd8b99',
                       'fe28247f5d8336840d72da0544d98792c10be9f5df73e257076dc5792bd1fc53',
                       'b748036240ed8a02000fea00f46980f9a7f479bad528000959cbc516d054f124',
                       '5e07336ad5c08a164ed0f5e3c9807fb0fdf60ebd384ea45a2073d9bd11060616',
                       '2dc254ab7498a76e47ad7ae82541a764f65350d0dc99b4bdf4ca10051b019bfb',
                       '7a2438d61f2b06601b672914768510d70a01fc8a185412bc83ce948a0c44ce14',
                       '13bb45485ed69da20c6d8600ba29ec911796ea1046554d973b0b8169710ba487',
                       '4ade8bf5dc145445db4552ee4d3e3a44f22f9750d2f5e5f51ae4f51038485382',
                       '9b08d236076e32590c443904099c3fce2fd0b030c8e3812718871797a805384b',
                       '96e4427d67c2949e98f83aa31a87b2c3226d2262618695a20d7ecdbd90fcee9b',
                       'fd7a6896bb270caa7fac6db5c7fbfc3565afe342cdf0f8963e63444707601b81',
                       '6033e839b3e8817d3fdb7da1cb6e6af044462d2fc6b03579755eb9630a2fe388',
                       '47992dd1451202c47c2a2aa3c4bc00c48ee98e6e22025418c6a8f36d6267e4c3',
                       '5d3f010053fdf7d69cf29e3475498811820ed3a1477af10c73c47810a3a0764b',
                       'b94116f58a188ecf7546d1e62988f6c3b5107a5d97f8c17484c84fde2e581508',
                       'b5fb50f26d825b3a000b3808a3a61e2179afa1f5320dac43345fb9508c5ced10',
                       '9b491d957426b02aedcc0e76f6a1cc9ff2863b9ee015d786ae74746556250584',
                       'c219f0329b5db13c21fdfae777d1ae801be88a1fbf1be86ec4a295d716b5e7f3',
                       '03533a76ccbe7e53d641a7e592c16147cd02d8c8fe67ce5e956fb52bc3a7c18d',
                       '3d4c228646a864cdd81bafd2bb2bbfb5ff810d26523870f39ac54ed6639f3389',
                       'a4a3f0d445168e4baf016c0de6f12bfb5a7f2de27b01aa627f2bf68994655d6a',
                       '5bc06520a482fd00e556903ea9685f6f01300bd2b4eec15a2eae19214f99618e',
                       'f5e89229cfe3e6280da6e065fdbaf0ebfeed6ac66db23d28cd1a89a0174d301c',
                       '201d6e26f7a0f4d323adbdbfe654b1182ccf426fb81742a5b5d62d3f923df8f4',
                       '228345c8df2e97f752c63d167ae89e5ca4dd63d9f65ea185162d678bcf24ac8a',
                       '45bb8f63072636ac70aa62d826034fabaf77212b0d49982de7113159664ea1c7',
                       'e97202a87137bab16bf612fb7ab192ca2727a4a1ec51494eeb63d803fe05e1d8',
                       '05fe5a4ccfc2963a4f7f7ed473b68a66a5d574d22f2a033f9459f1d4962a279c',
                       'e7769f751c5ba50c6fd093b58f3a6ae03757f957b9be0a3196f811828838a4d0',
                       '00cf608a1bf04bfa6b45df9e1c2591502799636c87b8697fab77161eae4e11c1',
                       '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                       'e0c2213c5e357edebacbb154b613c697fcca2a6aa0f9f734ee554a49eb20a572',
                       '51f9db1eb22f0cbe2016cf6a2763e092b757e910e13513d4ae0d6b8eafc51424',
                       '34b6ce3dce188f08a95244f9fee8b86b294a63dfaa88cd7518f9c220d1e4895a',
                       'c38cac55e1567d6f2d7a7a64a7aa55d936b7c6bf6f2551d9d4ccb802fb626a95',
                       '0c84154969d38495ae481aa06f2c95bff0a8fc486e705f128e14681baddcc6b9',
                       '508b7916908caaafdb781fd40051eed369ac30efbc6a0640afa1a18728f41916',
                       '2a010b786e3113002050961f46626c1acdb24ac8dbe105658c114528ddc3d4cc',
                       'b6ab2f07fccc0dbcf251546e7556122c77343665154a9fdcddefe9fc32edfb87',
                       '4027da6d6495fe3aeb5e30b90a90ddc669333f2d1aeb07c610fcc884d933651e',
                       '42c532bfa6d89120332e982f69923c58ac8c9cd7e6f5b6f0c7495c790b3828f7',
                       'cc87b6921832367825683ce89bfbd1e7f3d17a2261a1048b675e0ee0f101acf8',
                       'd32eab5bb52988d669af321ee04cc2a09789986b2cae70d135ad118da2902a8b',
                       'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                       '906310720212df86a9eadf6e8f68cf5f1a71d74379a0790393160d84f127a334',
                       '9c18621057df3c8314e7139fcb4bcf377397952eb6976a6c5d9cc7d3b8ad0db6',
                       'b30ab7e16c17b50d6aa9e3054e980b554fe3c914144c6c90c02e1b69e8d2395f',
                       '2951ee2929c6fbbfb11d2c960e4b8367722cd1a657b74def08469dc2070fdca3',
                       '7f6b032bed2b018f3dacba90e01338b93017bab801fbbe1dc4807de5ce9353ae',
                       'ac07924e0f367432ad0dd5b70eb7db01ab39d7a63c8fa6202abb08f9b2fd8538',
                       '2a702f954c0892fb96ec74ab3f8725c2b3b2a758e42aa3bf4c37cf94505970c9',
                       'b5755fb2133e4904a06beaeac2c7c403dcd7111d3ddd39b05fc01bd223a38bc8',
                       '05a61d57612d67d61ad23e6950ad16698c61c12c897a3c257877987b97d336bd',
                       '2f439e5077561d308b57c8b1bb45488d0821cf5224f4a2b6d472a201c4b2fb4e',
                       'a3cb6d7e0fbe71acb7fbea302871b11194595d878c0391bb4469252ca8d588af',
                       '0bf90c575d681099e25b889398ceb94ced9bd98d2fdf530386c4936901a6f15d',
                       '1c425e3f6af030288c47b97e4e735ee1a25d0bf668f51d2b4d782991a0f16231',
                       'f42a405c357fa71f260b0921951bf55fb520de08f34e82e4b6d1c1719a887cef',
                       '9aee160d14595e60e3315a4d0e910ba6106e3d821854d975b103b15d16c053c1',
                       '5d10997a2bc2ddab273b9954a5dd112afad0993bef782b671cdd102bdbf91808',
                       '876d61d332485ada99dbdbb95c4a5f4f2178f9c3cd7f943844bfc0d4f43c445a',
                       '609008b1003d544f13523067e3cec7878ec0619c26b31230926ad64d1a5e7e1c',
                       '2aabb779ed645b6b6939805360e6e89cc3eec80d640e39a667e23c080487dd99',
                       'e3294c106ebeddba8c49443136eb78501ec7e92220a864242ea32c1f3da0520c',
                       'f338ed2db939f4707f5b9d7cd49c473796b4ac8230b8864b8e7117d5bf6ce2fb',
                       '485e6264b78db278e5e86e4ca278afa75be3de96a6420e5b99aedac1a3fbaa26',
                       'ae3ae344e6f452d4f00b35fb72d81a574deda783c5c358d742dcf828fcb8a873',
                       '15f76f754c27c7d3982d1920287ea66aa052f35326d2ea6ba942d9eec6f101fa',
                       '9f8948f0087c611bd8945cdf50bc6648d88ad4d2fa674f72ba4d34391b338b93',
                       '9822177c3fb57cce5f0f76f612bc021f58f1aad73bf7727cd4d03a2769030165',
                       'acf0eceab7bf8d4266d61cfce9b73f3d46690fdff538c44f5585c7c11f153b79',
                       '8e271a3f880bc65a9c02c7835aa1dd6a1d2d2b87d541e7209df81c74a49d15b1',
                       'f89c1564cabe4c701b75ba987aae8caaa1f2d74ce44d5ac383d38daba79fe94e',
                       '4eca56bdc816df40811bda60a86aeb03466e22a3d21df9a36ff03ba3160a2034',
                       '22a35961bb8e1ee3932f2eef60bd2cabcd1f43c2ca18aece407b1f19a664785d',
                       '08ea522b3ee7a6b6ba52c99e46ce803de28234cbf5b391cbe1f899c24db309fa',
                       'bc81986f51b291d9e337fae7b08c0c78abef7890c66a77a59980d5af99ab085f',
                       '83451f145492331f460d26d0d2b88ff6bef56d4a2a23f64bfd28fbf99cb59554',
                       '7e37f64da61f9ee821b40be8de1e72d4c99df16fa884f4541e12a5a75aad9bfb',
                       '983518fe1b9dc8777d7275d3a40d1644b73609fc7fbd9cf1112a45ff4c2a9e3b',
                       '27561a661c2f2606c016488d9081c6772147bdc50b77196da5a9dbfdd2baa076',
                       'a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                       '3e68655b3ee17144ccc907f1700dfb9f969e00b230197a0b113cba129f3d42bb',
                       '715d83e7b670bb04e3239d49a117fa134240e40d65a8b210662c8978f652307c',
                       'ee5383342c02906c27f9b5cca50af548e806e343a394e55929aba2f787c6a774',
                       '7e12dbe9eb184a1ae1d0fe66f44f74c8f0da0da34a3dfa3963bf7f5ee0a514e8',
                       'c211a9bf7910430befd886c4a14d5deef62cd6c018bf0a784fbda92580f3dd97',
                       '08f12dffa57caf364607f30dfe64e4397e65eacfc624a651face1529e907cb19',
                       '1b6b51e98af9dc7d190c88837cc8dab2201f1e87af5886a2c674d1e0ffb51d4b',
                       'd12af0991ee29f3e7eb0760a5e2876ba7f841f622ac4fcf144f10a1d73642a3f',
                       '2aad94c31fd35ad8fe4b546cb3151ab23a87f353330189dbd501b6081d522279',
                       'b69476286d4465ca0a6b60479a622ca615c0a5d930e174692a3464f8aaa5a3a1',
                       'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f',
                       'c558f81ad5ac247be12e80b3386a230beeb941f61eee35ea1f2d5e47cefcb1ee',
                       '5107cafc28776208b80cf4fb0694828f42c6e8739799b2461eab9afa4ba8e911',
                       'a8a0b0e478f4bf14889838bb5ce95a468ebb102489a7fd92babd089595ca2c52',
                       '7d6fc2d25de2afe3ea46ba035ec3f68f7b0875488ddf74e017242d1db198a892',
                       'fbdbabdb5e2a66a3e0d720feef61378f489290d4bbd7684a1fcb86fba279d4ee',
                       '4479829dfd0f89b1241d3e1025004a97a89756c9e3075d01bd8340365ab322f2',
                       '3c51e71a7c265f32a7f37bd33c146705a03cd5e220ce339929da37413703443a',
                       '8118f646c7f68a2bbdc92b8ca9d1d540bd822c36a823aa9d0a40f9151669e89a',
                       '48827d7d5c84e1279b92605d1bb0f3687b0db12ad5700afa1f5e89aa02957387',
                       '6da21c0af5da679007a21ce8d0a1e62dedace4e6a0acefd6b1aea788c442573f',
                       'd8df1e499637585aa8414069ffeaf81613e9370f7831eeebcd57d31138846450',
                       '11e2ec2eb607e95aa267253936f93132d69342b24bd417f9d6fbacd9600ca4f7',
                       '0b1632d9f074c550d93c81aa6628db3d032078a5b831a07ad340fdf188116916',
                       '4682b6d4e99659e3f82d6102ab5d5c7b1f2e46b4fb1a7b21062e42165de124a3',
                       '2158089ba7af85fc29130961af27cf4fc1506ea1d84abb3b62f64b702b719182',
                       '0d3d8844290ed333550936f61c9c1727bf60b4aa403cc49234130bc3f3d98b75',
                       'f1a714678981738fdd4b901b048bf6e7cf63272413f879410f4bef94624a1cdb',
                       '86bbbbd29d943f0de3ddd8c5582be922433a93ef8c54dc6e79671b8a3acff561',
                       'f9df2fcfc709f66ad7aece0d2bd0cb6580c4a558c1ae7acb3202b6cab6469274',
                       'b194d74d3af7e4b4d06108d569388aa3f60248cb9a3c9841ec35cb587e2fe851',
                       '6114ec6e723d27ce2f5b4b60f791a64fd29eced8bb4f45117c6775918a861229',
                       '363fdb8ac8c23afce2b5e7508c492c400548675f58e0660b83251cb1d50ef57d',
                       'ebb34ca5c7de004b5131b3ea70bf8f25dc71577abb96805160f2fe06a4917eca',
                       '8790c648408f47f2de3812e1f9a23e852ef5d0c0d33d3036943e0425541a563a',
                       '6167e5a152ed5977f7c4bca75d3d58c9927f5c77f9a98c13da0b3446c3719c6b',
                       'c15bde00d40b0670171d1f18c9d6ddf545773a9534f8a58389f06131e92eb933',
                       'cd20950f20d3b7ad60ea0070b0413634c0ad23d079228251986d6d050b32cac7',
                       '61e62f04e0075c3ecd32eb71e0a4db06819234bb253a46f3269716574311c259',
                       '61e9fea5a4acbd7150b77bebea60de9d3f4a27df3bca7fd12023ce8329a6b828',
                       '90f0d13f0dd9a7ab9d98afa582a4b33a5df25515f04430f4f456d8578e19a8d6',
                       '5fcf726033d59cd956767c25d758527be699e648149368574dae56d389beeee0',
                       '10533ae935cc041e36b861304c4ee86aa516289ee8d0f8b672e2a727cceb5bab',
                       '0312299c7a165c42e3469e9144f5e363572642f2e76a772f196c430598d8a06d',
                       '17eb56f9f535e65a414b4a74d04a303f03a44e245bbe4f5e8fc27b23b11d8e14',
                       '7dc8a99591b8ad0faa2d8b598b22a7b9dcb0b459d090959e71d825e2c64ca04a',
                       '9b83a9edcbfe1205c800c6e2e1e5b56bb497fb29bf1affd2a43d8066e0be9bb0',
                       '7b88d37ade3d942aade2a5e604d6c4718cf0e29f47a9d7e859cb356fc2b18b5b',
                       '77e0bca4a12b905c98d04c937b2f46d1f09a3ddb849a381c7bc0a11829fdafdc',
                       '0d63b183557ca1b28bc7ae47ca3ebdcdde5674c789eb9022d028d7ca7e2e813c',
                       'f53a4ec2830d52fb7121f353d896ce10132e58f8a57abcb40c512f319a7a63f8',
                       'dc1b6362798c1eb605ab9e7286cc34ce6f2e34ef595129c8eaa75eb1eb1734ef',
                       '8b6a3a9a095c1642c4e1a9417ae847e46ecefe37c17a8997bc41fd9692baed44',
                       'ee324a8874702c738492e8ff5f0bd6d5611d0b5eb6a383d0f0441ce969aaa859',
                       '18efda30349c4735527b837130df59d4dfc6148aedab5a21923965b92cff629b',
                       'a0db774a3a3fc250c90c3b9bbeca37a0411fe16269f4b2dd682dabb5d535ffb3',
                       '9d72157e67c0040db7c7ab6e1968ef83931d69934b8d187527f037753f172b61',
                       '0220699d569ba3800e311bce8d13caec1f3b983d14e5c2584d9c34f74183f980',
                       '5d6c686f06ea1add96000462dffffc31005e7cb8a691a2c4269b057b56ec42fa',
                       '59339a5babbf051485026bee8a638a8690e3ec4b1cf0a24933a347d9dd05843d',
                       'f6d8856258e62ee1bb5b6f4afbdaaa59236e80f0895a4badb3bf51d01c7b7c3f',
                       '69e1397f1ac3477d96bd79cad4d5d01b931a83d7388f935fe294e08541fb716f',
                       '603635016280242024eeeab7f106d35d92cb7a4059b7bb71cd9f183eb87e5781',
                       'e237e55f6c8f2e1b2bb4d21e44c631b10e2cc24d764742e6406cd11b61c0f19e',
                       '84c59e31d1ea487995dd12fe4a1e994d279152c6ceaab66e39d659c608b7a5b1',
                       '28302ce0905e8d0cc449c9a49afb5693c87ec8cd994b99a8c27845443391b83b',
                       'e0ce0fcab64239b34b649791bb6f46cc892f1a6ce9367005cbf4fe3e9cd37f6c',
                       'c2920a4e02cf8e18e83203af01e542e7fdfcd63d8391849116f53781cb0d368e',
                       'a430e4db6e470d297b2c74f2affeec6eccb28895f978ace3eca4ccc0aea7cd32',
                       '84944f30bff1142cfc84ba8b81d690848ceabde8e585019b220e4e90a7853bce',
                       '4aeb09c83dfcc762d8bb05f9524d09046edf20884cd6014c09f880cb8aebd227',
                       'eb9f393159f0e85c066911df5e3ba05e909509aafb42db97f854808798d0e8d5',
                       'b86a1e876fb81dc866b52b64d2022af9b80e16e681ed1d303634b6b978d6548b'
    );

SET bug_touch='00cf608a1bf04bfa6b45df9e1c2591502799636c87b8697fab77161eae4e11c1';

-- 00cf608a1bf04bfa6b45df9e1c2591502799636c87b8697fab77161eae4e11c1
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg_bk.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow_bk.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
WHERE e.event_hash = $bug_touch
   OR t.TOUCH_ID = $bug_touch;

USE WAREHOUSE PIPE_XLARGE;

SELECT UPDATED_AT, COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg_bk.MODULE_TOUCHIFICATION
GROUP BY 1;

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.MODULE_TOUCHIFICATION
WHERE UPDATED_AT = '2020-02-22 02:38:44.530000000';

--original restitched users
SELECT COUNT(*)
FROM (
         SELECT e.EVENT_HASH,
                e.EVENT_TSTAMP,
                e.DERIVED_TSTAMP,
                e.EVENT_NAME,
                e.PAGE_URL,
                e.PAGE_REFERRER,
                e.DEVICE_PLATFORM,
                e.UNIQUE_BROWSER_ID,
                e.COOKIE_ID,
                e.SESSION_USERID

         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                             ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
         WHERE e.EVENT_NAME IN
               ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
           AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
           AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
           AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
     )
-- WHERE EVENT_HASH = '0fb5d830f422de542aa334d8fc1e980f902b069fcfbc9b7e15a9e8b7e47c69ed'
--0fb5d830f422de542aa334d8fc1e980f902b069fcfbc9b7e15a9e8b7e47c69ed this event wasn't resessionised
;
--adjusted restitched users
SELECT COUNT(*)
FROM (
         WITH events_from_restitched_users AS (
             SELECT e.EVENT_HASH
             FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                      INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                                 ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                    COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
             WHERE e.EVENT_NAME IN
                   ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
               AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
               AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
               AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
         ),
              touches_for_restitched_events AS (
                  --for all the events where a users has been restitched, get the touches associated to them
                  SELECT DISTINCT TOUCH_ID
                  FROM MODULE_TOUCHIFICATION
                  WHERE EVENT_HASH IN (SELECT * FROM events_from_restitched_users)
              )
--get all the events associated to any event that might have been affected by restitching
         SELECT e.EVENT_HASH,
                e.EVENT_TSTAMP,
                e.DERIVED_TSTAMP,
                e.EVENT_NAME,
                e.PAGE_URL,
                e.PAGE_REFERRER,
                e.DEVICE_PLATFORM,
                e.UNIQUE_BROWSER_ID,
                e.COOKIE_ID,
                e.SESSION_USERID
         FROM MODULE_TOUCHIFICATION t
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.EVENT_HASH = t.EVENT_HASH
         WHERE TOUCH_ID IN (SELECT * FROM touches_for_restitched_events)
     )
-- WHERE EVENT_HASH = '0fb5d830f422de542aa334d8fc1e980f902b069fcfbc9b7e15a9e8b7e47c69ed'
;

USE WAREHOUSE PIPE_XLARGE;


SELECT DISTINCT EVENT_NAME
FROM SNOWPLOW.ATOMIC.EVENTS;

------------------------------------------------------------------------------------------------------------------------
--after change, 5 touches that repeat.

DROP SCHEMA
    SELECT TOUCH_ID

FROM (
         SELECT t.TOUCH_ID,
                t.ATTRIBUTED_USER_ID                                        as ATTRIBUTED_USER_ID,
                t.STITCHED_IDENTITY_TYPE                                    as STITCHED_IDENTITY_TYPE,
                MIN(e.EVENT_TSTAMP)                                         AS TOUCH_START_TSTAMP,
                MAX(e.EVENT_TSTAMP)                                         AS TOUCH_END_TSTAMP,
                TIMEDIFF(seconds, MIN(e.EVENT_TSTAMP), MAX(e.EVENT_TSTAMP)) AS TOUCH_DURATION_SECONDS,
                COUNT(*)                                                    AS TOUCH_EVENT_COUNT,
                CASE
                    WHEN SUM(CASE WHEN e.EVENT_NAME = 'transaction_item' THEN 1 ELSE 0 END) > 0
                        THEN TRUE
                    ELSE FALSE END                                          AS TOUCH_HAS_BOOKING
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
         WHERE t.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
         GROUP BY 1, 2, 3
     )
GROUP BY 1
HAVING COUNT(*) > 1
;

SELECT t.*,
       e.EVENT_HASH,
       e.EVENT_TSTAMP,
       e.DERIVED_TSTAMP,
       e.EVENT_NAME,
       e.PAGE_URL,
       e.PAGE_REFERRER,
       e.DEVICE_PLATFORM,
       e.UNIQUE_BROWSER_ID,
       e.COOKIE_ID,
       e.SESSION_USERID,
       i.*
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
         INNER JOIN DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.module_identity_stitching i
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)

WHERE t.touch_id IN ('a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                     'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                     '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                     'f7df1636c8853e427d7389e11807b33b89a625838932f3fcbc60a3fcd63d2644',
                     'df0cd4304707599a7e98b0002783b2fc17415ff02d8bc491bf688dbc9aef05e5'
    );


USE WAREHOUSE PIPE_XLARGE;

SELECT *
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_BASIC_ATTRIBUTES_CLONE
WHERE TOUCH_ID IN ('a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                   'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                   '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                   'f7df1636c8853e427d7389e11807b33b89a625838932f3fcbc60a3fcd63d2644',
                   'df0cd4304707599a7e98b0002783b2fc17415ff02d8bc491bf688dbc9aef05e5');


SELECT UPDATED_AT, COUNT(*)
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
group by 1;

SELECT *
FROM (
         WITH events_from_restitched_users AS (
             SELECT e.EVENT_HASH
             FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                      INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                                 ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                    COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
             WHERE e.EVENT_NAME IN
                   ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
               AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
               AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
               AND i.UPDATED_AT >= TIMESTAMPADD('min', -35, '2020-02-21 00:00:00'::TIMESTAMP)
         ),
              touches_for_restitched_events AS (
                  --for all the events where a users has been restitched, get the touches associated to them
                  SELECT DISTINCT TOUCH_ID
                  FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
                  WHERE EVENT_HASH IN (SELECT * FROM events_from_restitched_users)
              )
              --get all the events associated to any event that might have been affected by restitching
         SELECT e.EVENT_HASH,
                e.EVENT_TSTAMP,
                e.DERIVED_TSTAMP,
                e.EVENT_NAME,
                e.PAGE_URL,
                e.PAGE_REFERRER,
                e.DEVICE_PLATFORM,
                e.UNIQUE_BROWSER_ID,
                e.COOKIE_ID,
                e.SESSION_USERID
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.EVENT_HASH = t.EVENT_HASH
         WHERE TOUCH_ID IN (SELECT * FROM touches_for_restitched_events)
     )
WHERE EVENT_HASH = '6e3ce0a64e8259ca12cd15227f9b42610113afec4a159d2a1f557d3cdf7f01a7';

SELECT *
FROM HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM e
         INNER JOIN DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_IDENTITY_STITCHING i
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)

WHERE e.EVENT_HASH = '6e3ce0a64e8259ca12cd15227f9b42610113afec4a159d2a1f557d3cdf7f01a7'
;

SELECT *
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFIABLE_EVENTS
WHERE EVENT_HASH = '6e3ce0a64e8259ca12cd15227f9b42610113afec4a159d2a1f557d3cdf7f01a7';


SELECT *
FROM (
         SELECT e.EVENT_HASH,
                i.ATTRIBUTED_USER_ID,
                i.STITCHED_IDENTITY_TYPE,
                e.EVENT_TSTAMP,
                FIRST_VALUE(e.EVENT_HASH)
                            OVER (PARTITION BY i.ATTRIBUTED_USER_ID,
                                d.TIME_DIFF_PARTITION,
                                u.UTM_REF_PARTITION,
                                e.DEVICE_PLATFORM ORDER BY e.EVENT_TSTAMP) AS TOUCH_ID,
                ROW_NUMBER()
                        OVER (PARTITION BY i.ATTRIBUTED_USER_ID,
                            d.TIME_DIFF_PARTITION,
                            u.UTM_REF_PARTITION,
                            e.DEVICE_PLATFORM ORDER BY e.EVENT_TSTAMP)     AS EVENT_INDEX_WITHIN_TOUCH

         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events e
                  INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                             ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
                  INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker u
                             ON e.EVENT_HASH = u.EVENT_HASH
                  INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker d
                             ON e.EVENT_HASH = d.EVENT_HASH
     )
WHERE EVENT_HASH = '6e3ce0a64e8259ca12cd15227f9b42610113afec4a159d2a1f557d3cdf7f01a7';

USE WAREHOUSE PIPE_XLARGE;


MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events AS TARGET
    --all events from any user that has been restitched
    USING (
        WITH events_from_restitched_users AS (
            SELECT e.EVENT_HASH
            FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                     INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                                ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                   COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
            WHERE e.EVENT_NAME IN
                  ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
              AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
              AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
              AND i.UPDATED_AT >= TIMESTAMPADD('min', -35, '2020-02-21 00:00:00'::TIMESTAMP)
        ),
             touches_for_restitched_events AS (
                 --for all the events where a users has been restitched, get the touches associated to them
                 SELECT DISTINCT TOUCH_ID
                 FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
                 WHERE EVENT_HASH IN (SELECT * FROM events_from_restitched_users)
             )
             --get all the events associated to any event that might have been affected by restitching
        SELECT e.EVENT_HASH,
               e.EVENT_TSTAMP,
               e.DERIVED_TSTAMP,
               e.EVENT_NAME,
               e.PAGE_URL,
               e.PAGE_REFERRER,
               e.DEVICE_PLATFORM,
               e.UNIQUE_BROWSER_ID,
               e.COOKIE_ID,
               e.SESSION_USERID
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                 INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.EVENT_HASH = t.EVENT_HASH
        WHERE TOUCH_ID IN (SELECT TOUCH_ID FROM touches_for_restitched_events)
    ) AS BATCH ON TARGET.EVENT_HASH = BATCH.EVENT_HASH
    WHEN NOT MATCHED
        THEN INSERT (
                     SCHEDULE_TSTAMP,
                     RUN_TSTAMP,
                     OPERATION_ID,
                     CREATED_AT,
                     UPDATED_AT,
                     EVENT_HASH,
                     EVENT_TSTAMP,
                     DERIVED_TSTAMP,
                     EVENT_NAME,
                     PAGE_URL,
                     PAGE_REFERRER,
                     DEVICE_PLATFORM,
                     UNIQUE_BROWSER_ID,
                     COOKIE_ID,
                     SESSION_USERID
        )
        VALUES ('2020-02-21 00:00:00',
                '2020-02-24 14:33:48',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/single_customer_view/snowplow/03_touchification/01_touchifiable_events.py__20200221T000000__daily',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                BATCH.EVENT_HASH,
                BATCH.EVENT_TSTAMP,
                BATCH.DERIVED_TSTAMP,
                BATCH.EVENT_NAME,
                BATCH.PAGE_URL,
                BATCH.PAGE_REFERRER,
                BATCH.DEVICE_PLATFORM,
                BATCH.UNIQUE_BROWSER_ID,
                BATCH.COOKIE_ID,
                BATCH.SESSION_USERID)
;

SELECT UPDATED_AT, COUNT(*)
FROM SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
GROUP BY 1;

DELETE
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
WHERE UPDATED_AT = '2020-02-24 14:55:45.231000000'


SELECT touch_id,
       COUNT(*)
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCH_BASIC_ATTRIBUTES_CLONE
GROUP BY 1
HAVING COUNT(*) > 1


SELECT EVENT_HASH,
       COUNT(*)
FROM (
         SELECT EVENT_HASH
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
         WHERE SCHEDULE_TSTAMP >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
     )
WHERE EVENT_HASH IN (
                     '05a61d57612d67d61ad23e6950ad16698c61c12c897a3c257877987b97d336bd',
                     'b5755fb2133e4904a06beaeac2c7c403dcd7111d3ddd39b05fc01bd223a38bc8',
                     'a5a625df594039192bd74bc8f9ce3da7fa25c1556ea38a60e6d1c0c8be8d3440',
                     'df0cd4304707599a7e98b0002783b2fc17415ff02d8bc491bf688dbc9aef05e5',
                     'd446b0c42da3617b9a826505332da1cfd5577d3b133e7b7be6952f5696437f6e',
                     'fdcf942c88aea1d948bc361c71e3bb7b7edd399442bb5c4ef146e38ed58ad925',
                     '5d10997a2bc2ddab273b9954a5dd112afad0993bef782b671cdd102bdbf91808',
                     '8d1efb3fdf45b2498d06a50a7cd09bac756a62aeddaf33b3d25d4c42223073bc',
                     '2dc254ab7498a76e47ad7ae82541a764f65350d0dc99b4bdf4ca10051b019bfb',
                     'b69476286d4465ca0a6b60479a622ca615c0a5d930e174692a3464f8aaa5a3a1',
                     'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f',
                     '56d9e18d3c70e0e2ebedfec186a8feb8409a289d6f1bb6f12aa43531fe052f65',
                     '0f606412efbfbfaa7c241d0cce420068b0a528f11f77654999474b4efc0e6f47',
                     'a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                     '5fcf726033d59cd956767c25d758527be699e648149368574dae56d389beeee0',
                     '2951ee2929c6fbbfb11d2c960e4b8367722cd1a657b74def08469dc2070fdca3',
                     'ac07924e0f367432ad0dd5b70eb7db01ab39d7a63c8fa6202abb08f9b2fd8538',
                     '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                     '34b6ce3dce188f08a95244f9fee8b86b294a63dfaa88cd7518f9c220d1e4895a',
                     '6da21c0af5da679007a21ce8d0a1e62dedace4e6a0acefd6b1aea788c442573f',
                     '8e271a3f880bc65a9c02c7835aa1dd6a1d2d2b87d541e7209df81c74a49d15b1',
                     '2aabb779ed645b6b6939805360e6e89cc3eec80d640e39a667e23c080487dd99',
                     '9442e51291b08fc1f8fc1ec9d19aa12927d256f22b265e1ac24d7fe6fcb1e50e',
                     'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                     '30ed57b03662192181e02eb2038ebc59840a1bb3327c807b9217e6d057c1e0e7',
                     '4479829dfd0f89b1241d3e1025004a97a89756c9e3075d01bd8340365ab322f2',
                     'b94116f58a188ecf7546d1e62988f6c3b5107a5d97f8c17484c84fde2e581508',
                     '0d63b183557ca1b28bc7ae47ca3ebdcdde5674c789eb9022d028d7ca7e2e813c',
                     '9aee160d14595e60e3315a4d0e910ba6106e3d821854d975b103b15d16c053c1',
                     '3d4c228646a864cdd81bafd2bb2bbfb5ff810d26523870f39ac54ed6639f3389',
                     '5bc06520a482fd00e556903ea9685f6f01300bd2b4eec15a2eae19214f99618e',
                     'b6ab2f07fccc0dbcf251546e7556122c77343665154a9fdcddefe9fc32edfb87',
                     'c211a9bf7910430befd886c4a14d5deef62cd6c018bf0a784fbda92580f3dd97',
                     '08f12dffa57caf364607f30dfe64e4397e65eacfc624a651face1529e907cb19',
                     '4eca56bdc816df40811bda60a86aeb03466e22a3d21df9a36ff03ba3160a2034',
                     '0d3d8844290ed333550936f61c9c1727bf60b4aa403cc49234130bc3f3d98b75'
    )
GROUP BY 1
;


SELECT TOUCH_ID,
       COUNT(*)
FROM (
         SELECT t.TOUCH_ID,
                t.ATTRIBUTED_USER_ID                                        as ATTRIBUTED_USER_ID,
                t.STITCHED_IDENTITY_TYPE                                    as STITCHED_IDENTITY_TYPE,
                MIN(e.EVENT_TSTAMP)                                         AS TOUCH_START_TSTAMP,
                MAX(e.EVENT_TSTAMP)                                         AS TOUCH_END_TSTAMP,
                TIMEDIFF(seconds, MIN(e.EVENT_TSTAMP), MAX(e.EVENT_TSTAMP)) AS TOUCH_DURATION_SECONDS,
                COUNT(*)                                                    AS TOUCH_EVENT_COUNT,
                CASE
                    WHEN SUM(CASE WHEN e.EVENT_NAME = 'transaction_item' THEN 1 ELSE 0 END) > 0
                        THEN TRUE
                    ELSE FALSE END                                          AS TOUCH_HAS_BOOKING
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.EVENT_HASH = e.EVENT_HASH
         WHERE t.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
         GROUP BY 1, 2, 3
     )
WHERE TOUCH_ID IN (
                   '05a61d57612d67d61ad23e6950ad16698c61c12c897a3c257877987b97d336bd',
                   'b5755fb2133e4904a06beaeac2c7c403dcd7111d3ddd39b05fc01bd223a38bc8',
                   'a5a625df594039192bd74bc8f9ce3da7fa25c1556ea38a60e6d1c0c8be8d3440',
                   'df0cd4304707599a7e98b0002783b2fc17415ff02d8bc491bf688dbc9aef05e5',
                   'd446b0c42da3617b9a826505332da1cfd5577d3b133e7b7be6952f5696437f6e',
                   'fdcf942c88aea1d948bc361c71e3bb7b7edd399442bb5c4ef146e38ed58ad925',
                   '5d10997a2bc2ddab273b9954a5dd112afad0993bef782b671cdd102bdbf91808',
                   '8d1efb3fdf45b2498d06a50a7cd09bac756a62aeddaf33b3d25d4c42223073bc',
                   '2dc254ab7498a76e47ad7ae82541a764f65350d0dc99b4bdf4ca10051b019bfb',
                   'b69476286d4465ca0a6b60479a622ca615c0a5d930e174692a3464f8aaa5a3a1',
                   'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f',
                   '56d9e18d3c70e0e2ebedfec186a8feb8409a289d6f1bb6f12aa43531fe052f65',
                   '0f606412efbfbfaa7c241d0cce420068b0a528f11f77654999474b4efc0e6f47',
                   'a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                   '5fcf726033d59cd956767c25d758527be699e648149368574dae56d389beeee0',
                   '2951ee2929c6fbbfb11d2c960e4b8367722cd1a657b74def08469dc2070fdca3',
                   'ac07924e0f367432ad0dd5b70eb7db01ab39d7a63c8fa6202abb08f9b2fd8538',
                   '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                   '34b6ce3dce188f08a95244f9fee8b86b294a63dfaa88cd7518f9c220d1e4895a',
                   '6da21c0af5da679007a21ce8d0a1e62dedace4e6a0acefd6b1aea788c442573f',
                   '8e271a3f880bc65a9c02c7835aa1dd6a1d2d2b87d541e7209df81c74a49d15b1',
                   '2aabb779ed645b6b6939805360e6e89cc3eec80d640e39a667e23c080487dd99',
                   '9442e51291b08fc1f8fc1ec9d19aa12927d256f22b265e1ac24d7fe6fcb1e50e',
                   'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                   '30ed57b03662192181e02eb2038ebc59840a1bb3327c807b9217e6d057c1e0e7',
                   '4479829dfd0f89b1241d3e1025004a97a89756c9e3075d01bd8340365ab322f2',
                   'b94116f58a188ecf7546d1e62988f6c3b5107a5d97f8c17484c84fde2e581508',
                   '0d63b183557ca1b28bc7ae47ca3ebdcdde5674c789eb9022d028d7ca7e2e813c',
                   '9aee160d14595e60e3315a4d0e910ba6106e3d821854d975b103b15d16c053c1',
                   '3d4c228646a864cdd81bafd2bb2bbfb5ff810d26523870f39ac54ed6639f3389',
                   '5bc06520a482fd00e556903ea9685f6f01300bd2b4eec15a2eae19214f99618e',
                   'b6ab2f07fccc0dbcf251546e7556122c77343665154a9fdcddefe9fc32edfb87',
                   'c211a9bf7910430befd886c4a14d5deef62cd6c018bf0a784fbda92580f3dd97',
                   '08f12dffa57caf364607f30dfe64e4397e65eacfc624a651face1529e907cb19',
                   '4eca56bdc816df40811bda60a86aeb03466e22a3d21df9a36ff03ba3160a2034',
                   '0d3d8844290ed333550936f61c9c1727bf60b4aa403cc49234130bc3f3d98b75'
    )
GROUP BY 1
;
--there are dupes in the merge. So not likely a retraction issue.

SELECT ATTRIBUTED_USER_ID,
       TOUCH_ID,
       COUNT(*)
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION

WHERE TOUCH_ID IN ('05a61d57612d67d61ad23e6950ad16698c61c12c897a3c257877987b97d336bd',
                   'b5755fb2133e4904a06beaeac2c7c403dcd7111d3ddd39b05fc01bd223a38bc8',
                   'a5a625df594039192bd74bc8f9ce3da7fa25c1556ea38a60e6d1c0c8be8d3440',
                   'df0cd4304707599a7e98b0002783b2fc17415ff02d8bc491bf688dbc9aef05e5',
                   'd446b0c42da3617b9a826505332da1cfd5577d3b133e7b7be6952f5696437f6e',
                   'fdcf942c88aea1d948bc361c71e3bb7b7edd399442bb5c4ef146e38ed58ad925',
                   '5d10997a2bc2ddab273b9954a5dd112afad0993bef782b671cdd102bdbf91808',
                   '8d1efb3fdf45b2498d06a50a7cd09bac756a62aeddaf33b3d25d4c42223073bc',
                   '2dc254ab7498a76e47ad7ae82541a764f65350d0dc99b4bdf4ca10051b019bfb',
                   'b69476286d4465ca0a6b60479a622ca615c0a5d930e174692a3464f8aaa5a3a1',
                   'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f',
                   '56d9e18d3c70e0e2ebedfec186a8feb8409a289d6f1bb6f12aa43531fe052f65',
                   '0f606412efbfbfaa7c241d0cce420068b0a528f11f77654999474b4efc0e6f47',
                   'a14f7a3751b8e9e5a96c3d36b04a968d029057860b34de1730f6cab95ec599a6',
                   '5fcf726033d59cd956767c25d758527be699e648149368574dae56d389beeee0',
                   '2951ee2929c6fbbfb11d2c960e4b8367722cd1a657b74def08469dc2070fdca3',
                   'ac07924e0f367432ad0dd5b70eb7db01ab39d7a63c8fa6202abb08f9b2fd8538',
                   '9a0fa5efb6653bde4c6793559ad9f4ccd9c3d325c36b46d6f1b253dc69a6e713',
                   '34b6ce3dce188f08a95244f9fee8b86b294a63dfaa88cd7518f9c220d1e4895a',
                   '6da21c0af5da679007a21ce8d0a1e62dedace4e6a0acefd6b1aea788c442573f',
                   '8e271a3f880bc65a9c02c7835aa1dd6a1d2d2b87d541e7209df81c74a49d15b1',
                   '2aabb779ed645b6b6939805360e6e89cc3eec80d640e39a667e23c080487dd99',
                   '9442e51291b08fc1f8fc1ec9d19aa12927d256f22b265e1ac24d7fe6fcb1e50e',
                   'e68c8b950504bdb7d56a273444f8ec4cf4691a4e5c8ed961d543c3d9791ef050',
                   '30ed57b03662192181e02eb2038ebc59840a1bb3327c807b9217e6d057c1e0e7',
                   '4479829dfd0f89b1241d3e1025004a97a89756c9e3075d01bd8340365ab322f2',
                   'b94116f58a188ecf7546d1e62988f6c3b5107a5d97f8c17484c84fde2e581508',
                   '0d63b183557ca1b28bc7ae47ca3ebdcdde5674c789eb9022d028d7ca7e2e813c',
                   '9aee160d14595e60e3315a4d0e910ba6106e3d821854d975b103b15d16c053c1',
                   '3d4c228646a864cdd81bafd2bb2bbfb5ff810d26523870f39ac54ed6639f3389',
                   '5bc06520a482fd00e556903ea9685f6f01300bd2b4eec15a2eae19214f99618e',
                   'b6ab2f07fccc0dbcf251546e7556122c77343665154a9fdcddefe9fc32edfb87',
                   'c211a9bf7910430befd886c4a14d5deef62cd6c018bf0a784fbda92580f3dd97',
                   '08f12dffa57caf364607f30dfe64e4397e65eacfc624a651face1529e907cb19',
                   '4eca56bdc816df40811bda60a86aeb03466e22a3d21df9a36ff03ba3160a2034',
                   '0d3d8844290ed333550936f61c9c1727bf60b4aa403cc49234130bc3f3d98b75'
    )
GROUP BY 1, 2;


--get events for one dupe touch
SELECT *
FROM SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
WHERE TOUCH_ID = 'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f';

SELECT e.EVENT_HASH,
       i.ATTRIBUTED_USER_ID,
       i.STITCHED_IDENTITY_TYPE,
       e.EVENT_TSTAMP,
       FIRST_VALUE(e.EVENT_HASH)
                   OVER (PARTITION BY i.ATTRIBUTED_USER_ID,
                       d.TIME_DIFF_PARTITION,
                       u.UTM_REF_PARTITION,
                       e.DEVICE_PLATFORM ORDER BY e.EVENT_TSTAMP) AS TOUCH_ID,
       ROW_NUMBER()
               OVER (PARTITION BY i.ATTRIBUTED_USER_ID,
                   d.TIME_DIFF_PARTITION,
                   u.UTM_REF_PARTITION,
                   e.DEVICE_PLATFORM ORDER BY e.EVENT_TSTAMP)     AS EVENT_INDEX_WITHIN_TOUCH

FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events e
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                    ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                       COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_utm_referrer_marker u
                    ON e.EVENT_HASH = u.EVENT_HASH
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_time_diff_marker d
                    ON e.EVENT_HASH = d.EVENT_HASH
WHERE e.EVENT_HASH IN ('c75760daf75746282c27bc118d8e6d370d6aa8061a8f58bcc1618efc53ae387a',
                       'e711d5235325776a95e559b3144fb9fd14a0c6878abe54a7685b0bb951497680',
                       '6b643fa1e4e64c9bbb0294ca957db063bdb43e45a25cc9cbc12c11703d1e1112',
                       '966ac1bacdc462635f0d339670f19c22e9a6ffe730a895e897f96707a9b1d9f7',
                       '92fcd46afaf0baa96f123879a088142033f17863e0d78b5410a7a1bc4cfc693f',
                       'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f'
    );

SELECT e.EVENT_HASH
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events e

WHERE e.EVENT_HASH IN ('c75760daf75746282c27bc118d8e6d370d6aa8061a8f58bcc1618efc53ae387a',
                       'e711d5235325776a95e559b3144fb9fd14a0c6878abe54a7685b0bb951497680',
                       '6b643fa1e4e64c9bbb0294ca957db063bdb43e45a25cc9cbc12c11703d1e1112',
                       '966ac1bacdc462635f0d339670f19c22e9a6ffe730a895e897f96707a9b1d9f7',
                       '92fcd46afaf0baa96f123879a088142033f17863e0d78b5410a7a1bc4cfc693f',
                       'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f'
    );


SELECT *
FROM (
         WITH events_from_restitched_users AS (
             SELECT e.EVENT_HASH
             FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                      INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                                 ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                    COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
             WHERE e.EVENT_NAME IN
                   ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
               AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
               AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
               AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
         ),
              touches_for_restitched_events AS (
                  --for all the events where a users has been restitched, get the touches associated to them
                  SELECT DISTINCT TOUCH_ID
                  FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
                  WHERE EVENT_HASH IN (SELECT EVENT_HASH FROM events_from_restitched_users)
              )
              --get all the events associated to any touch that might have been affected by restitching
         SELECT e.EVENT_HASH,
                e.EVENT_TSTAMP,
                e.DERIVED_TSTAMP,
                e.EVENT_NAME,
                e.PAGE_URL,
                e.PAGE_REFERRER,
                e.DEVICE_PLATFORM,
                e.UNIQUE_BROWSER_ID,
                e.COOKIE_ID,
                e.SESSION_USERID
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                  INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.EVENT_HASH = t.EVENT_HASH
         WHERE TOUCH_ID IN (SELECT TOUCH_ID FROM touches_for_restitched_events)
     )
WHERE EVENT_HASH IN ('c75760daf75746282c27bc118d8e6d370d6aa8061a8f58bcc1618efc53ae387a',
                     'e711d5235325776a95e559b3144fb9fd14a0c6878abe54a7685b0bb951497680',
                     '6b643fa1e4e64c9bbb0294ca957db063bdb43e45a25cc9cbc12c11703d1e1112',
                     '966ac1bacdc462635f0d339670f19c22e9a6ffe730a895e897f96707a9b1d9f7',
                     '92fcd46afaf0baa96f123879a088142033f17863e0d78b5410a7a1bc4cfc693f',
                     'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f'
    );
------------------------------------------------------------------------------------------------------------------------
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
--test prod
SELECT e.EVENT_HASH
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFIABLE_EVENTS e

WHERE e.EVENT_HASH IN ('c75760daf75746282c27bc118d8e6d370d6aa8061a8f58bcc1618efc53ae387a',
                       'e711d5235325776a95e559b3144fb9fd14a0c6878abe54a7685b0bb951497680',
                       '6b643fa1e4e64c9bbb0294ca957db063bdb43e45a25cc9cbc12c11703d1e1112',
                       '966ac1bacdc462635f0d339670f19c22e9a6ffe730a895e897f96707a9b1d9f7',
                       '92fcd46afaf0baa96f123879a088142033f17863e0d78b5410a7a1bc4cfc693f',
                       'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f');

--clone prod
CREATE OR REPLACE TABLE SCRATCH.ROBINPATEL.TOUCHIFIABLE_EVENTS_TEST CLONE DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFIABLE_EVENTS;
--check if events exist
SELECT e.EVENT_HASH
FROM SCRATCH.ROBINPATEL.TOUCHIFIABLE_EVENTS_TEST e

WHERE e.EVENT_HASH IN ('c75760daf75746282c27bc118d8e6d370d6aa8061a8f58bcc1618efc53ae387a',
                       'e711d5235325776a95e559b3144fb9fd14a0c6878abe54a7685b0bb951497680',
                       '6b643fa1e4e64c9bbb0294ca957db063bdb43e45a25cc9cbc12c11703d1e1112',
                       '966ac1bacdc462635f0d339670f19c22e9a6ffe730a895e897f96707a9b1d9f7',
                       '92fcd46afaf0baa96f123879a088142033f17863e0d78b5410a7a1bc4cfc693f',
                       'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f'
    );
--run merge into new table
MERGE INTO SCRATCH.ROBINPATEL.TOUCHIFIABLE_EVENTS_TEST AS TARGET
    --all events belonging to all touches from users that have been restitched.
    USING (
        WITH events_from_restitched_users AS (
            SELECT e.EVENT_HASH
            FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
                     INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                                ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                                   COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
            WHERE e.EVENT_NAME IN
                  ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
              AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
              AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
              AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
        ),
             touches_for_restitched_events AS (
                 --for all the events where a users has been restitched, get the touches associated to them
                 SELECT DISTINCT TOUCH_ID
                 FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
                 WHERE EVENT_HASH IN (SELECT EVENT_HASH FROM events_from_restitched_users)
             )
             --get all the events associated to any touch that might have been affected by restitching
        SELECT e.EVENT_HASH,
               e.EVENT_TSTAMP,
               e.DERIVED_TSTAMP,
               e.EVENT_NAME,
               e.PAGE_URL,
               e.PAGE_REFERRER,
               e.DEVICE_PLATFORM,
               e.UNIQUE_BROWSER_ID,
               e.COOKIE_ID,
               e.SESSION_USERID
        FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
                 INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.EVENT_HASH = t.EVENT_HASH
        WHERE TOUCH_ID IN (SELECT TOUCH_ID FROM touches_for_restitched_events)
    ) AS BATCH ON TARGET.EVENT_HASH = BATCH.EVENT_HASH
    WHEN NOT MATCHED
        THEN INSERT (
                     SCHEDULE_TSTAMP,
                     RUN_TSTAMP,
                     OPERATION_ID,
                     CREATED_AT,
                     UPDATED_AT,
                     EVENT_HASH,
                     EVENT_TSTAMP,
                     DERIVED_TSTAMP,
                     EVENT_NAME,
                     PAGE_URL,
                     PAGE_REFERRER,
                     DEVICE_PLATFORM,
                     UNIQUE_BROWSER_ID,
                     COOKIE_ID,
                     SESSION_USERID
        )
        VALUES ('2020-02-21 00:00:00',
                '2020-02-25 11:30:43',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/single_customer_view/snowplow/03_touchification/01_touchifiable_events.py__20200221T000000__daily',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                BATCH.EVENT_HASH,
                BATCH.EVENT_TSTAMP,
                BATCH.DERIVED_TSTAMP,
                BATCH.EVENT_NAME,
                BATCH.PAGE_URL,
                BATCH.PAGE_REFERRER,
                BATCH.DEVICE_PLATFORM,
                BATCH.UNIQUE_BROWSER_ID,
                BATCH.COOKIE_ID,
                BATCH.SESSION_USERID)
;
--check if they are now in
SELECT e.EVENT_HASH
FROM SCRATCH.ROBINPATEL.TOUCHIFIABLE_EVENTS_TEST e

WHERE e.EVENT_HASH IN ('c75760daf75746282c27bc118d8e6d370d6aa8061a8f58bcc1618efc53ae387a',
                       'e711d5235325776a95e559b3144fb9fd14a0c6878abe54a7685b0bb951497680',
                       '6b643fa1e4e64c9bbb0294ca957db063bdb43e45a25cc9cbc12c11703d1e1112',
                       '966ac1bacdc462635f0d339670f19c22e9a6ffe730a895e897f96707a9b1d9f7',
                       '92fcd46afaf0baa96f123879a088142033f17863e0d78b5410a7a1bc4cfc693f',
                       'fffa76d5449dc6d516fd24f45412f370460c3a706f5a6065ef869838cd681e4f'
    );

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

------------------------------------------------------------------------------------------------------------------------
--after setting caching to false

WITH events_from_restitched_users AS (
    SELECT e.EVENT_HASH
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching i
             INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e
                        ON COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) =
                           COALESCE(i.UNIQUE_BROWSER_ID, i.COOKIE_ID, i.SESSION_USERID)
    WHERE e.EVENT_NAME IN
          ('page_view', 'screen_view', 'transaction_item', 'transaction')          -- explicitly define the events we want to touchify
      AND COALESCE(e.UNIQUE_BROWSER_ID, e.COOKIE_ID, e.SESSION_USERID) IS NOT NULL -- we only want to sessionise events that can be attributed to a user
      AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                          -- remove extra computation required to resessionise robot events
      AND i.UPDATED_AT >= TIMESTAMPADD('day', -1, '2020-02-21 00:00:00'::TIMESTAMP)
),
     touches_for_restitched_events AS (
         --for all the events where a users has been restitched, get the touches associated to them
         SELECT DISTINCT TOUCH_ID
         FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
         WHERE EVENT_HASH IN (SELECT EVENT_HASH FROM events_from_restitched_users)
     )
     --get all the events associated to any touch that might have been affected by restitching
SELECT e.EVENT_HASH,
       e.EVENT_TSTAMP,
       e.DERIVED_TSTAMP,
       e.EVENT_NAME,
       e.PAGE_URL,
       e.PAGE_REFERRER,
       e.DEVICE_PLATFORM,
       e.UNIQUE_BROWSER_ID,
       e.COOKIE_ID,
       e.SESSION_USERID
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON e.EVENT_HASH = t.EVENT_HASH
WHERE TOUCH_ID IN (SELECT TOUCH_ID FROM touches_for_restitched_events)

SELECT UPDATED_AT
FROM SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
GROUP BY 1;

DELETE
FROM DATA_VAULT_MVP_DEV_ROBIN.SINGLE_CUSTOMER_VIEW_STG.MODULE_TOUCHIFICATION
WHERE UPDATED_AT = '2020-02-25 14:50:22.380000000';

