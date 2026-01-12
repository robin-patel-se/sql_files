USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA COLLAB.SS_TRACKING_QA;

CREATE OR REPLACE TABLE SS_EVENTS AS (
    SELECT CASE
               WHEN V_TRACKER LIKE 'py-%' THEN 'TB'
               WHEN V_TRACKER LIKE 'java-%' THEN 'SE' END AS SS_TRACKING_PLATFORM,
           *
    FROM SNOWPLOW.ATOMIC.EVENTS
    WHERE (
                  V_TRACKER LIKE 'py-%' OR --travel bird
                  V_TRACKER LIKE 'java-%' --se core
              )
);

SELECT *
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view';

SELECT SS_TRACKING_PLATFORM,
       APP_ID,
       COUNT(*) AS EVENTS
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
GROUP BY 1, 2
ORDER BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
--SE SS tracking QA
SELECT *
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE';

--SE logged out
SELECT CASE WHEN USER_ID IS NOT NULL THEN 'logged_in' ELSE 'logged_out' END as login_status,
       count(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1;

--SE domain userid
SELECT CASE
           WHEN DOMAIN_USERID IS NULL THEN 'domain_userid_null'
           else 'domain_userid_not_null' end as domain_userid_status,
       count(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1;



SELECT CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['se_user_id']::INT
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE';

--environment context
SELECT --CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1,
--        CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['affiliate']::VARCHAR,
       CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['device_platform']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['environment']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['tracking_platform']::VARCHAR,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

SELECT CASE
           WHEN CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['affiliate']::VARCHAR IS NULL THEN 'affiliate_null'
           ELSE 'affiliate_populated' END AS affiliate_status,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--user context
SELECT CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['booker_type']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['member_join_date']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['original_affiliate_name']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['se_user_id']::VARCHAR,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
  AND USER_ID IS NOT NULL -- logged in users
GROUP BY 1
ORDER BY 2 DESC;

--content context
SELECT
--        CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['category']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['name']::VARCHAR,
CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['sub_category']::VARCHAR,
COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2;

SELECT *
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
  AND CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['name']::VARCHAR IS NULL;

--sale context
SELECT
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['active_sale'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['configuration']::VARCHAR,
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['end_date']::DATE,
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['id'],
CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['line'], -- flash
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['name'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['start_date'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['tech_provider']::VARCHAR,
COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2;

SELECT CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
  AND CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['id'] IS NULL;

--product display context
SELECT
-- CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory']::VARCHAR,
-- CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory_language']::VARCHAR,
-- CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['se_group_brand']::VARCHAR,
-- CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['se_group_business_unit'],
CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['tech_platform'],
COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--user agent
SELECT USERAGENT,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--dvce type
SELECT DVCE_TYPE,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--device_platform
SELECT CASE
           WHEN APP_ID LIKE 'ios_app%'-- native events
               OR
                USERAGENT like '%mobile_native_v3%' -- webkit wrapped forwarded via native app
               THEN 'native app'
           WHEN USERAGENT like '%mobile_wrap:{platform:ios%' THEN 'mobile wrap ios'
           WHEN USERAGENT like '%mobile_wrap: {"platform":"android"%' THEN 'mobile wrap android'
           WHEN DVCE_TYPE = 'Computer' THEN 'web'
           WHEN DVCE_TYPE = 'Mobile' THEN 'mobile web'
           WHEN DVCE_TYPE = 'Tablet' THEN 'tablet web'
           WHEN DVCE_TYPE != 'Unknown' THEN lower(DVCE_TYPE)
           ELSE 'not specified'
           END AS device_platform,

       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;


--event tstamp
SELECT DATE_TRUNC(hour, CASE
                            WHEN DERIVED_TSTAMP > COLLECTOR_TSTAMP THEN COLLECTOR_TSTAMP
    --to correct for when the derived time is set in the future
                            WHEN APP_ID LIKE 'ios_app%' AND DVCE_SENT_TSTAMP <= COLLECTOR_TSTAMP THEN DVCE_SENT_TSTAMP
    -- to correct for incorrect dvce_created_tstamp on all native app events
                            WHEN DATEDIFF(day, DERIVED_TSTAMP, COLLECTOR_TSTAMP) > 1 AND
                                 DVCE_SENT_TSTAMP <= COLLECTOR_TSTAMP
                                THEN DVCE_SENT_TSTAMP
    --to correct occurrences of the derived tstamp being so out due to the dvce created and dvce sent tstamp being so disparate
                            WHEN DATEDIFF(day, DERIVED_TSTAMP, COLLECTOR_TSTAMP) > 1 AND
                                 DVCE_SENT_TSTAMP > dateadd(hour, 1, COLLECTOR_TSTAMP)
                                THEN COLLECTOR_TSTAMP
    --to correct occurrences of the derived tstamp being so out due to the dvce created and dvce sent tstamp being so disparate AND dvce tstamps being in the future
                            ELSE DERIVED_TSTAMP
    END) as event_tstamp,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--event hash

SELECT SHA2(
                   COALESCE(EVENT_ID, '') ||
                   COALESCE(EVENT_FINGERPRINT, '') ||
                   COALESCE(COLLECTOR_TSTAMP, '1970-01-01 00:00:00') || --coalesce to a tstamp incase of null to avoid errors
                   COALESCE(DERIVED_TSTAMP, '1970-01-01 00:00:00') ||
                   COALESCE(USER_IPADDRESS, '') ||
                   COALESCE(DVCE_SENT_TSTAMP, '1970-01-01 00:00:00')
           ) AS event_hash,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC;

SELECT EVENT_ID,
       EVENT_FINGERPRINT,
       COLLECTOR_TSTAMP,
       DERIVED_TSTAMP,
       USER_IPADDRESS,
       DVCE_SENT_TSTAMP

FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
  AND (EVENT_ID IS NULL OR
       EVENT_FINGERPRINT IS NULL OR
       COLLECTOR_TSTAMP IS NULL OR
       DERIVED_TSTAMP IS NULL OR
       USER_IPADDRESS IS NULL OR
       DVCE_SENT_TSTAMP IS NULL
    )
ORDER BY 2 DESC;

SELECT SUM(CASE WHEN DVCE_SENT_TSTAMP IS NULL THEN 1 END),
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'

--identity fragment
SELECT COALESCE(
               DOMAIN_USERID,
               CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar,
               CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::varchar,
               TI_ORDERID)
           AS identity_fragment,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;



SELECT CASE
           WHEN COALESCE(
                   DOMAIN_USERID,
                   CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar,
                   CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::varchar,
                   TI_ORDERID) IS NOT NULL THEN 'has_identity_fragment'
           else 'no_identity_fragment' end
           AS identity_fragment_availability,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--robot spider
SELECT CASE
           WHEN BR_FAMILY = 'Robot/Spider'
               OR
                USERAGENT REGEXP
                '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
               THEN TRUE
           ELSE FALSE END AS is_robot_spider_event,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE'
GROUP BY 1
ORDER BY 2 DESC;

--ip address
SELECT DISTINCT USER_IPADDRESS
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'SE';
