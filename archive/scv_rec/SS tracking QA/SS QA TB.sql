USE WAREHOUSE PIPE_LARGE;

CREATE SCHEMA COLLAB.SS_TRACKING_QA;
USE SCHEMA COLLAB.SS_TRACKING_QA;


------------------------------------------------------------------------------------------------------------------------
--TB SS tracking QA
SELECT COLLECTOR_TSTAMP,
       ETL_TSTAMP,
       DERIVED_TSTAMP,
       DVCE_SENT_TSTAMP,
       DVCE_CREATED_TSTAMP
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB';

--TB null pageurl
SELECT CASE
           WHEN PAGE_URL IS NOT NULL THEN 'page_url_is_not_null'
           ELSE 'page_url_is_null' END as page_url_null_status,
       count(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

--TB logged out
SELECT CASE WHEN USER_ID IS NOT NULL THEN 'logged_in' ELSE 'logged_out' END as login_status,
       count(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

--TB null domain userid
SELECT CASE
           WHEN DOMAIN_USERID IS NULL THEN 'domain_userid_null'
           else 'domain_userid_not_null' end as domain_userid_status,
       count(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;


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
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2 DESC;

SELECT CASE
           WHEN CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['affiliate']::VARCHAR IS NULL THEN 'affiliate_null'
           ELSE 'affiliate_populated' END AS affiliate_status,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2 DESC;

SELECT COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND CONTEXTS_COM_SECRETESCAPES_ENVIRONMENT_CONTEXT_1[0]['device_platform']::VARCHAR != 'n/a';

--user context
SELECT
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['booker_type']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['member_join_date']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['original_affiliate_name']::VARCHAR,
CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['se_user_id']::VARCHAR,
COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND USER_ID IS NOT NULL -- logged in users
GROUP BY 1
ORDER BY 2 DESC;

SELECT CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['member_join_date']::VARCHAR IS NOT NULL
;

--content context
SELECT
--        CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['category']::VARCHAR,
--        CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['name']::VARCHAR,
CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['sub_category']::VARCHAR,
COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2;

SELECT *
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND CONTEXTS_COM_SECRETESCAPES_CONTENT_CONTEXT_1[0]['name']::VARCHAR IS NULL;


--sale context
SELECT
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['active_sale'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['configuration']::VARCHAR,
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['end_date'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['id']::VARCHAR,
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['line'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['name'],
-- CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['start_date'],
CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['tech_provider'],
COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2;

SELECT COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND CONTEXTS_COM_SECRETESCAPES_SECRET_ESCAPES_SALE_CONTEXT_1[0]['id']::VARCHAR IS NULL;

--display context
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
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2 DESC;

--user agent
SELECT USERAGENT,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2 DESC;

--dvce type
SELECT DVCE_TYPE,

       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
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
  AND SS_TRACKING_PLATFORM = 'TB'
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
  AND SS_TRACKING_PLATFORM = 'TB'
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
  AND SS_TRACKING_PLATFORM = 'TB'
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
  AND SS_TRACKING_PLATFORM = 'TB'
  AND (EVENT_ID IS NULL OR
       EVENT_FINGERPRINT IS NULL OR
       COLLECTOR_TSTAMP IS NULL OR
       DERIVED_TSTAMP IS NULL OR
       USER_IPADDRESS IS NULL OR
       DVCE_SENT_TSTAMP IS NULL
    )
ORDER BY 2 DESC;

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
  AND SS_TRACKING_PLATFORM = 'TB'
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
       EVENT,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1, 2
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
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1
ORDER BY 2 DESC;

--ip address
SELECT USER_IPADDRESS,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

--booking events
SELECT CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR, *
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'page_view'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1 IS NOT NULL;

SELECT UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

SELECT CASE
           WHEN UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR IS NOT NULL
               THEN 'update_event_not_null'
           else 'update_event_null' end as update_event_status,
       COUNT(*)
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

SELECT *
FROM SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

--booking context
SELECT CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
       *
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

SELECT CASE
           WHEN CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR IS NULL THEN 'booking_id_is_null'
           ELSE 'booking_id_is_not_null' END AS booking_id_status,
       count(*)
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

SELECT CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1,
       CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1,
       UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

--user state context
SELECT COUNT(*)
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1 IS NOT NULL;

--user context

SELECT CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

SELECT CASE
           WHEN CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['se_user_id']::VARCHAR IS NULL THEN 'null_se_user_id'
           ELSE 'non_null_se_user_id' end AS se_user_id_status,
       count(*)
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

SELECT CASE
           WHEN CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['email_address']::VARCHAR IS NULL THEN 'null_email_address'
           ELSE 'non_null_email_address' end AS email_address_status,
       count(*)
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

SELECT CASE
           WHEN CONTEXTS_COM_SECRETESCAPES_USER_CONTEXT_1[0]['original_affiliate_name']::VARCHAR IS NULL
               THEN 'null_original_affiliate_name'
           ELSE 'non_null_original_affiliate_name' end AS original_affiliate_name_status,
       count(*)
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

--lift booking id, bookings that occurred on the 22nd of January 2020
SELECT CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
       RIGHT(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
             LENGTH(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                 -
             POSITION('-' IN CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
           )
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

SELECT CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
       RIGHT(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
             LENGTH(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                 -
             POSITION('-' IN CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
           )
FROM SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE = '2020-01-24'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB';

SELECT COLLECTOR_TSTAMP::DATE,
       COUNT(distinct CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR)

FROM COLLAB.SS_TRACKING_QA.SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE >= '2020-01-01'
  AND COLLECTOR_TSTAMP::DATE <= '2020-01-26'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
GROUP BY 1;

SELECT TRANSACTION_ID, TERRITORY
FROM RAW_VAULT_MVP.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY;

SELECT DISTINCT SS_territory,
                BS_TERRITORY

FROM (
         SELECT RIGHT(tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
                      LENGTH(
                              tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                          -
                      POSITION('-' IN
                               tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
                    )                                                                                   as booking_id,
                tbss.CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory']::VARCHAR AS SS_territory,
                tbbs.TERRITORY                                                                          as BS_TERRITORY

         FROM SS_EVENTS tbss
                  INNER JOIN RAW_VAULT_MVP.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY tbbs ON
                 RIGHT(tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
                       LENGTH(
                               tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                           -
                       POSITION('-' IN
                                tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
                     ) = tbbs.TRANSACTION_ID

         WHERE tbss.UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR =
               'booking confirmed'
           AND tbss.COLLECTOR_TSTAMP::DATE >= '2020-01-01'
           AND tbss.COLLECTOR_TSTAMP::DATE <= '2020-01-27'
           AND tbss.EVENT_NAME = 'booking_update_event'
           AND tbss.SS_TRACKING_PLATFORM = 'TB'
           AND tbss.CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory']::VARCHAR != tbbs.TERRITORY
     )
GROUP BY 1, 2;

SELECT TERRITORY,
       count(distinct TRANSACTION_ID) as bookings
FROM RAW_VAULT_MVP.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY
WHERE CUSTOMER_ID IS NULL
group by 1;


SELECT RIGHT(tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
             LENGTH(
                     tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                 -
             POSITION('-' IN
                      tbss.CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
           )                                                                                   as booking_id,
       tbss.CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory']::VARCHAR AS SS_territory

FROM SS_EVENTS tbss
WHERE tbss.UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR =
      'booking confirmed'
  AND tbss.COLLECTOR_TSTAMP::DATE = '2020-01-22'
  AND tbss.EVENT_NAME = 'booking_update_event'
  AND tbss.SS_TRACKING_PLATFORM = 'TB';


SELECT RIGHT(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
             LENGTH(
                     CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                 -
             POSITION('-' IN
                      CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
           )                                                                              as booking_id,
       CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory']::VARCHAR AS SS_territory

FROM COLLAB.SS_TRACKING_QA.SS_EVENTS
WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
  AND COLLECTOR_TSTAMP::DATE >= '2020-01-01'
  AND COLLECTOR_TSTAMP::DATE <= '2020-01-26'
  AND EVENT_NAME = 'booking_update_event'
  AND SS_TRACKING_PLATFORM = 'TB'
  AND RIGHT(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
            LENGTH(
                    CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- length of entire string
                -
            POSITION('-' IN
                     CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR) -- position of hyphen
          )
    IN (
        '21888076',
        '21889781',
        '21887657',
        '21888764',
        '21889123',
        '21889208',
        '21889332')
;

SELECT DISTINCT UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE EVENT_NAME = 'booking_update_event'


SELECT *
FROM RAW_VAULT_MVP.TRAVELBIRD_CATALOGUE.BOOKING_SUMMARY
WHERE TRANSACTION_ID
          IN (
              '21888076',
              '21889781',
              '21887657',
              '21888764',
              '21889123',
              '21889208',
              '21889332');


--adam's
SELECT COLLECTOR_TSTAMP::DATE                                                  AS DATE,
       UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category'] AS BOOKING_EVENT_NAME,
       SUM(CASE
               WHEN CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id'] IS NOT NULL
                   THEN 1 END)                                                 AS BOOKING_ID_VALID_ROW_COUNT,
       SUM(CASE
               WHEN CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id'] IS NULL
                   THEN 1 END)                                                 AS BOOKING_ID_NULL_ROW_COUNT,
       COUNT(EVENT)                                                            as EVENTS_COUNT
FROM COLLAB.SS_TRACKING_QA.SS_EVENTS
WHERE COLLECTOR_TSTAMP::DATE >= '2020-01-01'
  AND EVENT NOT IN ('page_ping', 'page_view')
  AND UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category'] LIKE 'booking confirmed'
  AND SS_TRACKING_PLATFORM = 'TB' -- note this is not a persisted field in snowplow atomic events
GROUP BY DATE, BOOKING_EVENT_NAME;

SELECT CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR,
       *
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE COLLECTOR_TSTAMP::DATE >= '2020-02-01'
  AND UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed';

