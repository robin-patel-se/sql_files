USE WAREHOUSE PIPE_LARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE TABLE SNOWPLOW_SAMPLE_EVENTS AS (
    SELECT *
    FROM SNOWPLOW.ATOMIC.EVENTS
    WHERE COLLECTOR_TSTAMP >= '2019-10-01'
--     OR (COLLECTOR_TSTAMP >= '2019-01-01' AND COLLECTOR_TSTAMP <= '2019-01-31')
--     OR (COLLECTOR_TSTAMP >= '2018-07-01' AND COLLECTOR_TSTAMP <= '2018-07-31')
);

SELECT COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS; -- 383891237

SELECT COUNT(DISTINCT EVENT_ID)
FROM SNOWPLOW_SAMPLE_EVENTS; --379632993


SELECT EVENT_ID,
       COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS
GROUP BY 1
order by 2 desc;

CREATE OR REPLACE TABLE DUPE_EVENT_IDS AS ( -- Create a table with event ids that are duped.
    SELECT EVENT_ID,
           COUNT(*) AS no_of_dupes
    FROM SNOWPLOW_SAMPLE_EVENTS
    GROUP BY 1
    HAVING COUNT(*) > 1
);

SELECT APP_ID, USERAGENT, BR_FAMILY
from SNOWPLOW_SAMPLE_EVENTS_UID;

SELECT USERAGENT,
       BR_FAMILY,
       USERAGENT REGEXP
       '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*' AS test
FROM SNOWPLOW_SAMPLE_EVENTS
WHERE EVENT_ID = '0beb927a-3557-434d-85c7-6484f3317761';
--example of mobile event that has different tstamps and ip address but same event id

------------------------------------------------------------------------------------------------------------------------
--unique id
SELECT COUNT(*)
FROM (
         SELECT EVENT_ID,
                COUNT(*)
         FROM SNOWPLOW_SAMPLE_EVENTS
         GROUP BY 1
         HAVING COUNT(*) > 1
     );--431384

SELECT COUNT(*)
FROM (
         SELECT EVENT_FINGERPRINT,
                COUNT(*)
         FROM SNOWPLOW_SAMPLE_EVENTS
         GROUP BY 1
         HAVING COUNT(*) > 1
     );--418186


SELECT COUNT(*)
FROM (
         SELECT EVENT_ID,
                EVENT_FINGERPRINT,
                COUNT(*)
         FROM SNOWPLOW_SAMPLE_EVENTS
         GROUP BY 1, 2
         HAVING COUNT(*) > 1
     );--371801


SELECT COUNT(*)
FROM (
         SELECT EVENT_ID,
                EVENT_FINGERPRINT,
                COLLECTOR_TSTAMP,
                COUNT(*)
         FROM SNOWPLOW_SAMPLE_EVENTS
         GROUP BY 1, 2, 3
         HAVING COUNT(*) > 1
     );--11

SELECT COUNT(*)
FROM (
         SELECT EVENT_ID,
                EVENT_FINGERPRINT,
                DVCE_SENT_TSTAMP,
                COUNT(*)
         FROM SNOWPLOW_SAMPLE_EVENTS
         GROUP BY 1, 2, 3
         HAVING COUNT(*) > 1
     );--11

SELECT COUNT(*)
FROM (
         SELECT EVENT_ID,
                EVENT_FINGERPRINT,
                COLLECTOR_TSTAMP,
                DERIVED_TSTAMP,
                COUNT(*)
         FROM SNOWPLOW_SAMPLE_EVENTS
         GROUP BY 1, 2, 3, 4
         HAVING COUNT(*) > 1
     );--0

CREATE OR REPLACE TABLE SNOWPLOW_SAMPLE_EVENTS_UID AS (
    SELECT e.*,
           SHA2(
                       COALESCE(EVENT_ID, '') ||
                       COALESCE(EVENT_FINGERPRINT, '') ||
                       COALESCE(COLLECTOR_TSTAMP, '') ||
                       COALESCE(DERIVED_TSTAMP, '') ||
                       COALESCE(USER_IPADDRESS, '') ||
                       COALESCE(DVCE_SENT_TSTAMP, '')) AS UEVENT_ID
    FROM SNOWPLOW_SAMPLE_EVENTS e
);
--SHA2 ON A NULL WILL OUTPUT A NULL SO COALESCE INCASE

SELECT UEVENT_ID,
       COUNT(*) AS no_of_dupes
FROM SNOWPLOW_SAMPLE_EVENTS_UID
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT *
FROM SNOWPLOW_SAMPLE_EVENTS_UID;


------------------------------------------------------------------------------------------------------------------------
--identity_fragment
SELECT EVENT_NAME,
       SUM(CASE WHEN DOMAIN_USERID IS NULL THEN 1 ELSE 0 END) AS DOMAIN_USER_IDS,
       COUNT(*)                                               AS EVENTS
FROM SNOWPLOW_SAMPLE_EVENTS_UID
GROUP BY 1;

SELECT CONTEXTS_COM_SECRETESCAPES_SCREEN_CONTEXT_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE EVENT_NAME = 'screen_view';

SELECT UEVENT_ID,
       EVENT_NAME,
       DOMAIN_USERID,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar,
       COALESCE(DOMAIN_USERID,
                CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar) AS identity_fragment
FROM SNOWPLOW_SAMPLE_EVENTS_UID;

SELECT COUNT(*)
FROM (
         SELECT UEVENT_ID,
                EVENT_NAME,
                DOMAIN_USERID,
                CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar,
                COALESCE(DOMAIN_USERID,
                         CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1[0]['userId']::varchar) AS identity_fragment
         FROM SNOWPLOW_SAMPLE_EVENTS_UID) as e
WHERE e.identity_fragment IS NULL
;
------------------------------------------------------------------------------------------------------------------------
--user_id
--found instances where the user id is filled with ""

SELECT COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE USER_ID = '""';

SELECT USER_ID,
       try_to_number(USER_ID) AS se_user_id
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE USER_ID = '""';

------------------------------------------------------------------------------------------------------------------------
--robot spider

SELECT e.*,
       CASE
           WHEN BR_FAMILY = 'Robot/Spider'
               OR
                USERAGENT REGEXP
                '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
               THEN TRUE
           ELSE FALSE END AS robot_spider_event
FROM SNOWPLOW_SAMPLE_EVENTS_UID e
;


SELECT robot_spider_event, count(*)
FROM (
         SELECT e.*,
                CASE
                    WHEN BR_FAMILY = 'Robot/Spider'
                        OR
                         USERAGENT REGEXP
                         '.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt).*'
                        THEN TRUE
                    ELSE FALSE END AS robot_spider_event
         FROM SNOWPLOW_SAMPLE_EVENTS_UID e
     )
GROUP BY 1
;
-- 3,707,860 out of 380,183,377


------------------------------------------------------------------------------------------------------------------------
--tstamps
SELECT DVCE_SENT_TSTAMP,
       DVCE_CREATED_TSTAMP,
       DERIVED_TSTAMP,
       COLLECTOR_TSTAMP,
       ETL_TSTAMP,
       TRUE_TSTAMP,
       REFR_DVCE_TSTAMP
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE APP_ID LIKE 'ios_app%';

SELECT date_trunc(month, DVCE_CREATED_TSTAMP), count(*)
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE APP_ID LIKE 'ios_app%'
  AND USERAGENT
group by 1;

------------------------------------------------------------------------------------------------------------------------
--taxonomy for important fields
--territory
SELECT APP_ID,
       regexp_replace(APP_ID, 'ios_app ') as territory
FROM SNOWPLOW_SAMPLE_EVENTS_UID
group by 1;

--platform type

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
           END AS platform_type, --
       COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS_UID
group by 1;

--device type

SELECT DVCE_TYPE,
       CASE
           WHEN APP_ID LIKE 'ios_app%' OR USERAGENT like '%mobile_native_v3%' THEN 'Mobile'
           WHEN DVCE_TYPE != 'Unknown' THEN DVCE_TYPE
           ELSE 'Unknown'
           END AS DEVICE_CATEGORY,
       COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS_UID
group by 1, 2
;
------------------------------------------------------------------------------------------------------------------------
--enrichment

SELECT CONTEXTS_COM_SECRETESCAPES_ALL_PAGES_SESSION_LOGIN_TYPE_CONTEXT_1[0]['session_login_type']::VARCHAR AS LOGIN_TYPE,            --SEMI_MANUAL_LOGIN, EMAIL_SEMI_LOGIN, REMEMBERED, LOGGED_OUT, REGISTERED, FB_LOGIN, UNKNOWN, MANUAL_LOGIN, GOOGLE_REGISTER, FB_REGISTER, GOOGLE_LOGIN, EMAIL
       CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['sale_id']::VARCHAR                               AS SALE_ID,
       CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['sale_type']::VARCHAR                             AS SALE_TYPE,             -- HOTEL, PACKAGE, FLASH, DAY, TRAVEL, HOTEL_PLUS, UNKNOWN
       CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['travel_type_info']::VARCHAR                      AS SALE_TRAVEL_TYPE_INFO, -- HOTEL, HOTEL_PLUS, STATIC_PACKAGE, IHP_CONNECTED, DYNAMIC_PACKAGE, UNSUPPORTED, VOUCHER, UNKNOWN
       CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1[0]['territory']::VARCHAR                             AS SALE_TERRITORY,        -- DE, UK, IT, SE, FR, NL, DK, BE, CH, ES, US, MY, HK
       CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1[0]['affiliate_id']::VARCHAR                         AS AFFILIATE_ID,
       CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1[0]['affiliate_name']::VARCHAR                       AS AFFILIATE_NAME,        -- Secret Escapes DE, ES magazine, Secret Escapes IT, Google CPL Germany - Brand, Secret Escapes SE, Secret Escapes FR, Google PPC Brand Sign-ups, Secret Escapes NL, Secret Escapes DE for mobile wrap, Brand PPC Members (CPA), Google CPL Germany - Non-Brand, Google CPL Italy - Brand
       CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1[0]['original_affiliate_id']::VARCHAR                AS ORIGINAL_AFFILIATE_ID,
       CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1[0]['original_affiliate_name']::VARCHAR              AS ORIGINAL_AFFILIATE_NAME
FROM SNOWPLOW_SAMPLE_EVENTS_UID;

------------------------------------------------------------------------------------------------------------------------
--parsing utm params for marketing information

SELECT PAGE_URLQUERY,
       MKT_SOURCE,
       REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e'),
       MKT_MEDIUM,
       REGEXP_SUBSTR(page_urlquery, 'utm_medium=([^($|&)]+)', 1, 1, 'e'),
       MKT_CAMPAIGN,
       REGEXP_SUBSTR(page_urlquery, 'utm_campaign=([^($|&)]+)', 1, 1, 'e')
FROM SNOWPLOW_SAMPLE_EVENTS_UID;


SELECT PAGE_URLQUERY,
       MKT_SOURCE,
       REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e')
FROM (
         SELECT e.*,
                CASE
                    WHEN MKT_SOURCE is not distinct from
                         REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e') THEN 1
                    ELSE 0 END
                    AS mkt_source_match
         FROM SNOWPLOW_SAMPLE_EVENTS_UID e
     )
WHERE mkt_source_match = 0;

SELECT COUNT(*)
FROM SNOWPLOW_SAMPLE_EVENTS_UID
WHERE REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e') IS NOT NULL; --137620346

SELECT PAGE_URL, MKT_SOURCE, REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e')
FROM (
         SELECT e.*,
                CASE
                    WHEN MKT_SOURCE is not distinct from
                         REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e') THEN 1
                    ELSE 0 END
                    AS mkt_source_match
         FROM SNOWPLOW_SAMPLE_EVENTS_UID e
     )
WHERE mkt_source_match = 0;--44160 0.03% have dupe non matching utm_source

SELECT PAGE_URLQUERY
FROm SNOWPLOW_SAMPLE_EVENTS_UID
wHERE PAGE_URLQUERY IS NOT NULL;

SELECT PAGE_URLQUERY,
       REGEXP_SUBSTR(page_urlquery, 'utm_source=([^($|&)]+)', 1, 1, 'e')   as UTM_SOURCE,
--        MKT_SOURCE,
       REGEXP_SUBSTR(page_urlquery, 'utm_campaign=([^($|&)]+)', 1, 1, 'e') as UTM_CAMPAIGN,
--        MKT_CAMPAIGN,
       REGEXP_SUBSTR(page_urlquery, 'utm_content=([^($|&)]+)', 1, 1, 'e')  as UTM_CONTENT,
--        MKT_CONTENT,
       REGEXP_SUBSTR(page_urlquery, 'utm_medium=([^($|&)]+)', 1, 1, 'e')   as UTM_MEDIUM,
--        MKT_MEDIUM,
       REGEXP_SUBSTR(page_urlquery, 'utm_term=([^($|&)]+)', 1, 1, 'e')     as UTM_TERM,
--        MKT_TERM,
       CASE
           WHEN
               REGEXP_SUBSTR(page_urlquery, 'gclid=([^($|&)]+)', 1, 1, 'e') IS NOT NULL
               THEN
               REGEXP_SUBSTR(page_urlquery, 'gclid=([^($|&)]+)', 1, 1, 'e')
           WHEN
               REGEXP_SUBSTR(page_urlquery, 'msclkid=([^($|&)]+)', 1, 1, 'e') IS NOT NULL
               THEN
               REGEXP_SUBSTR(page_urlquery, 'msclkid=([^($|&)]+)', 1, 1, 'e')
           WHEN
               REGEXP_SUBSTR(page_urlquery, 'dclid=([^($|&)]+)', 1, 1, 'e') IS NOT NULL
               THEN
               REGEXP_SUBSTR(page_urlquery, 'dclid=([^($|&)]+)', 1, 1, 'e')
           WHEN
               REGEXP_SUBSTR(page_urlquery, 'clickid=([^($|&)]+)', 1, 1, 'e') IS NOT NULL
               THEN
               REGEXP_SUBSTR(page_urlquery, 'clickid=([^($|&)]+)', 1, 1, 'e')
           ELSE NULL
           END                                                             AS UTM_CLICKID
--        MKT_CLICKID
FROM SNOWPLOW_SAMPLE_EVENTS_UID;

-- List of some of the utm params
-- jl_cmpn
-- jl_uid
-- utm_campaign
-- utm_source
-- utm_content
-- noPasswordSignIn
-- utm_medium
-- clickid
-- gclid
-- dclid
-- msclkid
-- irgwc
-- utm_medium
-- utm_source
-- utm_campaign
-- utm_content
-- saff
-- endecken
-- affiliateUrlString
-- referrerId
-- affiliate
-- sfmc_sub
-- mid
-- se_source
-- gce_ukper
-- fromApp
-- Snowplow


------------------------------------------------------------------------------------------------------------------------

SELECT APP_ID,
       PLATFORM,
       ETL_TSTAMP,
       COLLECTOR_TSTAMP,
       DVCE_CREATED_TSTAMP,
       EVENT,
       EVENT_ID,
       TXN_ID,
       NAME_TRACKER,
       V_TRACKER,
       V_COLLECTOR,
       V_ETL,
       USER_ID,
       USER_IPADDRESS,
       USER_FINGERPRINT,
       DOMAIN_USERID,
       DOMAIN_SESSIONIDX,
       NETWORK_USERID,
       GEO_COUNTRY,
       GEO_REGION,
       GEO_CITY,
       GEO_ZIPCODE,
       GEO_LATITUDE,
       GEO_LONGITUDE,
       GEO_REGION_NAME,
       IP_ISP,
       IP_ORGANIZATION,
       IP_DOMAIN,
       IP_NETSPEED,
       PAGE_URL,
       PAGE_TITLE,
       PAGE_REFERRER,
       PAGE_URLSCHEME,
       PAGE_URLHOST,
       PAGE_URLPORT,
       PAGE_URLPATH,
       PAGE_URLQUERY,
       PAGE_URLFRAGMENT,
       REFR_URLSCHEME,
       REFR_URLHOST,
       REFR_URLPORT,
       REFR_URLPATH,
       REFR_URLQUERY,
       REFR_URLFRAGMENT,
       REFR_MEDIUM,
       REFR_SOURCE,
       REFR_TERM,
       MKT_MEDIUM,
       MKT_SOURCE,
       MKT_TERM,
       MKT_CONTENT,
       MKT_CAMPAIGN,
       SE_CATEGORY,
       SE_ACTION,
       SE_LABEL,
       SE_PROPERTY,
       SE_VALUE,
       TR_ORDERID,
       TR_AFFILIATION,
       TR_TOTAL,
       TR_TAX,
       TR_SHIPPING,
       TR_CITY,
       TR_STATE,
       TR_COUNTRY,
       TI_ORDERID,
       TI_SKU,
       TI_NAME,
       TI_CATEGORY,
       TI_PRICE,
       TI_QUANTITY,
       PP_XOFFSET_MIN,
       PP_XOFFSET_MAX,
       PP_YOFFSET_MIN,
       PP_YOFFSET_MAX,
       USERAGENT,
       BR_NAME,
       BR_FAMILY,
       BR_VERSION,
       BR_TYPE,
       BR_RENDERENGINE,
       BR_LANG,
       BR_FEATURES_PDF,
       BR_FEATURES_FLASH,
       BR_FEATURES_JAVA,
       BR_FEATURES_DIRECTOR,
       BR_FEATURES_QUICKTIME,
       BR_FEATURES_REALPLAYER,
       BR_FEATURES_WINDOWSMEDIA,
       BR_FEATURES_GEARS,
       BR_FEATURES_SILVERLIGHT,
       BR_COOKIES,
       BR_COLORDEPTH,
       BR_VIEWWIDTH,
       BR_VIEWHEIGHT,
       OS_NAME,
       OS_FAMILY,
       OS_MANUFACTURER,
       OS_TIMEZONE,
       DVCE_TYPE,
       DVCE_ISMOBILE,
       DVCE_SCREENWIDTH,
       DVCE_SCREENHEIGHT,
       DOC_CHARSET,
       DOC_WIDTH,
       DOC_HEIGHT,
       TR_CURRENCY,
       TR_TOTAL_BASE,
       TR_TAX_BASE,
       TR_SHIPPING_BASE,
       TI_CURRENCY,
       TI_PRICE_BASE,
       BASE_CURRENCY,
       GEO_TIMEZONE,
       MKT_CLICKID,
       MKT_NETWORK,
       ETL_TAGS,
       DVCE_SENT_TSTAMP,
       REFR_DOMAIN_USERID,
       REFR_DVCE_TSTAMP,
       DOMAIN_SESSIONID,
       DERIVED_TSTAMP,
       EVENT_VENDOR,
       EVENT_NAME,
       EVENT_FORMAT,
       EVENT_VERSION,
       EVENT_FINGERPRINT,
       TRUE_TSTAMP,
       CONTEXTS_COM_OPTIMIZELY_SNOWPLOW_OPTIMIZELY_SUMMARY_1,
       CONTEXTS_COM_SECRETESCAPES_ALL_PAGES_SESSION_LOGIN_TYPE_CONTEXT_1,
       CONTEXTS_COM_SECRETESCAPES_SALE_PAGE_CONTEXT_1,
       CONTEXTS_COM_SECRETESCAPES_USER_STATE_CONTEXT_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_UA_PARSER_CONTEXT_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_WEB_PAGE_1,
       CONTEXTS_ORG_W3_PERFORMANCE_TIMING_1,
       UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_LINK_CLICK_1,
       CONTEXTS_COM_OPTIMIZELY_OPTIMIZELYX_SUMMARY_1,
       CONTEXTS_COM_SECRETESCAPES_COLLECTION_CONTEXT_1,
       CONTEXTS_COM_SECRETESCAPES_FILTER_CONTEXT_1,
       CONTEXTS_COM_SECRETESCAPES_SCREEN_CONTEXT_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_APPLICATION_BACKGROUND_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_APPLICATION_FOREGROUND_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_CLIENT_SESSION_1,
       CONTEXTS_COM_SNOWPLOWANALYTICS_SNOWPLOW_MOBILE_CONTEXT_1,
       UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_SNOWPLOW_SCREEN_VIEW_1,
       UNSTRUCT_EVENT_COM_SECRETESCAPES_SEARCHED_WITH_REFINEMENT_EVENT_1,
       CONTEXTS_COM_OPTIMIZELY_EXPERIMENT_1,
       CONTEXTS_COM_OPTIMIZELY_STATE_1,
       CONTEXTS_COM_OPTIMIZELY_VARIATION_1,
       CONTEXTS_COM_OPTIMIZELY_VISITOR_1,
       CONTEXTS_COM_OPTIMIZELY_VISITOR_AUDIENCE_1,
       CONTEXTS_COM_OPTIMIZELY_VISITOR_DIMENSION_1,
       UNSTRUCT_EVENT_COM_SNOWPLOWANALYTICS_MOBILE_SCREEN_VIEW_1
FROM SNOWPLOW_SAMPLE_EVENTS;

