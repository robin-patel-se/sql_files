-- input sessions - distinct list of sessions that have occurred within a certain criteria
-- persist session level data (i.e. dates, utms, booking info)

USE DATABASE SCRATCH;
USE SCHEMA ROBINPATELDEV35777;
USE ROLE PERSONAL_ROLE__ROBINPATEL;
use warehouse PIPE_MEDIUM;

--Definte a distinct list of session ids used to model for output, for now select events that have started in the last
CREATE OR REPLACE TRANSIENT TABLE step01_static_session_list AS (
    SELECT DOMAIN_SESSIONID,
           DOMAIN_USERID,
           MIN(COLLECTOR_TSTAMP) AS COLLECTOR_TSTAMP
    FROM SNOWPLOW.ATOMIC.EVENTS
    WHERE COLLECTOR_TSTAMP::DATE >= DATEADD(days, -7, current_date)
    GROUP BY 1, 2
);

CREATE OR REPLACE TRANSIENT TABLE step01_static_session_list_bkup CLONE step01_static_session_list;

SELECT COUNT(*)
FROM step01_static_session_list;

--extract all page_view events that match input session id
CREATE OR REPLACE TRANSIENT TABLE step02_events as (
    WITH dedupe AS ( --several duplicate events, possibly due to late arriving.
        SELECT e.EVENT_ID,
               e.DOMAIN_SESSIONID            AS DOMAIN_SESSIONID,
               e.DOMAIN_USERID,
               MAX(e.EVENT_NAME)             AS EVENT_NAME,
               MAX(TRY_TO_NUMBER(e.USER_ID)) AS USER_ID,
               MIN(e.COLLECTOR_TSTAMP)       AS COLLECTOR_TSTAMP,
               MIN(e.DERIVED_TSTAMP)         AS DERIVED_TSTAMP,
               MAX(e.MKT_CAMPAIGN)           AS MKT_CAMPAIGN,
               MAX(e.MKT_CONTENT)            AS MKT_CONTENT,
               MAX(e.MKT_TERM)               AS MKT_TERM,
               MAX(e.MKT_MEDIUM)             AS MKT_MEDIUM,
               MAX(e.MKT_SOURCE)             AS MKT_SOURCE,
               MAX(e.MKT_CLICKID)            AS MKT_CLICKID,
               MAX(e.REFR_MEDIUM)            AS REFR_MEDIUM,
               MAX(e.REFR_SOURCE)            AS REFR_SOURCE,
               MAX(e.REFR_URLHOST)           AS REFR_URLHOST,
               MAX(e.PAGE_URLPATH)           AS PAGE_URLPATH,
               MAX(e.TI_ORDERID)             AS TI_ORDERID, -- BOOKING ID
               MAX(e.TI_SKU)                 AS TI_SKU,     -- SALE ID,
               MAX(e.APP_ID)                 AS APP_ID
        FROM SNOWPLOW.ATOMIC.EVENTS e
                 INNER JOIN step01_static_session_list sl
                            ON e.DOMAIN_USERID = sl.DOMAIN_USERID AND e.DOMAIN_SESSIONID = sl.DOMAIN_SESSIONID
        WHERE EVENT_NAME IN ('page_view', 'transaction_item')
          AND e.COLLECTOR_TSTAMP >= DATEADD(DAY, -1, (SELECT MIN(COLLECTOR_TSTAMP) FROM step01_static_session_list))
          AND BR_FAMILY != 'Robot/Spider' -- remove robots
--           AND USERAGENT_FAMILY NOT IN -- this enrichment is not yet activated in snowplow but will better inform robots in future
--           ('Googlebot', 'PingdomBot', 'PhantomJS', 'Slurp', 'BingPreview', 'YandexBot')
        GROUP BY 1, 2, 3
    )
    SELECT EVENT_ID,
           DOMAIN_USERID,
           DOMAIN_SESSIONID,
           EVENT_NAME,
           USER_ID,
           COLLECTOR_TSTAMP,
           DERIVED_TSTAMP,
           MKT_CAMPAIGN,
           MKT_CONTENT,
           MKT_TERM,
           MKT_MEDIUM,
           MKT_SOURCE,
           MKT_CLICKID,
           REFR_MEDIUM,
           REFR_SOURCE,
           REFR_URLHOST,
           PAGE_URLPATH,
           TI_ORDERID,
           TI_SKU,
           APP_ID,
           ROW_NUMBER()
                   OVER (PARTITION BY DOMAIN_SESSIONID, DOMAIN_USERID ORDER BY COLLECTOR_TSTAMP, DERIVED_TSTAMP) AS PAGE_INDEX
    FROM dedupe
);

CREATE OR REPLACE TRANSIENT TABLE step03_sessions AS (
    WITH session_aggregates AS ( --aggregate data
        SELECT DOMAIN_USERID,
               DOMAIN_SESSIONID,
               MIN(COLLECTOR_TSTAMP) AS SESSION_START_TSTAMP,
               MAX(COLLECTOR_TSTAMP) AS SESSION_END_TSTAMP,
               MAX(USER_ID)          AS USER_ID, -- capture sessions where the user id is captured later on in the session
               COUNT(1)              AS SESSION_PAGE_VIEWS
        FROM step02_events
        WHERE EVENT_NAME = 'page_view'
        GROUP BY 1, 2
    )
    SELECT --landing page marketing and referrer data
           pv.DOMAIN_SESSIONID,
           pv.EVENT_ID AS SESSION_ID, -- assign the first page view event id as the session id.
           pv.DOMAIN_USERID,
           sa.SESSION_START_TSTAMP,
           sa.SESSION_END_TSTAMP,
           sa.SESSION_PAGE_VIEWS,
           sa.USER_ID,
           pv.MKT_CAMPAIGN,
           pv.MKT_CONTENT,
           pv.MKT_TERM,
           pv.MKT_MEDIUM,
           pv.MKT_SOURCE,
           pv.MKT_CLICKID,
           pv.REFR_MEDIUM,
           pv.REFR_SOURCE,
           pv.REFR_URLHOST,
           pv.APP_ID
    FROM step02_events pv
             LEFT JOIN session_aggregates AS sa
                       ON pv.DOMAIN_SESSIONID = sa.DOMAIN_SESSIONID AND pv.DOMAIN_USERID = sa.DOMAIN_USERID
    WHERE pv.PAGE_INDEX = 1
      AND pv.EVENT_NAME = 'page_view'
    --select the first page view (landing page)
);

--extract all transaction events that match domain user id and domain session id
--this is separated from sessionisation as a user may convert more than once in a session
CREATE OR REPLACE TRANSIENT TABLE step04_transaction_events as (
    SELECT e.COLLECTOR_TSTAMP,
           e.DERIVED_TSTAMP,
           e.EVENT_ID,
           e.DOMAIN_SESSIONID,
           e.DOMAIN_USERID,
           e.USER_ID,
           e.EVENT_NAME,
           e.PAGE_URLPATH,
           e.TI_ORDERID, -- BOOKING ID
           e.TI_SKU,     -- SALE ID,
           CASE WHEN LEFT(TI_SKU::varchar, 1) = 'A' THEN 'NEW' ELSE 'OLD' END AS SALE_DATA_MODEL
    FROM step02_events e
             INNER JOIN step01_static_session_list sl
                        ON e.DOMAIN_USERID = sl.DOMAIN_USERID AND e.DOMAIN_SESSIONID = sl.DOMAIN_SESSIONID
    WHERE EVENT_NAME = 'transaction_item'
);


--extract the most recent booking status for bookings for the transactions listed in step 4
CREATE OR REPLACE TRANSIENT TABLE step05_booking_status AS (
    SELECT DISTINCT b.ID,
                    LAST_VALUE(b.STATUS) OVER (PARTITION BY b.ID ORDER BY EXTRACTED_AT) AS STATUS
    FROM RAW_VAULT.CMS_MYSQL.BOOKING b
    WHERE b.ID IN (
        SELECT DISTINCT TI_ORDERID FROM step04_transaction_events WHERE SALE_DATA_MODEL = 'OLD')
);

--extract the most recent reservation status for reservations for the transactions listed in step 4
CREATE OR REPLACE TRANSIENT TABLE step06_reservation_status AS (
    SELECT DISTINCT r.ID,
                    LAST_VALUE(r.STATUS) OVER (PARTITION BY r.ID ORDER BY EXTRACTED_AT) AS STATUS
    FROM RAW_VAULT.CMS_MYSQL.RESERVATION r
    WHERE r.ID IN (
        SELECT DISTINCT TI_ORDERID FROM step04_transaction_events WHERE SALE_DATA_MODEL = 'NEW')
);

--extract the booking summaries for the transactions listed in step 4
CREATE OR REPLACE TRANSIENT TABLE step07_booking_summaries AS (
    WITH extract_summaries AS ( -- extract booking summaries for all the transactions in step 4
        SELECT TRIM(RECORD['_id'])                                         AS ID, --mixture of booking and reservation ids
               RECORD['customerId']                                        AS CUSTOMER_ID,
               TRIM(RECORD['currency'])                                    AS CURRENCY,
               TO_TIMESTAMP(RECORD['dateTimeBooked']['$date']::INT / 1000) AS DATE_TIME_BOOKED,
               TO_TIMESTAMP(RECORD['checkIn']['$date']::INT / 1000)        AS CHECKIN_DATE,
               TO_TIMESTAMP(RECORD['checkOut']['$date']::INT / 1000)       AS CHECKOUT_DATE,
               RECORD['noNights']                                          AS NO_NIGHTS,
               RECORD['grossBookingValue'] / 100                           AS TOTAL_PRICE,
               RECORD['commissionExVat'] / 100                             AS COMMISSION_EX_VAT,
               RECORD['bookingFeeNetRate'] / 100                           AS BOOKING_FEE_NET_RATE,
               RECORD['paymentSurchargeNetRate'] / 100                     AS PAYMENT_SURCHARGE_NET_RATE,
               RECORD['rateToGbp'] / 100000                                AS RATE_TO_GBP,
               TRIM(RECORD['type'])                                        AS BOOKING_SALE_TYPE,
               TRIM(RECORD['saleId'])                                      AS BOOKING_SALE_ID,
               RECORD['adults']                                            AS ADULTS,
               RECORD['children']                                          AS CHILDREN,
               TRIM(RECORD['territory'])                                   AS BOOKING_TERRITORY,
               RECORD['affiliateId']                                       AS BOOKING_AFFILIATE_ID,
               EXTRACTED_AT
        FROM RAW_VAULT.CMS_MONGODB.BOOKING_SUMMARY
        WHERE TRIM(RECORD['_id']) IN (
            SELECT DISTINCT CASE WHEN SALE_DATA_MODEL = 'NEW' THEN 'A' || TI_ORDERID ELSE TI_ORDERID END
            FROM STEP04_TRANSACTION_EVENTS
            ORDER BY 1
        )
    ),
         rank AS ( --rank extracted summaries based on most recent extracted at
             SELECT bs.*,
                    ROW_NUMBER() OVER (PARTITION BY ID ORDER BY EXTRACTED_AT DESC) AS rn --most recent summary
             FROM extract_summaries bs
         )
    SELECT ID,
           CUSTOMER_ID,
           CURRENCY,
           DATE_TIME_BOOKED,
           CHECKIN_DATE::DATE                              AS CHECKIN_DATE,
           CHECKOUT_DATE::DATE                             AS CHECKOUT_DATE,
           NO_NIGHTS,
           TOTAL_PRICE,
           TOTAL_PRICE * RATE_TO_GBP                       AS TOTAL_PRICE_GBP,
           COMMISSION_EX_VAT,
           BOOKING_FEE_NET_RATE,
           PAYMENT_SURCHARGE_NET_RATE,
           RATE_TO_GBP,
           BOOKING_SALE_TYPE,
           BOOKING_SALE_ID,
           ADULTS,
           CHILDREN,
           BOOKING_AFFILIATE_ID,
           BOOKING_TERRITORY,
           COMMISSION_EX_VAT
               + BOOKING_FEE_NET_RATE
               + PAYMENT_SURCHARGE_NET_RATE                AS MARGIN_GROSS_TOMS,
           (COMMISSION_EX_VAT
               + BOOKING_FEE_NET_RATE
               + PAYMENT_SURCHARGE_NET_RATE) * RATE_TO_GBP AS MARGIN_GROSS_TOMS_GBP
    FROM rank
    WHERE rn = 1
);

--combine all booking information to transaction level
CREATE OR REPLACE TRANSIENT TABLE step08_purchase_events AS (
    SELECT
        --        e.COLLECTOR_TSTAMP,
        --        e.DERIVED_TSTAMP,
        e.EVENT_ID,
        e.DOMAIN_SESSIONID,
        e.DOMAIN_USERID,
        e.USER_ID,
        e.SALE_DATA_MODEL,
        bs.ID                              AS BOOKING_ID,
        bs.CUSTOMER_ID,
        bs.BOOKING_SALE_TYPE,
        bs.BOOKING_SALE_ID,
        bs.DATE_TIME_BOOKED,
        bs.CHECKIN_DATE,
        bs.CHECKOUT_DATE,
        bs.NO_NIGHTS,
        bs.CURRENCY,
        bs.TOTAL_PRICE,
        ROUND(bs.TOTAL_PRICE_GBP, 2)       AS TOTAL_PRICE_GBP,
        bs.MARGIN_GROSS_TOMS,
        ROUND(bs.MARGIN_GROSS_TOMS_GBP, 2) AS MARGIN_GROSS_TOMS_GBP,
        bs.RATE_TO_GBP,
        bs.ADULTS,
        bs.CHILDREN,
        bs.BOOKING_TERRITORY,
        bs.BOOKING_AFFILIATE_ID,

        COALESCE(s.STATUS, r.STATUS)       AS BOOKING_STATUS
    FROM step04_transaction_events e
             LEFT JOIN step07_booking_summaries bs
                       ON (CASE WHEN e.SALE_DATA_MODEL = 'NEW' THEN 'A' || e.TI_ORDERID ELSE e.TI_ORDERID END) = bs.ID
             LEFT JOIN step05_booking_status s ON e.SALE_DATA_MODEL = 'OLD' AND e.TI_ORDERID = s.ID
             LEFT JOIN step06_reservation_status r ON e.SALE_DATA_MODEL = 'NEW' AND e.TI_ORDERID = r.ID
);

--Create a session level output that will have a single row for each session but may have duplicate sessions if more
--than one booking occurs against the same session
CREATE OR REPLACE TRANSIENT TABLE SESSION_BOOKINGS AS (
    SELECT s.DOMAIN_SESSIONID,
           s.SESSION_ID,
           s.DOMAIN_USERID,
           s.SESSION_START_TSTAMP,
           s.SESSION_END_TSTAMP,
           s.SESSION_PAGE_VIEWS,
           s.USER_ID,
           s.MKT_CAMPAIGN,
           s.MKT_CONTENT,
           s.MKT_TERM,
           s.MKT_MEDIUM,
           s.MKT_SOURCE,
           s.MKT_CLICKID,
           s.REFR_MEDIUM,
           s.REFR_SOURCE,
           s.REFR_URLHOST,
           s.APP_ID,
           pe.SALE_DATA_MODEL,
           pe.BOOKING_ID,
           pe.CUSTOMER_ID,
           pe.BOOKING_SALE_TYPE,
           pe.BOOKING_SALE_ID,
           pe.DATE_TIME_BOOKED,
           pe.CHECKIN_DATE,
           pe.CHECKOUT_DATE,
           pe.NO_NIGHTS,
           pe.CURRENCY,
           pe.TOTAL_PRICE,
           pe.TOTAL_PRICE_GBP,
           pe.MARGIN_GROSS_TOMS,
           pe.MARGIN_GROSS_TOMS_GBP,
           pe.RATE_TO_GBP,
           pe.ADULTS,
           pe.CHILDREN,
           pe.BOOKING_AFFILIATE_ID,
           pe.BOOKING_TERRITORY,
           pe.BOOKING_STATUS
    FROM step03_sessions s
             LEFT JOIN step08_purchase_events pe
                       ON s.DOMAIN_USERID = pe.DOMAIN_USERID AND s.DOMAIN_SESSIONID = pe.DOMAIN_SESSIONID
);

SELECT * FROM SESSION_BOOKINGS;

--query to look at session level data alongside event level data for proof.
SELECT s.SESSION_ID,
       s.SESSION_PAGE_VIEWS,
       s.BOOKING_ID,
       s.USER_ID,
       pv.USER_ID,
       pv.COLLECTOR_TSTAMP,
       pv.DERIVED_TSTAMP,
       pv.EVENT_NAME,
       pv.PAGE_URLPATH,
       pv.PAGE_INDEX,
       pv.MKT_CAMPAIGN,
       pv.MKT_CONTENT,
       pv.MKT_TERM,
       pv.MKT_MEDIUM,
       pv.MKT_SOURCE,
       pv.MKT_CLICKID,
       pv.REFR_MEDIUM,
       pv.REFR_SOURCE,
       pv.REFR_URLHOST,
       pv.TI_ORDERID,
       pv.TI_SKU
FROM SESSION_BOOKINGS s
         LEFT JOIN step02_events pv
                   ON s.DOMAIN_SESSIONID = pv.DOMAIN_SESSIONID AND s.DOMAIN_USERID = pv.DOMAIN_USERID
WHERE s.BOOKING_ID IS NOT NULL
ORDER BY SESSION_ID, PAGE_INDEX;

-- SELECT s.SESSION_ID,
--        s.SESSION_PAGE_VIEWS,
--        s.BOOKING_ID,
--        s.USER_ID,
--        pv.EVENT_ID,
--        pv.USER_ID,
--        pv.DOMAIN_USERID,
--        pv.DOMAIN_SESSIONID,
--        pv.COLLECTOR_TSTAMP,
--        pv.DERIVED_TSTAMP,
--        pv.EVENT_NAME,
--        pv.PAGE_URLPATH,
--        pv.PAGE_INDEX,
--        pv.MKT_CAMPAIGN,
--        pv.MKT_CONTENT,
--        pv.MKT_TERM,
--        pv.MKT_MEDIUM,
--        pv.MKT_SOURCE,
--        pv.MKT_CLICKID,
--        pv.REFR_MEDIUM,
--        pv.REFR_SOURCE,
--        pv.REFR_URLHOST,
--        pv.TI_ORDERID,
--        pv.TI_SKU
-- FROM SESSION_BOOKINGS s
--          LEFT JOIN step02_events pv
--                    ON s.DOMAIN_SESSIONID = pv.DOMAIN_SESSIONID AND s.DOMAIN_USERID = pv.DOMAIN_USERID
-- WHERE s.BOOKING_ID IS NOT NULL AND SESSION_ID='000d56aa-ec1f-4806-aca5-551c68529d99'
-- ORDER BY SESSION_ID, PAGE_INDEX;

--Build session channels
CREATE OR REPLACE TRANSIENT TABLE session_channelling AS (
    SELECT sb.*,
           CASE
               WHEN -- no utm or glcid params
                       sb.MKT_CAMPAIGN IS NULL AND
                       sb.MKT_CONTENT IS NULL AND
                       sb.MKT_TERM IS NULL AND
                       sb.MKT_MEDIUM IS NULL AND
                       sb.MKT_SOURCE IS NULL AND
                       sb.MKT_CLICKID IS NULL
                   THEN
                   CASE
                       WHEN (
                               (sb.REFR_MEDIUM IS NULL AND sb.REFR_SOURCE IS NULL AND sb.REFR_URLHOST IS NULL)
                               OR
                               (sb.REFR_MEDIUM = 'internal')
                               OR
                               (sb.REFR_MEDIUM = 'unknown' AND
                                (sb.REFR_URLHOST LIKE '%secretescapes.%' OR
                                 sb.REFR_URLHOST LIKE 'evasionssecretes.%' OR
                                 sb.REFR_URLHOST LIKE 'travelbird.%' OR
                                 sb.REFR_URLHOST LIKE '%.travelist.%' OR
                                 sb.REFR_URLHOST LIKE '%.pigsback.%'
                                    )
                                   )
                           )
                           THEN 'Direct'
                       WHEN sb.REFR_MEDIUM = 'social' THEN 'Social'

                       WHEN
                           (
                                   sb.REFR_MEDIUM = 'search' OR
                                   sb.REFR_URLHOST LIKE '%googlequicksearchbox' OR
                                   sb.REFR_URLHOST LIKE 'com.google.android.gm'
                               )
                        THEN 'Organic Search'

                       WHEN sb.REFR_MEDIUM = 'unknown' AND
                            (sb.REFR_URLHOST LIKE '%urlaub%' OR
                             sb.REFR_URLHOST LIKE '%butterholz%' OR
                             sb.REFR_URLHOST LIKE '%mydealz%' OR
                             sb.REFR_URLHOST LIKE '%travel-dealz%' OR
                             sb.REFR_URLHOST LIKE '%travel-dealz%' OR
                             sb.REFR_URLHOST LIKE '%discountvouchers%'
                                ) THEN 'Partner'
                       END
               --when the utm or gclid params aren't all null
               WHEN sb.MKT_MEDIUM = 'email' THEN 'Email' --need to expand on
               WHEN sb.MKT_MEDIUM = 'facebookads' THEN 'Paid Social'
               WHEN sb.MKT_MEDIUM = 'organic-social' THEN 'Organic Social'
               WHEN sb.MKT_MEDIUM = 'display' THEN 'Display'
               WHEN sb.MKT_CLICKID IS NOT NULL THEN 'PPC'
               WHEN (
                        sb.REFR_URLHOST LIKE '%secretescapes%' OR
                        sb.REFR_URLHOST LIKE '%evasionssecretes%' OR
                        sb.REFR_URLHOST LIKE '%travelbird&'
                   ) THEN 'Direct'
               END
               AS SESSION_CHANNEL
    FROM SESSION_BOOKINGS sb
);

SELECT * FROM session_channelling;

SELECT * FROM session_channelling WHERE SESSION_CHANNEL='Email';


SELECT SESSION_CHANNEL,
       count(*)
FROM session_channelling
group by 1;

SELECT MKT_CONTENT, MKT_CAMPAIGN, MKT_MEDIUM, MKT_SOURCE from  session_channelling where SESSION_CHANNEL='PPC';


--help find channels that have not yet been categorised
SELECT SESSION_CHANNEL,
       MKT_CAMPAIGN,
       MKT_CONTENT,
       MKT_TERM,
       MKT_MEDIUM,
       MKT_SOURCE,
       MKT_CLICKID,
       REFR_MEDIUM,
       REFR_SOURCE,
       REFR_URLHOST,
       COUNT(*)
FROM session_channelling
WHERE SESSION_CHANNEL IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10
ORDER BY 11 DESC;


SELECT ETL_TSTAMP FROM SNOWPLOW.ATOMIC.EVENTS

--for all the glcid in url check if mkt_clickid exists.


--stuff for carmen:

-- SELECT s.SESSION_ID,
--        s.SESSION_PAGE_VIEWS,
--        s.BOOKING_ID,
--        COUNT(distinct pv.MKT_CAMPAIGN)
-- --        pv.*
-- FROM SESSION_BOOKINGS s
--          LEFT JOIN step02_events pv
--                    ON s.DOMAIN_SESSIONID = pv.DOMAIN_SESSIONID AND s.DOMAIN_USERID = pv.DOMAIN_USERID
-- WHERE s.BOOKING_ID IS NOT NULL
-- GROUP BY 1,2,3
-- HAVING COUNT(distinct pv.MKT_CAMPAIGN)>1
-- ORDER BY 4 DESC;
--
--
-- SELECT pv.COLLECTOR_TSTAMP,
--        pv.DOMAIN_USERID,
--        pv.DOMAIN_SESSIONID,
--        pv.EVENT_NAME,
--        pv.PAGE_URLPATH,
--        pv.PAGE_INDEX,
--        pv.MKT_CAMPAIGN,
--        pv.MKT_CONTENT,
--        pv.MKT_TERM,
--        pv.MKT_MEDIUM,
--        pv.MKT_SOURCE,
--        pv.MKT_CLICKID,
--        pv.REFR_MEDIUM,
--        pv.REFR_SOURCE,
--        pv.REFR_URLHOST
-- --        pv.*
-- FROM SESSION_BOOKINGS s
--          LEFT JOIN step02_events pv
--                    ON s.DOMAIN_SESSIONID = pv.DOMAIN_SESSIONID AND s.DOMAIN_USERID = pv.DOMAIN_USERID
-- WHERE s.BOOKING_ID IS NOT NULL AND SESSION_ID='19393d6e-bcaf-498e-a4e9-536cc3919198'
-- ORDER BY SESSION_ID, PAGE_INDEX;



SELECT * FROM SNOWPLOW.ATOMIC.EVENTS WHERE TI_ORDERID IS NOT NULL LIMIT 10;

SELECT * FROM SESSION_BOOKINGS WHERE BOOKING_ID IS NOT NULL;