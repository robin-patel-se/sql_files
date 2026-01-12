SELECT *
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE COLLECTOR_TSTAMP::DATE >= '2019-10-13'
LIMIT 50;

SELECT FIRST_VALUE(EVENT_ID) OVER (PARTITION BY DOMAIN_SESSIONID, USER_ID ORDER BY COLLECTOR_TSTAMP) AS FIRST_PAGE,
       RANK() OVER (PARTITION BY DOMAIN_SESSIONID, USER_ID ORDER BY COLLECTOR_TSTAMP)                AS PAGE_INDEX,
       COLLECTOR_TSTAMP,
       DERIVED_TSTAMP,
       EVENT_ID,
       DOMAIN_SESSIONID,
       USER_ID,
       EVENT_NAME,
       PAGE_URLPATH,
       MKT_CAMPAIGN,
       MKT_CONTENT,
       MKT_TERM,
       MKT_MEDIUM,
       MKT_SOURCE,
       MKT_CLICKID,
       REFR_MEDIUM,
       REFR_SOURCE,
       REFR_URLHOST,
       TI_ORDERID, -- BOOKING ID
       TI_CATEGORY,
       TI_CURRENCY,
       TI_NAME,
       TI_PRICE,
       TI_PRICE_BASE

from SNOWPLOW.ATOMIC.EVENTS
WHERE COLLECTOR_TSTAMP >= '2019-10-13'
--   AND DOMAIN_SESSIONID IN ('e56d7c42-1faf-4eb9-8716-acc78c0458d7', '49eb0e1b-623d-416e-80a3-6a843fecd7ca')
  AND EVENT_NAME NOT IN ('page_ping', 'link_click', 'transaction')
ORDER BY DOMAIN_SESSIONID, COLLECTOR_TSTAMP;


SELECT FIRST_VALUE(EVENT_ID) OVER (PARTITION BY DOMAIN_SESSIONID, USER_ID ORDER BY COLLECTOR_TSTAMP) AS FIRST_PAGE,
       RANK() OVER (PARTITION BY DOMAIN_SESSIONID, USER_ID ORDER BY COLLECTOR_TSTAMP)                AS PAGE_INDEX,
       COLLECTOR_TSTAMP,
       DERIVED_TSTAMP,
       EVENT_ID,
       DOMAIN_SESSIONID,
       USER_ID,
       EVENT_NAME,
       MKT_CAMPAIGN,
       MKT_CONTENT,
       MKT_TERM,
       MKT_MEDIUM,
       MKT_SOURCE,
       MKT_CLICKID,
       REFR_MEDIUM,
       TI_ORDERID, -- BOOKING ID
       TI_CATEGORY,
       TI_CURRENCY,
       TI_NAME,
       TI_PRICE,
       TI_PRICE_BASE

from SNOWPLOW.ATOMIC.EVENTS
WHERE USER_ID = '54953496'
  AND COLLECTOR_TSTAMP > '2019-08-01'
  AND EVENT_NAME NOT IN ('page_ping', 'link_click');

--example DOMAIN_SESSIONID='49eb0e1b-623d-416e-80a3-6a843fecd7ca'

SELECT TI_ORDERID, DOMAIN_SESSIONID
FROM SNOWPLOW.ATOMIC.EVENTS
WHERE EVENT_NAME = 'transaction_item'
  AND COLLECTOR_TSTAMP >= '2019-09-30'
LIMIT 50;

SELECT RECORD['_id']                                                        AS ID,
       RECORD['customerId']                                                 AS CUSTOMER_ID,
       RECORD['currency']                                                   AS CURRENCY,
       TO_TIMESTAMP(RECORD['dateTimeBooked']['$date']::INT / 1000)          AS DATE_TIME_BOOKED,
       TO_TIMESTAMP(RECORD['checkIn']['$date']::INT / 1000)                 AS CHECKIN_DATE,
       TO_TIMESTAMP(RECORD['checkOut']['$date']::INT / 1000)                AS CHECKOUT_DATE,
       RECORD['noNights']                                                   AS NO_NIGHTS,
--            RECORD['supplier']                AS SUPPLIER,
       RECORD['vatOnCommission']                                            AS VAT_ON_COMMISSION,
       RECORD['grossBookingValue'] / 100                                    AS GROSS_BOOKING_VALUE,
       (RECORD['grossBookingValue'] / 100) * (RECORD['rateToGbp'] / 100000) AS GROSS_BOOKING_VALUE_GBP,
       RECORD['customerTotalPrice'] / 100                                   AS CUSTOMER_TOTAL_PRICE,
       RECORD['commissionExVat'] / 100                                      AS COMMISSION_EX_VAT,
       RECORD['bookingFeeNetRate']                                          AS BOOKING_FEE_NET_RATE,
       RECORD['paymentSurchargeNetRate']                                    AS PAYMENT_SURCHARGE_NET_RATE,
       RECORD['rateToGbp'] / 100000                                         AS RATE_TO_GBP,
       (RECORD['commissionExVat'] / 100) * (RECORD['rateToGbp'] / 100000)   AS COMMISSION_EX_VAT_GBP,
--          RECORD['affiliate']               AS AFFILIATE,
--          RECORD['affiliateId']             AS AFFILIATEID,
       RECORD['customerEmail']                                              AS customer_email,
       RECORD['type']                                                       AS sale_type,
       RECORD['bookingStatus']                                              AS bookingStatus,
       RECORD['saleId']                                                     AS sale_id,
       RECORD['adults']                                                     AS adults,
       RECORD['children']                                                   AS children,
       EXTRACTED_AT


FROM RAW_VAULT.CMS_MONGODB.BOOKING_SUMMARY
WHERE EXTRACTED_AT > '2019-09-30'
  AND RECORD['_id'] IN (50500327, 50470640);



WITH distinct_member AS (
    SELECT DISTINCT su.ID                                  AS "USER_ID",
                    DATE_TRUNC(day, su.DATE_CREATED)::DATE AS "SIGNUP_DATE",
                    su.ORIGINAL_AFFILIATE_ID
    FROM RAW_VAULT.CMS_MYSQL.SHIRO_USER AS su
    WHERE USER_ID IN ('53047297', '32844627')
),
     affiliate AS ( -- original affiliate
         SELECT DISTINCT ID::INT                                                                    AS ID,
                         LAST_VALUE(TERRITORY_ID) OVER (PARTITION BY ID ORDER BY EXTRACTED_AT)::INT AS CURRENT_TERRITORY_ID
         FROM RAW_VAULT.CMS_MYSQL.AFFILIATE
     ),
     territory AS ( -- original affiliate territory
         SELECT DISTINCT ID::INT                                                       AS ID,
                         LAST_VALUE(NAME) OVER (PARTITION BY ID ORDER BY EXTRACTED_AT) AS CURRENT_NAME
         FROM RAW_VAULT.CMS_MYSQL.TERRITORY
--          WHERE NAME IN ('UK', 'DE')
     )

SELECT DISTINCT dm.USER_ID,
                dm.ORIGINAL_AFFILIATE_ID,
                dm.SIGNUP_DATE::DATE SIGNUP_DATE,
                t.CURRENT_NAME


FROM distinct_member AS dm
         INNER JOIN affiliate AS a ON dm.ORIGINAL_AFFILIATE_ID = a.ID
         INNER JOIN territory AS t ON a.CURRENT_TERRITORY_ID = t.ID