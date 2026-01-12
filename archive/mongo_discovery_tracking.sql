WITH page_visits AS (SELECT
       TO_TIMESTAMP(RECORD['c']['$date']::int / 1000)::DATE AS DATE,
       TRIM(RECORD['t']) AS event_name,
       TRIM(RECORD['d']['page']) AS URL,
       TRIM(PARSE_URL(TRIM(RECORD['d']['page']),1)['host']) AS hostname

FROM RAW_VAULT.CMS_MONGODB.EVENTS_PAGE_VISIT

WHERE TO_TIMESTAMP(RECORD['c']['$date']::int / 1000)::DATE >= '2019-06-01')

SELECT
DATE_TRUNC(MONTH, DATE),
hostname,
COUNT(1) AS page_views
FROM page_visits
GROUP BY 1,2
ORDER BY 1;

SELECT
       TO_TIMESTAMP(RECORD['c']['$date']::int / 1000)::DATE AS DATE,
       TRIM(RECORD['t']) AS event_name,
       TRIM(RECORD['d']['page']) AS URL,
       TRIM(PARSE_URL(TRIM(RECORD['d']['page']),1)['host']) AS hostname,
       PARSE_URL(TRIM(RECORD['d']['page']),1)

FROM RAW_VAULT.CMS_MONGODB.EVENTS_PAGE_VISIT

WHERE TO_TIMESTAMP(RECORD['c']['$date']::int / 1000)::DATE >= '2019-09-30'