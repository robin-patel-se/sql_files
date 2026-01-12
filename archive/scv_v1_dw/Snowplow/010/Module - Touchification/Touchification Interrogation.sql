USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

SELECT e.EVENT_HASH,
       e.EVENT_TSTAMP,
       e.identity_fragment,
       e.SE_USER_ID,
       e.DEVICE_PLATFORM,
       i.ATTRIBUTED_USER_ID
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND e.SE_USER_ID IS NULL
  AND i.ATTRIBUTED_USER_ID IS NOT NULL
ORDER BY ATTRIBUTED_USER_ID DESC NULLS LAST

;
--find users with multiple platforms
SELECT
       i.ATTRIBUTED_USER_ID,
       COUNT(distinct e.DEVICE_PLATFORM)
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND e.SE_USER_ID IS NULL
  AND i.ATTRIBUTED_USER_ID IS NOT NULL
group by 1
having COUNT(distinct e.DEVICE_PLATFORM)>2
ORDER BY 2 DESC
;
--user 35135226 has multiple platforms, backfilled user id and has param data
--user 65790769 has multiple platforms, backfilled user id and has param data

SELECT e.EVENT_HASH,
       e.EVENT_TSTAMP,
       e.identity_fragment,
       e.SE_USER_ID,
       e.DEVICE_PLATFORM,
       i.ATTRIBUTED_USER_ID,
       e.USER_IPADDRESS,
       u.PARAMETER,
       u.PARAMETER_VALUE,
       u.UTM_INDEX
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
         LEFT JOIN MODULE_URL_PARAMS u ON e.PAGE_URL = u.URL
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND i.ATTRIBUTED_USER_ID = 35135226
ORDER BY EVENT_TSTAMP
;

SELECT e.EVENT_HASH,
       e.PAGE_URL,
       e.EVENT_TSTAMP,
       e.identity_fragment,
       e.SE_USER_ID,
       e.DEVICE_PLATFORM,
       i.ATTRIBUTED_USER_ID,
       e.USER_IPADDRESS,
       p.UTM_CAMPAIGN,
       p.UTM_MEDIUM,
       p.UTM_SOURCE,
       p.UTM_TERM,
       p.UTM_CONTENT,
       p.CLICK_ID,
       p.SUB_AFFILIATE_NAME,
       p.FROM_APP,
       p.SNOWPLOW_ID,
       r.URL_HOSTNAME
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
         LEFT JOIN MODULE_EXTRACTED_PARAMS p ON e.PAGE_URL = p.URL
         LEFT JOIN MODULE_URL_HOSTNAME r ON e.PAGE_REFERRER = r.URL
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND i.ATTRIBUTED_USER_ID = '65790769'
ORDER BY EVENT_TSTAMP;

------------------------------------------------------------------------------------------------------------------------
--utm partition marker
CREATE OR REPLACE TEMPORARY TABLE UTM_REFERRER_MARKER AS (
    WITH utm_referrer_marker as (
        --identify changes to utm/referrer data ignoring internal referrers
        SELECT e.EVENT_HASH,
               e.EVENT_TSTAMP,
               e.DERIVED_TSTAMP,
               i.ATTRIBUTED_USER_ID,
               SHA2(NULLIF(COALESCE(p.UTM_CAMPAIGN, '') ||
                           COALESCE(p.UTM_MEDIUM, '') ||
                           COALESCE(p.UTM_SOURCE, '') ||
                           COALESCE(p.UTM_TERM, '') ||
                           COALESCE(p.UTM_CONTENT, '') ||
                           COALESCE(p.CLICK_ID, '') ||
                           COALESCE(r.URL_HOSTNAME, ''), '')) AS partition_marker

        FROM TOUCHIFIABLE_EVENTS e
                 LEFT JOIN MODULE_IDENTITY_STITCHING i
                           ON e.identity_fragment = i.identity_fragment
                 LEFT JOIN MODULE_EXTRACTED_PARAMS p ON e.PAGE_URL = p.URL
            -- join referrer hostnames but exclude internal and payment gateway referrers so they aren't considered as a utm marker.
                 LEFT JOIN MODULE_URL_HOSTNAME r ON e.PAGE_REFERRER = r.URL
            AND r.URL_MEDIUM NOT IN ('internal', 'payment_gateway')
--           WHERE i.ATTRIBUTED_USER_ID = 65790769 -- TODO: remove, only here for testing
    ),
         partition_grouping AS (
             --persist the parition marker until the next partition marker
             SELECT u.EVENT_HASH,
                    u.EVENT_TSTAMP,
                    u.ATTRIBUTED_USER_ID,
--        u.partition_marker,
                    CASE
                        WHEN LAST_VALUE(u.partition_marker)
                                        IGNORE NULLS OVER (PARTITION BY u.ATTRIBUTED_USER_ID
                                            ORDER BY u.EVENT_TSTAMP, u.DERIVED_TSTAMP rows between unbounded preceding and current row) IS NULL
                            THEN 'first_group' -- when there are no unique utm params or referrer hostname for the first set of events
                        ELSE LAST_VALUE(u.partition_marker)
                                        IGNORE NULLS OVER (PARTITION BY u.ATTRIBUTED_USER_ID
                                            ORDER BY u.EVENT_TSTAMP, u.DERIVED_TSTAMP rows between unbounded preceding and current row) END
                        AS partition_group
             FROM utm_referrer_marker u
         )
            ,
         partition_flag AS (
             --flag the partition if utm params/referrer hostname change
             SELECT pg.EVENT_HASH,
                    pg.EVENT_TSTAMP,
                    pg.ATTRIBUTED_USER_ID,
                    pg.partition_group,
                    CASE
                        WHEN pg.partition_group =
                             LAG(pg.partition_group) OVER (PARTITION BY pg.ATTRIBUTED_USER_ID ORDER BY pg.EVENT_TSTAMP)
                            THEN 0
                        ELSE 1 END AS utm_referrer_partition_marker
             FROM partition_grouping pg
         )
    SELECT p.EVENT_HASH,
--        p.EVENT_TSTAMP,
--        p.ATTRIBUTED_USER_ID,
           SUM(p.utm_referrer_partition_marker)
               OVER (PARTITION BY p.ATTRIBUTED_USER_ID
                   ORDER BY p.EVENT_TSTAMP rows between unbounded preceding and current row) AS utm_ref_partition
    FROM partition_flag AS p
);



--        SHA2(
--                    COALESCE(p.UTM_CAMPAIGN, '') ||
--                    COALESCE(p.UTM_MEDIUM, '') ||
--                    COALESCE(p.UTM_SOURCE, '') ||
--                    COALESCE(p.UTM_TERM, '') ||
--                    COALESCE(p.UTM_CONTENT, '') ||
--                    COALESCE(p.CLICK_ID, '') ||
--                    COALESCE(r.URL_HOSTNAME, '')) AS channel_partition_marker

--check utm marker
SELECT e.event_hash,
       e.EVENT_TSTAMP,
       e.COLLECTOR_TSTAMP,
       p.UTM_CAMPAIGN,
       p.UTM_MEDIUM,
       p.UTM_SOURCE,
       p.UTM_TERM,
       p.UTM_CONTENT,
       p.CLICK_ID,
       r.URL_HOSTNAME,
       u.UTM_REF_PARTITION
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i
                   ON e.identity_fragment = i.identity_fragment
         LEFT JOIN MODULE_EXTRACTED_PARAMS p ON e.PAGE_URL = p.URL
         LEFT JOIN MODULE_URL_HOSTNAME r ON e.PAGE_REFERRER = r.URL AND r.URL_MEDIUM NOT IN ('internal', 'payment_gateway')
         INNER JOIN MODULE_UTM_REFERRER_MARKER u ON e.EVENT_HASH = u.EVENT_HASH
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND i.ATTRIBUTED_USER_ID = '65790769' -- TODO: remove, only here for testing
  AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
ORDER BY EVENT_TSTAMP, DERIVED_TSTAMP;


------------------------------------------------------------------------------------------------------------------------
--30min partition marker
CREATE OR REPLACE TEMPORARY TABLE TIME_DIFF_MARKER AS (
    WITH diff_marker AS (
        SELECT e.EVENT_HASH,
               i.ATTRIBUTED_USER_ID,
               e.EVENT_TSTAMP,
               timediff(second,
                        LAG(e.EVENT_TSTAMP) OVER (PARTITION BY i.attributed_user_id ORDER BY e.EVENT_TSTAMP),
                        e.EVENT_TSTAMP) AS diff,
               CASE
                   WHEN timediff(second,
                                 LAG(e.EVENT_TSTAMP) OVER (PARTITION BY i.attributed_user_id ORDER BY e.EVENT_TSTAMP),
                                 e.EVENT_TSTAMP) < 1800
                       THEN 0
                   ELSE 1 END           AS diff_partition_marker
        FROM EVENT_STREAM e
                 LEFT JOIN MODULE_IDENTITY_STITCHING i
                           ON e.identity_fragment = i.identity_fragment
        WHERE IS_ROBOT_SPIDER_EVENT = FALSE
          AND i.ATTRIBUTED_USER_ID = 65790769 -- TODO: remove, only here for testing
          AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
        ORDER BY EVENT_TSTAMP
    )
    SELECT d.EVENT_HASH,
--        d.diff_partition_marker,
           SUM(diff_partition_marker)
               OVER (partition by d.ATTRIBUTED_USER_ID ORDER BY d.EVENT_TSTAMP rows between unbounded preceding and current row) AS time_diff_partition
    FROM diff_marker d
);

--check time diff marker
SELECT e.event_hash,
       p.UTM_CAMPAIGN,
       p.UTM_MEDIUM,
       p.UTM_SOURCE,
       p.UTM_TERM,
       p.UTM_CONTENT,
       p.CLICK_ID,
       r.URL_HOSTNAME,
       e.EVENT_TSTAMP,
       d.time_diff_partition
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i
                   ON e.identity_fragment = i.identity_fragment
         LEFT JOIN MODULE_EXTRACTED_PARAMS p ON e.PAGE_URL = p.URL
         LEFT JOIN MODULE_URL_HOSTNAME r ON e.PAGE_REFERRER = r.URL AND r.URL_MEDIUM NOT IN ('internal', 'payment_gateway')
         LEFT JOIN TIME_DIFF_MARKER d ON e.EVENT_HASH = d.EVENT_HASH
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND i.ATTRIBUTED_USER_ID = 65790769 -- TODO: remove, only here for testing
  AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
ORDER BY EVENT_TSTAMP;


------------------------------------------------------------------------------------------------------------------------
SELECT
       e.EVENT_HASH,
--        e.USER_IPADDRESS,
--        e.identity_fragment,
       e.SE_USER_ID,
       i.ATTRIBUTED_USER_ID,
       e.PAGE_URLQUERY,
--        p.UTM_CAMPAIGN,
--        p.UTM_MEDIUM,
--        p.UTM_SOURCE,
--        p.UTM_TERM,
--        p.UTM_CONTENT,
--        p.CLICK_ID,
--        r.URL_HOSTNAME,
       e.EVENT_TSTAMP,
       e.DEVICE_PLATFORM,
       e.EVENT_NAME,
       e.USERAGENT,
       p.FROM_APP
FROM EVENT_STREAM e
     LEFT JOIN MODULE_IDENTITY_STITCHING i
               ON e.identity_fragment = i.identity_fragment
     LEFT JOIN MODULE_EXTRACTED_PARAMS p ON e.PAGE_URL = p.URL
     LEFT JOIN MODULE_URL_HOSTNAME r ON e.PAGE_REFERRER = r.URL AND r.URL_MEDIUM NOT IN ('internal', 'payment_gateway')

WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND i.ATTRIBUTED_USER_ID IN (70430658) -- TODO: remove, only here for testing
  AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
ORDER BY ATTRIBUTED_USER_ID, EVENT_TSTAMP;

--find users with multiple platforms
SELECT
       i.ATTRIBUTED_USER_ID,
       COUNT(distinct e.DEVICE_PLATFORM)
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.EVENT_USER_IDENTIFIER = i.EVENT_USER_IDENTIFIER
WHERE IS_ROBOT_SPIDER_EVENT = FALSE
  AND e.SE_USER_ID IS NULL
  AND i.ATTRIBUTED_USER_ID IS NOT NULL
group by 1
having COUNT(distinct e.DEVICE_PLATFORM)>2
ORDER BY 2 DESC
;


------------------------------------------------------------------------------------------------------------------------
--explore late arriving

SELECT MIN(ETL_TSTAMP)
FROM EVENT_STREAM; --earliest etl tstamp in sample

SELECT e.EVENT_HASH,
       e.EVENT_NAME,
       e.DERIVED_TSTAMP,
       e.DVCE_CREATED_TSTAMP,
       e.DVCE_SENT_TSTAMP,
       e.ETL_TSTAMP,
       e.COLLECTOR_TSTAMP,
       e.EVENT_TSTAMP,
       UPDATED_AT,
       TIMEDIFF(minute, e.EVENT_TSTAMP, e.ETL_TSTAMP)     AS event_etl_diff,
       TIMEDIFF(minute, e.COLLECTOR_TSTAMP, e.ETL_TSTAMP) AS collector_etl_diff
FROM EVENT_STREAM e
WHERE e.IS_ROBOT_SPIDER_EVENT = FALSE
  AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
-- AND e.EVENT_TSTAMP < '2019-10-01'
ORDER BY EVENT_TSTAMP;

SELECT MIN(EVENT_TSTAMP)       min_tstamp,
       MAX(EVENT_TSTAMP)       max_tstamp,
       MIN(event_etl_diff)     min_event_etl_diff,
       MAX(event_etl_diff)     max_event_etl_diff,
       MIN(collector_etl_diff) min_collector_etl_diff,
       MAX(collector_etl_diff) max_collector_etl_diff

FROM (
         SELECT e.EVENT_TSTAMP,
                e.ETL_TSTAMP,
                e.COLLECTOR_TSTAMP,
                TIMEDIFF(minute, e.EVENT_TSTAMP, e.ETL_TSTAMP)     AS event_etl_diff,
                TIMEDIFF(minute, e.COLLECTOR_TSTAMP, e.ETL_TSTAMP) AS collector_etl_diff

         FROM EVENT_STREAM e
         WHERE e.IS_ROBOT_SPIDER_EVENT = FALSE
           AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
     )
;


SELECT SUM("within_1hr")    AS "within_1hr",
       SUM("1hr_to_3hrs")   AS "1hr_to_3hrs",
       SUM("3hrs_to_6hrs")  AS "3hrs_to_6hrs",
       SUM("6hrs_to_12hrs") AS "6hrs_to_12hrs",
       SUM("12hrs_to_1d")   AS "12hrs_to_1d",
       SUM("1d_to_2d")      AS "1d_to_2d",
       SUM("2d_to_5d")      AS "2d_to_5d",
       SUM("5d_to_10")      AS "5d_to_10",
       SUM("greater_10d")   AS "greater_10d",
       COUNT(*)
FROM (
         SELECT *,
                CASE WHEN event_etl_diff < 60 THEN 1 ELSE 0 END                               AS "within_1hr",
                CASE WHEN event_etl_diff >= 60 AND event_etl_diff < 180 THEN 1 ELSE 0 END     AS "1hr_to_3hrs",
                CASE WHEN event_etl_diff >= 180 AND event_etl_diff < 360 THEN 1 ELSE 0 END    AS "3hrs_to_6hrs",
                CASE WHEN event_etl_diff >= 360 AND event_etl_diff < 720 THEN 1 ELSE 0 END    AS "6hrs_to_12hrs",
                CASE WHEN event_etl_diff >= 720 AND event_etl_diff < 1440 THEN 1 ELSE 0 END   AS "12hrs_to_1d",
                CASE WHEN event_etl_diff >= 1440 AND event_etl_diff < 2880 THEN 1 ELSE 0 END  AS "1d_to_2d",
                CASE WHEN event_etl_diff >= 2880 AND event_etl_diff < 7200 THEN 1 ELSE 0 END  AS "2d_to_5d",
                CASE WHEN event_etl_diff >= 7200 AND event_etl_diff < 14400 THEN 1 ELSE 0 END AS "5d_to_10",
                CASE WHEN event_etl_diff >= 14400 THEN 1 ELSE 0 END                           AS "greater_10d"

         FROM (
                  SELECT e.EVENT_TSTAMP,
                         e.ETL_TSTAMP,
                         e.COLLECTOR_TSTAMP,
                         TIMEDIFF(minute, e.EVENT_TSTAMP, e.ETL_TSTAMP)     AS event_etl_diff,
                         TIMEDIFF(minute, e.COLLECTOR_TSTAMP, e.ETL_TSTAMP) AS collector_etl_diff

                  FROM EVENT_STREAM e
                  WHERE e.IS_ROBOT_SPIDER_EVENT = FALSE
                    AND e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
              )
     );

------------------------------------------------------------------------------------------------------------------------

SELECT e.event_hash,
       e.EVENT_TSTAMP,
       e.SE_USER_ID,
       e.DVCE_SENT_TSTAMP,
       e.DERIVED_TSTAMP,
       e.COLLECTOR_TSTAMP,
       e.DEVICE_PLATFORM,
       e.EVENT_NAME,
       p.UTM_CAMPAIGN,
       p.UTM_MEDIUM,
       p.UTM_SOURCE,
       p.UTM_TERM,
       p.UTM_CONTENT,
       p.CLICK_ID,
       r.URL_HOSTNAME,
       CASE
           WHEN t.touch_id = LAG(t.touch_id)
                                 OVER (PARTITION BY t.ATTRIBUTED_USER_ID ORDER BY e.EVENT_TSTAMP) THEN 0
           ELSE 1 END AS new_touch_flag,
       t.touch_id,
       t.EVENT_INDEX_WITHIN_TOUCH
FROM EVENT_STREAM e
         INNER JOIN TOUCHIFIABLE_EVENTS te ON e.EVENT_HASH = te.EVENT_HASH
         LEFT JOIN MODULE_EXTRACTED_PARAMS p ON e.PAGE_URL = p.URL
         LEFT JOIN MODULE_URL_HOSTNAME r
                   ON e.PAGE_REFERRER = r.URL AND r.URL_MEDIUM NOT IN ('internal', 'payment_gateway')
         LEFT JOIN MODULE_TOUCHIFICATION t ON e.EVENT_HASH = t.EVENT_HASH
WHERE t.ATTRIBUTED_USER_ID = '4195190' --65790769
;
