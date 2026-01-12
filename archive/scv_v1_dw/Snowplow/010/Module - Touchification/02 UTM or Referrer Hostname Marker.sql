USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

--utm/referrer partition
--Identify changes to a combination of url params and referrer hostname
--URL params and Referrer hostname do not persist through all events in a touch

CREATE OR REPLACE TRANSIENT TABLE MODULE_TOUCH_UTM_REFERRER_MARKER AS (
    WITH utm_referrer_marker as (
        --identify changes to utm/referrer data ignoring internal and payment gateway referrers
        SELECT e.EVENT_HASH,
               e.EVENT_TSTAMP,
               e.DERIVED_TSTAMP,
               e.IDENTITY_FRAGMENT,
               i.ATTRIBUTED_USER_ID,
               SHA2(NULLIF(COALESCE(p.UTM_CAMPAIGN, '') ||
                           COALESCE(p.UTM_MEDIUM, '') ||
                           COALESCE(p.UTM_SOURCE, '') ||
                           COALESCE(p.UTM_TERM, '') ||
                           COALESCE(p.UTM_CONTENT, '') ||
                           COALESCE(p.CLICK_ID, '') ||
                           COALESCE(r.URL_HOSTNAME, ''), '')) AS partition_marker

        FROM MODULE_TOUCHIFIABLE_EVENTS e
                 INNER JOIN MODULE_IDENTITY_STITCHING i
                            ON e.IDENTITY_FRAGMENT = i.IDENTITY_FRAGMENT
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
                    u.IDENTITY_FRAGMENT,
                    u.ATTRIBUTED_USER_ID,
--        u.partition_marker,
                    CASE
                        WHEN LAST_VALUE(u.partition_marker)
                                        IGNORE NULLS OVER (PARTITION BY u.ATTRIBUTED_USER_ID
                                            ORDER BY u.EVENT_TSTAMP, u.DERIVED_TSTAMP rows between unbounded preceding and current row) IS NULL
                            THEN 'first_group' -- when there are no unique utm params or referrer hostname for the first set of events
                        ELSE LAST_VALUE(u.partition_marker)
                                        IGNORE NULLS OVER (PARTITION BY COALESCE(u.ATTRIBUTED_USER_ID, u.IDENTITY_FRAGMENT)
                                            ORDER BY u.EVENT_TSTAMP, u.DERIVED_TSTAMP rows between unbounded preceding and current row) END
                        AS partition_group
             FROM utm_referrer_marker u
         )
            ,
         partition_flag AS (
             --flag the partition if utm params/referrer hostname change
             SELECT pg.EVENT_HASH,
                    pg.EVENT_TSTAMP,
                    pg.IDENTITY_FRAGMENT,
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

------------------------------------------------------------------------------------------------------------------------
--assertion
--check there are a unique set of events by event hash
SELECT CASE WHEN COUNT(*) > 1 THEN FALSE ELSE TRUE END AS UNIQUE_HASH_FOR_UTM_REF_MARKER
FROM (
         SELECT EVENT_HASH
         FROM MODULE_UTM_REFERRER_MARKER
         GROUP BY 1
         HAVING COUNT(*) > 1
     );

--check that all the events that should be touchified have done and the number of rows match.
SELECT CASE
           WHEN
                       (SELECT COUNT(*) FROM MODULE_UTM_REFERRER_MARKER) !=
                       (SELECT COUNT(*) FROM MODULE_TOUCHIFIABLE_EVENTS) THEN FALSE
           ELSE TRUE END AS ALL_TOUCHIFIABLE_EVENTS_UTM_REFERRER_TOUCHIFIED;
