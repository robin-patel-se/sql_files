USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

------------------------------------------------------------------------------------------------------------------------
--time diff partition
--identify a time diff partition of 30mins

CREATE OR REPLACE TRANSIENT TABLE MODULE_TIME_DIFF_MARKER AS (
    WITH partition_flag AS (
        SELECT e.EVENT_HASH,
               i.ATTRIBUTED_USER_ID,
               e.IDENTITY_FRAGMENT,
               e.EVENT_TSTAMP,
               timediff(second,
                        LAG(e.EVENT_TSTAMP) OVER (PARTITION BY i.ATTRIBUTED_USER_ID ORDER BY e.EVENT_TSTAMP),
                        e.EVENT_TSTAMP) AS diff,
               CASE
                   WHEN timediff(second,
                                 LAG(e.EVENT_TSTAMP) OVER (PARTITION BY i.ATTRIBUTED_USER_ID ORDER BY e.EVENT_TSTAMP),
                                 e.EVENT_TSTAMP) < 1800 --30 mins
                       THEN 0
                   ELSE 1 END           AS diff_partition_marker
        FROM MODULE_TOUCHIFIABLE_EVENTS e
                 INNER JOIN MODULE_IDENTITY_STITCHING i
                            ON e.IDENTITY_FRAGMENT = i.IDENTITY_FRAGMENT
--           WHERE i.ATTRIBUTED_USER_ID = 65790769 -- TODO: remove, only here for testing
    )
    SELECT d.EVENT_HASH,
--        d.diff_partition_marker,
           SUM(diff_partition_marker)
               OVER (partition by d.ATTRIBUTED_USER_ID
                   ORDER BY d.EVENT_TSTAMP rows between unbounded preceding and current row) AS time_diff_partition
    FROM partition_flag d
);

------------------------------------------------------------------------------------------------------------------------
--assertion
--check there are a unique set of events by event hash
SELECT CASE WHEN COUNT(*) > 1 THEN FALSE ELSE TRUE END AS MODULE_UNIQUE_HASH_FOR_TIME_DIFF_MARKER
FROM (
         SELECT EVENT_HASH
         FROM MODULE_TIME_DIFF_MARKER
         GROUP BY 1
         HAVING COUNT(*) > 1
     );

--check that all the events that should be touchified have done and the number of rows match.
SELECT CASE
           WHEN
               (SELECT COUNT(*) FROM MODULE_TIME_DIFF_MARKER) !=
               (SELECT COUNT(*) FROM MODULE_TOUCHIFIABLE_EVENTS) THEN FALSE
           ELSE TRUE END AS ALL_TOUCHIFIABLE_EVENTS_TIME_LAG_TOUCHIFIED;