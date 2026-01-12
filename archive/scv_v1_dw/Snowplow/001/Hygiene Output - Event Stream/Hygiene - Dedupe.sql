-- Found instances where the event identifier identifies duplication of events that snowplow deduplication step did
-- not identify. The queries in this file identifies the dupes and then removes them from the hygiene table.
-- this process has not been baked into the incremental load because occurrences of these dupes halted after
-- 29th april 2019. Single one off process to remove these duplications on historic data and process is documented should
-- dupes of similar nature become apparent via the unique id assertion or if we need to reprocess the hygiene step.

------------------------------------------------------------------------------------------------------------------------
--dedupe process for events that have exactly the same attributes bar the etl tstamp

USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TRANSIENT TABLE DUPES_TO_DELETE AS (
    --create a list of event hashes combined with the etl tstamps of events that should be removed from event stream as duplicates
    WITH dupe_hashes AS (-- identify dupe hashes
        SELECT e.EVENT_HASH
        FROM EVENT_STREAM e
        GROUP BY 1
        HAVING COUNT(*) > 1),

         rank as ( -- rank the events so we can select the 'non first' rows
             SELECT e.EVENT_HASH,
                    e.ETL_TSTAMP,
                    row_number() OVER (PARTITION BY e.EVENT_HASH ORDER BY e.ETL_TSTAMP) AS rn
             FROM dupe_hashes d
                      INNER JOIN ROBINPATEL.EVENT_STREAM e ON e.EVENT_HASH = d.EVENT_HASH
             ORDER BY EVENT_HASH)

    SELECT EVENT_HASH,
           ETL_TSTAMP
    FROM rank
    WHERE rn != 1
)
;

DELETE
FROM ROBINPATEL.EVENT_STREAM target
    USING DUPES_TO_DELETE batch
WHERE target.EVENT_HASH = batch.EVENT_HASH
  AND target.ETL_TSTAMP = batch.ETL_TSTAMP;

DROP TABLE DUPES_TO_DELETE;

------------------------------------------------------------------------------------------------------------------------
-- duplication interrogation queries


SELECT COUNT(*),
       count(distinct EVENT_HASH)
FROM (
         SELECT e.EVENT_HASH,
                COUNT(*)
         FROM EVENT_STREAM e
         GROUP BY 1
         HAVING COUNT(*) > 1
         ORDER BY 2 DESC
     ); -- 223463 dupe events


SELECT e.*
FROM (
         SELECT e.EVENT_HASH,
                COUNT(*)
         FROM EVENT_STREAM e
         GROUP BY 1
         HAVING COUNT(*) > 1
     ) as d
         INNER JOIN ROBINPATEL.EVENT_STREAM e ON e.EVENT_HASH = d.EVENT_HASH
ORDER BY EVENT_HASH;

SELECT e.EVENT_TSTAMP::DATE,
       COUNT(distinct e.EVENT_HASH)
FROM (
         SELECT e.EVENT_HASH,
                COUNT(*)
         FROM EVENT_STREAM e
         GROUP BY 1
         HAVING COUNT(*) > 1
     ) as d
         INNER JOIN ROBINPATEL.EVENT_STREAM e ON e.EVENT_HASH = d.EVENT_HASH
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE TABLE EVENT_STREAM_BAK CLONE EVENT_STREAM;

