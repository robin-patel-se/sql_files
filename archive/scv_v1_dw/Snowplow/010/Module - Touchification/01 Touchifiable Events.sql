USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;
------------------------------------------------------------------------------------------------------------------------
--------------------------------------------STREAM OF TOUCHIFIABLE EVENTS-----------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--filter to stream of events that need to be touchified
--filter only to event types that we want to touchify e.g. e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item')
--filter to all events from any member that has been re-identity stitched
--filter to all new events
--filter to all events from members that have a late arriving event

--output is a stream of events that need to be touchified that can then flow into touchification
--note if we change filter logic on what type of events should be touchified then we will need to also adjust the assertion on
--the final touchification stage

--first run
CREATE OR REPLACE TABLE MODULE_TOUCHIFIABLE_EVENTS AS (
    SELECT e.EVENT_HASH,
           e.EVENT_TSTAMP,
           e.DERIVED_TSTAMP,
           e.COLLECTOR_TSTAMP,
           e.IDENTITY_FRAGMENT,
           e.EVENT_NAME,
           e.PAGE_URL,
           e.PAGE_REFERRER,
           e.DEVICE_PLATFORM,
           e.UPDATED_AT
    FROM EVENT_STREAM e
    WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
      AND e.IDENTITY_FRAGMENT IS NOT NULL
      AND e.IS_ROBOT_SPIDER_EVENT = FALSE -- remove extra computation required to resessionise robot events
);

------------------------------------------------------------------------------------------------------------------------
--incremental run
--TODO: remove events from users that have been identified as robots as opposed to events that have been.
CREATE OR REPLACE TABLE MODULE_TOUCHIFIABLE_EVENTS AS (
    --new events that have arrived since touchification last run
    (SELECT e.EVENT_HASH,
            e.EVENT_TSTAMP,
            e.DERIVED_TSTAMP,
            e.IDENTITY_FRAGMENT,
            e.EVENT_NAME,
            e.PAGE_URL,
            e.PAGE_REFERRER,
            e.DEVICE_PLATFORM,
            e.UPDATED_AT
     FROM EVENT_STREAM e
     WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
       AND e.IDENTITY_FRAGMENT IS NOT NULL
       AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                  -- remove extra computation required to resessionise robot events
       -- AND e.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
       AND e.UPDATED_AT::DATE = '2019-12-11' --TODO: Remove, just for incremental testing

    )

    UNION
    --all events from newly stitched members
    (SELECT e.EVENT_HASH,
            e.EVENT_TSTAMP,
            e.DERIVED_TSTAMP,
            e.IDENTITY_FRAGMENT,
            e.EVENT_NAME,
            e.PAGE_URL,
            e.PAGE_REFERRER,
            e.DEVICE_PLATFORM,
            e.UPDATED_AT
     FROM MODULE_IDENTITY_STITCHING i
              INNER JOIN EVENT_STREAM e ON e.IDENTITY_FRAGMENT = i.IDENTITY_FRAGMENT
     WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
       AND e.IDENTITY_FRAGMENT IS NOT NULL
       AND e.IS_ROBOT_SPIDER_EVENT = FALSE
--        AND i.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
       AND i.UPDATED_AT::DATE = '2019-12-11' --TODO: Remove, just for incremental testing


    )

    UNION
    --all events from members that have late arriving events, restitch all their events as we don't know where they might land
    (
        WITH late_arriving_users AS ( -- identify event user identifiers that have late arriving records
            SELECT DISTINCT e.IDENTITY_FRAGMENT
            FROM EVENT_STREAM e
            WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
              AND e.IS_ROBOT_SPIDER_EVENT = FALSE                                  -- remove extra computation required to resessionise robot events
              AND e.IDENTITY_FRAGMENT IS NOT NULL
              -- AND e.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- new event  TODO: for batch incremental load
              AND e.UPDATED_AT::DATE = '2019-12-11'                                --TODO: Remove, just for incremental testing
              AND e.EVENT_TSTAMP <= (SELECT MAX(EVENT_TSTAMP) FROM MODULE_TOUCHIFICATION) -- late arriving
        )
        SELECT e.EVENT_HASH,
               e.EVENT_TSTAMP,
               e.DERIVED_TSTAMP,
               e.IDENTITY_FRAGMENT,
               e.EVENT_NAME,
               e.PAGE_URL,
               e.PAGE_REFERRER,
               e.DEVICE_PLATFORM,
               e.UPDATED_AT
        FROM late_arriving_users l
                 INNER JOIN EVENT_STREAM e ON l.IDENTITY_FRAGMENT = e.IDENTITY_FRAGMENT
        WHERE e.EVENT_NAME IN ('page_view', 'screen_view', 'transaction_item', 'transaction') -- explicitly define the events we want to touchify
          AND e.IS_ROBOT_SPIDER_EVENT = FALSE
    )
);
------------------------------------------------------------------------------------------------------------------------
--assertions

--check there's a unique list of event hashes in the touchifiable events
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_NO_OF_EVENTS
FROM (
         SELECT EVENT_HASH
         FROM MODULE_TOUCHIFIABLE_EVENTS
         GROUP BY 1
         HAVING COUNT(*) > 1
     );




