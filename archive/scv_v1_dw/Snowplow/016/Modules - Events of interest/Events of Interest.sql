USE WAREHOUSE PIPE_LARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE VIEW MODULE_EVENTS_OF_INTEREST AS (
    SELECT *
    FROM (
             -- spvs
             SELECT EVENT_HASH,
                    TOUCH_ID,
                    EVENT_TSTAMP,
                    SE_SALE_ID,
                    NULL AS BOOKING_ID,
                    EVENT_CATEGORY,
                    EVENT_SUBCATEGORY,
                    UPDATED_AT
             FROM MODULE_TOUCHED_SPVS
             UNION
             -- transactions
             SELECT EVENT_HASH,
                    TOUCH_ID,
                    EVENT_TSTAMP,
                    NULL AS SE_SALE_ID,
                    BOOKING_ID,
                    EVENT_CATEGORY,
                    EVENT_SUBCATEGORY,
                    UPDATED_AT
             FROM MODULE_TOUCHED_TRANSACTIONS
         )
);

SELECT DATE_TRUNC(day, EVENT_TSTAMP)::DATE,
       COUNT(*)
FROM MODULE_EVENTS_OF_INTEREST
WHERE EVENT_SUBCATEGORY = 'SPV'
GROUP BY 1
ORDER BY 1;

CREATE VIEW IF NOT EXISTS