USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_TOUCHED_TRANSACTIONS
(
    EVENT_HASH        VARCHAR,
    TOUCH_ID          VARCHAR,
    EVENT_TSTAMP      TIMESTAMPNTZ,
    BOOKING_ID        VARCHAR,
    EVENT_CATEGORY    VARCHAR,
    EVENT_SUBCATEGORY VARCHAR,
    UPDATED_AT        TIMESTAMP_LTZ
);

MERGE INTO MODULE_TOUCHED_TRANSACTIONS AS TARGET
    USING (
--Transcation events
        SELECT e.EVENT_HASH,
               t.TOUCH_ID,
               e.EVENT_TSTAMP,
               e.TI_ORDERID      AS booking_id,
               'transaction'     AS event_category,
               'transaction'     AS event_subcategory,
               CURRENT_TIMESTAMP AS updated_at --TODO: replace with '{schedule_tstamp}'

        FROM MODULE_TOUCHIFICATION t
                 INNER JOIN EVENT_STREAM e
                            ON e.EVENT_HASH = t.EVENT_HASH
        WHERE e.EVENT_NAME IN ('transaction_item', 'transaction')
        AND TI_ORDERID IS NOT NULL
--           AND t.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
    )
        AS BATCH ON TARGET.EVENT_HASH = BATCH.EVENT_HASH
    WHEN NOT MATCHED
        THEN INSERT (
                     EVENT_HASH,
                     TOUCH_ID,
                     EVENT_TSTAMP,
                     BOOKING_ID,
                     EVENT_CATEGORY,
                     EVENT_SUBCATEGORY,
                     UPDATED_AT)
        VALUES (BATCH.EVENT_HASH,
                BATCH.TOUCH_ID,
                BATCH.EVENT_TSTAMP,
                BATCH.BOOKING_ID,
                BATCH.EVENT_CATEGORY,
                BATCH.EVENT_SUBCATEGORY,
                BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.TOUCH_ID != BATCH.TOUCH_ID
        THEN UPDATE SET
        TARGET.TOUCH_ID = BATCH.TOUCH_ID,
        TARGET.EVENT_TSTAMP = BATCH.EVENT_TSTAMP,
        TARGET.BOOKING_ID = BATCH.BOOKING_ID,
        TARGET.EVENT_CATEGORY = BATCH.EVENT_CATEGORY,
        TARGET.EVENT_SUBCATEGORY = BATCH.EVENT_SUBCATEGORY,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT;

------------------------------------------------------------------------------------------------------------------------
--assertions
--all spvs have a booking id
SELECT CASE
           WHEN (SELECT COUNT(*) FROM MODULE_TOUCHED_TRANSACTIONS WHERE BOOKING_ID IS NULL)
               > 0 THEN FALSE
           ELSE TRUE END AS ALL_TRANSACTIONS_HAVE_BOOKING_ID;

--unique booking per event hash
SELECT CASE
           WHEN COUNT(*) > 0 THEN FALSE
           ELSE TRUE END AS UNIQUE_EVENT_HASH
FROM (
         SELECT EVENT_HASH,
                COUNT(*)
         FROM MODULE_TOUCHED_TRANSACTIONS
         GROUP BY 1
         HAVING COUNT(*) > 1);