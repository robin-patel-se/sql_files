USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_CURRENT_SALE_FLIGHT_CONFIG
(
    ID                      NUMBER,
    VERSION                 NUMBER,
    IS_ABLE_TO_SELL_FLIGHTS BOOLEAN,
    SALE_ID                 NUMBER,
    TERRITORY_ID            NUMBER,
    DATASET_NAME            VARCHAR,
    DATASET_SOURCE          VARCHAR,
    SCHEDULE_INTERVAL       VARCHAR,
    SCHEDULE_TSTAMP         TIMESTAMPNTZ,
    RUN_TSTAMP              TIMESTAMPNTZ,
    LOADED_AT               TIMESTAMPNTZ,
    FILENAME                VARCHAR,
    FILE_ROW_NUMBER         NUMBER,
    UPDATED_AT              TIMESTAMPLTZ
)
    CLUSTER BY (ID, SALE_ID, UPDATED_AT)
;

MERGE INTO MODULE_CURRENT_SALE_FLIGHT_CONFIG AS TARGET
    USING (
        WITH row_number AS ( --create index based on import recency
            SELECT f.*,
                   ROW_NUMBER() OVER (PARTITION BY f.SALE_ID ORDER BY f.LOADED_AT DESC) AS rn
            FROM SALE_FLIGHT_CONFIG_STREAM f
--              WHERE f.UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
        )
        SELECT ID,
               VERSION,
               IS_ABLE_TO_SELL_FLIGHTS,
               SALE_ID,
               TERRITORY_ID,
               DATASET_NAME,
               DATASET_SOURCE,
               SCHEDULE_INTERVAL,
               SCHEDULE_TSTAMP,
               RUN_TSTAMP,
               LOADED_AT,
               FILENAME,
               FILE_ROW_NUMBER,
               CURRENT_TIMESTAMP AS UPDATED_AT
        FROM row_number
        WHERE rn = 1
    ) AS BATCH ON TARGET.SALE_ID = BATCH.SALE_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     ID,
                     VERSION,
                     IS_ABLE_TO_SELL_FLIGHTS,
                     SALE_ID,
                     TERRITORY_ID,
                     DATASET_NAME,
                     DATASET_SOURCE,
                     SCHEDULE_INTERVAL,
                     SCHEDULE_TSTAMP,
                     RUN_TSTAMP,
                     LOADED_AT,
                     FILENAME,
                     FILE_ROW_NUMBER,
                     UPDATED_AT
        )
        VALUES (BATCH.ID,
                BATCH.VERSION,
                BATCH.IS_ABLE_TO_SELL_FLIGHTS,
                BATCH.SALE_ID,
                BATCH.TERRITORY_ID,
                BATCH.DATASET_NAME,
                BATCH.DATASET_SOURCE,
                BATCH.SCHEDULE_INTERVAL,
                BATCH.SCHEDULE_TSTAMP,
                BATCH.RUN_TSTAMP,
                BATCH.LOADED_AT,
                BATCH.FILENAME,
                BATCH.FILE_ROW_NUMBER,
                BATCH.UPDATED_AT)
    WHEN MATCHED AND TARGET.LOADED_AT < BATCH.LOADED_AT
        THEN UPDATE SET
        TARGET.ID = BATCH.ID,
        TARGET.VERSION = BATCH.VERSION,
        TARGET.IS_ABLE_TO_SELL_FLIGHTS = BATCH.IS_ABLE_TO_SELL_FLIGHTS,
        TARGET.SALE_ID = BATCH.SALE_ID,
        TARGET.TERRITORY_ID = BATCH.TERRITORY_ID,
        TARGET.DATASET_NAME = BATCH.DATASET_NAME,
        TARGET.DATASET_SOURCE = BATCH.DATASET_SOURCE,
        TARGET.SCHEDULE_INTERVAL = BATCH.SCHEDULE_INTERVAL,
        TARGET.SCHEDULE_TSTAMP = BATCH.SCHEDULE_TSTAMP,
        TARGET.RUN_TSTAMP = BATCH.RUN_TSTAMP,
        TARGET.LOADED_AT = BATCH.LOADED_AT,
        TARGET.FILENAME = BATCH.FILENAME,
        TARGET.FILE_ROW_NUMBER = BATCH.FILE_ROW_NUMBER,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
;

------------------------------------------------------------------------------------------------------------------------
--assertions
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_SALE_ID
FROM (
         SELECT SALE_ID
         FROM MODULE_CURRENT_SALE_FLIGHT_CONFIG
         GROUP BY 1
         HAVING COUNT(*) > 1);