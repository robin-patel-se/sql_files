USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE SALE_FLIGHT_CONFIG_STREAM
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

MERGE INTO SALE_FLIGHT_CONFIG_STREAM AS TARGET
    USING (
        SELECT ID,
               VERSION,
               IS_ABLE_TO_SELL_FLIGHTS, -- TRUE, FALSE
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
        FROM RAW_VAULT_MVP.CMS_MYSQL.SALE_FLIGHT_CONFIG
--         WHERE LOADED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
    ) AS BATCH ON TARGET.ID = BATCH.ID AND TARGET.LOADED_AT = BATCH.LOADED_AT
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
        ) VALUES (BATCH.ID,
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
                  BATCH.UPDATED_AT);
