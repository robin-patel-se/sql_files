USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_CURRENT_HOTEL
(
    ID                             NUMBER,
    VERSION                        NUMBER,
    BASE_CURRENCY                  VARCHAR,
    CITY_DISTRICT_ID               NUMBER,
    COMMISSION                     DOUBLE,
    COMMISSION_TYPE                VARCHAR,
    COMPANY_ID                     NUMBER,
    CONTRACTOR_ID                  NUMBER,
    DEFAULT_PREFERRED_AIRPORT_CODE VARCHAR,
    HOTEL_CODE                     VARCHAR,
    JOINT_CONTRACTOR_ID            NUMBER,
    LATITUDE                       DOUBLE,
    LOCATION_INFO_ID               NUMBER,
    LONGITUDE                      DOUBLE,
    SEND_SUMMARY                   NUMBER,
    TRIP_ADVISOR_RATINGS_IMAGE_URL VARCHAR,
    VAT_EXCLUSIVE                  NUMBER,
    MAP_ZOOM_LEVEL                 NUMBER,
    DATASET_NAME                   VARCHAR,
    DATASET_SOURCE                 VARCHAR,
    SCHEDULE_INTERVAL              VARCHAR,
    SCHEDULE_TSTAMP                TIMESTAMPNTZ,
    RUN_TSTAMP                     TIMESTAMPNTZ,
    LOADED_AT                      TIMESTAMPNTZ,
    FILENAME                       VARCHAR,
    FILE_ROW_NUMBER                NUMBER,
    UPDATED_AT                     TIMESTAMPLTZ
)
    CLUSTER BY (ID, UPDATED_AT);

MERGE INTO MODULE_CURRENT_HOTEL AS TARGET
    USING (
        WITH row_number AS ( --create index based on import recency
            SELECT h.*,
                   ROW_NUMBER() OVER (PARTITION BY h.id ORDER BY h.LOADED_AT DESC) AS rn
            FROM HOTEL_STREAM h
--              WHERE UPDATED_AT >= TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
        )
        SELECT ID,
               VERSION,
               BASE_CURRENCY,
               CITY_DISTRICT_ID,
               COMMISSION,
               COMMISSION_TYPE,
               COMPANY_ID,
               CONTRACTOR_ID,
               DEFAULT_PREFERRED_AIRPORT_CODE,
               HOTEL_CODE,
               JOINT_CONTRACTOR_ID,
               LATITUDE,
               LOCATION_INFO_ID,
               LONGITUDE,
               SEND_SUMMARY,
               TRIP_ADVISOR_RATINGS_IMAGE_URL,
               VAT_EXCLUSIVE,
               MAP_ZOOM_LEVEL,
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
    ) AS BATCH ON TARGET.ID = BATCH.ID
    WHEN NOT MATCHED
        THEN INSERT (
                     ID,
                     VERSION,
                     BASE_CURRENCY,
                     CITY_DISTRICT_ID,
                     COMMISSION,
                     COMMISSION_TYPE,
                     COMPANY_ID,
                     CONTRACTOR_ID,
                     DEFAULT_PREFERRED_AIRPORT_CODE,
                     HOTEL_CODE,
                     JOINT_CONTRACTOR_ID,
                     LATITUDE,
                     LOCATION_INFO_ID,
                     LONGITUDE,
                     SEND_SUMMARY,
                     TRIP_ADVISOR_RATINGS_IMAGE_URL,
                     VAT_EXCLUSIVE,
                     MAP_ZOOM_LEVEL,
                     DATASET_NAME,
                     DATASET_SOURCE,
                     SCHEDULE_INTERVAL,
                     SCHEDULE_TSTAMP,
                     RUN_TSTAMP,
                     LOADED_AT,
                     FILENAME,
                     FILE_ROW_NUMBER,
                     UPDATED_AT)
        VALUES (BATCH.ID,
                BATCH.VERSION,
                BATCH.BASE_CURRENCY,
                BATCH.CITY_DISTRICT_ID,
                BATCH.COMMISSION,
                BATCH.COMMISSION_TYPE,
                BATCH.COMPANY_ID,
                BATCH.CONTRACTOR_ID,
                BATCH.DEFAULT_PREFERRED_AIRPORT_CODE,
                BATCH.HOTEL_CODE,
                BATCH.JOINT_CONTRACTOR_ID,
                BATCH.LATITUDE,
                BATCH.LOCATION_INFO_ID,
                BATCH.LONGITUDE,
                BATCH.SEND_SUMMARY,
                BATCH.TRIP_ADVISOR_RATINGS_IMAGE_URL,
                BATCH.VAT_EXCLUSIVE,
                BATCH.MAP_ZOOM_LEVEL,
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
        TARGET.BASE_CURRENCY = BATCH.BASE_CURRENCY,
        TARGET.CITY_DISTRICT_ID = BATCH.CITY_DISTRICT_ID,
        TARGET.COMMISSION = BATCH.COMMISSION,
        TARGET.COMMISSION_TYPE = BATCH.COMMISSION_TYPE,
        TARGET.COMPANY_ID = BATCH.COMPANY_ID,
        TARGET.CONTRACTOR_ID = BATCH.CONTRACTOR_ID,
        TARGET.DEFAULT_PREFERRED_AIRPORT_CODE = BATCH.DEFAULT_PREFERRED_AIRPORT_CODE,
        TARGET.HOTEL_CODE = BATCH.HOTEL_CODE,
        TARGET.JOINT_CONTRACTOR_ID = BATCH.JOINT_CONTRACTOR_ID,
        TARGET.LATITUDE = BATCH.LATITUDE,
        TARGET.LOCATION_INFO_ID = BATCH.LOCATION_INFO_ID,
        TARGET.LONGITUDE = BATCH.LONGITUDE,
        TARGET.SEND_SUMMARY = BATCH.SEND_SUMMARY,
        TARGET.TRIP_ADVISOR_RATINGS_IMAGE_URL = BATCH.TRIP_ADVISOR_RATINGS_IMAGE_URL,
        TARGET.VAT_EXCLUSIVE = BATCH.VAT_EXCLUSIVE,
        TARGET.MAP_ZOOM_LEVEL = BATCH.MAP_ZOOM_LEVEL,
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
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_HOTEL_ID
FROM (
         SELECT ID
         FROM MODULE_CURRENT_HOTEL
         GROUP BY 1
         HAVING COUNT(*) > 1);