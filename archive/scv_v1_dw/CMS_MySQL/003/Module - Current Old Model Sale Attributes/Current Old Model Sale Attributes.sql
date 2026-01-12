USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_CURRENT_OLD_MODEL_ATTRIBUTES
(
    SALE_ID                 VARCHAR,
    TYPE                    VARCHAR,
    HOTEL_CHAIN_LINK        VARCHAR,
    CLOSEST_AIRPORT_CODE    VARCHAR,
    IS_ABLE_TO_SELL_FLIGHTS BOOLEAN,
    SALE_PRODUCT            VARCHAR,
    SALE_TYPE               VARCHAR,
    PRODUCT_TYPE            VARCHAR,
    PRODUCT_CONFIGURATION   VARCHAR,
    PRODUCT_LINE            VARCHAR,
    SALE_MODEL              VARCHAR,
    UPDATED_AT              TIMESTAMPLTZ
)
    CLUSTER BY (SALE_ID);

MERGE INTO MODULE_CURRENT_OLD_MODEL_ATTRIBUTES AS TARGET
    USING (
        SELECT s.SALE_ID,
               s.TYPE,
               s.HOTEL_CHAIN_LINK,
               s.CLOSEST_AIRPORT_CODE,
               c.IS_ABLE_TO_SELL_FLIGHTS,
               s.TYPE            AS SALE_PRODUCT,

               CASE
                   WHEN s.TYPE IN ('PACKAGE', 'TRAVEL') THEN
                       CASE
                           WHEN s.HOTEL_CHAIN_LINK IS NOT NULL THEN 'WRD'
                           ELSE
                               CASE
                                   WHEN s.CLOSEST_AIRPORT_CODE IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                       THEN 'IHP - dynamic'
                                   ELSE
                                       CASE
                                           WHEN s.IS_TEAM20PACKAGE = 1 THEN 'IHP - static'
                                           ELSE '3PP'
                                           END
                                   END
                           END
                   ELSE CASE
                            WHEN s.TYPE = 'HOTEL' THEN
                                CASE
                                    WHEN s.HOTEL_CHAIN_LINK IS NOT NULL THEN 'WRD'
                                    ELSE
                                        CASE
                                            WHEN s.CLOSEST_AIRPORT_CODE IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                                THEN 'Hotel Plus'
                                            ELSE 'Hotel'
                                            END
                                    END
                            ELSE 'N/A'
                       END
                   END           AS SALE_TYPE,

               CASE
                   WHEN s.TYPE = 'HOTEL' THEN 'Hotel'
                   WHEN s.TYPE = 'DAY' THEN 'Day Experience'
                   WHEN s.TYPE IN ('PACKAGE', 'TRAVEL') THEN 'Package'
                   END
                                 AS PRODUCT_TYPE,


               CASE
                   WHEN s.TYPE IN ('PACKAGE', 'TRAVEL') THEN
                       CASE
                           WHEN s.HOTEL_CHAIN_LINK IS NOT NULL THEN 'WRD'
                           ELSE
                               CASE
                                   WHEN s.CLOSEST_AIRPORT_CODE IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                       THEN 'IHP - dynamic'
                                   ELSE
                                       CASE
                                           WHEN s.IS_TEAM20PACKAGE = 1 THEN 'IHP - static'
                                           ELSE '3PP'
                                           END
                                   END
                           END
                   ELSE CASE
                            WHEN s.TYPE = 'HOTEL' THEN
                                CASE
                                    WHEN s.HOTEL_CHAIN_LINK IS NOT NULL THEN 'WRD'
                                    ELSE
                                        CASE
                                            WHEN s.CLOSEST_AIRPORT_CODE IS NOT NULL AND c.is_able_to_sell_flights = TRUE
                                                THEN 'Hotel Plus'
                                            ELSE 'Hotel'
                                            END
                                    END
                            ELSE 'N/A'
                       END
                   END           AS PRODUCT_CONFIGURATION,

               'flash'           AS PRODUCT_LINE,

               'Old Model'       AS SALE_MODEL,
               CURRENT_TIMESTAMP AS UPDATED_AT

        FROM MODULE_CURRENT_SALE s
                 LEFT JOIN MODULE_CURRENT_SALE_FLIGHT_CONFIG c ON s.SALE_ID = c.SALE_ID
--         WHERE s.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP)
--            OR c.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load
    ) AS BATCH ON TARGET.SALE_ID = BATCH.SALE_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     SALE_ID,
                     TYPE,
                     HOTEL_CHAIN_LINK,
                     CLOSEST_AIRPORT_CODE,
                     IS_ABLE_TO_SELL_FLIGHTS,
                     SALE_PRODUCT,
                     SALE_TYPE,
                     PRODUCT_TYPE,
                     PRODUCT_CONFIGURATION,
                     PRODUCT_LINE,
                     SALE_MODEL,
                     UPDATED_AT
        )
        VALUES (BATCH.SALE_ID,
                BATCH.TYPE,
                BATCH.HOTEL_CHAIN_LINK,
                BATCH.CLOSEST_AIRPORT_CODE,
                BATCH.IS_ABLE_TO_SELL_FLIGHTS,
                BATCH.SALE_PRODUCT,
                BATCH.SALE_TYPE,
                BATCH.PRODUCT_TYPE,
                BATCH.PRODUCT_CONFIGURATION,
                BATCH.PRODUCT_LINE,
                BATCH.SALE_MODEL,
                BATCH.UPDATED_AT)
    WHEN MATCHED
        THEN UPDATE SET
        TARGET.SALE_ID = BATCH.SALE_ID,
        TARGET.TYPE = BATCH.TYPE,
        TARGET.HOTEL_CHAIN_LINK = BATCH.HOTEL_CHAIN_LINK,
        TARGET.CLOSEST_AIRPORT_CODE = BATCH.CLOSEST_AIRPORT_CODE,
        TARGET.IS_ABLE_TO_SELL_FLIGHTS = BATCH.IS_ABLE_TO_SELL_FLIGHTS,
        TARGET.SALE_PRODUCT = BATCH.SALE_PRODUCT,
        TARGET.SALE_TYPE = BATCH.SALE_TYPE,
        TARGET.PRODUCT_TYPE = BATCH.PRODUCT_TYPE,
        TARGET.PRODUCT_CONFIGURATION = BATCH.PRODUCT_CONFIGURATION,
        TARGET.PRODUCT_LINE = BATCH.PRODUCT_LINE,
        TARGET.SALE_MODEL = BATCH.SALE_MODEL,
        TARGET.UPDATED_AT = BATCH.UPDATED_AT
;

------------------------------------------------------------------------------------------------------------------------
--assertions
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_SALE_ID
FROM (
         SELECT SALE_ID
         FROM MODULE_CURRENT_OLD_MODEL_ATTRIBUTES
         GROUP BY 1
         HAVING COUNT(*) > 1);