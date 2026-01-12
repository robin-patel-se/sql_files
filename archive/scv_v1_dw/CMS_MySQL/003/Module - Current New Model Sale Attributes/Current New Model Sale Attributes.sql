USE WAREHOUSE PIPE_XLARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE TABLE MODULE_CURRENT_NEW_MODEL_ATTRIBUTES
(
    SALE_ID                        VARCHAR,
    CLASS                          VARCHAR,
    HAS_FLIGHTS_AVAILABLE          BOOLEAN,
    DEFAULT_PREFERRED_AIRPORT_CODE VARCHAR,
    SALE_PRODUCT                   VARCHAR,
    SALE_TYPE                      VARCHAR,
    PRODUCT_TYPE                   VARCHAR,
    PRODUCT_CONFIGURATION          VARCHAR,
    PRODUCT_LINE                   VARCHAR,
    SALE_MODEL                     VARCHAR,
    UPDATED_AT                     TIMESTAMPLTZ
)
    CLUSTER BY (SALE_ID);

MERGE INTO MODULE_CURRENT_NEW_MODEL_ATTRIBUTES AS TARGET
    USING (
        SELECT bs.SALE_ID,
               bs.CLASS,
               bs.HAS_FLIGHTS_AVAILABLE,
               h.DEFAULT_PREFERRED_AIRPORT_CODE,
               CASE
                   WHEN CLASS = 'com.flashsales.sale.HotelSale' THEN 'HOTEL'
                   WHEN CLASS = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN 'WRD'
                   WHEN CLASS = 'com.flashsales.sale.IhpSale' THEN 'IHP'
                   ELSE 'N/A'
                   END           AS SALE_PRODUCT,

               CASE
                   WHEN bs.CLASS = 'com.flashsales.sale.HotelSale'
                       THEN
                       CASE
                           WHEN bs.HAS_FLIGHTS_AVAILABLE = TRUE AND
                                h.DEFAULT_PREFERRED_AIRPORT_CODE IS NOT NULL
                               THEN 'Hotel Plus'
                           ELSE 'Hotel Only'
                           END
                   ELSE
                       CASE
                           WHEN bs.CLASS = 'com.flashsales.sale.ConnectedWebRedirectSale'
                               THEN 'Catalogue'
                           ELSE
                               CASE
                                   WHEN bs.CLASS = 'com.flashsales.sale.IhpSale'
                                       THEN 'IHP - C'
                                   ELSE 'N/A'
                                   END
                           END
                   END           AS SALE_TYPE,

               CASE
                   WHEN CLASS = 'com.flashsales.sale.HotelSale' THEN 'Hotel'
                   WHEN CLASS = 'com.flashsales.sale.IhpSale' THEN 'Package'
                   ELSE 'N/A'
                   END           AS PRODUCT_TYPE,

               CASE
                   WHEN bs.CLASS = 'com.flashsales.sale.HotelSale'
                       THEN
                       CASE
                           WHEN bs.HAS_FLIGHTS_AVAILABLE = TRUE AND
                                h.DEFAULT_PREFERRED_AIRPORT_CODE IS NOT NULL
                               THEN 'Hotel Plus'
                           ELSE 'Hotel Only'
                           END
                   ELSE
                       CASE
                           WHEN bs.CLASS = 'com.flashsales.sale.ConnectedWebRedirectSale'
                               THEN 'Catalogue'
                           ELSE
                               CASE
                                   WHEN bs.CLASS = 'com.flashsales.sale.IhpSale'
                                       THEN 'IHP - connected'
                                   ELSE 'N/A'
                                   END
                           END
                   END           AS PRODUCT_CONFIGURATION,


               'flash'           AS PRODUCT_LINE,
               'New Model'       AS SALE_MODEL,
               CURRENT_TIMESTAMP AS UPDATED_AT

        FROM MODULE_CURRENT_BASE_SALE bs
                 LEFT JOIN MODULE_CURRENT_HOTEL h ON bs.DEFAULT_HOTEL_OFFER_ID = h.ID
        WHERE CLASS != 'com.flashsales.sale.ConnectedWebRedirectSale' -- remove WRD catalogue sales
--         WHERE h.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP)
--            OR bs.UPDATED_AT >=
--               TIMESTAMPADD('hour', -1, '{schedule_tstamp}'::TIMESTAMP) -- TODO: for batch incremental load

    ) AS BATCH ON TARGET.SALE_ID = BATCH.SALE_ID
    WHEN NOT MATCHED
        THEN INSERT (
                     SALE_ID,
                     CLASS,
                     HAS_FLIGHTS_AVAILABLE,
                     DEFAULT_PREFERRED_AIRPORT_CODE,
                     SALE_PRODUCT,
                     SALE_TYPE,
                     PRODUCT_TYPE,
                     PRODUCT_CONFIGURATION,
                     PRODUCT_LINE,
                     SALE_MODEL,
                     UPDATED_AT
        )
        VALUES (BATCH.SALE_ID,
                BATCH.CLASS,
                BATCH.HAS_FLIGHTS_AVAILABLE,
                BATCH.DEFAULT_PREFERRED_AIRPORT_CODE,
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
        TARGET.CLASS = BATCH.CLASS,
        TARGET.HAS_FLIGHTS_AVAILABLE = BATCH.HAS_FLIGHTS_AVAILABLE,
        TARGET.DEFAULT_PREFERRED_AIRPORT_CODE = BATCH.DEFAULT_PREFERRED_AIRPORT_CODE,
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
         FROM MODULE_CURRENT_NEW_MODEL_ATTRIBUTES
         GROUP BY 1
         HAVING COUNT(*) > 1);