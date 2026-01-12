USE WAREHOUSE PIPE_LARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

CREATE OR REPLACE VIEW MODULE_CURRENT_SALE_ATTRIBUTES AS (
    SELECT SALE_ID,
           CLASS,
           HAS_FLIGHTS_AVAILABLE,
           DEFAULT_PREFERRED_AIRPORT_CODE,
           NULL AS TYPE,
           NULL AS HOTEL_CHAIN_LINK,
           NULL AS CLOSEST_AIRPORT_CODE,
           NULL AS IS_ABLE_TO_SELL_FLIGHTS,
           SALE_PRODUCT,
           SALE_TYPE,
           PRODUCT_TYPE,
           PRODUCT_CONFIGURATION,
           PRODUCT_LINE,
           SALE_MODEL,
           UPDATED_AT
    FROM MODULE_CURRENT_NEW_MODEL_ATTRIBUTES

    UNION

    SELECT SALE_ID,
           NULL AS CLASS,
           NULL AS HAS_FLIGHTS_AVAILABLE,
           NULL AS DEFAULT_PREFERRED_AIRPORT_CODE,
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
    FROM MODULE_CURRENT_OLD_MODEL_ATTRIBUTES
);

------------------------------------------------------------------------------------------------------------------------
--assertions
SELECT CASE WHEN COUNT(*) > 0 THEN FALSE ELSE TRUE END AS UNIQUE_SALE_ID
FROM (
         SELECT SALE_ID
         FROM MODULE_CURRENT_SALE_ATTRIBUTES
         GROUP BY 1
         HAVING COUNT(*) > 1);



-- SELECT SALE_MODEL,
--        SALE_PRODUCT,
--        SALE_TYPE,
--
--        COUNT(*) AS no_of_sales
-- FROM MODULE_CURRENT_SALE_ATTRIBUTES
-- GROUP BY 1, 2, 3
-- ORDER BY 1, 2;

------------------------------------------------------------------------------------------------------------------------

--new version

SELECT * FROM
COLLAB.MUSE_DATA_MODELLING.SALE_DIMENSIONS;

GRANT USAGE ON SCHEMA COLLAB.MUSE_DATA_MODELLING TO ROLE PERSONAL_ROLE__KIRSTENGRIEVE;