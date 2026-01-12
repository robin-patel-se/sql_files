USE DATABASE SCRATCH;
use role PERSONAL_ROLE__ROBINPATEL;
USE SCHEMA CARMENMARDIROSDEV36082;

SELECT *
FROM RAW_SNOWPLOW_MOCK_EVENTS;

CREATE OR REPLACE VIEW RP_BUNDLES AS (
    SELECT USER_ID,
           EVENT_NTH,
           PRODUCT_BUNDLE.value['id']::varchar                     AS bundle_id,
           PRODUCT_BUNDLE.value['tech_provider']::varchar          AS bundle_tech_provider,
           PRODUCT_BUNDLE.value['business_provider']::varchar      AS bundle_business_provider,
           PRODUCT_BUNDLE.value['concept']::varchar                AS bundle_concept,
           PRODUCT_BUNDLE.value['line']::varchar                   AS bundle_line,
           PRODUCT_BUNDLE.value['type']::varchar                   AS bundle_type,
           PRODUCT_BUNDLE.value['name']::varchar                   AS bundle_name,
           PRODUCT_BUNDLE.value['start_date']::varchar             AS bundle_start_date,
           PRODUCT_BUNDLE.value['end_date']::varchar               AS bundle_end_date,
           PRODUCT_BUNDLE.value['secret_escapes_sale_id']::varchar AS bundle_secret_escapes_sale_id,
           COALESCE(PRODUCT_BUNDLE.value['secret_escapes_sale_id'],
                    PRODUCT_BUNDLE.value['id']::varchar)           AS sale_id
    FROM SCRATCH.CARMENMARDIROSDEV36082.RAW_SNOWPLOW_MOCK_EVENTS
       , LATERAL FLATTEN(INPUT => PRODUCT_BUNDLE_CONTEXT, outer => true) PRODUCT_BUNDLE
    WHERE PRODUCT_BUNDLE.value['id']::varchar IS NOT NULL
);

CREATE OR REPLACE VIEW RP_PRODUCTS AS (
    SELECT DISTINCT USER_ID,
                    EVENT_NTH,
                    product.value['id']::varchar   AS product_id,
                    product.value['name']::varchar AS product_name,
                    product.value['type']::varchar AS product_type
    FROM SCRATCH.CARMENMARDIROSDEV36082.RAW_SNOWPLOW_MOCK_EVENTS,
         LATERAL FLATTEN(INPUT => PRODUCT_CONTEXT, outer => true) product
    WHERE product.value['id'] IS NOT NULL
);

CREATE OR REPLACE VIEW RP_PRODUCT_BUNDLE_LINKS AS (
    SELECT DISTINCT USER_ID,
                    EVENT_NTH,
                    product.value['id']::varchar                    AS product_id,
                    product.value['name']::varchar                  AS product_name,
                    product.value['type']::varchar                  AS product_type,
                    product_bundles.value['id']::varchar            AS bundle_id,
                    product_bundles.value['tech_provider']::varchar AS tech_provider
    FROM SCRATCH.CARMENMARDIROSDEV36082.RAW_SNOWPLOW_MOCK_EVENTS,
         LATERAL FLATTEN(INPUT => PRODUCT_CONTEXT, outer => true) product,
         LATERAL FLATTEN(INPUT => product.VALUE['product_bundle_links'], outer => true) product_bundles
    WHERE product.value['id'] IS NOT NULL
);

SELECT *
FROM RP_PRODUCTS;

SELECT *
FROM RP_PRODUCT_BUNDLE_LINKS;

SELECT *
FROM RP_PRODUCTS p
         LEFT JOIN RP_PRODUCT_BUNDLE_LINKS pbl
                   ON p.USER_ID = pbl.USER_ID AND p.EVENT_NTH = pbl.EVENT_NTH AND p.product_id = pbl.product_id;


CREATE OR REPLACE VIEW RP_BOOKING_PRODUCTS AS (
    SELECT USER_ID,
           EVENT_NTH,
--            PAGE_CONTEXT[0]['pageType']::varchar                      AS page_type,
           booking_snapshot_context[0]['booking']['id']::varchar     AS booking_id,
           booking_snapshot_context[0]['booking']['status']::varchar AS booking_status,
           booking_shapshots.VALUE['id']::varchar                    AS product_id,
           booking_shapshots.VALUE['name']::varchar                  AS product_name,
           product_bundles.value['id']::varchar                      as bundle_id,
           product_bundles.value['tech_provider']::varchar           as bundle_tech_provider
    FROM RAW_SNOWPLOW_MOCK_EVENTS,
         LATERAL FLATTEN(INPUT => booking_snapshot_context[0]['products'], outer => true) booking_shapshots,
         LATERAL FLATTEN(INPUT => booking_shapshots.value['product_bundle_links'], outer => true) product_bundles
    WHERE booking_shapshots.VALUE['id'] IS NOT NULL
);
-- aggregated list of customer view
SELECT e.USER_ID,
       e.EVENT_NTH,
       e.EVENT_NAME,
       e.page_context[0]['pageType']::varchar                      as page_type,
       LISTAGG(DISTINCT b.sale_id, ', ')                           AS bundle_ids,
       COUNT(DISTINCT b.sale_id)                                   AS no_of_bundles,
       LISTAGG(p.product_name, ',')                                AS products,
       COUNT(distinct p.product_id)                                AS no_of_products,
       e.booking_snapshot_context[0]['booking']['id']::varchar     AS booking_id,
       e.booking_snapshot_context[0]['booking']['status']::varchar AS booking_status,
       e.COMMENT

FROM SCRATCH.CARMENMARDIROSDEV36082.RAW_SNOWPLOW_MOCK_EVENTS e
         LEFT JOIN RP_BUNDLES b ON e.USER_ID = b.USER_ID AND e.EVENT_NTH = b.EVENT_NTH
         LEFT JOIN RP_PRODUCTS p ON e.USER_ID = p.USER_ID AND e.EVENT_NTH = p.EVENT_NTH
GROUP BY 1, 2, 3, 4, 9, 10, 11
ORDER BY e.USER_ID, e.EVENT_NTH;



SELECT e.USER_ID,
       e.EVENT_NTH,
       e.EVENT_NAME,
       e.page_context[0]['pageType']::varchar as page_type,
       b.bundle_id,
       b.bundle_name,
       p.product_id,
       p.product_name
FROM RAW_SNOWPLOW_MOCK_EVENTS e
         LEFT JOIN RP_BUNDLES b ON e.EVENT_NTH = b.EVENT_NTH AND e.USER_ID = b.USER_ID
         LEFT JOIN RP_PRODUCTS p ON e.USER_ID = p.USER_ID AND e.EVENT_NTH = p.EVENT_NTH
WHERE e.EVENT_NTH = 3
;

--SPVs by sale
SELECT b.sale_id,
       b.bundle_tech_provider,
       COUNT(1) AS spvs
FROM RP_BUNDLES b
         LEFT JOIN RAW_SNOWPLOW_MOCK_EVENTS e
                   ON e.EVENT_NAME = 'page_view'
                       AND e.USER_ID = b.USER_ID
                       AND e.EVENT_NTH = b.EVENT_NTH
group by 1, 2;


--SPVs by product
SELECT product_id,
       product_name,
       COUNT(1) AS spvs
FROM RP_PRODUCTS p
         LEFT JOIN RAW_SNOWPLOW_MOCK_EVENTS e
                   ON e.EVENT_NAME = 'page_view'
                       AND e.USER_ID = p.USER_ID
                       AND e.EVENT_NTH = p.EVENT_NTH
GROUP BY 1, 2;

--what products have currently been booked
CREATE OR REPLACE VIEW RP_CURRENT_BOOKING_PRODUCTS AS (
    WITH current_booking_snapshot AS (
        SELECT booking_id,
               MAX(EVENT_NTH) most_recent_snapshot
        FROM RP_BOOKING_PRODUCTS
        group by 1)

    SELECT bp.*
    FROM RP_BOOKING_PRODUCTS bp
             INNER JOIN current_booking_snapshot c
                        ON bp.booking_id = c.booking_id AND bp.EVENT_NTH = c.most_recent_snapshot
);

SELECT USER_ID,
       EVENT_NTH,
       EVENT_NAME,
       BOOKING_UPDATE_EVENT,
       BOOKING_UPDATE_EVENT['action']::varchar        as action,
       BOOKING_UPDATE_EVENT['booking']['id']::varchar as booking_id,
       BOOKING_UPDATE_EVENT['booking']['total_price']::varchar as total_price,
       BOOKING_UPDATE_EVENT['booking']['amount']::varchar as booking_amount,
       BOOKING_UPDATE_EVENT['amount']::varchar        as booking_amount,
       product_bundle_links.value['id']::varchar as product_bundles,
       products.VALUE['name']::varchar as product_name
FROM SCRATCH.CARMENMARDIROSDEV36082.RAW_SNOWPLOW_MOCK_EVENTS,
LATERAL FLATTEN(INPUT => BOOKING_UPDATE_EVENT['products'], outer => true) products,
LATERAL FLATTEN(INPUT => products.value['product_bundles'], outer => true) product_bundle_links
-- WHERE --product_bundle_links.value['business_provider']::varchar='Secret Escapes'

