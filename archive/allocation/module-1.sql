USE WAREHOUSE pipe_medium;

--module 1 -- original code
WITH hotel_rate_plan_snapshot
         AS (
        SELECT *
        FROM (
                 SELECT *
                 FROM raw_vault_mvp.cms_mysql.hotel_rate_plan
                     QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
             )
    ),
     hotel_sale_offer_snapshot
         AS (
         SELECT *
         FROM (
                  SELECT *
                  FROM raw_vault_mvp.cms_mysql.hotel_sale_offer
                      QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
              )
     ),
     product_provider_snapshot
         AS (
         SELECT *
         FROM (
                  SELECT *
                  FROM raw_vault_mvp.cms_mysql.product_provider
                      QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
              )
     )
SELECT base_sale.salesforce_opportunity_id                                    AS global_sale_id,
       base_sale.id                                                           AS territory_sale_id,
-- base_sale.version,
-- base_sale.active,
       base_sale.commission,
       base_sale.commission_type,
       base_sale.contractor_id,                                                                                 --- to join to get current contractor (to get contractor at time of booking Mike has told me you need to get from Mongo)
       base_sale.date_created,
       base_sale.destination_type,
       base_sale.start_date,
       base_sale.end_date,
       territory.name,
       territory.country_name,
       territory.currency,
-- hotel_sale_offer.HOTEL_SALE_ID,
-- hotel_sale_offer.hotel_offer_id,
-- hotel_sale_offer.id,
       base_offer.id                                                          AS offer_id,
-- base_offer.version
       base_offer.active                                                      AS offer_active,
-- base_offer.class AS OFFER_CLASS,
       base_offer_product.base_offer_products_id,
       base_offer_product.product_id,
       hotel.id                                                               AS hotel_id,
       hotel.version,
       hotel.base_currency,
       hotel.city_district_id,
-- hotel.commission,
-- hotel.commission_type,
       hotel.company_id,
-- hotel.contractor_id,
-- hotel.joint_contractor_id,
       hotel.hotel_code,
       hotel.latitude,
       hotel.longitude,
       hotel.location_info_id,
       product_provider.name,
       product.board_type,
-- product.HOTEL_ID AS PRODUCT_HOTEL_ID,
       hotel_rate_plan.id                                                     AS hotel_rate_plan_id,
       hotel_rate_plan.rate_code,
       hotel_rate_plan.rack_rate_code,
-- hotel_rate_plan.HOTEL_PRODUCT_ID,
       CONCAT(hotel_rate_plan.rate_code, ':', hotel_rate_plan.rack_rate_code) AS hotel_rate_plan_code_rack_code -- this is the way to join to MARI
FROM data_vault_mvp.cms_mysql_snapshots.product_snapshot product
         INNER JOIN hotel_rate_plan_snapshot hotel_rate_plan ON hotel_rate_plan.hotel_product_id = product.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot hotel ON product.hotel_id = hotel.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot base_offer_product
                   ON base_offer_product.product_id = product.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot base_offer
                   ON base_offer.id = base_offer_product.base_offer_products_id
         LEFT JOIN hotel_sale_offer_snapshot hotel_sale_offer ON base_offer.id = hotel_sale_offer.hotel_offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot base_sale
                   ON hotel_sale_offer.hotel_sale_id = base_sale.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot territory ON base_sale.territory_id = territory.id
         LEFT JOIN product_provider_snapshot product_provider ON product.product_provider_id = product_provider.id
WHERE base_sale.class = 'com.flashsales.sale.HotelSale'
-- AND BASE_SALE.SALESFORCE_OPPORTUNITY_ID = '0061r00001DXGhj';

------------------------------------------------------------------------------------------------------------------------
--module 1 -- refactored code
SELECT bs.salesforce_opportunity_id               AS global_sale_id,
       bs.sale_id                                 AS territory_sale_id,
-- bs.version,
-- bs.active,
       bs.commission,
       bs.commission_type,
       bs.contractor_id,                                                            --- to join to get current contractor (to get contractor at time of booking Mike has told me you need to get from Mongo)
       bs.date_created,
       bs.destination_type,
       bs.start_date,
       bs.end_date,
       t.name,
       t.country_name,
       t.currency,
-- hso.hotel_sale_id,
-- hso.hotel_offer_id,
-- hso.id,
       bo.id                                      AS offer_id,
-- base_offer.version
       bo.active                                  AS offer_active,
-- base_offer.class AS OFFER_CLASS,
--        bop.base_offer_products_id, --just offer id
       bop.product_id,
       h.id                                       AS hotel_id,
       h.version,
       h.base_currency,
       h.city_district_id,
-- h.commission,
-- h.commission_type,
       h.company_id,
-- h.contractor_id,
-- h.joint_contractor_id,
       h.hotel_code,
       h.latitude,
       h.longitude,
       h.location_info_id,
       pp.name,
       p.board_type,
-- p.hotel_id AS product_hotel_id,
       hrp.id                                     AS hotel_rate_plan_id,
       hrp.rate_code,
       hrp.rack_rate_code,
-- hrp.hotel_product_id,
       hrp.rate_code || ':' || hrp.rack_rate_code AS hotel_rate_plan_code_rack_code -- this is the way to join to MARI
FROM data_vault_mvp.cms_mysql_snapshots.product_snapshot p
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp ON hrp.hotel_product_id = p.id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON p.hotel_id = h.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                   ON bop.product_id = p.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bo
                   ON bo.id = bop.base_offer_products_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso ON bo.id = hso.hotel_offer_id
         LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs
                   ON hso.hotel_sale_id = bs.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON bs.territory_id = t.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot pp ON p.product_provider_id = pp.id
WHERE bs.class = 'com.flashsales.sale.HotelSale';


------------------------------------------------------------------------------------------------------------------------
CREATE SCHEMA collab.allocation_reporting_dev;

--Flash sales can only have one hotel per sale
--sale attributes module
CREATE OR REPLACE TABLE collab.allocation_reporting_dev.sale_attributes AS (
    WITH bs_translation AS (
        --aggregate sale names in different territorys up to sale
        SELECT 'A' || sale_id        AS territory_sale_id,
               LISTAGG(title, ' | ') AS sale_name
        FROM data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot
        GROUP BY 1
    )

    SELECT bs.salesforce_opportunity_id AS global_sale_id,
           bs.sale_id                   AS territory_sale_id,
           bst.sale_name,
           bs.commission,
           bs.commission_type,
           bs.contractor_id, --- to join to get current contractor (to get contractor at time of booking Mike has told me you need to get from Mongo)
           bs.date_created,
           bs.destination_type,
           bs.start_date,
           bs.end_date,
           h.id                         AS hotel_id,
           h.base_currency,
           h.city_district_id,
           h.company_id,
           h.hotel_code,
           h.latitude,
           h.longitude,
           h.location_info_id,
           t.name                       AS posa_territory_name,
           t.country_name               AS posa_country,
           t.currency                   AS posa_currency,
           cd.name                      AS posu_division,
           ct.name                      AS posu_country,
           cy.name                      AS posu_city
    FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs
             --join on base sale default hotel offer id direct to base offer product to get only one offer per sale
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                       ON bs.default_hotel_offer_id = bop.base_offer_products_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot pr ON bop.product_id = pr.id
             LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON pr.hotel_id = h.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON bs.territory_id = t.id
             LEFT JOIN bs_translation bst ON bs.sale_id = bst.territory_sale_id
        -- to get posu data
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot li ON h.location_info_id = li.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot cd ON li.division_id = cd.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot ct ON li.country_id = ct.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot cy ON li.city_id = cy.id
    WHERE bs.class = 'com.flashsales.sale.HotelSale'
)
;


SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot;

SELECT *
FROM raw_vault_mvp.cms_mysql.sale;

------------------------------------------------------------------------------------------------------------------------
--offer attributes module
CREATE OR REPLACE TABLE collab.allocation_reporting_dev.offer_attributes AS (
    WITH offer_translation_dedupe AS (
        SELECT id,
               offer_id,
               name
        FROM raw_vault_mvp.cms_mysql.base_offer_translation
            QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
    ),
         offer_tranlation AS (
             --aggregate sale names in different territorys up to sale
             SELECT offer_id,
                    LISTAGG(DISTINCT name, ' | ') AS offer_name
             FROM offer_translation_dedupe
             WHERE name IS NOT NULL
             GROUP BY 1
         )

    SELECT DISTINCT
           bo.id         AS se_offer_id,
           bo.id         AS base_offer_id,
           bot.offer_name,
           bo.active = 1 AS offer_active,
           bop.product_id,
           pp.name,
           p.board_type,
           p.internal_note

    FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bo
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                       ON bo.id = bop.base_offer_products_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON bop.product_id = p.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot pp ON p.product_provider_id = pp.id
             LEFT JOIN offer_tranlation bot ON bo.id = bot.offer_id
    WHERE bo.class = 'com.flashsales.offer.HotelOffer'
)
;

SELECT se_offer_id, count(*)
FROM collab.allocation_reporting_dev.offer_attributes
GROUP BY 1
HAVING count(*) > 1;


SELECT bo.id,
       COUNT(*)
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bo
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                   ON bo.id = bop.base_offer_products_id
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bo
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                   ON bo.id = bop.base_offer_products_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON bop.product_id = p.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot pp ON p.product_provider_id = pp.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso ON bo.id = hso.hotel_offer_id
WHERE bo.id = 194;

------------------------------------------------------------------------------------------------------------------------
--link table outside of mari data
CREATE OR REPLACE TABLE collab.allocation_reporting_dev.mari_rate_plan_link AS (
    SELECT hrp.id                                     AS hotel_rate_plan_id,
           hrp.hotel_product_id                       AS product_id,
           bop.base_offer_products_id                 AS offer_id,
           hrp.rate_code,
           hrp.rack_rate_code,
           hrp.rate_code || ':' || hrp.rack_rate_code AS hotel_rate_plan_code_rack_code -- this is the way to join to MARI data
    FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
             --note a rate plan can have multiple offers, this doesn't occur often, but it can.
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                        ON hrp.hotel_product_id = bop.product_id
)
;

--need hotel code in join as well otherwise creates dupes
SELECT hrp.id                     AS hotel_rate_plan_id,
       hrp.hotel_product_id       AS product_id,
       bop.base_offer_products_id AS offer_id,
       h.hotel_code,
       hrp.rate_code,
       hrp.rack_rate_code,
       h.hotel_code || ':' ||
       hrp.rate_code || ':' ||
       hrp.rack_rate_code         AS hotel_rate_rack_code -- this is the way to join to MARI data
FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
         --note a rate plan can have multiple offers, this doesn't occur often, but it can.
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                    ON hrp.hotel_product_id = bop.product_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON hrp.hotel_product_id = p.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
;
self_describing_task --include 'se/data/se_cms_mari_link'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT offer_id,
       count(*)
FROM collab.allocation_reporting_dev.mari_rate_plan_link
GROUP BY 1
HAVING count(*) > 1;

SELECT hotel_product_id,
       count(*)
FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot
GROUP BY 1
HAVING count(*) > 1;

SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       updated_at,
       se_sale_id,
       base_sale_id,
       sale_id,
       salesforce_opportunity_id,
       sale_name,
       class,
       has_flights_available,
       default_preferred_airport_code,
       type,
       hotel_chain_link,
       closest_airport_code,
       is_team20package,
       sale_able_to_sell_flights,
       sale_product,
       sale_type,
       product_type,
       product_configuration,
       product_line,
       data_model,
       hotel_location_info_id,
       default_hotel_offer_id,
       commission,
       commission_type,
       contractor_id,
       date_created,
       destination_type,
       start_date,
       end_date,
       hotel_id,
       base_currency,
       city_district_id,
       company_id,
       hotel_code,
       latitude,
       longitude,
       location_info_id,
       posa_territory,
       posa_country,
       posa_currency,
       posu_division,
       posu_country,
       posu_city
FROM data_vault_mvp.dwh.se_sale;

SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       updated_at,
       se_offer_id,
       base_offer_id,
       offer_name,
       offer_name_object,
       offer_active,
       product_id,
       provider_name,
       board_type,
       internal_note
FROM data_vault_mvp.dwh.se_offer;

------------------------------------------------------------------------------------------------------------------------
--View to combine modules

SELECT l.hotel_rate_plan_id,
       l.product_id,
       l.offer_id,
       o.se_offer_id,
       o.offer_active,
       o.product_id,
       o.name,
       o.board_type,
       o.internal_note,
       s.territory_sale_id,
       s.global_sale_id,
       s.territory_sale_id,
       s.commission,
       s.commission_type,
       s.contractor_id,
       s.date_created,
       s.destination_type,
       s.start_date,
       s.end_date,
       s.hotel_id,
       s.base_currency,
       s.city_district_id,
       s.company_id,
       s.hotel_code,
       s.latitude,
       s.longitude,
       s.location_info_id,
       s.posa_territory_name,
       s.country_name,
       s.currency,
       l.rate_code,
       l.rack_rate_code,
       l.hotel_rate_plan_code_rack_code
FROM collab.allocation_reporting_dev.mari_rate_plan_link l
         LEFT JOIN collab.allocation_reporting_dev.offer_attributes o ON l.offer_id = o.se_offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso ON o.se_offer_id = hso.hotel_offer_id
         LEFT JOIN collab.allocation_reporting_dev.sale_attributes s ON 'A' || hso.hotel_sale_id = s.territory_sale_id
;

GRANT USAGE ON SCHEMA collab.allocation_reporting_dev TO ROLE personal_role__kirstengrieve;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA collab.allocation_reporting_dev TO ROLE personal_role__kirstengrieve;

GRANT USAGE ON SCHEMA collab.allocation_reporting_dev TO ROLE personal_role__gianniraftis;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA collab.allocation_reporting_dev TO ROLE personal_role__gianniraftis;

------------------------------------------------------------------------------------------------------------------------
--update sale attributes

DROP TABLE data_vault_mvp_dev_robin.dwh.se_sale;

self_describing_task --include 'dv/dwh/transactional/se_sale'  --method 'run' --start '2020-02-28 03:00:00' --end '2020-02-28 03:00:00'

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.location_info_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_translation_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.offer_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.allocation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.allocation_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot;



SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale; --2020-03-12 13:44:04.709000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel; --2020-05-19 01:03:39.892000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale; --2020-02-28 17:44:42.470000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config;
--2020-05-19 01:04:55.054000000

--fixed
SELECT product_configuration,
       product_line,
       product_type,
       count(*)
FROM data_vault_mvp_dev_robin.dwh.se_sale
GROUP BY 1, 2, 3;

--old
SELECT product_configuration,
       product_line,
       product_type,
       count(*)
FROM data_vault_mvp.dwh.se_sale
GROUP BY 1, 2, 3;


SELECT *
FROM data_vault_mvp.dwh.se_sale
WHERE data_model = 'New Data Model'
  AND class = 'com.flashsales.sale.HotelSale';

CREATE OR REPLACE TABLE data_vault_mvp.dwh.se_sale CLONE data_vault_mvp_dev_robin.dwh.se_sale;

------------------------------------------------------------------------------------------------------------------------
--update offer attributes

--replicate base offer translation snapshot
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_translation_snapshot AS (
    SELECT *
    FROM raw_vault_mvp.cms_mysql.base_offer_translation
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY loaded_at DESC) = 1
);

self_describing_task --include 'dv/dwh/transactional/se_offer'  --method 'run' --start '2020-02-28 03:00:00' --end '2020-02-28 03:00:00'

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_provider_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_provider_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.hotel_sale_offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;

USE WAREHOUSE pipe_xlarge;

DROP TABLE data_vault_mvp_dev_robin.dwh.se_offer;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_offer;

airflow backfill --start_date '2020-05-19 03:00:00' --end_date '2020-05-19 03:00:00' --task_regex '.*' dwh__transactional__sale__daily_at_03h00
USE WAREHOUSE pipe_xlarge;

SELECT *
FROM data_vault_mvp.dwh.se_offer;

SELECT id,
       hotel_sale_id,
       hotel_offer_id

FROM data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot;

SELECT *
FROM data_vault_mvp.dwh.se_offer so
WHERE left(so.se_offer_id, 1) = 'A';


