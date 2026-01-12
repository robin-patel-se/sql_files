SELECT *
FROM data_vault_mvp.dwh.se_sale sa
WHERE sa.se_sale_id IN (
    SELECT se_sale_id
    FROM data_vault_mvp.dwh.se_sale
    GROUP BY 1
    HAVING COUNT(*) > 1
)

SELECT bs.sale_id
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs
GROUP BY 1
HAVING count(*) > 1;

--new model sale dimension categorisation given by tech
--https://docs.google.com/drawings/d/1mPCteIc88oX59ppKxH4cxG8OlNJ8omnh2GcYtZgj5Ns/edit -- sale_product
--https://docs.google.com/drawings/d/1Mxpw_tjFVvPojRqo4oERqW9y63flsrUvwLKCLFeNm48/edit -- sale_type

SELECT bs.sale_id                                   AS se_sale_id,
       bs.id                                        AS base_sale_id,
       bs.salesforce_opportunity_id,
--        bst.sale_name,
--        bst.sale_name_object,
       bs.active = 1
           --casting start and end date to include the entire day
           --this is because sales are set to start at 1min past midnight
           --and end 1 min before midnight
           AND bs.start_date::DATE <= current_date
           AND bs.end_date::DATE + 1 > current_date AS sale_active,

       bs.class,
       bs.has_flights_available,
--        h.default_preferred_airport_code,
       CASE
           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN 'Hotel'
           WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN 'WRD'
           WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN 'Package'
           WHEN bs.class = 'com.flashsales.sale.WebRedirectSale' THEN 'WRD - direct'
           ELSE 'N/A'
           END                                      AS sale_product, --known as `product` in cube, and `type` in cms

       CASE
           WHEN bs.class = 'com.flashsales.sale.HotelSale'
               THEN
               CASE
                   WHEN bs.has_flights_available = TRUE AND ''
--                         h.default_preferred_airport_code IS NOT NULL
                       THEN 'Hotel Plus'
                   ELSE 'Hotel'
                   END
           ELSE
               CASE
                   WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale'
                       THEN 'Catalogue'
                   WHEN bs.class = 'com.flashsales.sale.IhpSale'
                       THEN 'IHP - C'
                   WHEN bs.class = 'com.flashsales.sale.WebRedirectSale'
                       THEN 'WRD - direct'
                   ELSE 'N/A'
                   END
           END                                      AS sale_type,    --known as `sale_type` in clude and `sale_dimension` in cms

       --new naming convention was created to handle known business reporting issues identified by key stakeholders.
       --resulting document formulated and agreed on to handle the issues:
       --https://docs.google.com/presentation/d/1tP1urQuQAzJ1UBYfx06SuaSIvmfR8AlN-kNMJSSgtlk/edit#slide=id.g70c7fa579c_0_8
       sale_product                                 AS product_type,

       CASE
           WHEN sale_type = 'IHP - C' THEN 'IHP - connected'
           ELSE sale_type
           END                                      AS product_configuration,

       --as of 24th Feb 2019 se deals are all flash, TB catalogue deals will be enriched by another process and
       --so they are removed via the where clause
       'Flash'                                      AS product_line,
       'New Data Model'                             AS data_model,

       --location info
--        h.location_info_id                                                   AS hotel_location_info_id,


       --attribute fields
       bs.active,
       bs.default_hotel_offer_id,
       bs.commission,
       bs.commission_type,
       bs.contractor_id,                                             --- to join to get current contractor (to get contractor at time of booking Mike has told me you need to get from Mongo)
       bs.date_created,
       bs.destination_type,
       bs.start_date,
       bs.end_date,
--        h.id                                                                 AS hotel_id,
--        h.base_currency,
--        h.city_district_id,
--        COALESCE(hc.id::VARCHAR, ic.ihp_company_id::VARCHAR, wc.id::VARCHAR) AS company_id,
--        COALESCE(hc.name, ic.ihp_company, wc.name)                           AS company_name,
--        ic.company_array,
--        h.hotel_code,
--        h.latitude,
--        h.longitude,
--        h.location_info_id,

--        t.name                                                               AS posa_territory,
--        t.country_name                                                       AS posa_country,
--        t.currency                                                           AS posa_currency,
       ''
--        COALESCE(hcod.name, icod.name, wcod.name)                            AS posu_division,
--        COALESCE(hcou.name, icou.name, wcou.name)                            AS posu_country,
--        COALESCE(hcit.name, icit.name, wcit.name)                            AS posu_city

FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
--                    ON bs.default_hotel_offer_id = bop.base_offer_products_id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot pr ON bop.product_id = pr.id
--          LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON pr.hotel_id = h.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON bs.territory_id = t.id
--          LEFT JOIN data_vault_mvp.dwh.se_sale__base_sale_translation bst ON bs.sale_id = bst.sale_id

     -- hotel only posu data
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot hli ON h.location_info_id = hli.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot hcod ON hli.division_id = hcod.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot hcou ON hli.country_id = hcou.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot hcit ON hli.city_id = hcit.id

     -- hotel only company
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot hc ON h.company_id = hc.id

     -- ihp posu data
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.in_house_package_snapshot ihp ON bs.ihp_id = ihp.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot ili ON ihp.location_info_id = ili.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot icou ON ili.country_id = icou.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot icod ON ili.division_id = icod.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot icit ON ili.city_id = icit.id

     -- ihp company
--          LEFT JOIN data_vault_mvp.dwh.se_sale__ihp_company ic ON bs.id = ic.ihp_sale_id

     -- wrd posu data
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot wrd ON bs.web_redirect_id = wrd.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot wli ON wrd.location_info_id = wli.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot wcou ON wli.country_id = wcou.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot wcod ON wli.division_id = wcod.id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot wcit ON wli.city_id = wcit.id

     --wrd company
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot wrdc ON bs.id = wrdc.web_redirect_companies_id
--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot wc ON wrdc.company_id = wc.id


     --actively removed catalogue deals because we will get these directly from travelbird data.
     --at this point in time this is the best method of excluding these, but there might be an instance
     --in the future where we may have web redirect sales that are not exclusively TB and this will need to be updated
WHERE bs.class != 'com.flashsales.sale.ConnectedWebRedirectSale' -- remove WRD catalogue sales
  AND bs.sale_id IN (
    SELECT se_sale_id
    FROM data_vault_mvp.dwh.se_sale
    GROUP BY 1
    HAVING COUNT(*) > 1
)

--          LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
--                    ON bs.default_hotel_offer_id = bop.base_offer_products_id

SELECT bs.sale_id,
       bs.default_hotel_offer_id,
       bops.product_id,
       bops.*
FROM hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bops
                   ON bs.default_hotel_offer_id = bops.base_offer_products_id
WHERE bs.sale_id IN ( -- dupes
    SELECT se_sale_id
    FROM data_vault_mvp.dwh.se_sale
    GROUP BY 1
    HAVING COUNT(*) > 1
)
;



SELECT base_offer_product_snapshot.base_offer_products_id
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot
GROUP BY 1
HAVING count(*) > 1;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bops
WHERE bops.base_offer_products_id = 12899;

SELECT *
FROM raw_vault_mvp.cms_mysql.base_offer_product bop
WHERE bop.loaded_at = (
    SELECT MAX(b.loaded_at)
    FROM raw_vault_mvp.cms_mysql.base_offer_product b
)

SELECT bops.base_offer_products_id,
       bops.product_id,
       bops.extract_metadata
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bops;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.base_offer_product CLONE raw_vault_mvp.cms_mysql.base_offer_product;

self_describing_task --include 'dv/cms_snapshots/cms_mysql_base_offer_product.py'  --method 'run' --start '2020-07-15 03:00:00' --end '2020-07-15 03:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot bops
WHERE bops.base_offer_products_id = 12899;

SELECT *
FROM raw_vault_mvp.cms_mysql.base_offer_product bop
WHERE bop.loaded_at = (
    SELECT MAX(loaded_at)
    FROM raw_vault_mvp.cms_mysql.base_offer_product b
);