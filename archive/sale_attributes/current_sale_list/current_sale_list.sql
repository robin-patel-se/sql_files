SELECT s.id,
       group_concat(DISTINCT st.title SEPARATOR ' | ')                              AS title,
       group_concat(DISTINCT st.destination_name SEPARATOR ' | ')                   AS destination_name,
       group_concat(DISTINCT cou.name SEPARATOR ' | ')                              AS country,
       group_concat(DISTINCT cod.name SEPARATOR ' | ')                              AS division,
       group_concat(DISTINCT cit.name SEPARATOR ' | ')                              AS city,
       s.start,
       s.end,
       s.type,
       CASE WHEN s.repeated = 0 THEN 'New' ELSE 'Repeat' END                        AS 'repeat',
       s.destination_type,
       CASE WHEN s.closest_airport_code IS NOT NULL THEN s.closest_airport_code END AS closest_airport,
       CASE WHEN cn.name IS NOT NULL THEN cn.name END                               AS company,
       group_concat(DISTINCT a.domain SEPARATOR ', ') EXCLUSIVE,
    group_concat(DISTINCT con.name SEPARATOR ' | ') contractor,
    group_concat(DISTINCT jointcontractor.name SEPARATOR ' | ') joint_contractor,
    con.region contractor_region,
    group_concat(DISTINCT hpft.name SEPARATOR ' | ') dp_territories,
    group_concat(DISTINCT t.name SEPARATOR ' | ') territory_name,
    CASE WHEN cn.id IS NOT NULL THEN cn.id END AS company_id,
    sup.id supplier_id,
    group_concat(DISTINCT tag.name SEPARATOR ' , ') tags,
    CASE WHEN S.instant AND !S.smart_stay THEN 'impulse' WHEN S.smart_stay THEN 'smart stay' ELSE 'flash' END AS provider_name,
    S.salesforce_opportunity_id AS 'sf_id',
    S.zero_deposit AS 'zero_deposit',
    S.active AS 'active',
    S.is_overnight_flight AS 'overnight_flight',
    '' is_multi_destination
FROM sale AS S
    LEFT JOIN sale_translation st
ON st.sale_id = S.id
    LEFT JOIN sale_company sc ON sc.sale_id = S.id
    LEFT JOIN company cn ON cn.id = sc.company_id
    LEFT JOIN sale_affiliate sa ON sa.sale_affiliates_id = S.id
    LEFT JOIN location_info li ON S.location_info_id = li.id
    LEFT JOIN country cou ON li.country_id = cou.id
    LEFT JOIN country_division cod ON li.division_id = cod.id
    LEFT JOIN city cit ON li.city_id = cit.id
    LEFT JOIN affiliate a ON sa.affiliate_id = a.id
    LEFT JOIN contractor con ON S.contractor_id = con.id
    LEFT JOIN contractor jointcontractor ON (S.joint_contractor_id) = jointcontractor.id
    LEFT JOIN sale_flight_config sfc ON sfc.sale_id = S.id AND sfc.is_able_to_sell_flights= TRUE
    LEFT JOIN sale_territory stn ON stn.sale_id = S.id
    LEFT JOIN territory t ON t.id = stn.territory_id
    LEFT JOIN territory hpft ON hpft.id = sfc.territory_id
    LEFT JOIN supplier sup ON (sup.id = S.supplier_id)
    LEFT JOIN tag_links tl ON tl.tag_ref = S.id AND tl.type = 'sale'
    LEFT JOIN tags tag ON tag.id = tl.tag_id
GROUP BY S.id
UNION ALL
SELECT concat('A', bs.id),
       group_concat(DISTINCT bst.title SEPARATOR ' | '),
       group_concat(DISTINCT bst.destination_name SEPARATOR ' | '),
       group_concat(DISTINCT (CASE WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icou.name ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcou.name ELSE wcou.name END END) SEPARATOR ' | '),
       group_concat(DISTINCT (CASE WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icod.name ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcod.name ELSE wcod.name END END) SEPARATOR ' | '),
       group_concat(DISTINCT (CASE WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icit.name ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcit.name ELSE wcit.name END END) SEPARATOR ' | '),
       bs.start,
       bs.end,
       CASE
           WHEN bs.class IN ('com.flashsales.sale.IhpSale', 'com.flashsales.sale.ConnectedWebRedirectSale') THEN 'PACKAGE'
           WHEN bs.class IN ('com.flashsales.sale.WebRedirectSale') THEN 'WRD'
           ELSE 'HOTEL' END    AS end,
       'New',
       bs.destination_type,
       '',
       CASE
           WHEN hcn.name IS NOT NULL THEN hcn.name
           WHEN icn.name IS NOT NULL THEN icn.name
           WHEN wcn.name IS NOT NULL THEN wcn.name END,
       group_concat(DISTINCT a.domain SEPARATOR ', '),
       group_concat(DISTINCT con.name SEPARATOR ' | '),
       group_concat(DISTINCT jointcontractor.name SEPARATOR ' | '),
       con.region,
       CASE WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN group_concat(DISTINCT t.name SEPARATOR ' | ') ELSE '' END,
       group_concat(DISTINCT t.name SEPARATOR ' | '),
       CASE
           WHEN hcn.id IS NOT NULL THEN hcn.id
           WHEN icn.id IS NOT NULL THEN icn.id
           WHEN wcn.id IS NOT NULL
               THEN wcn.id END AS company_id,
       CASE
           WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN sup.id
           ELSE CASE
                    WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN wrdsup.id
                    ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN bs.supplier_id ELSE '' END END END,
       group_concat(DISTINCT tag.name SEPARATOR ' , '),
       CASE WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN 'Travelbird' ELSE '' END,
       bs.salesforce_opportunity_id,
       NULL,
       bs.active,
       NULL,
       CASE
           WHEN (
                    SELECT DISTINCT COUNT(id)
                    FROM ihp_sale_company
                    WHERE ihp_sale_id = bs.id
                ) > 1 THEN 'true'
           ELSE 'false' END
FROM base_sale AS bs
         LEFT JOIN base_sale_translation bst ON bst.sale_id = bs.id
         LEFT JOIN hotel_sale_offer hso ON hso.hotel_sale_id = bs.id
         LEFT JOIN base_offer bo ON bo.id = hso.hotel_offer_id
         LEFT JOIN base_offer_product bop ON bop.base_offer_products_id = bo.id
         LEFT JOIN product p ON p.id = bop.product_id
         LEFT JOIN hotel h ON h.id = p.hotel_id
         LEFT JOIN ihp_sale_company isc ON isc.ihp_sale_id = bs.id
         LEFT JOIN company hcn ON hcn.id = h.company_id
         LEFT JOIN company icn ON icn.id = isc.company_id
         LEFT JOIN base_sale_affiliate bsa ON bsa.base_sale_affiliates_id = bs.id
         LEFT JOIN location_info hli ON h.location_info_id = hli.id
         LEFT JOIN country hcou ON hli.country_id = hcou.id
         LEFT JOIN country_division hcod ON hli.division_id = hcod.id
         LEFT JOIN city hcit ON hli.city_id = hcit.id

         LEFT JOIN location_info wli ON wrd.location_info_id = wli.id
         LEFT JOIN country wcou ON wli.country_id = wcou.id
         LEFT JOIN country_division wcod ON wli.division_id = wcod.id
         LEFT JOIN city wcit ON wli.city_id = wcit.id

         LEFT JOIN web_redirect wrd ON bs.web_redirect_id = wrd.id
         LEFT JOIN web_redirect_company wrdc ON wrdc.web_redirect_companies_id = wrd.id

         LEFT JOIN company wcn ON wcn.id = wrdc.company_id
         LEFT JOIN in_house_package ihp ON bs.ihp_id = ihp.id
         LEFT JOIN location_info ili ON ihp.location_info_id = ili.id
         LEFT JOIN country icou ON ili.country_id = icou.id
         LEFT JOIN country_division icod ON ili.division_id = icod.id
         LEFT JOIN city icit ON ili.city_id = icit.id
         LEFT JOIN affiliate a ON bsa.affiliate_id = a.id
         LEFT JOIN contractor con ON bs.contractor_id = con.id
         LEFT JOIN contractor jointcontractor ON bs.joint_contractor_id = jointcontractor.id
         LEFT JOIN territory t ON t.id = bs.territory_id
         LEFT JOIN tag_links tl ON tl.tag_ref = bs.id
         LEFT JOIN tags tag ON tag.id = tl.tag_id
         LEFT JOIN supplier hotelsup ON hotelsup.id = bs.supplier_id
         LEFT JOIN supplier sup ON sup.salesforce_account_id = '001w000001cSt6u'
         LEFT JOIN supplier wrdsup ON wrdsup.id = wrd.supplier_id
GROUP BY bs.id;

------------------------------------------------------------------------------------------------------------------------
SELECT s.id,
       LISTAGG(DISTINCT st.title, ' | ')                     AS title,
       LISTAGG(DISTINCT st.destination_name, ' | ')          AS destination_name,
       LISTAGG(DISTINCT cou.name, ' | ')                     AS country,
       LISTAGG(DISTINCT cod.name, ' | ')                     AS division,
       LISTAGG(DISTINCT cit.name, ' | ')                     AS city,
       s.start_date,
       s.end_date,
       s.type,
       CASE WHEN s.repeated = 0 THEN 'New' ELSE 'Repeat' END AS repeat,
       s.destination_type,
       s.closest_airport_code                                AS closest_airport,
       cn.name                                               AS company,
--        LISTAGG(DISTINCT a.domain, ', ')                                             AS "exclusive",
       LISTAGG(DISTINCT con.name, ' | ')                     AS contractor,
       LISTAGG(DISTINCT jointcontractor.name, ' | ')         AS joint_contractor,
       con.region                                            AS contractor_region,
       LISTAGG(DISTINCT hpft.name, ' | ')                    AS dp_territories,
       LISTAGG(DISTINCT t.name, ' | ')                       AS territory_name,
       cn.id                                                 AS company_id,
       sup.id                                                AS supplier_id,
       LISTAGG(DISTINCT tag.name, ' , ')                     AS tags,
       CASE
           WHEN s.instant = 1 AND s.smart_stay = 0 THEN 'impulse'
           WHEN s.smart_stay = 0 THEN 'smart stay'
           ELSE 'flash' END                                  AS provider_name,
       s.salesforce_opportunity_id                           AS sf_id,
       s.zero_deposit                                        AS zero_deposit,
       s.active,
       s.is_overnight_flight                                 AS overnight_flight,
       NULL                                                  AS is_multi_destination
FROM data_vault_mvp.cms_mysql_snapshots.sale_snapshot AS s
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_translation_snapshot st ON st.sale_id = s.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_company_snapshot sc ON sc.sale_id = s.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot cn ON cn.id = sc.company_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_affiliate_snapshot sa ON sa.sale_affiliates_id = s.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot li ON s.location_info_id = li.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot cou ON li.country_id = cou.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot cod ON li.division_id = cod.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot cit ON li.city_id = cit.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON sa.affiliate_id = a.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot con ON s.contractor_id = con.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot jointcontractor
                   ON (s.joint_contractor_id) = jointcontractor.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_flight_config_snapshot sfc
                   ON sfc.sale_id = s.id AND sfc.is_able_to_sell_flights = TRUE
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot stn ON stn.sale_id = s.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON t.id = stn.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot hpft ON hpft.id = sfc.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.supplier_snapshot sup ON (sup.id = s.supplier_id)
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot tl ON tl.tag_ref = s.id AND tl.type = 'sale'
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tags_snapshot tag ON tag.id = tl.tag_id
GROUP BY s.id, s.start_date, s.end_date, s.type, repeat, s.destination_type, s.closest_airport_code, cn.name, con.region, cn.id,
         sup.id, s.salesforce_opportunity_id, s.zero_deposit, s.active, s.is_overnight_flight, is_multi_destination, s.instant,
         s.smart_stay;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot sts;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
ORDER BY sale_id;

------------------------------------------------------------------------------------------------------------------------

UNION ALL

SELECT concat('A', bs.id),
       LISTAGG(DISTINCT bst.title, ' | ')                                                                  AS title,
       LISTAGG(DISTINCT bst.destination_name, ' | ')                                                       AS destination_name,
       LISTAGG(DISTINCT (CASE
                             WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icou.name
                             ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcou.name ELSE wcou.name END END),
               ' | ')                                                                                      AS country,
       LISTAGG(DISTINCT (CASE
                             WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icod.name
                             ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcod.name ELSE wcod.name END END),
               ' | ')                                                                                      AS division,
       LISTAGG(DISTINCT (CASE
                             WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN icit.name
                             ELSE CASE WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN hcit.name ELSE wcit.name END END),
               ' | ')                                                                                      AS city,
       bs.start_date,
       bs.end_date,
       CASE
           WHEN bs.class IN ('com.flashsales.sale.IhpSale', 'com.flashsales.sale.ConnectedWebRedirectSale') THEN 'PACKAGE'
           WHEN bs.class IN ('com.flashsales.sale.WebRedirectSale') THEN 'WRD'
           ELSE 'HOTEL' END                                                                                AS type,
       'New'                                                                                               AS repeat,
       bs.destination_type,
       ''                                                                                                  AS closest_airport_code,
       CASE
           WHEN hcn.name IS NOT NULL THEN hcn.name
           WHEN icn.name IS NOT NULL THEN icn.name
           WHEN wcn.name IS NOT NULL THEN wcn.name
           END                                                                                             AS company,
       LISTAGG(DISTINCT a.domain, ', ')                                                                    AS a "exclusive",
       LISTAGG(DISTINCT con.name, ' | ')                                                                   AS contractor,
       LISTAGG(DISTINCT jointcontractor.name, ' | ')                                                       AS joint_contractor,
       con.region                                                                                          AS contractor_region,
       CASE WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN LISTAGG(DISTINCT t.name, ' | ') ELSE '' END AS dp_territories,
       LISTAGG(DISTINCT t.name, ' | ')                                                                     AS territory_name,
       CASE
           WHEN hcn.id IS NOT NULL THEN hcn.id
           WHEN icn.id IS NOT NULL THEN icn.id
           WHEN wcn.id IS NOT NULL
               THEN wcn.id END                                                                             AS company_id,
       CASE
           WHEN bs.class = 'com.flashsales.sale.IhpSale' THEN sup.id
           WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN wrdsup.id
           WHEN bs.class = 'com.flashsales.sale.HotelSale' THEN bs.supplier_id
           ELSE ''
           END                                                                                             AS supplier_id,
       LISTAGG(DISTINCT tag.name, ' , ')                                                                   AS tags,
       CASE WHEN bs.class = 'com.flashsales.sale.ConnectedWebRedirectSale' THEN 'Travelbird' ELSE '' END   AS provider_name,
       bs.salesforce_opportunity_id,
       NULL                                                                                                AS zero_deposit,
       bs.active,
       NULL                                                                                                AS overnight_flight,
       CASE
           WHEN (
                    SELECT DISTINCT COUNT(id)
                    FROM data_vault_mvp.cms_mysql_snapshots.ihp_sale_company_snapshot
                    WHERE ihp_sale_id = bs.id
                ) > 1 THEN TRUE
           ELSE FALSE END                                                                                  AS is_multi_destination
FROM data_vault_mvp.cms_mysql_snapshots.base_sale_snapshot AS bs
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot bst ON bst.sale_id = bs.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso ON hso.hotel_sale_id = bs.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bo ON bo.id = hso.hotel_offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop ON bop.base_offer_products_id = bo.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON p.id = bop.product_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON h.id = p.hotel_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.ihp_sale_company_snapshot isc ON isc.ihp_sale_id = bs.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot hcn ON hcn.id = h.company_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot icn ON icn.id = isc.company_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_sale_affiliate_snapshot bsa ON bsa.base_sale_affiliates_id = bs.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot hli ON h.location_info_id = hli.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot hcou ON hli.country_id = hcou.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot hcod ON hli.division_id = hcod.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot hcit ON hli.city_id = hcit.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot wrd ON bs.web_redirect_id = wrd.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot wli ON wrd.location_info_id = wli.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot wcou ON wli.country_id = wcou.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot wcod ON wli.division_id = wcod.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot wcit ON wli.city_id = wcit.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot wrdc
                   ON wrdc.web_redirect_companies_id = wrd.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.company_snapshot wcn ON wcn.id = wrdc.company_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.in_house_package_snapshot ihp ON bs.ihp_id = ihp.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot ili ON ihp.location_info_id = ili.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot icou ON ili.country_id = icou.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot icod ON ili.division_id = icod.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot icit ON ili.city_id = icit.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON bsa.affiliate_id = a.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot con ON bs.contractor_id = con.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot jointcontractor
                   ON bs.joint_contractor_id = jointcontractor.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot t ON t.id = bs.territory_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tag_links_snapshot tl ON tl.tag_ref = bs.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.tags_snapshot tag ON tag.id = tl.tag_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.supplier_snapshot hotelsup ON hotelsup.id = bs.supplier_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.supplier_snapshot sup ON sup.salesforce_account_id = '001w000001cSt6u'
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.supplier_snapshot wrdsup ON wrdsup.id = wrd.supplier_id
GROUP BY bs.id;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.location_info_snapshot lis;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.sale_affiliate CLONE raw_vault_mvp.cms_mysql.sale_affiliate;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.base_sale_affiliate CLONE raw_vault_mvp.cms_mysql.base_sale_affiliate;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.web_redirect CLONE raw_vault_mvp.cms_mysql.web_redirect;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.cms_mysql.amendment CLONE raw_vault_mvp.cms_mysql.amendment;
SELECT *
FROM raw_vault_mvp.cms_mysql.base_sale_affiliate sa;

SELECT *
FROM raw_vault_mvp.cms_mysql.web_redirect sa;

DROP TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.amendment_snapshot;

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/cms_snapshots/cms_mysql_snapshot_bulk_wave2'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


SELECT ssc.se_sale_id,
       count(*)
FROM data_vault_mvp.dwh.se_sale_companies ssc
GROUP BY ssc.se_sale_id
HAVING count(*) > 1;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss;

SELECT wrd.id, --each base_sale can only have one web_redirect_id if is an wrd sale
       LISTAGG(DISTINCT cou.name, ' | ')             AS wrd_country,
       OBJECT_AGG(li.country_id, cou.name::VARIANT)  AS wrd_country_object,
       LISTAGG(DISTINCT cod.name, ' | ')             AS wrd_country_division,
       OBJECT_AGG(li.division_id, cod.name::VARIANT) AS wrd_country_division_object,
       LISTAGG(DISTINCT cit.name, ' | ')             AS wrd_city,
       OBJECT_AGG(li.city_id, cit.name::VARIANT)     AS wrd_city_object,
       to_json()
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot wrd
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot li ON wrd.location_info_id = li.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot cou ON li.country_id = cou.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot cod ON li.division_id = cod.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot cit ON li.city_id = cit.id
GROUP BY 1;

SELECT wrd.id,
       object_construct('country_id', cou.id, 'country_name', cou.name) AS country_object


-- count(*)
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot wrd
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot li ON wrd.location_info_id = li.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot cou ON li.country_id = cou.id
WHERE li.id = 2419136;



SELECT ihp.id, --each base_sale can only have one ihp id if is an ihp sale
       LISTAGG(DISTINCT cou.name, ' | ')             AS ihp_country,
       OBJECT_AGG('country', cou.name::VARIANT)      AS ihp_country_object,
       LISTAGG(DISTINCT cod.name, ' | ')             AS ihp_country_division,
       OBJECT_AGG(li.division_id, cod.name::VARIANT) AS ihp_country_division_object,
       LISTAGG(DISTINCT cit.name, ' | ')             AS ihp_city,
       OBJECT_AGG(li.city_id, cit.name::VARIANT)     AS ihp_city_object
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.in_house_package_snapshot ihp
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot li ON ihp.location_info_id = li.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot cou ON li.country_id = cou.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot cod ON li.division_id = cod.id
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot cit ON li.city_id = cit.id
GROUP BY 1;


SELECT isc.ihp_sale_id,
       LISTAGG(DISTINCT ic.name, ' | ') AS ihp_company,
       LISTAGG(DISTINCT ic.id, ' | ')   AS ihp_company_id,
       ARRAY_AGG(ic.name)               AS ihp_company_object
FROM data_vault_mvp_dev_robin.cms_mysql_snapshots.ihp_sale_company_snapshot isc
         LEFT JOIN data_vault_mvp_dev_robin.cms_mysql_snapshots.company_snapshot ic ON isc.company_id = ic.id
GROUP BY 1
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss;

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.cms_mysql_snapshots CLONE data_vault_mvp.cms_mysql_snapshots;
CREATE OR REPLACE SCHEMA hygiene_snapshot_vault_mvp_dev_robin.cms_mysql CLONE hygiene_snapshot_vault_mvp.cms_mysql;

SELECT *
FROM se.data.se_room_type_rooms_and_rates;

SELECT *
FROM se.data.se_hotel_room_availability;

SELECT ss.product_configuration, count(*)
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
GROUP BY 1;

SELECT ss.product_configuration, count(*)
FROM data_vault_mvp.dwh.se_sale ss
GROUP BY 1;

SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale ss WHERE ss.posa_territory IS NULL;

SELECT * FROM data_vault_mvp.cms_report_snapshots.sales s WHERE s.territory_name IS NULL;

SELECT * FROM data_vault_mvp.dwh.se_sale ss;

SELECT * FROM se.data.se_sale_attributes ssa;

self_describing_task --include 'se/data/worldpay_transaction_summary.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data/worldpay_transaction_summary.py

SELECT
       wts.filename,
       wts.file_row_number,
       wts.administration_code,
       wts.merchant_code,
       wts.order_code,
       wts.event_date,
       wts.payment_method,
       wts.status,
       wts.currency_code,
       wts.amount,
       wts.commission,
       wts.batch_id,
       wts.refusal_reason
FROM se_dev_robin.data.worldpay_transaction_summary wts;