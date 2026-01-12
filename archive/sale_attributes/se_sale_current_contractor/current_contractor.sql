WITH sale AS (
    SELECT ss.se_sale_id,
           ss.product_configuration,
           IFF(ss.product_configuration LIKE 'IHP%', TRUE, FALSE)                                                    AS ihp_sale,
           CASE
               WHEN ss.product_configuration = '3PP' THEN '3PP'
               WHEN ss.product_configuration LIKE 'IHP%' THEN 'IHP'
               WHEN ss.product_configuration IN ('Hotel', 'Hotel Plus') THEN 'Hotel/Hotel Plus'
               WHEN ss.product_configuration = 'WRD' THEN 'WRD'
               ELSE ss.product_configuration
               END                                                                                                   AS window_categories,
           ss.date_created,
           ss.company_id,
           ss.company_name,
           ss.original_contractor_id,
           oc.name                                                                                                   AS original_contractor_name,
           ss.original_joint_contractor_id,
           ojc.name                                                                                                  AS original_joint_contractor_name,
           LAST_VALUE(ss.original_contractor_id)
                      IGNORE NULLS OVER (PARTITION BY ss.company_id, window_categories ORDER BY ss.date_created ASC) AS current_contractor_id,
           IFF(ihp_sale,
               COALESCE(
                       LAST_VALUE(IFF(ihp_sale, NULL, ss.original_contractor_id))
                                  IGNORE NULLS OVER (PARTITION BY ss.company_id ORDER BY ss.date_created),
                       LAST_VALUE(ss.original_joint_contractor_id)
                                  IGNORE NULLS OVER (PARTITION BY ss.company_id ORDER BY ss.date_created)
                   ),
               NULL)                                                                                                 AS current_joint_contractor_id
    FROM data_vault_mvp_dev_robin.dwh.se_sale ss
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot oc ON ss.original_contractor_id = oc.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot ojc ON ss.original_joint_contractor_id = ojc.id
    WHERE ss.company_name = 'A Roma Lifestyle Hotel'
    ORDER BY company_id, date_created
)
SELECT s.se_sale_id,
       s.product_configuration,
       s.ihp_sale,
       s.date_created,
       s.company_id,
       s.company_name,
       s.original_contractor_id,
       s.original_contractor_name,
       s.original_joint_contractor_id,
       s.original_joint_contractor_name,
       s.current_contractor_id,
       cc.name  AS current_contractor_name,
       s.current_joint_contractor_id,
       cjc.name AS current_joint_contractor_name
FROM sale s
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot cc ON s.current_contractor_id = cc.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot cjc ON s.current_joint_contractor_id = cjc.id
ORDER BY company_id, date_created
;

--Castel Monastero, Leonardo Hotel, Banyan Tree Ungasan, La Bobadilla a Royal Hideaway Hotel, Shanti Maurice Resort & Spa, 100 Rizes Luxury Seaside Resort


CREATE SCHEMA collab.sale_contractor;
GRANT USAGE ON SCHEMA collab.sale_contractor TO ROLE personal_role__niroshanbalakumar;
GRANT USAGE ON SCHEMA collab.sale_contractor TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.sale_contractor TO ROLE personal_role__janhitzke;

GRANT SELECT ON TABLE collab.sale_contractor.se_sale TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON TABLE collab.sale_contractor.se_sale TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.sale_contractor.se_sale TO ROLE personal_role__janhitzke;


CREATE OR REPLACE TABLE collab.sale_contractor.se_sale COPY GRANTS CLONE data_vault_mvp_dev_robin.dwh.se_sale;

SELECT ss.se_sale_id,
       ss.date_created,
       ss.company_name,
       ss.product_configuration,
       ss.original_contractor_id,
       ss.current_contractor_id,
       ss.original_contractor_name,
       ss.current_contractor_name,

       ss.original_joint_contractor_id,
       ss.current_joint_contractor_id,
       ss.original_joint_contractor_name,
       ss.current_joint_contractor_name


FROM collab.sale_contractor.se_sale ss
-- WHERE ss.company_name = 'Fairplay Golf and Spa Resort'
ORDER BY ss.company_id, date_created;


SELECT ss.se_sale_id,
       ss.date_created,
       ss.company_name,
       ss.product_configuration,
       ss.original_contractor_id,
       ss.current_contractor_id,
       ss.original_contractor_name,
       ss.current_contractor_name,
       ss.original_joint_contractor_id,
       ss.current_joint_contractor_id,
       ss.original_joint_contractor_name,
       ss.current_joint_contractor_name
FROM collab.sale_contractor.se_sale ss
-- WHERE ss.company_name = 'A Roma Lifestyle Hotel'
ORDER BY ss.company_id, date_created ASC;

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.company_snapshot cs
WHERE name IN (
    SELECT name
    FROM data_vault_mvp.cms_mysql_snapshots.company_snapshot cs
    GROUP BY 1
    HAVING count(*) > 1
)
ORDER BY name, loaded_at;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.company_snapshot cs;


SELECT se_sale_id,
       base_sale_id,
       sale_id,
       salesforce_opportunity_id,
       sale_name,
       sale_name_object,
       sale_active,
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
       active,
       default_hotel_offer_id,
       commission,
       commission_type,
       original_contractor_id,
       original_joint_contractor_id,
       date_created,
       destination_type,
       start_date,
       end_date,
       hotel_id,
       base_currency,
       city_district_id,
       company_id,
       company_name,
       hotel_code,
       latitude,
       longitude,
       location_info_id,
       posa_territory,
       posa_country,
       posa_currency,
       posu_division,
       posu_country,
       posu_city,
       supplier_id,
       supplier_name
FROM data_vault_mvp_dev_robin.dwh.se_sale;

DROP SCHEMA collab.sale_contractor;