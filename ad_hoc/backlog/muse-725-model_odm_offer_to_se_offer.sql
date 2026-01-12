SELECT *
FROM data_vault_mvp.dwh.se_offer so;
WITH offer_translation AS (
    --for offer translation
    SELECT offer_id,
           LISTAGG(DISTINCT name, ' | ') WITHIN GROUP (ORDER BY name) AS offer_name,
           OBJECT_AGG(locale, name::VARIANT)                          AS offer_name_object
    FROM hygiene_snapshot_vault_mvp.cms_mysql.offer_translation
    WHERE name IS NOT NULL
    GROUP BY 1
)
SELECT o.id                                                 AS se_offer_id,
       NULL                                                 AS base_offer_id,
       o.id                                                 AS offer_id,
       o.name                                               AS offer_name,
       ot.offer_name_object,
       o.active                                             AS cms_active_flag,
       NULL                                                 AS count_connected_sales,
       o.active                                             AS offer_active,
       NULL                                                 AS hotel_rate_plan_id,
       NULL                                                 AS product_id,
       ss.hotel_code,
       o.rate_code,
       NULL                                                 AS rack_rate_code,
       IFF(ss.sale_id IS NOT NULL, 1, 0)                    AS connected_sales,
       IFF(ss.sale_id IS NOT NULL AND ss.sale_active, 1, 0) AS connected_active_sales,
       'TBD'                                                AS global_sales,
       'TBD'                                                AS active_global_sales,

       o.commission,
       o.commission_type,

       o.sale_id                                            AS list_of_se_sale_ids,
       'Old Data Model'                                     AS data_model

FROM hygiene_snapshot_vault_mvp.cms_mysql.offer o
    LEFT JOIN offer_translation ot ON o.id = ot.offer_id
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ss ON o.sale_id::VARCHAR = ss.sale_id
;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.offer_translation ot;

SELECT ssa.hotel_code
FROM se.data.se_sale_attributes ssa
WHERE ssa.data_model = 'New Data Model'

SELECT a.*
FROM se.data.se_sale_attributes ssa
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON ssa.salesforce_opportunity_id = LEFT(o.id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
WHERE ssa.data_model = 'Old Data Model'



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.contractor_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.contractor_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.location_info_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_promotion CLONE data_vault_mvp.dwh.se_promotion;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.supplier_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.supplier_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.allocation CLONE hygiene_snapshot_vault_mvp.cms_mysql.allocation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.company CLONE hygiene_snapshot_vault_mvp.cms_mysql.company;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.days_before_policy CLONE hygiene_snapshot_vault_mvp.cms_mysql.days_before_policy;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.in_house_package CLONE hygiene_snapshot_vault_mvp.cms_mysql.in_house_package;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.ihp_sale_company CLONE hygiene_snapshot_vault_mvp.cms_mysql.ihp_sale_company;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product CLONE hygiene_snapshot_vault_mvp.cms_mysql.product;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale CLONE latest_vault.cms_mysql.sale;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_company CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_company;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list CLONE hygiene_snapshot_vault_mvp.se_api.sales_list;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.account CLONE hygiene_snapshot_vault_mvp.sfsc.account;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;



self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2022-03-15 00:00:00' --end '2022-03-15 00:00:00'


SELECT ssa.hotel_code
FROM data_vault_mvp_dev_robin.dwh.se_sale ssa
WHERE ssa.data_model = 'Old Data Model';

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_offer_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_offer_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel_sale_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer_translation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_mari_link CLONE data_vault_mvp.dwh.cms_mari_link;

self_describing_task --include 'dv/dwh/transactional/se_offer.py'  --method 'run' --start '2022-03-15 00:00:00' --end '2022-03-15 00:00:00'


SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_offer__step07__model_odm_offers');
CREATE OR REPLACE TRANSIENT TABLE se_offer__step07__model_odm_offers
(
    se_offer_id            NUMBER(38, 0),
    base_offer_id          VARCHAR(16777216),
    offer_id               NUMBER(38, 0),
    offer_name             VARCHAR(16777216),
    offer_name_object      OBJECT,
    cms_active_flag        BOOLEAN,
    count_connected_sales  VARCHAR(16777216),
    offer_active           BOOLEAN,
    hotel_rate_plan_id     VARCHAR(16777216),
    product_id             VARCHAR(16777216),
    hotel_code             VARCHAR(16777216),
    rate_code              VARCHAR(16777216),
    rack_rate_code         VARCHAR(16777216),
    connected_sales        NUMBER(1, 0),
    connected_active_sales NUMBER(1, 0),
    global_sales           NUMBER(1, 0),
    active_global_sales    NUMBER(1, 0),
    commission             NUMBER(19, 3),
    commission_type        VARCHAR(16777216),
    list_of_se_sale_ids    NUMBER(38, 0),
    data_model             VARCHAR(14)
);



SELECT ndm.se_offer_id,
--        ndm.base_offer_id,
--        NULL AS offer_id,
--        ndm.offer_name,
--        ndm.offer_name_object,
--        ndm.cms_active_flag,
--        ndm.count_connected_sales,
--        ndm.offer_active,
--        ndm.hotel_rate_plan_id,
--        ndm.product_id,
--        ndm.hotel_code,
--        ndm.rate_code,
--        ndm.rack_rate_code,
--        ndm.connected_sales,
--        ndm.connected_active_sales,
--        ndm.global_sales,
--        ndm.active_global_sales,
--        ndm.commission,
--        ndm.commission_type,
       ndm.list_of_se_sale_ids,
       ndm.data_model
FROM data_vault_mvp_dev_robin.dwh.se_offer__step05__model_ndm_offers ndm
UNION ALL

SELECT odm.se_offer_id,
--        NULL AS base_offer_id,
--        odm.offer_id,
--        odm.offer_name,
--        odm.offer_name_object,
--        odm.cms_active_flag,
--        NULL AS count_connected_sales,
--        odm.offer_active,
--        NULL AS hotel_rate_plan_id,
--        NULL AS product_id,
--        odm.hotel_code,
--        odm.rate_code,
--        NULL AS rack_rate_code,
--        odm.connected_sales,
--        odm.connected_active_sales,
--        odm.global_sales,
--        odm.active_global_sales,
--        odm.commission,
--        odm.commission_type,
       odm.list_of_se_sale_ids,
       odm.data_model
FROM data_vault_mvp_dev_robin.dwh.se_offer__step07__model_odm_offers odm;


SELECT COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.se_offer
WHERE data_model = 'Old Data Model'
  AND offer_active