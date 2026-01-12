SELECT DISTINCT ss.product_configuration
FROM data_vault_mvp.dwh.se_sale ss


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.contractor_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.contractor_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.days_before_policy CLONE hygiene_snapshot_vault_mvp.cms_mysql.days_before_policy;
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

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2022-02-27 00:00:00' --end '2022-02-27 00:00:00'

SELECT ssa.se_sale_id,
       ssa.date_created,
       ssa.current_contractor_id,
       ssa.current_contractor_name,
       ssa.product_configuration,
       ssa.company_name
FROM se.data.se_sale_attributes ssa
    EXCEPT
SELECT ss2.se_sale_id,
       ss2.date_created,
       ss2.current_contractor_id,
       ss2.current_contractor_name,
       ss2.product_configuration,
       ss2.company_name
FROM data_vault_mvp_dev_robin.dwh.se_sale ss2
ORDER BY company_name, date_created;


SELECT ssa.se_sale_id,
       ssa.date_created,
       ssa.current_contractor_id,
       ssa.current_contractor_name AS current_current_contractor_name,
       ss2.current_contractor_name AS new_current_current_contractor_name,
       ssa.product_configuration,
       ssa.company_name
FROM data_vault_mvp.dwh.se_sale ssa
    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_sale ss2 ON ssa.se_sale_id = ss2.se_sale_id
WHERE ssa.se_sale_id = '113036'
ORDER BY company_name, date_created;



WITH stack AS (

    SELECT ssa.se_sale_id,
           ssa.date_created,
           ssa.original_contractor_id,
           ssa.original_contractor_name,
           ssa.current_contractor_id,
           ssa.current_contractor_name,
           ssa.product_configuration,
           ssa.company_name,
           'prod' AS source
    FROM se.data.se_sale_attributes ssa
    UNION ALL
    SELECT ss2.se_sale_id,
           ss2.date_created,
           ss2.original_contractor_id,
           ss2.original_contractor_name,
           ss2.current_contractor_id,
           ss2.current_contractor_name,
           ss2.product_configuration,
           ss2.company_name,
           'dev' AS source
    FROM data_vault_mvp_dev_robin.dwh.se_sale ss2
)
SELECT *
FROM stack
ORDER BY date_created;


SELECT ssa.se_sale_id,
       ssa.date_created,
       ssa.original_contractor_id,
       ssa.original_contractor_name,
       ssa.current_contractor_id,
       ssa.current_contractor_name,
       ssa.product_configuration,
       ssa.company_name
FROM data_vault_mvp.dwh.se_sale ssa
    EXCEPT
SELECT ss2.se_sale_id,
       ss2.date_created,
       ss2.original_contractor_id,
       ss2.original_contractor_name,
       ss2.current_contractor_id,
       ss2.current_contractor_name,
       ss2.product_configuration,
       ss2.company_name
FROM data_vault_mvp_dev_robin.dwh.se_sale ss2


SELECT ss.se_sale_id,
       ss.date_created,
       ss.original_contractor_id,
       ss.original_contractor_name,
       ss.current_contractor_id,
       ss.current_contractor_name,
       ss.product_configuration,
       ss.company_name
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.se_sale_id = '113036';


SELECT ss.se_sale_id,
       ss.date_created,
       ss.original_contractor_id,
       ss.original_contractor_name,
       ss.current_contractor_id,
       ss.current_contractor_name,
       ss.product_configuration,
       ss.company_name
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.company_name = 'Grand Millennium Dubai | Medhufushi Island Resort';


SELECT ss.se_sale_id,
       ss.date_created,
       ss.original_contractor_id,
       ss.original_contractor_name,
       ss.current_contractor_id,
       ss.current_contractor_name,
       ss.product_configuration,
       ss.company_name
FROM data_vault_mvp.dwh.se_sale ss
WHERE ss.company_name = 'Grand Millennium Dubai | Medhufushi Island Resort'


SELECT ss2.se_sale_id,
       ss2.date_created,
       ss2.original_contractor_id,
       ss2.original_contractor_name,
       ss2.current_contractor_id,
       ss2.current_contractor_name,
       ss2.product_configuration,
       ss2.company_name,
       ss2.window_categories,
       ss2.supplier_id,
       ss2.supplier_name
FROM data_vault_mvp_dev_robin.dwh.se_sale__step14__model_contractor ss2
WHERE ss2.company_name = 'The Grange';



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.aviate.tig_transaction_report CLONE latest_vault.aviate.tig_transaction_report;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.finance.enett__van_settlement_report_netsuite AS
SELECT *
FROM data_vault_mvp.finance.enett__van_settlement_report_netsuite;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.flightservice__order_orderchange CLONE data_vault_mvp.dwh.flightservice__order_orderchange;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.hotel_plus_user_ab_test CLONE data_vault_mvp.bi.hotel_plus_user_ab_test;


self_describing_task --include 'dv/bi/ab_test/hotel_plus_bookings.py'  --method 'run' --start '2022-02-28 00:00:00' --end '2022-02-28 00:00:00'



WITH stack AS (

    SELECT ssa.se_sale_id,
           ssa.date_created,
           ssa.original_contractor_id,
           ssa.original_contractor_name,
           ssa.current_contractor_id,
           ssa.current_contractor_name,
           ssa.product_configuration,
           ssa.company_name,
           supplier_name,

           'prod' AS source
    FROM data_vault_mvp.dwh.se_sale ssa
    UNION ALL
    SELECT ss2.se_sale_id,
           ss2.date_created,
           ss2.original_contractor_id,
           ss2.original_contractor_name,
           ss2.current_contractor_id,
           ss2.current_contractor_name,
           ss2.product_configuration,
           ss2.company_name,
           supplier_name,
           'dev' AS source
    FROM data_vault_mvp_dev_robin.dwh.se_sale ss2
),
     exceptions AS (

         SELECT ssa.se_sale_id,
                ssa.date_created,
                ssa.original_contractor_id,
                ssa.original_contractor_name,
                ssa.current_contractor_id,
                ssa.current_contractor_name,
                ssa.product_configuration,
                ssa.company_name,
                supplier_name

         FROM data_vault_mvp.dwh.se_sale ssa
             EXCEPT
         SELECT ss2.se_sale_id,
                ss2.date_created,
                ss2.original_contractor_id,
                ss2.original_contractor_name,
                ss2.current_contractor_id,
                ss2.current_contractor_name,
                ss2.product_configuration,
                ss2.company_name,
                supplier_name

         FROM data_vault_mvp_dev_robin.dwh.se_sale ss2
     )
SELECT *
FROM stack s
    INNER JOIN exceptions e ON s.se_sale_id = e.se_sale_id
ORDER BY s.date_created;


SELECT ss2.se_sale_id,
       ss2.date_created,
       ss2.original_contractor_id,
       ss2.original_contractor_name,
       ss2.current_contractor_id,
       ss2.current_contractor_name,
       ss2.product_configuration,
       ss2.company_name,
       supplier_name,
       'dev' AS source
FROM data_vault_mvp_dev_robin.dwh.se_sale ss2
WHERE ss2.company_name = 'Grand Millennium Dubai | Medhufushi Island Resort';


------------------------------------------------------------------------------------------------------------------------


