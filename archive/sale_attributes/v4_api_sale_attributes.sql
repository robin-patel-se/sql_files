self_describing_task --include 'hygiene/se_api/sales.py'  --method 'run' --start '2020-07-14 00:00:00' --end '2020-07-14 00:00:00'
self_describing_task --include 'hygiene_snapshots/se_api/sales_list.py'  --method 'run' --start '2020-07-14 00:00:00' --end '2020-07-14 00:00:00'
airflow backfill --start_date '2020-07-14 01:00:00' --end_date '2020-07-14 01:00:00' --task_regex '.*' hygiene_snapshots__se_api__sales_list__daily_at_01h00
airflow backfill --start_date '2020-10-14 01:00:00' --end_date '2020-10-14 01:00:00' --task_regex '.*' -m hygiene_snapshots__se_api__sales_list__daily_at_01h00

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.se_api.sales CLONE raw_vault_mvp.se_api.sales;
SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.se_api.sales s; --2020-07-14 14:23:17.797407000

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.se_api.sales s; --2020-07-14 14:23:17.797407000

DROP TABLE hygiene_vault_mvp_dev_robin.se_api.sales;

SELECT get_ddl('table', 'hygiene_vault_mvp.se_api.sales');
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.se_api.sales s;
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp.se_api.sales s;
SELECT COUNT(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list s;
SELECT COUNT(DISTINCT sale_id)
FROM raw_vault_mvp.se_api.sales;
SELECT COUNT(DISTINCT sale_id)
FROM hygiene_vault_mvp.se_api.sales;
SELECT COUNT(DISTINCT sale_id)
FROM hygiene_vault_mvp_dev_robin.se_api.sales;
SELECT COUNT(DISTINCT sale_id)
FROM hygiene_snapshot_vault_mvp.se_api.sales;
SELECT COUNT(DISTINCT sale_id)
FROM hygiene_vault_mvp_dev_robin.se_api.sales;

------------------------------------------------------------------------------------------------------------------------
--populate historic

CREATE OR REPLACE TABLE scratch.robinpatel.sales
(
    -- (lineage) metadata for the current job
    schedule_tstamp                                          TIMESTAMP,
    run_tstamp                                               TIMESTAMP,
    operation_id                                             VARCHAR,
    created_at                                               TIMESTAMP,
    updated_at                                               TIMESTAMP,

    -- (lineage) original metadata columns from previous step
    row_dataset_name                                         VARCHAR,
    row_dataset_source                                       VARCHAR,
    row_loaded_at                                            TIMESTAMP,
    row_schedule_tstamp                                      TIMESTAMP,
    row_run_tstamp                                           TIMESTAMP,
    row_filename                                             VARCHAR,
    row_file_row_number                                      INT,

    -- hygiened columns
    url                                                      VARCHAR,

    -- original columns (extracted from JSON)
    latitude                                                 DOUBLE,
    longitude                                                DOUBLE,
    sale_link                                                VARCHAR,
    discount                                                 NUMBER,
    show_discount                                            BOOLEAN,
    deposit_from_price_for_display                           VARCHAR,
    deposit_from_price_unit                                  DECIMAL,
    deposit_from_price_unit_per_person                       DECIMAL,
    lead_rate_for_display                                    VARCHAR,
    lead_rate_unit                                           DECIMAL,
    lead_rate_unit_per_person                                DECIMAL,
    show_prices                                              BOOLEAN,
    show_rack_rate                                           BOOLEAN,
    pricing_model_for_display                                VARCHAR,
    max_number_of_adults                                     INT,
    number_of_hotel_nights                                   INT,
    has_flights_included                                     BOOLEAN,
    currency_code                                            VARCHAR,

    -- original columns that don't require any hygiene
    record__o                                                VARIANT,
    sale_id                                                  VARCHAR,
    affiliate_url_string                                     VARCHAR,

    -- hygiene flags
    failed_some_validation                                   INT,
    fails_validation__sale_id__expected_nonnull              INT,
    fails_validation__affiliate_url_string__expected_nonnull INT
);



INSERT INTO scratch.robinpatel.sales
SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       updated_at,
       row_dataset_name,
       row_dataset_source,
       row_loaded_at,
       row_schedule_tstamp,
       row_run_tstamp,
       row_filename,
       row_file_row_number,
       url,
       latitude,
       longitude,
       sale_link,
       discount,
       show_discount,
       deposit_from_price_for_display,
       deposit_from_price_unit,
       deposit_from_price_unit_per_person,
       lead_rate_for_display,
       lead_rate_unit,
       lead_rate_unit_per_person,
       show_prices,
       record__o['prices']['pricingRules']['showRackRate']::BOOLEAN AS show_rack_rate,
       pricing_model_for_display,
       max_number_of_adults,
       number_of_hotel_nights,
       has_flights_included,
       record__o['prices']['currency']['currencyCode']::VARCHAR     AS currency_code,

       record__o,
       sale_id,
       affiliate_url_string,
       failed_some_validation,
       fails_validation__sale_id__expected_nonnull,
       fails_validation__affiliate_url_string__expected_nonnull
FROM hygiene_vault_mvp.se_api.sales;

SELECT *
FROM scratch.robinpatel.sales s;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.se_api.sales CLONE scratch.robinpatel.sales;

SELECT count(DISTINCT sl.sale_id),
       count(*)
FROM hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list sl;

SELECT sl.schedule_tstamp,
       sl.run_tstamp,
       sl.operation_id,
       sl.created_at,
       sl.updated_at,
       sl.row_dataset_name,
       sl.row_dataset_source,
       sl.row_loaded_at,
       sl.row_schedule_tstamp,
       sl.row_run_tstamp,
       sl.row_filename,
       sl.row_file_row_number,
       sl.url,
       sl.latitude,
       sl.longitude,
       sl.sale_link,
       sl.discount,
       sl.show_discount,
       sl.deposit_from_price_for_display,
       sl.deposit_from_price_unit,
       sl.deposit_from_price_unit_per_person,
       sl.lead_rate_for_display,
       sl.lead_rate_unit,
       sl.lead_rate_unit_per_person,
       sl.show_prices,
       sl.show_rack_rate,
       sl.pricing_model_for_display,
       sl.max_number_of_adults,
       sl.number_of_hotel_nights,
       sl.has_flights_included,
       sl.currency_code,
       sl.record__o,
       sl.sale_id,
       sl.affiliate_url_string
FROM hygiene_snapshot_vault_mvp_dev_robin.se_api.sales_list sl;

self_describing_task --include 'data_vault_mvp_dev_robin/dwh/transactional clone data_vault_mvp./se_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00';dwh.00;

SELECT get_ddl('VIEW', 'data_vault_mvp_dev_robin.dwh.se_sale__union_odm_ndm');

SELECT u.se_sale_id,
       u.base_sale_id,
       u.sale_id,
       u.salesforce_opportunity_id,
       u.sale_name,
       u.sale_name_object,
       u.sale_active,
       u.class,
       u.has_flights_available,
       u.default_preferred_airport_code,
       u.type,
       u.hotel_chain_link,
       u.closest_airport_code,
       u.is_team20package,
       u.sale_able_to_sell_flights,
       u.sale_product,
       u.sale_type,
       u.product_type,
       u.product_configuration,
       u.product_line,
       u.data_model,
       u.hotel_location_info_id,
       u.active,
       u.default_hotel_offer_id,
       u.commission,
       u.commission_type,
       u.original_contractor_id,
       u.original_joint_contractor_id,
       u.date_created,
       u.destination_type,
       u.start_date,
       u.end_date,
       u.hotel_id,
       u.base_currency,
       u.city_district_id,
       u.company_id,
       u.company_name,
       u.company_array,
       u.hotel_code,
       u.latitude,
       u.longitude,
       u.location_info_id,
       u.posa_territory,
       u.posa_territory_array,
       u.posa_country,
       u.posa_country_array,
       u.posa_currency,
       u.posa_currency_array,
       u.posu_division,
       u.posu_country,
       u.posu_city,
       u.supplier_id,
       u.supplier_name,
       u.deal_category,
       u.travel_type
FROM data_vault_mvp_dev_robin.dwh.se_sale__union_odm_ndm u;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss
WHERE ss.sale_active
  AND ss.hotel_code = '001w000001DVHS5';

SELECT *
FROM raw_vault_mvp.se_api.sales sl
WHERE sl.sale_id = 'A5664';


--base_sale
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.base_sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.base_sale;
--sale
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale;
--hotel
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.hotel CLONE hygiene_snapshot_vault_mvp.cms_mysql.hotel;
--sale_flight_config
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_flight_config CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_flight_config;
--opportunity
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
--in_house_package
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.in_house_package_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.in_house_package_snapshot;
--location_info
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.location_info_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.location_info_snapshot;
--country
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_snapshot;
--country_division
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
--city
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.city_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.city_snapshot;
--base_sale_translation
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_translation_snapshot;
--base_offer_product
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot;
--product
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.product_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.product_snapshot;
--country_div_table_ref
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.country_division_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.country_division_snapshot;
--territory
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.territory_snapshot;
--sale_translation
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_translation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_translation_snapshot;
--offer
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.offer_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.offer_snapshot;
--allocation
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.allocation_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.allocation_snapshot;
--sale_territory
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_territory_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_territory_snapshot;
--sale_affiliate
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_affiliate_snapshot;
--base_sale_affiliate
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_sale_affiliate_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.base_sale_affiliate_snapshot;
--web_redirect
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_snapshot;
--company
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.company_snapshot;
--sale_company
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.sale_company_snapshot;
--ihp_sale_company
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.ihp_sale_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.ihp_sale_company_snapshot;
--web_redirect_company
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.web_redirect_company_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.web_redirect_company_snapshot;
--supplier
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.supplier_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.supplier_snapshot;
--contractor
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.contractor_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.contractor_snapshot;
--sf_opportunity
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.sfsc_snapshots.opportunity_snapshot CLONE data_vault_mvp.sfsc_snapshots.opportunity_snapshot;
--se_promotion
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_promotion CLONE data_vault_mvp.dwh.se_promotion;

SELECT sp.se_sale_id
FROM data_vault_mvp.dwh.se_promotion sp
WHERE sp.sale_active_promotion
GROUP BY 1
HAVING COUNT(*) > 1;

self_describing_task --include 'se/data/dwh/se_sale_attributes.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00';

SELECT *
FROM se_dev_robin.data.se_sale_attributes ssa
WHERE ssa.sale_active;

SELECT booking_id,
       cancellation_date    AS bk_cnx_date,
       last_updated         AS bk_cnx_last_updated,
       fault                AS bk_cnx_fault,
       reason               AS bk_cnx_reason,
       refund_channel       AS bk_cnx_refund_channel,
       refund_type          AS bk_cnx_refund_type,
       who_pays             AS bk_cnx_who_pays,
       cancel_with_provider AS bk_cnx_cancel_with_provider
FROM data_vault_mvp.dwh.booking_cancellation;

self_describing_task --include 'dv/dwh/master_booking_list/master_se_booking_list.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_request_cases CLONE hygiene_vault_mvp.sfsc.rebooking_request_cases;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_request_cases CLONE hygiene_snapshot_vault_mvp.sfsc.rebooking_request_cases;

SELECT *
FROM se.data.se_booking_summary_extended;

SELECT get_ddl('table', 'se.data.se_sale_attributes');


CREATE OR REPLACE VIEW se_sale_attributes
    COPY GRANTS
AS
    --primary key se_sale_id
SELECT ss.se_sale_id,
       ss.base_sale_id,
       ss.sale_id,
       ss.salesforce_opportunity_id,
       ss.sale_name,
       ss.sale_name_object,
       ss.sale_active,
       ss.class,
       ss.has_flights_available,
       ss.default_preferred_airport_code,
       ss.type,
       ss.hotel_chain_link,
       ss.closest_airport_code,
       ss.is_team20package,
       ss.sale_able_to_sell_flights,
       ss.sale_product,
       ss.sale_type,
       ss.product_type,
       ss.product_configuration,
       ss.product_line,
       ss.data_model,
       ss.hotel_location_info_id,
       ss.active,
       ss.default_hotel_offer_id,
       ss.commission,
       ss.commission_type,
       ss.original_contractor_id,
       ss.original_contractor_name,
       ss.original_joint_contractor_id,
       ss.original_joint_contractor_name,
       ss.current_contractor_id,
       ss.current_contractor_name,
       ss.current_joint_contractor_id,
       ss.current_joint_contractor_name,
       ss.date_created,
       ss.destination_type,
       ss.start_date,
       ss.end_date,
       ss.hotel_id,
       ss.base_currency,
       ss.city_district_id,
       ss.company_id,
       ss.company_name,
       ss.hotel_code,
       ss.latitude,
       ss.longitude,
       ss.location_info_id,
       ss.posa_territory,
       ss.posa_country,
       ss.posa_currency,
       ss.posu_division,
       ss.posu_country,
       ss.posu_city,
       ss.supplier_id,
       ss.supplier_name,
       ss.deal_category,
       ss.travel_type,
       ss.salesforce_opportunity_id_full,
       ss.salesforce_account_id,
       ss.deal_profile,
       ss.salesforce_proposed_start_date,
       ss.salesforce_deal_label_multi,
       ss.salesforce_stage_name,
       ss.promotion_label,
       ss.promotion_description,
       ss.se_api_lead_rate,
       ss.se_api_lead_rate_per_person,
       ss.se_api_currency,
       ss.se_api_show_discount,
       ss.se_api_show_prices,
       ss.se_api_discount,
       ss.se_api_url,
       COALESCE(pc.posu_sub_region, 'Other') AS posu_sub_region,
       COALESCE(pc.posu_region, 'Other')     AS posu_region,
       COALESCE(pc.posu_cluster, 'Other')    AS posu_cluster
FROM data_vault_mvp.dwh.se_sale ss
         LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation pc
                   ON ss.posu_categorisation_id = pc.posu_categorisation_id
WHERE class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale'
-- remove WRD catalogue sales
--actively removed catalogue deals because we will get these directly from travelbird data.
--at this point in time this is the best method of excluding these, but there might be an instance
--in the future where we may have web redirect sales that are not exclusively TB and this will need to be updated
;

SELECT ssa.se_sale_id,
       ssa.sale_name,
       ssa.hotel_code,
       ssa.salesforce_opportunity_id,

       ssa.promotion_label,
       ssa.promotion_description,
       ssa.se_api_lead_rate,
       ssa.se_api_lead_rate_per_person,
       ssa.se_api_currency,
       ssa.se_api_show_discount,
       ssa.se_api_show_prices,
       ssa.se_api_discount,
       ssa.se_api_url
FROM se.data.se_sale_attributes ssa
WHERE ssa.hotel_code = '001w000001DVHS5';

SELECT * FROM se.data_pii.crm_events_sends ces;
