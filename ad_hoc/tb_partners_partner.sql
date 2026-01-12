python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name partners_partner \
    --primary_key_cols id \
    --new_record_col updated_at_dts \



CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.partners_partner CLONE raw_vault_mvp.travelbird_mysql.partners_partner;

self_describing_task --include 'staging/hygiene/travelbird_mysql/partners_partner.py'  --method 'run' --start '2021-03-01 00:00:00' --end '2021-03-01 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/partners_partner.py'  --method 'run' --start '2021-03-01 00:00:00' --end '2021-03-01 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.travelbird_mysql.partners_partner;
SELECT MIN(loaded_at)
FROM raw_vault_mvp.travelbird_mysql.partners_partner pp; --2021-03-01 16:54:32.348768000

self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/partners_partner.py'  --method 'run' --start '2021-07-08 00:00:00' --end '2021-07-08 00:00:00'

airflow backfill --start_date '2021-03-01 00:00:00' --end_date '2021-03-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__partners_partner__daily_at_01h00

SELECT *
FROM data_vault_mvp.dwh.tb_order_item_changelog toic;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderevent CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderevent;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_flightorderitem CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_flightorderitem;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.flights_flightproduct CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.flights_flightproduct;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates;

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.partners_partner;


airflow backfill --start_date '2021-03-01 00:00:00' --end_date '2021-03-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__partners_partner__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__partners_partner__daily_at_01h00

------------------------------------------------------------------------------------------------------------------------

--offers_offerconcept

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name offers_offerconcept \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.offers_offerconcept CLONE raw_vault_mvp.travelbird_mysql.offers_offerconcept;
SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.offers_offerconcept; --2020-02-06 18:46:52.781090000

self_describing_task --include 'staging/hygiene/travelbird_mysql/offers_offerconcept.py'  --method 'run' --start '2020-02-06 00:00:00' --end '2020-02-06 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/offers_offerconcept.py'  --method 'run' --start '2020-02-06 00:00:00' --end '2020-02-06 00:00:00'

airflow backfill --start_date '2020-02-06 00:00:00' --end_date '2020-02-07 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offerconcept__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_offerconcept__daily_at_01h00

------------------------------------------------------------------------------------------------------------------------

--offers_category

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name offers_category \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.offers_category CLONE raw_vault_mvp.travelbird_mysql.offers_category;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.offers_category; --2020-02-06 18:39:37.541847000

self_describing_task --include 'staging/hygiene/travelbird_mysql/offers_category.py'  --method 'run' --start '2020-02-06 00:00:00' --end '2020-02-06 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/offers_category.py'  --method 'run' --start '2020-02-06 00:00:00' --end '2020-02-06 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_category;

airflow backfill --start_date '2020-02-06 00:00:00' --end_date '2020-02-07 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_category__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_category__daily_at_01h00

------------------------------------------------------------------------------------------------------------------------

--offers_offerstaff

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name offers_offerstaff \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.offers_offerstaff CLONE raw_vault_mvp.travelbird_mysql.offers_offerstaff;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.offers_offerstaff; --2021-07-10 00:37:36.844902000

self_describing_task --include 'staging/hygiene/travelbird_mysql/offers_offerstaff.py'  --method 'run' --start '2021-07-10 00:00:00' --end '2021-07-10 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/offers_offerstaff.py'  --method 'run' --start '2021-07-10 00:00:00' --end '2021-07-10 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offerstaff;

airflow backfill --start_date '2020-02-06 00:00:00' --end_date '2020-02-07 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offerstaff__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_offerstaff__daily_at_01h00

------------------------------------------------------------------------------------------------------------------------

--offers_accountmanager

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name offers_accountmanager \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.offers_accountmanager CLONE raw_vault_mvp.travelbird_mysql.offers_accountmanager;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.offers_accountmanager; --2021-02-25 17:04:26.944291000

self_describing_task --include 'staging/hygiene/travelbird_mysql/offers_accountmanager.py'  --method 'run' --start '2021-05-25 00:00:00' --end '2021-05-25 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/offers_accountmanager.py'  --method 'run' --start '2021-05-25 00:00:00' --end '2021-05-25 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_accountmanager;

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-26 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_accountmanager__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_accountmanager__daily_at_01h00

------------------------------------------------------------------------------------------------------------------------

--offers_categorymanager

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name offers_categorymanager \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.offers_categorymanager CLONE raw_vault_mvp.travelbird_mysql.offers_categorymanager;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.offers_categorymanager; --2021-02-25 17:03:30.905879000

self_describing_task --include 'staging/hygiene/travelbird_mysql/offers_categorymanager.py'  --method 'run' --start '2021-05-25 00:00:00' --end '2021-05-25 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/offers_categorymanager.py'  --method 'run' --start '2021-05-25 00:00:00' --end '2021-05-25 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_categorymanager;

airflow backfill --start_date '2021-02-25 00:00:00' --end_date '2021-02-26 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_categorymanager__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_categorymanager__daily_at_01h00


------------------------------------------------------------------------------------------------------------------------

--reviews_npsscore

python biapp/
bau/
hygiene/
gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name reviews_npsscore \
    --primary_key_cols id \
    --new_record_col updated_at_dts \

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore CLONE raw_vault_mvp.travelbird_mysql.reviews_npsscore;

SELECT MIN(loaded_at)
FROM raw_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore; --2020-02-06 18:47:11.794963000

self_describing_task --include 'staging/hygiene/travelbird_mysql/reviews_npsscore.py'  --method 'run' --start '2020-02-06 00:00:00' --end '2020-02-06 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/travelbird_mysql/reviews_npsscore.py'  --method 'run' --start '2020-02-06 00:00:00' --end '2020-02-06 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore;

airflow backfill --start_date '2021-02-25 00:00:00' --end_date '2021-02-26 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__reviews_npsscore__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__reviews_npsscore__daily_at_01h00


------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2021-07-09 00:00:00' --end '2021-07-09 00:00:00'

DROP TABLE data_vault_mvp_dev_robin.dwh.tb_offer;

------------------------------------------------------------------------------------------------------------------------
--post deployment steps:

DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.offers_offerconcept_snapshot;
DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.offers_category_snapshot;
DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.offers_offerstaff_snapshot;
DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.offers_accountmanager_snapshot;
DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.offers_categorymanager_snapshot;
DROP TABLE data_vault_mvp.travelbird_mysql_snapshots.reviews_npsscore_snapshot;

airflow backfill --start_date '2021-03-01 00:00:00' --end_date '2021-03-02 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__partners_partner__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__partners_partner__daily_at_01h00

airflow backfill --start_date '2020-02-06 00:00:00' --end_date '2020-02-07 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offerconcept__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_offerconcept__daily_at_01h00

airflow backfill --start_date '2020-02-06 00:00:00' --end_date '2020-02-07 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_category__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_category__daily_at_01h00

airflow backfill --start_date '2020-02-06 00:00:00' --end_date '2020-02-07 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_offerstaff__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_offerstaff__daily_at_01h00

airflow backfill --start_date '2020-02-25 00:00:00' --end_date '2020-02-26 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_accountmanager__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_accountmanager__daily_at_01h00

airflow backfill --start_date '2021-02-25 00:00:00' --end_date '2021-02-26 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_categorymanager__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_categorymanager__daily_at_01h00

airflow backfill --start_date '2021-02-25 00:00:00' --end_date '2021-02-26 00:00:00' --task_regex '.*' hygiene_snapshots__travelbird_mysql__offers_categorymanager__daily_at_01h00
airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --task_regex '.*' -m hygiene_snapshots__travelbird_mysql__offers_categorymanager__daily_at_01h00

airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --reset_dagruns --task_regex '.*' dv_create_views__travelbird_mysql__daily_at_07h00

airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --reset_dagruns --task_regex '.*' dwh__transactional__sale__daily_at_03h00

airflow backfill --start_date '2021-07-09 00:00:00' --end_date '2021-07-10 00:00:00' --reset_dagruns --task_regex '.*' se_data_object_creation__daily_at_07h00


SELECT t.supplier_name, t.partner_title
FROM se.data.dim_sale ds
         INNER JOIN data_vault_mvp_dev_robin.dwh.tb_offer t ON ds.se_sale_id = t.se_sale_id
WHERE ds.tech_platform = 'TRAVELBIRD'
;



SELECT ss.salesforce_opportunity_id
FROM data_vault_mvp.dwh.se_sale ss
WHERE ss.product_configuration = 'WRD';

SELECT *
FROM se.data.se_booking sb;

self_describing_task --include 'dv/dwh/transactional/dim_sale.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00';

CREATE OR REPLACE TRANSIENT TABLE;



SELECT *
FROM data_vault_mvp_dev_robin.dwh.dim_sale;

self_describing_task
--include 'se/bi/dim_sale.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00'
CREATE OR REPLACE VIEW se_dev_robin.data.se_sale_tags AS
SELECT *
FROM se.data.se_sale_tags;

GRANT SELECT ON TABLE collab.travel_trust.travel_trust_money_out TO ROLE personal_role__sebastianmaczka;

SELECT *
FROM se.finance.travel_trust_money_out ttmo

CREATE OR REPLACE VIEW se_dev_robin.bi.fact_sale_metrics AS
SELECT *
FROM se.bi.fact_sale_metrics;

GRANT USAGE ON SCHEMA collab.travel_trust TO ROLE personal_role__sebastianmaczka;

CREATE OR REPLACE VIEW se_dev_robin.data.se_company_attributes AS
SELECT *
FROM se.data.se_company_attributes;

self_describing_task --include 'se/data/dwh/se_sale_attributes.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00'

self_describing_task --include 'dv/dwh/transactional/global_sale_attributes.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00'

self_describing_task --include 'se/data_pii/dwh/global_sale_attributes.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00'
self_describing_task --include 'dv/dwh/salesforce/salesforce_sale_opportunity.py'  --method 'run' --start '2021-07-13 00:00:00' --end '2021-07-13 00:00:00'
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;

SELECT *
FROM se_dev_robin.bi.dim_sale;

--clone
--row count test
--except test

------------------------------------------------------------------------------------------------------------------------

SELECT COUNT(*)
FROM se.bi.dim_sale ds;
SELECT COUNT(*)
FROM se_dev_robin.bi.dim_sale ds;

WITH prod AS (
    SELECT ds.se_sale_id,
           ds.sale_name,
           ds.sale_product,
           ds.sale_type,
           ds.product_type,
           ds.product_configuration,
           ds.product_line,
           ds.data_model,
           ds.sale_start_date,
           ds.sale_end_date,
           ds.sale_active,
           ds.posa_territory_o,
           ds.posa_territory,
           ds.posa_country,
           ds.posu_country,
           ds.posu_division,
           ds.posu_city,
           ds.travel_type,
           ds.target_account_list,
           ds.posu_sub_region,
           ds.posu_region,
           ds.posu_cluster,
           ds.posu_cluster_region,
           ds.posu_cluster_sub_region,
           ds.cm_region,
           ds.tech_platform,
           ds.base_sale_id,
           ds.sale_id,
--        ds.salesforce_opportunity_id,
           ds.class,
           ds.has_flights_available,
           ds.default_preferred_airport_code,
           ds.type,
           ds.hotel_chain_link,
           ds.closest_airport_code,
           ds.is_team20package,
           ds.sale_able_to_sell_flights,
           ds.hotel_location_info_id,
           ds.default_hotel_offer_id,
           ds.commission,
           ds.commission_type,
           ds.original_contractor_id,
           ds.original_contractor_name,
           ds.original_joint_contractor_id,
           ds.original_joint_contractor_name,
           ds.current_contractor_id,
           ds.current_contractor_name,
           ds.current_joint_contractor_id,
           ds.current_joint_contractor_name,
           ds.date_created,
           ds.destination_type,
           ds.hotel_id,
           ds.base_currency,
           ds.city_district_id,
           ds.hotel_code,
           ds.latitude,
           ds.longitude,
           ds.location_info_id,
--        ds.supplier_id, --removed because were part of refactor
--        ds.supplier_name, --removed because were part of refactor
           ds.promotion_label,
           ds.promotion_description,
           ds.se_api_lead_rate,
           ds.se_api_lead_rate_per_person,
           ds.se_api_currency,
           ds.se_api_show_discount,
           ds.se_api_show_prices,
           ds.se_api_discount,
           ds.se_api_url,
           ds.cancellation_policy_id,
           ds.cancellation_policy_number_of_days,
           ds.cancellation_policy_percentage,
           ds.is_flashsale,
           ds.posa_category,
           ds.company_id,
           ds.company_name,
           ds.company_country,
           ds.company_margin_gross_lifetime,
           ds.company_margin_net_canx_lifetime,
           ds.company_margin_net_canx_lly,
           ds.company_margin_net_canx_ly,
           ds.company_margin_net_canx_ty,
           ds.company_segment_lly,
           ds.company_segment_ly,
           ds.company_segment_ty,
           ds.jetlore_sale,
           ds.beach_sale,
           ds.spa_sale,
           ds.country_sale,
           ds.city_sale,
           ds.flash_sale,
           ds.first_sale_start_date,
           ds.deal_segment,
           ds.global_sale_start_date,
           ds.destination_manager,
           ds.number_of_incomplete_actions,
           ds.deal_profile,
           ds.proposed_start_date,
           ds.deal_label_multi,
           ds.stage_name,
           ds.repeat,
           ds.deal_category,
           ds.pulled_type,
           ds.pulled_reason,
           ds.currency,
           ds.account_owner_name,
           ds.owner,
           ds.owner_role,
           ds.joint_owner,
           ds.percentage_commission,
           ds.account_id,
           ds.account_name,
           ds.account_business_legal_name,
           ds.account_shipping_street,
           ds.account_shipping_city,
           ds.account_shipping_country,
           ds.account_shipping_postcode,
           ds.account_shipping_state,
           ds.account_billing_street,
           ds.account_billing_city,
           ds.account_billing_country,
           ds.account_billing_postcode,
           ds.account_billing_state,
           ds.account_cms_url,
           ds.account_contract_type,
           ds.account_currency,
           ds.account_business_status,
           ds.account_contract_status,
           ds.account_longitude,
           ds.account_latitude,
           ds.account_currency_hotel_sales,
           ds.account_opted_in_for_always_on,
           ds.account_opted_in_for_refundable_deals,
           ds.account_opted_in_for_suvc,
           ds.account_red_flag,
           ds.account_red_flag_reason,
           ds.account_no_rooms,
           ds.account_target_account_list,
           ds.account_star_rating,
           ds.account_rating_booking_com,
           ds.parent_account_id,
           ds.parent_account_name,
           ds.concept_name,
           ds.tb_deal_catogory,
           ds.account_manager,
           ds.category_manager,
           ds.tb_cms_url,
           ds.nps_score,
           ds.nps_number_reviews,
           ds.pub_date,
           ds.price
    FROM se.bi.dim_sale ds

),
     dev AS (

         SELECT ds.se_sale_id,
                ds.sale_name,
                ds.sale_product,
                ds.sale_type,
                ds.product_type,
                ds.product_configuration,
                ds.product_line,
                ds.data_model,
                ds.sale_start_date,
                ds.sale_end_date,
                ds.sale_active,
                ds.posa_territory_o,
                ds.posa_territory,
                ds.posa_country,
                ds.posu_country,
                ds.posu_division,
                ds.posu_city,
                ds.travel_type,
                ds.target_account_list,
                ds.posu_sub_region,
                ds.posu_region,
                ds.posu_cluster,
                ds.posu_cluster_region,
                ds.posu_cluster_sub_region,
                ds.cm_region,
--        ds.supplier_id,
--        ds.supplier_name,
--        ds.partner_id,
--        ds.partner_title,
                ds.tech_platform,
--        ds.salesforce_opportunity_id,
                ds.base_sale_id,
                ds.sale_id,
                ds.class,
                ds.has_flights_available,
                ds.default_preferred_airport_code,
                ds.type,
                ds.hotel_chain_link,
                ds.closest_airport_code,
                ds.is_team20package,
                ds.sale_able_to_sell_flights,
                ds.hotel_location_info_id,
                ds.default_hotel_offer_id,
                ds.commission,
                ds.commission_type,
                ds.original_contractor_id,
                ds.original_contractor_name,
                ds.original_joint_contractor_id,
                ds.original_joint_contractor_name,
                ds.current_contractor_id,
                ds.current_contractor_name,
                ds.current_joint_contractor_id,
                ds.current_joint_contractor_name,
                ds.date_created,
                ds.destination_type,
                ds.hotel_id,
                ds.base_currency,
                ds.city_district_id,
                ds.hotel_code,
                ds.latitude,
                ds.longitude,
                ds.location_info_id,
                ds.promotion_label,
                ds.promotion_description,
                ds.se_api_lead_rate,
                ds.se_api_lead_rate_per_person,
                ds.se_api_currency,
                ds.se_api_show_discount,
                ds.se_api_show_prices,
                ds.se_api_discount,
                ds.se_api_url,
                ds.cancellation_policy_id,
                ds.cancellation_policy_number_of_days,
                ds.cancellation_policy_percentage,
                ds.is_flashsale,
                ds.posa_category,
                ds.company_id,
                ds.company_name,
                ds.company_country,
                ds.company_margin_gross_lifetime,
                ds.company_margin_net_canx_lifetime,
                ds.company_margin_net_canx_lly,
                ds.company_margin_net_canx_ly,
                ds.company_margin_net_canx_ty,
                ds.company_segment_lly,
                ds.company_segment_ly,
                ds.company_segment_ty,
                ds.jetlore_sale,
                ds.beach_sale,
                ds.spa_sale,
                ds.country_sale,
                ds.city_sale,
                ds.flash_sale,
                ds.first_sale_start_date,
                ds.deal_segment,
                ds.global_sale_start_date,
                ds.destination_manager,
                ds.number_of_incomplete_actions,
                ds.deal_profile,
                ds.proposed_start_date,
                ds.deal_label_multi,
                ds.stage_name,
                ds.repeat,
                ds.deal_category,
                ds.pulled_type,
                ds.pulled_reason,
                ds.currency,
                ds.account_owner_name,
                ds.owner,
                ds.owner_role,
                ds.joint_owner,
                ds.percentage_commission,
                ds.account_id,
                ds.account_name,
                ds.account_business_legal_name,
                ds.account_shipping_street,
                ds.account_shipping_city,
                ds.account_shipping_country,
                ds.account_shipping_postcode,
                ds.account_shipping_state,
                ds.account_billing_street,
                ds.account_billing_city,
                ds.account_billing_country,
                ds.account_billing_postcode,
                ds.account_billing_state,
                ds.account_cms_url,
                ds.account_contract_type,
                ds.account_currency,
                ds.account_business_status,
                ds.account_contract_status,
                ds.account_longitude,
                ds.account_latitude,
                ds.account_currency_hotel_sales,
                ds.account_opted_in_for_always_on,
                ds.account_opted_in_for_refundable_deals,
                ds.account_opted_in_for_suvc,
                ds.account_red_flag,
                ds.account_red_flag_reason,
                ds.account_no_rooms,
                ds.account_target_account_list,
                ds.account_star_rating,
                ds.account_rating_booking_com,
                ds.parent_account_id,
                ds.parent_account_name,
                ds.concept_name,
                ds.tb_deal_catogory,
                ds.account_manager,
                ds.category_manager,
                ds.tb_cms_url,
                ds.nps_score,
                ds.nps_number_reviews,
                ds.pub_date,
                ds.price
         FROM se_dev_robin.bi.dim_sale ds
     ),
     excpt AS (
         SELECT *
         FROM prod
             EXCEPT
         SELECT *
         FROM dev
     ),
     unn AS (
         SELECT *
         FROM prod
         UNION ALL
         SELECT *
         FROM dev
     )
SELECT *
FROM unn
         INNER JOIN excpt ON unn.se_sale_id = excpt.se_sale_id
;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer_20210715 CLONE data_vault_mvp.dwh.tb_offer;

SELECT t.id,
       t.se_sale_id,
       t.site_id,
       t.product_line__o,
       t.sale_product,
       t.sale_type,
       t.product_type,
       t.product_configuration,
       t.product_line,
       t.data_model,
       t.pub_date,
       t.end_date,
       t.sale_active,
       t.active,
       t.title,
       t.short_title,
       t.banner_title,
       t.price,
       t.price_title,
       t.old_price,
       t.payment_option,
       t.booking_fee,
       t.included,
       t.excluded_short,
       t.package_price_per_night,
       t.concept_name,
       t.posa_territory,
       t.posa_country,
       t.posa_currency,
       t.posu_division,
       t.posu_country,
       t.posu_city,
       t.original_contractor_id,
       t.original_contractor_name,
       t.original_joint_contractor_id,
       t.original_joint_contractor_name,
       t.current_contractor_id,
       t.current_contractor_name,
       t.current_joint_contractor_id,
       t.current_joint_contractor_name,
       t.supplier_id,
       t.supplier_name,
       t.posu_categorisation_id,
       t.travel_type,
       t.target_account_list,
       t.deal_category,
       t.account_manager,
       t.category_manager,
       t.tb_cms_url,
       t.nps_score,
       t.nps_number_reviews
FROM data_vault_mvp.dwh.tb_offer t
MINUS
SELECT tb_offer_20210715.id,
       tb_offer_20210715.se_sale_id,
       tb_offer_20210715.site_id,
       tb_offer_20210715.product_line__o,
       tb_offer_20210715.sale_product,
       tb_offer_20210715.sale_type,
       tb_offer_20210715.product_type,
       tb_offer_20210715.product_configuration,
       tb_offer_20210715.product_line,
       tb_offer_20210715.data_model,
       tb_offer_20210715.pub_date,
       tb_offer_20210715.end_date,
       tb_offer_20210715.sale_active,
       tb_offer_20210715.active,
       tb_offer_20210715.title,
       tb_offer_20210715.short_title,
       tb_offer_20210715.banner_title,
       tb_offer_20210715.price,
       tb_offer_20210715.price_title,
       tb_offer_20210715.old_price,
       tb_offer_20210715.payment_option,
       tb_offer_20210715.booking_fee,
       tb_offer_20210715.included,
       tb_offer_20210715.excluded_short,
       tb_offer_20210715.package_price_per_night,
       tb_offer_20210715.concept_name,
       tb_offer_20210715.posa_territory,
       tb_offer_20210715.posa_country,
       tb_offer_20210715.posa_currency,
       tb_offer_20210715.posu_division,
       tb_offer_20210715.posu_country,
       tb_offer_20210715.posu_city,
       tb_offer_20210715.original_contractor_id,
       tb_offer_20210715.original_contractor_name,
       tb_offer_20210715.original_joint_contractor_id,
       tb_offer_20210715.original_joint_contractor_name,
       tb_offer_20210715.current_contractor_id,
       tb_offer_20210715.current_contractor_name,
       tb_offer_20210715.current_joint_contractor_id,
       tb_offer_20210715.current_joint_contractor_name,
       tb_offer_20210715.supplier_id,
       tb_offer_20210715.supplier_name,
       tb_offer_20210715.posu_categorisation_id,
       tb_offer_20210715.travel_type,
       tb_offer_20210715.target_account_list,
       tb_offer_20210715.deal_category,
       tb_offer_20210715.account_manager,
       tb_offer_20210715.category_manager,
       tb_offer_20210715.tb_cms_url,
       tb_offer_20210715.nps_score,
       tb_offer_20210715.nps_number_reviews
FROM data_vault_mvp_dev_robin.dwh.tb_offer_20210715;

------------------------------------------------------------------------------------------------------------------------

--surface in se bi


CREATE OR REPLACE VIEW se_dev_robin.bi.fact_sale_metrics AS
SELECT *
FROM se.bi.fact_sale_metrics;
CREATE OR REPLACE VIEW se_dev_robin.data.sale_active AS
SELECT *
FROM se.data.sale_active;
CREATE OR REPLACE VIEW se_dev_robin.data.dim_sale AS
SELECT *
FROM se.data.dim_sale;
CREATE OR REPLACE VIEW se_dev_robin.data.se_sale_tags AS
SELECT *
FROM se.data.se_sale_tags;
CREATE OR REPLACE VIEW se_dev_robin.data.se_company_attributes AS
SELECT *
FROM se.data.se_company_attributes;
CREATE OR REPLACE VIEW se_dev_robin.data.global_sale_attributes AS
SELECT *
FROM se.data.global_sale_attributes;
CREATE OR REPLACE VIEW se_dev_robin.data.tb_offer AS
SELECT *
FROM se.data.tb_offer;
CREATE OR REPLACE VIEW se_dev_robin.data.se_sale_attributes AS
SELECT *
FROM se.data.se_sale_attributes;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.sale_translation_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.sale_translation_snapshot;
CREATE OR REPLACE VIEW hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory AS
SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.territory_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.territory_snapshot;


self_describing_task --include 'se/bi/dim_sale.py'  --method 'run' --start '2021-07-15 00:00:00' --end '2021-07-15 00:00:00'

WITH dev AS (
    SELECT ds.se_sale_id,
--            ds.sale_name,
           ds.sale_product,
           ds.sale_type,
           ds.product_type,
           ds.product_configuration,
           ds.product_line,
           ds.data_model,
           ds.sale_start_date,
           ds.sale_end_date,
           ds.sale_active,
           ds.posa_territory_o,
           ds.posa_territory,
           ds.posa_country,
           ds.posu_country,
           ds.posu_division,
           ds.posu_city,
           ds.travel_type,
           ds.target_account_list,
           ds.posu_sub_region,
           ds.posu_region,
           ds.posu_cluster,
           ds.posu_cluster_region,
           ds.posu_cluster_sub_region,
           ds.cm_region,
--        ds.supplier_id,
--        ds.supplier_name,
--        ds.partner_id,
--        ds.partner_title,
           ds.tech_platform,
           ds.base_sale_id,
           ds.sale_id,
--        ds.salesforce_opportunity_id,
           ds.class,
           ds.has_flights_available,
           ds.default_preferred_airport_code,
           ds.type,
           ds.hotel_chain_link,
           ds.closest_airport_code,
           ds.is_team20package,
           ds.sale_able_to_sell_flights,
           ds.hotel_location_info_id,
           ds.default_hotel_offer_id,
           ds.commission,
           ds.commission_type,
           ds.original_contractor_id,
           ds.original_contractor_name,
           ds.original_joint_contractor_id,
           ds.original_joint_contractor_name,
           ds.current_contractor_id,
           ds.current_contractor_name,
           ds.current_joint_contractor_id,
           ds.current_joint_contractor_name,
           ds.date_created,
           ds.destination_type,
           ds.hotel_id,
           ds.base_currency,
           ds.city_district_id,
           ds.hotel_code,
           ds.latitude,
           ds.longitude,
           ds.location_info_id,
           ds.promotion_label,
           ds.promotion_description,
           ds.se_api_lead_rate,
           ds.se_api_lead_rate_per_person,
           ds.se_api_currency,
           ds.se_api_show_discount,
           ds.se_api_show_prices,
           ds.se_api_discount,
           ds.se_api_url,
           ds.cancellation_policy_id,
           ds.cancellation_policy_number_of_days,
           ds.cancellation_policy_percentage,
           ds.is_flashsale,
           ds.posa_category,
           ds.company_id,
           ds.company_name,
           ds.company_country,
           ds.company_margin_gross_lifetime,
           ds.company_margin_net_canx_lifetime,
           ds.company_margin_net_canx_lly,
           ds.company_margin_net_canx_ly,
           ds.company_margin_net_canx_ty,
           ds.company_segment_lly,
           ds.company_segment_ly,
           ds.company_segment_ty,
           ds.jetlore_sale,
           ds.beach_sale,
           ds.spa_sale,
           ds.country_sale,
           ds.city_sale,
           ds.flash_sale,
           ds.first_sale_start_date,
--        ds.deal_segment,
--        ds.global_sale_start_date,
--        ds.destination_manager,
--        ds.number_of_incomplete_actions,
--        ds.deal_profile,
--        ds.proposed_start_date,
--        ds.deal_label_multi,
--        ds.stage_name,
--        ds.repeat,
--        ds.deal_category,
--        ds.pulled_type,
--        ds.pulled_reason,
--        ds.currency,
--        ds.account_owner_name,
--        ds.owner,
--        ds.owner_role,
--        ds.joint_owner,
--        ds.percentage_commission,
--        ds.account_id,
--        ds.account_name,
--        ds.account_business_legal_name,
--        ds.account_shipping_street,
--        ds.account_shipping_city,
--        ds.account_shipping_country,
--        ds.account_shipping_postcode,
--        ds.account_shipping_state,
--        ds.account_billing_street,
--        ds.account_billing_city,
--        ds.account_billing_country,
--        ds.account_billing_postcode,
--        ds.account_billing_state,
--        ds.account_cms_url,
--        ds.account_contract_type,
--        ds.account_currency,
--        ds.account_business_status,
--        ds.account_contract_status,
--        ds.account_longitude,
--        ds.account_latitude,
--        ds.account_currency_hotel_sales,
--        ds.account_opted_in_for_always_on,
--        ds.account_opted_in_for_refundable_deals,
--        ds.account_opted_in_for_suvc,
--        ds.account_red_flag,
--        ds.account_red_flag_reason,
--        ds.account_no_rooms,
--        ds.account_target_account_list,
--        ds.account_star_rating,
--        ds.account_rating_booking_com,
--        ds.parent_account_id,
--        ds.parent_account_name,
           ds.concept_name,
           ds.tb_deal_catogory,
           ds.account_manager,
           ds.category_manager,
           ds.tb_cms_url,
           ds.nps_score,
           ds.nps_number_reviews,
           ds.pub_date,
           ds.price
    FROM se_dev_robin.bi.dim_sale ds
)
   , prod AS (
    SELECT ds.se_sale_id,
--            ds.sale_name,
           ds.sale_product,
           ds.sale_type,
           ds.product_type,
           ds.product_configuration,
           ds.product_line,
           ds.data_model,
           ds.sale_start_date,
           ds.sale_end_date,
           ds.sale_active,
           ds.posa_territory_o,
           ds.posa_territory,
           ds.posa_country,
           ds.posu_country,
           ds.posu_division,
           ds.posu_city,
           ds.travel_type,
           ds.target_account_list,
           ds.posu_sub_region,
           ds.posu_region,
           ds.posu_cluster,
           ds.posu_cluster_region,
           ds.posu_cluster_sub_region,
           ds.cm_region,
           ds.tech_platform,
           ds.base_sale_id,
           ds.sale_id,
--        ds.salesforce_opportunity_id,
           ds.class,
           ds.has_flights_available,
           ds.default_preferred_airport_code,
           ds.type,
           ds.hotel_chain_link,
           ds.closest_airport_code,
           ds.is_team20package,
           ds.sale_able_to_sell_flights,
           ds.hotel_location_info_id,
           ds.default_hotel_offer_id,
           ds.commission,
           ds.commission_type,
           ds.original_contractor_id,
           ds.original_contractor_name,
           ds.original_joint_contractor_id,
           ds.original_joint_contractor_name,
           ds.current_contractor_id,
           ds.current_contractor_name,
           ds.current_joint_contractor_id,
           ds.current_joint_contractor_name,
           ds.date_created,
           ds.destination_type,
           ds.hotel_id,
           ds.base_currency,
           ds.city_district_id,
           ds.hotel_code,
           ds.latitude,
           ds.longitude,
           ds.location_info_id,
--        ds.supplier_id,
--        ds.supplier_name,
           ds.promotion_label,
           ds.promotion_description,
           ds.se_api_lead_rate,
           ds.se_api_lead_rate_per_person,
           ds.se_api_currency,
           ds.se_api_show_discount,
           ds.se_api_show_prices,
           ds.se_api_discount,
           ds.se_api_url,
           ds.cancellation_policy_id,
           ds.cancellation_policy_number_of_days,
           ds.cancellation_policy_percentage,
           ds.is_flashsale,
           ds.posa_category,
           ds.company_id,
           ds.company_name,
           ds.company_country,
           ds.company_margin_gross_lifetime,
           ds.company_margin_net_canx_lifetime,
           ds.company_margin_net_canx_lly,
           ds.company_margin_net_canx_ly,
           ds.company_margin_net_canx_ty,
           ds.company_segment_lly,
           ds.company_segment_ly,
           ds.company_segment_ty,
           ds.jetlore_sale,
           ds.beach_sale,
           ds.spa_sale,
           ds.country_sale,
           ds.city_sale,
           ds.flash_sale,
           ds.first_sale_start_date,
--        ds.deal_segment,
--        ds.global_sale_start_date,
--        ds.destination_manager,
--        ds.number_of_incomplete_actions,
--        ds.deal_profile,
--        ds.proposed_start_date,
--        ds.deal_label_multi,
--        ds.stage_name,
--        ds.repeat,
--        ds.deal_category,
--        ds.pulled_type,
--        ds.pulled_reason,
--        ds.currency,
--        ds.account_owner_name,
--        ds.owner,
--        ds.owner_role,
--        ds.joint_owner,
--        ds.percentage_commission,
--        ds.account_id,
--        ds.account_name,
--        ds.account_business_legal_name,
--        ds.account_shipping_street,
--        ds.account_shipping_city,
--        ds.account_shipping_country,
--        ds.account_shipping_postcode,
--        ds.account_shipping_state,
--        ds.account_billing_street,
--        ds.account_billing_city,
--        ds.account_billing_country,
--        ds.account_billing_postcode,
--        ds.account_billing_state,
--        ds.account_cms_url,
--        ds.account_contract_type,
--        ds.account_currency,
--        ds.account_business_status,
--        ds.account_contract_status,
--        ds.account_longitude,
--        ds.account_latitude,
--        ds.account_currency_hotel_sales,
--        ds.account_opted_in_for_always_on,
--        ds.account_opted_in_for_refundable_deals,
--        ds.account_opted_in_for_suvc,
--        ds.account_red_flag,
--        ds.account_red_flag_reason,
--        ds.account_no_rooms,
--        ds.account_target_account_list,
--        ds.account_star_rating,
--        ds.account_rating_booking_com,
--        ds.parent_account_id,
--        ds.parent_account_name,
           ds.concept_name,
           ds.tb_deal_catogory,
           ds.account_manager,
           ds.category_manager,
           ds.tb_cms_url,
           ds.nps_score,
           ds.nps_number_reviews,
           ds.pub_date,
           ds.price
    FROM se.bi.dim_sale ds
)
   , exc AS (
    -- find dupes
    SELECT dev.*
    FROM dev
        EXCEPT
    SELECT prod.*
    FROM prod
)
   , unn AS (
    SELECT 'dev' AS platform,
           dev.*
    FROM dev
    UNION ALL
    SELECT 'prod' AS platform,
           prod.*
    FROM prod
)
SELECT unn.*
FROM unn
         INNER JOIN exc ON unn.se_sale_id = exc.se_sale_id AND unn.posa_territory = exc.posa_territory
ORDER BY unn.se_sale_id, unn.posa_territory;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.se_bi_dim_sale AS SELECT * FROM se.bi.dim_sale ds;


SELECT DISTINCT s.channel_category
FROM se.data.scv_touch_marketing_channel s;


CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.se_sale_tags AS SELECT * FROM se.data.se_sale_tags;

airflow backfill --start_date '2020-03-24 00:00:00' --end_date '2020-03-24 00:00:00' --task_regex '.*' hygiene_snapshots__cms_mysql__tags__daily_at_01h00

