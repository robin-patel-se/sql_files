dataset_task  --include 'cms_mysql.sale$' --operation ExtractOperation --method 'run' --start '2022-02-23 00:30:00' --end '2022-02-23 00:30:00'
dataset_task  --include 'cms_mysql.sale$' --operation IngestOperation --method 'run' --start '2011-06-09 00:30:00' --end '2011-06-09 00:30:00'
dataset_task  --include 'cms_mysql.sale$' --operation HygieneOperation --method 'run' --start '2011-06-09 00:30:00' --end '2011-06-09 00:30:00'
dataset_task  --include 'cms_mysql.sale$' --operation LatestRecordsOperation --method 'run' --upstream --start '2011-06-09 00:30:00' --end '2011-06-09 00:30:00'


dataset_task  --include 'cms_mysql.sale$' --operation LatestRecordsOperation --method 'run' --upstream --start '2022-02-23 00:30:00' --end '2022-02-23 00:30:00'

dataset_task  --include 'cms_mysql.sale$' --operation LatestRecordsOperation --method 'run' --upstream --start '2011-06-09 00:30:00' --end '2011-06-09 00:30:00'

SELECT *
FROM raw_vault_mvp.cms_mysql.affiliate a;

DROP TABLE raw_vault_dev_robin.cms_mysql.sale;
DROP TABLE hygiene_vault_dev_robin.cms_mysql.sale;
DROP TABLE latest_vault_dev_robin.cms_mysql.sale;

SELECT *
FROM latest_vault_dev_robin.cms_mysql.sale;
SELECT sale.schedule_tstamp,
       sale.run_tstamp,
       sale.operation_id,
       sale.created_at,
       sale.updated_at,
       sale.row_dataset_name,
       sale.row_dataset_source,
       sale.row_loaded_at,
       sale.row_schedule_tstamp,
       sale.row_run_tstamp,
       sale.row_filename,
       sale.row_file_row_number,
       sale.sale_id,
       sale.id,
       sale.version,
       sale.active,
       sale.date_created,
       sale.destination_name,
       sale.end_date,
       sale.last_updated,
       sale.location,
       sale.slug,
       sale.start_date,
       sale.title,
       sale.top_discount,
       sale.type,
       sale.promotion,
       sale.commission,
       sale.discount_note,
       sale.board_type,
       sale.destination_type,
       sale.halo,
       sale.promoted,
       sale.travel_type,
       sale.contractor_id,
       sale.room_description,
       sale.vat_exclusive,
       sale.require_address,
       sale.require_age,
       sale.require_passport,
       sale.require_title,
       sale.custom_url_slug,
       sale.commission_type,
       sale.default_offer_id,
       sale.show_discount_prefix,
       sale.enable_hold,
       sale.mysterious,
       sale.mysterious_title,
       sale.premium,
       sale.instant,
       sale.instant_destination,
       sale.instant_type,
       sale.base_currency,
       sale.deposit,
       sale.closest_airport_code,
       sale.require_date_of_birth,
       sale.top_discount_eur,
       sale.top_discount_gbp,
       sale.top_discount_sek,
       sale.main_photo_id,
       sale.county,
       sale.destination_country,
       sale.show_price,
       sale.repeated,
       sale.location_info_id,
       sale.city_district_id,
       sale.jb_hotel_id,
       sale.top_discount_usd,
       sale.top_discount_dkk,
       sale.send_summary,
       sale.top_discount_nok,
       sale.top_discount_chf,
       sale.with_shared_allocations,
       sale.salesforce_opportunity_id,
       sale.sale_ancillary_details_id,
       sale.supplier_id,
       sale.smart_stay,
       sale.hotel_chain_link,
       sale.excluded_from_api,
       sale.top_discount_pln,
       sale.top_discount_sgd,
       sale.top_discount_php,
       sale.top_discount_idr,
       sale.top_discount_hkd,
       sale.top_discount_myr,
       sale.trip_advisor_ratings_image,
       sale.top_discount_czk,
       sale.top_discount_huf,
       sale.is_ean_secret_price_sale,
       sale.is_cee_sale,
       sale.is_able_to_sell_flights,
       sale.joint_contractor_id,
       sale.is_team20package,
       sale.is_overnight_flight,
       sale.zero_deposit,
       sale.ski_insurance,
       sale.additional_text,
       sale.main_paragraph,
       sale.map_location,
       sale.need_to_know,
       sale.second_opinion,
       sale.we_like,
       sale.hotel_details,
       sale.travel_details,
       sale.reason_to_love,
       sale.expired_copy,
       sale.reviews,
       sale.deal_includes,
       sale.price_compare,
       sale.notes,
       sale."EXCLUSIVE",
       sale.latitude,
       sale.longitude
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale;

------------------------------------------------------------------------------------------------------------------------
DROP TABLE raw_vault_dev_robin.cms_mysql.sale;
DROP TABLE hygiene_vault_dev_robin.cms_mysql.sale;
DROP TABLE latest_vault_dev_robin.cms_mysql.sale;

self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2022-02-23 00:00:00' --end '2022-02-23 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_promotion CLONE data_vault_mvp.dwh.se_promotion;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review;

self_describing_task --include 'dv/dwh/transactional/se_promotions.py'  --method 'run' --start '2022-02-23 00:00:00' --end '2022-02-23 00:00:00'
self_describing_task --include 'dv/dwh/transactional/se_sale_companies.py'  --method 'run' --start '2022-02-23 00:00:00' --end '2022-02-23 00:00:00'


dataset_task  --include 'cms_mysql.sale$' --operation LatestRecordsOperation --method 'run' --upstream --start '2022-02-23 00:30:00' --end '2022-02-23 00:30:00'
dataset_task  --include 'cms_mysql.sale$' --operation HygieneOperation --method 'run' --upstream --start '2022-02-23 00:30:00' --end '2022-02-23 00:30:00'


SELECT * FROM data_vault_mvp_dev_robin.dwh.se_sale;


SELECT
   ndm.se_sale_id,
   ndm.base_sale_id,
   NULL          AS sale_id,
   ndm.salesforce_opportunity_id,
   ndm.exclusive_sale,
--    ndm.smart_stay_sale,
   ndm.sale_name,
   ndm.destination_name,
   ndm.sale_name_object,
   ndm.sale_active,

   ndm.class,
   ndm.has_flights_available,
   ndm.default_preferred_airport_code,
   NULL          AS type,
   NULL          AS hotel_chain_link,
   NULL          AS closest_airport_code,
   NULL          AS is_team20package,
   NULL          AS sale_able_to_sell_flights,

   ndm.sale_product,
   ndm.sale_type,

   ndm.product_type,
   ndm.product_configuration,
   ndm.product_line,
   ndm.data_model,

   ndm.hotel_location_info_id,

   ndm.active,
   ndm.default_hotel_offer_id,
   ndm.commission,
   ndm.commission_type,
   ndm.original_contractor_id,
   ndm.original_joint_contractor_id,
   ndm.hotel_contractor_name,
   ndm.date_created,
   ndm.destination_type,
   ndm.start_date,
   ndm.end_date,
   ndm.hotel_id,
   ndm.base_currency,
   ndm.city_district_id,
   ndm.company_id,
   ndm.company_name,
   ndm.company_array,
   ndm.hotel_code,
   ndm.latitude  AS sale_latitude,
   ndm.longitude AS sale_longitude,
   ndm.location_info_id,
   ndm.redirect_url,

   ndm.posa_territory,
   NULL          AS posa_territory_array,
   ndm.posa_country,
   NULL          AS posa_country_array,
   ndm.posa_currency,
   NULL          AS posa_currency_array,

   ndm.posu_division,
   ndm.posu_country,
   ndm.posu_city,
   ndm.supplier_id,
   ndm.supplier_name,
   ndm.travel_type,
   ndm.is_flashsale,
   ndm.cancellation_policy_id,
   ndm.cancellation_policy_number_of_days,
   ndm.cancellation_policy_percentage
FROM data_vault_mvp_dev_robin.dwh.se_sale__step04__new_model_source_batch ndm

UNION ALL

SELECT
       odm.se_sale_id,
       NULL          AS base_sale_id,
       odm.sale_id,
       odm.salesforce_opportunity_id,
       odm.exclusive_sale,
--        odm.smart_stay_sale,
       odm.sale_name,
       odm.destination_name,
       odm.sale_name_object,
       odm.sale_active,

       NULL          AS class,
       NULL          AS has_flights_available,
       NULL          AS default_preferred_airport_code,
       odm.type,
       odm.hotel_chain_link,
       odm.closest_airport_code,
       odm.is_team20package,
       odm.sale_able_to_sell_flights,

       odm.sale_product,
       odm.sale_type,

       odm.product_type,
       odm.product_configuration,
       odm.product_line,
       odm.data_model,

       odm.hotel_location_info_id,
       odm.active,

       odm.default_hotel_offer_id,
       odm.commission,
       odm.commission_type,
       odm.original_contractor_id,
       odm.original_joint_contractor_id,
       NULL          AS hotel_contractor_name,
       odm.date_created,
       odm.destination_type,
       odm.start_date,
       odm.end_date,
       NULL          AS hotel_id,
       odm.base_currency,
       NULL          AS city_district_id,
       odm.company_id,
       odm.company_name,
       odm.company_array,
       NULL          AS hotel_code,
       odm.latitude  AS sale_latitude,
       odm.longitude AS sale_longitude,
       odm.location_info_id,
       NULL          AS redirect_url,

       odm.posa_territory,
       odm.posa_territory_array,
       odm.posa_country,
       odm.posa_country_array,
       odm.posa_currency,
       odm.posa_currency_array,

       odm.posu_division,
       odm.posu_country,
       odm.posu_city,
       odm.supplier_id,
       odm.supplier_name,
       --these are null as this functionality is only for NDM
       NULL          AS is_flashsale,
       --not included because odm sales can exist across multiple territories
       NULL          AS travel_type,
       --these are null as this functionality is only for NDM
       NULL          AS cancellation_policy_id,
       --these are null as this functionality is only for NDM
       NULL          AS cancellation_policy_number_of_days,
       --these are null as this functionality is only for NDM
       NULL          AS cancellation_policy_percentage
FROM data_vault_mvp_dev_robin.dwh.se_sale__step10__old_model_source_batch odm



airflow backfill --start_date '2011-06-09 00:00:00' --end_date '2011-06-10 00:00:00' --task_regex '.*' incoming__cms_mysql__sale__daily_at_00h30
airflow backfill --start_date '2022-02-27 00:00:00' --end_date '2022-02-28 00:00:00' --task_regex '.*' --m incoming__cms_mysql__sale__daily_at_00h30

SELECT * FROM latest_vault.cms_mysql.sale;
SELECT * FROM hygiene_snapshot_vault_mvp.cms_mysql.sale;