SELECT
       sd.id,
       sd.version,
       sd.base_currency,
       sd.date_created,
       sd.image,
       sd.last_updated,
       sd.location_info_id,
       sd.package_discount,
       sd.provider_name,
       sd.sale_destination_name,
       sd.sale_destination_type,
       sd.sale_end,
       sd.sale_hotel_name,
       sd.sale_start,
       sd.sale_title,
       sd.sale_top_discount,
       sd.sale_type
FROM hygiene_snapshot_vault_mvp.cms_mysql.sale_details sd;

SELECT
       od.id,
       od.version,
       od.commission,
       od.commission_type,
       od.date_created,
       od.description,
       od.last_updated,
       od.max_adults,
       od.max_children,
       od.max_dependants,
       od.max_infants,
       od.name,
       od.rate_code,
       od.summary,
       od.travel_date_length
FROM hygiene_snapshot_vault_mvp.cms_mysql.offer_details od;

SELECT r.schedule_tstamp,
       r.run_tstamp,
       r.operation_id,
       r.created_at,
       r.updated_at,
       r.row_dataset_name,
       r.row_dataset_source,
       r.row_loaded_at,
       r.row_schedule_tstamp,
       r.row_run_tstamp,
       r.row_filename,
       r.row_file_row_number,
       r.booking_id,
       r.booking_date,
       r.check_in_date,
       r.check_out_date,
       r.id,
       r.version,
       r.affiliate_user_id,
       r.agency,
       r.booking_fee,
       r.check_in,
       r.check_out,
       r.completion_date,
       r.credits,
       r.currency,
       r.date_created,
       r.last_updated,
       r.passenger_first_name,
       r.passenger_last_name,
       r.passenger_phone_number,
       r.payment_id,
       r.sale_id,
       r.sale_id__o,
       r.status,
       r.surname,
       r.type,
       r.unique_transaction_reference,
       r.user_id,
       r.agent_id,
       r.passenger_address1,
       r.passenger_address2,
       r.passenger_city_name,
       r.passenger_country_name,
       r.passenger_postcode,
       r.vcc_enabled,
       r.cancellation_policy_id,
       r.supplier_to_user_currency_exchange_rate_id,
       r.sale_details_id,
       r.offer_details_id
FROM hygiene_snapshot_vault_mvp.cms_mysql.reservation r;
SELECT * FROM raw_vault_mvp.cms_mysql.booking b;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-08-17 00:00:00' --end '2021-08-17 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_details;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.amendment CLONE hygiene_snapshot_vault_mvp.cms_mysql.amendment;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.days_before_policy CLONE hygiene_snapshot_vault_mvp.cms_mysql.days_before_policy;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product_reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.product_reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_exchange_rate CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_exchange_rate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation CLONE data_vault_mvp.dwh.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;

SELECT * FROm data_vault_mvp_dev_robin.dwh.se_booking sb;