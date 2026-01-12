self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-08-22 00:00:00' --end '2021-08-22 00:00:00'


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

WITH two_tables AS (
    SELECT sb.booking_id,
           sb.transaction_id,
           sb.unique_transaction_reference,
           sb.last_updated,
           sb.last_updated_booking_summary,
           sb.last_updated_bookings,
           sb.last_updated_reservations,
           sb.territory,
           sb.booking_status,
           sb.currency,
           sb.booking_completed_date,
           sb.booking_created_date,
           sb.booking_completed_timestamp,
           sb.booking_created_timestamp,
           sb.cs_agent_booking,
           sb.shiro_user_id,
           sb.affiliate_user_id,
           sb.device_platform,
           sb.cc_rate_to_gbp,
           sb.cc_rate_to_sc,
           sb.gbp_rate_to_sc,
           sb.gross_revenue_cc,
           sb.customer_total_price_cc,
           sb.gross_booking_value_cc,
           sb.margin_gross_of_toms_cc,
           sb.vat_on_commission_cc,
           sb.commission_ex_vat_cc,
           sb.booking_fee_cc,
           sb.booking_fee_net_rate_cc,
           sb.payment_surcharge_cc,
           sb.payment_surcharge_net_rate_cc,
           sb.insurance_commission_cc,
           sb.flight_amount_cc,
           sb.flight_commission_cc,
           sb.credits_used_cc,
           sb.total_custom_tax_cc,
           sb.atol_fee_cc,
           sb.total_sell_rate_cc,
           sb.insurance_price_cc,
           sb.gross_revenue_gbp,
           sb.gross_revenue_gbp_constant_currency,
           sb.customer_total_price_gbp,
           sb.customer_total_price_gbp_constant_currency,
           sb.gross_booking_value_gbp,
           sb.margin_gross_of_toms_gbp,
           sb.margin_gross_of_toms_gbp_constant_currency,
           sb.vat_on_commission_gbp,
           sb.commission_ex_vat_gbp,
           sb.booking_fee_gbp,
           sb.booking_fee_net_rate_gbp,
           sb.payment_surcharge_gbp,
           sb.payment_surcharge_net_rate_gbp,
           sb.insurance_commission_gbp,
           sb.flight_amount_gbp,
           sb.flight_commission_gbp,
           sb.credits_used_gbp,
           sb.total_custom_tax_gbp,
           sb.atol_fee_gbp,
           sb.total_sell_rate_gbp,
           sb.insurance_price_gbp,
           sb.gross_revenue_sc,
           sb.customer_total_price_sc,
           sb.gross_booking_value_sc,
           sb.margin_gross_of_toms_sc,
           sb.vat_on_commission_sc,
           sb.commission_ex_vat_sc,
           sb.booking_fee_sc,
           sb.booking_fee_net_rate_sc,
           sb.payment_surcharge_sc,
           sb.payment_surcharge_net_rate_sc,
           sb.insurance_commission_sc,
           sb.flight_amount_sc,
           sb.flight_commission_sc,
           sb.credits_used_sc,
           sb.total_custom_tax_sc,
           sb.atol_fee_sc,
           sb.total_sell_rate_sc,
           sb.insurance_price_sc,
           sb.gross_revenue_eur_constant_currency,
           sb.margin_gross_of_toms_eur_constant_currency,
           sb.sale_id,
           sb.sale_base_currency,
           sb.offer_id,
           sb.offer_name,
           sb.offer_rate_code,
           sb.offer_travel_date_length,
           sb.bundle_id,
           sb.check_in_date,
           sb.check_out_date,
           sb.outbound_flight_departure_date,
           sb.inbound_flight_arrival_date,
           sb.booking_lead_time_days,
           sb.booking_type,
           sb.no_nights,
           sb.rooms,
           sb.room_nights,
           sb.adult_guests,
           sb.child_guests,
           sb.infant_guests,
           sb.sale_product,
           sb.sale_type,
           sb.has_flights,
           sb.price_per_night,
           sb.price_per_person_per_night,
           sb.supplier_name,
           sb.rebooked,
           sb.date_of_rebooking,
           sb.original_check_in_date,
           sb.original_check_out_date,
           sb.voucher_stay_by_date,
           sb.is_new_model_booking,
           sb.is_staff_booking,
           sb.staff_booking_discount_type,
           sb.affiliate_id,
           sb.affiliate,
           sb.affiliate_domain,
           sb.agent_id,
           sb.payment_id,
           sb.hold_id,
           sb.insurance_provider,
           sb.payment_type,
           sb.supplier_to_user_currency_exchange_rate_id,
           sb.is_affiliate_booking,
           sb.refund_type,
           sb.total_refunded_cc,
           sb.total_refunded_gbp,
           sb.total_refunded_sc,
           sb.cancellation_date,
           sb.cancellation_tstamp,
           sb.cancellation_fault,
           sb.cancellation_reason,
           sb.cancellation_refund_channel,
           sb.cancellation_status,
           sb.cancellation_requested_by,
           sb.cancellation_requested_by_domain,
           sb.cancellation_payment_provider_refund_status,
           sb.cancellation_policy_id,
           sb.cancellation_policy_number_of_days,
           sb.cancellation_policy_percentage
    FROM data_vault_mvp.dwh.se_booking sb

    UNION ALL

    SELECT s.booking_id,
           s.transaction_id,
           s.unique_transaction_reference,
           s.last_updated,
           s.last_updated_booking_summary,
           s.last_updated_bookings,
           s.last_updated_reservations,
           s.territory,
           s.booking_status,
           s.currency,
           s.booking_completed_date,
           s.booking_created_date,
           s.booking_completed_timestamp,
           s.booking_created_timestamp,
           s.cs_agent_booking,
           s.shiro_user_id,
           s.affiliate_user_id,
           s.device_platform,
           s.cc_rate_to_gbp,
           s.cc_rate_to_sc,
           s.gbp_rate_to_sc,
           s.gross_revenue_cc,
           s.customer_total_price_cc,
           s.gross_booking_value_cc,
           s.margin_gross_of_toms_cc,
           s.vat_on_commission_cc,
           s.commission_ex_vat_cc,
           s.booking_fee_cc,
           s.booking_fee_net_rate_cc,
           s.payment_surcharge_cc,
           s.payment_surcharge_net_rate_cc,
           s.insurance_commission_cc,
           s.flight_amount_cc,
           s.flight_commission_cc,
           s.credits_used_cc,
           s.total_custom_tax_cc,
           s.atol_fee_cc,
           s.total_sell_rate_cc,
           s.insurance_price_cc,
           s.gross_revenue_gbp,
           s.gross_revenue_gbp_constant_currency,
           s.customer_total_price_gbp,
           s.customer_total_price_gbp_constant_currency,
           s.gross_booking_value_gbp,
           s.margin_gross_of_toms_gbp,
           s.margin_gross_of_toms_gbp_constant_currency,
           s.vat_on_commission_gbp,
           s.commission_ex_vat_gbp,
           s.booking_fee_gbp,
           s.booking_fee_net_rate_gbp,
           s.payment_surcharge_gbp,
           s.payment_surcharge_net_rate_gbp,
           s.insurance_commission_gbp,
           s.flight_amount_gbp,
           s.flight_commission_gbp,
           s.credits_used_gbp,
           s.total_custom_tax_gbp,
           s.atol_fee_gbp,
           s.total_sell_rate_gbp,
           s.insurance_price_gbp,
           s.gross_revenue_sc,
           s.customer_total_price_sc,
           s.gross_booking_value_sc,
           s.margin_gross_of_toms_sc,
           s.vat_on_commission_sc,
           s.commission_ex_vat_sc,
           s.booking_fee_sc,
           s.booking_fee_net_rate_sc,
           s.payment_surcharge_sc,
           s.payment_surcharge_net_rate_sc,
           s.insurance_commission_sc,
           s.flight_amount_sc,
           s.flight_commission_sc,
           s.credits_used_sc,
           s.total_custom_tax_sc,
           s.atol_fee_sc,
           s.total_sell_rate_sc,
           s.insurance_price_sc,
           s.gross_revenue_eur_constant_currency,
           s.margin_gross_of_toms_eur_constant_currency,
           s.sale_id,
           s.sale_base_currency,
           s.offer_id,
           s.offer_name,
           s.offer_rate_code,
           s.offer_travel_date_length,
           s.bundle_id,
           s.check_in_date,
           s.check_out_date,
           s.outbound_flight_departure_date,
           s.inbound_flight_arrival_date,
           s.booking_lead_time_days,
           s.booking_type,
           s.no_nights,
           s.rooms,
           s.room_nights,
           s.adult_guests,
           s.child_guests,
           s.infant_guests,
           s.sale_product,
           s.sale_type,
           s.has_flights,
           s.price_per_night,
           s.price_per_person_per_night,
           s.supplier_name,
           s.rebooked,
           s.date_of_rebooking,
           s.original_check_in_date,
           s.original_check_out_date,
           s.voucher_stay_by_date,
           s.is_new_model_booking,
           s.is_staff_booking,
           s.staff_booking_discount_type,
           s.affiliate_id,
           s.affiliate,
           s.affiliate_domain,
           s.agent_id,
           s.payment_id,
           s.hold_id,
           s.insurance_provider,
           s.payment_type,
           s.supplier_to_user_currency_exchange_rate_id,
           s.is_affiliate_booking,
           s.refund_type,
           s.total_refunded_cc,
           s.total_refunded_gbp,
           s.total_refunded_sc,
           s.cancellation_date,
           s.cancellation_tstamp,
           s.cancellation_fault,
           s.cancellation_reason,
           s.cancellation_refund_channel,
           s.cancellation_status,
           s.cancellation_requested_by,
           s.cancellation_requested_by_domain,
           s.cancellation_payment_provider_refund_status,
           s.cancellation_policy_id,
           s.cancellation_policy_number_of_days,
           s.cancellation_policy_percentage
    FROM data_vault_mvp_dev_robin.dwh.se_booking s
)
SELECT *
FROM two_tables tt
    QUALIFY COUNT(*) OVER (PARTITION BY booking_id) > 1
ORDER BY booking_id;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.booking_status = 'COMPLETE'
  AND sb.non_cash_credits_used_cc > 0;

SELECT (CURRENT_TIMESTAMP || ' +0000')::timestamp_tz;

SELECT TO_VARCHAR(se_booking.booking_completed_timestamp, 'HH:mm')

SELECT LEFT(DATE_TRUNC('MINUTE', CURRENT_TIMESTAMP::TIME), 5)


SELECT
    -- a booking can have multiple adjustments but we needs the dates associated with
    -- the most recent one
    COALESCE(a.booking_id::VARCHAR, 'A' || pr.reservation_id)::VARCHAR AS booking_id,
    a.date_created                                                     AS date_of_rebooking, --date the rebooking occurred
    a.check_in_date                                                    AS adjusted_check_in_date,
    a.check_out_date                                                   AS adjusted_check_out_date,
    a.stay_by_date                                                     AS voucher_stay_by_date,
    a.*
FROM hygiene_snapshot_vault_mvp.cms_mysql.amendment a
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.product_reservation pr ON a.product_reservation_id = pr.id;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;



DROP TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
self_describing_task --include '/staging/hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2021-01-01 00:00:00' --end '2021-01-01 00:00:00'
CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
WHERE bs.credits_used_cc > 0;

self_describing_task --include 'staging/hygiene_snapshots/cms_mongodb/booking_summary.py'  --method 'run' --start '2021-08-23 00:00:00' --end '2021-08-23 00:00:00'

DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;
SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;

SELECT sb.sale_product, sb.sale_type, sb.booking_category, COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
GROUP BY 1, 2, 3;

SELECT DISTINCT , supplier_name
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.supplier_name LIKE '%Secret Escapes%';

SELECT DISTINCT has_flights
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs;

SELECT DISTINCT has_flights
FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.cash_credits_used_cc > 0;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.cms_mongodb.booking_summary_20210825 CLONE hygiene_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary_20210825 CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

DROP TABLE hygiene_vault_mvp.cms_mongodb.booking_summary;
DROP TABLE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;


self_describing_task --include 'dv/dwh/transactional/fact_booking.py'  --method 'run' --start '2021-08-24 00:00:00' --end '2021-08-24 00:00:00'
self_describing_task --include 'se/data/dwh/se_booking_summary_extended.py'  --method 'run' --start '2021-08-24 00:00:00' --end '2021-08-24 00:00:00'

SELECT *
FROM se.data.fact_booking fb
WHERE fb.inbound_flight_arrival_date IS NOT NULL;