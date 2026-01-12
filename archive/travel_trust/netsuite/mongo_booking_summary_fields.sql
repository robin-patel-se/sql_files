SELECT bs.record,
       bs.record['departureAirportCode']        AS departure_airport_code,
       bs.record['flightInboundArrivalDate']    AS flight_inbound_arrival_date,
       bs.record['flightOutboundDepartureDate'] AS flight_outbound_departure_date

FROM raw_vault_mvp.cms_mongodb.booking_summary bs
WHERE;


SELECT bs.record__o,
       bs.record__o['departureAirportCode']::VARCHAR AS departure_airport_code,
       bs.record__o['flightInboundArrivalDate']      AS flight_inbound_arrival_date,
       bs.record__o['flightOutboundDepartureDate']   AS flight_outbound_departure_date,
       *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.has_flights = 'true'

CREATE OR REPLACE TRANSIENT TABLE raw_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE raw_vault_mvp.cms_mongodb.booking_summary;
DROP TABLE hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary;
DROP TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary;

self_describing_task --include 'staging/hygiene/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-01-15 00:00:00' --end '2020-01-15 00:00:00'

SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary
WHERE has_flights = 'true';

self_describing_task --include 'staging/hygiene_snapshots/cms_mongodb/booking_summary.py'  --method 'run' --start '2020-01-15 00:00:00' --end '2020-01-15 00:00:00'


SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary
WHERE has_flights = 'true';


SELECT bs.record['totalSellRate']::VARCHAR,
       bs.record['nonCashCreditsUsed']::VARCHAR,
       bs.record['flightOnlyPrice']::VARCHAR,
       bs.record['flightOnlyPriceInSupplierCurrency']::VARCHAR,
       bs.record['flightVatOnCommission']::VARCHAR
FROM raw_vault_mvp.cms_mongodb.booking_summary bs;


SELECT *
FROM hygiene_vault_mvp_dev_robin.cms_mongodb.booking_summary
WHERE has_flights = 'true';

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.cms_mongodb.booking_summary_20210804 CLONE hygiene_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary_20210804 CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

DROP TABLE hygiene_vault_mvp.cms_mongodb.booking_summary;
DROP TABLE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

SELECT MIN(loaded_at)
FROM raw_vault_mvp.cms_mongodb.booking_summary bs -- 2020-01-15 17:14:57.464234000
;

airflow backfill --start_date '2020-01-15 00:00:00' --end_date '2020-01-16 00:00:00' --reset_dagruns --task_regex '.*' hygiene_snapshots__cms_mongodb__booking_summary__hourly
;
USE WAREHOUSE pipe_2xlarge;
;


WITH prod AS (
    SELECT 'production'                                      AS source,
           bs.no_nights,
           bs.rooms,
           bs.adult_guests,
           bs.child_guests,
           bs.infant_guests,
           bs.rate_to_gbp::DECIMAL(13, 4)                    AS rate_to_gbp,
           bs.cc_rate_to_sc::DECIMAL(13, 4)                  AS cc_rate_to_sc,
           bs.gbp_rate_to_sc::DECIMAL(13, 4)                 AS gbp_rate_to_sc,
           bs.last_updated,
           bs.date_time_booked,
           bs.booking_date,
           bs.check_in_timestamp,
           bs.check_in_date,
           bs.check_out_timestamp,
           bs.check_out_date,
           bs.booking_lead_time_days,
           bs.margin_gross_of_toms_cc::DECIMAL(13, 4)        AS margin_gross_of_toms_cc,
           bs.gross_revenue_cc::DECIMAL(13, 4)               AS gross_revenue_cc,
           bs.customer_total_price_cc::DECIMAL(13, 4)        AS customer_total_price_cc,
           bs.gross_booking_value_cc::DECIMAL(13, 4)         AS gross_booking_value_cc,
           bs.vat_on_commission_cc::DECIMAL(13, 4)           AS vat_on_commission_cc,
           bs.booking_fee_cc::DECIMAL(13, 4)                 AS booking_fee_cc,
           bs.booking_fee_net_rate_cc::DECIMAL(13, 4)        AS booking_fee_net_rate_cc,
           bs.payment_surcharge_cc::DECIMAL(13, 4)           AS payment_surcharge_cc,
           bs.payment_surcharge_net_rate_cc::DECIMAL(13, 4)  AS payment_surcharge_net_rate_cc,
           bs.commission_ex_vat_cc::DECIMAL(13, 4)           AS commission_ex_vat_cc,
           bs.insurance_commission_cc::DECIMAL(13, 4)        AS insurance_commission_cc,
           bs.flight_amount_cc::DECIMAL(13, 4)               AS flight_amount_cc,
           bs.flight_commission_cc::DECIMAL(13, 4)           AS flight_commission_cc,
           bs.credits_used_cc::DECIMAL(13, 4)                AS credits_used_cc,
           bs.total_custom_tax_cc::DECIMAL(13, 4)            AS total_custom_tax_cc,
           bs.atol_fee_cc::DECIMAL(13, 4)                    AS atol_fee_cc,
           bs.total_sell_rate_cc::DECIMAL(13, 4)             AS total_sell_rate_cc,
           bs.insurance_price_cc::DECIMAL(13, 4)             AS insurance_price_cc,

           bs.margin_gross_of_toms_gbp::DECIMAL(13, 4)       AS margin_gross_of_toms_gbp,
           bs.gross_revenue_gbp::DECIMAL(13, 4)              AS gross_revenue_gbp,
           bs.customer_total_price_gbp::DECIMAL(13, 4)       AS customer_total_price_gbp,
           bs.gross_booking_value_gbp::DECIMAL(13, 4)        AS gross_booking_value_gbp,
           bs.vat_on_commission_gbp::DECIMAL(13, 4)          AS vat_on_commission_gbp,
           bs.booking_fee_gbp::DECIMAL(13, 4)                AS booking_fee_gbp,
           bs.booking_fee_net_rate_gbp::DECIMAL(13, 4)       AS booking_fee_net_rate_gbp,
           bs.payment_surcharge_gbp::DECIMAL(13, 4)          AS payment_surcharge_gbp,
           bs.payment_surcharge_net_rate_gbp::DECIMAL(13, 4) AS payment_surcharge_net_rate_gbp,
           bs.commission_ex_vat_gbp::DECIMAL(13, 4)          AS commission_ex_vat_gbp,
           bs.insurance_commission_gbp::DECIMAL(13, 4)       AS insurance_commission_gbp,
           bs.flight_amount_gbp::DECIMAL(13, 4)              AS flight_amount_gbp,
           bs.flight_commission_gbp::DECIMAL(13, 4)          AS flight_commission_gbp,
           bs.credits_used_gbp::DECIMAL(13, 4)               AS credits_used_gbp,
           bs.total_custom_tax_gbp::DECIMAL(13, 4)           AS total_custom_tax_gbp,
           bs.atol_fee_gbp::DECIMAL(13, 4)                   AS atol_fee_gbp,
           bs.total_sell_rate_gbp::DECIMAL(13, 4)            AS total_sell_rate_gbp,
           bs.insurance_price_gbp::DECIMAL(13, 4)            AS insurance_price_gbp,

           bs.margin_gross_of_toms_sc::DECIMAL(13, 4)        AS margin_gross_of_toms_sc,
           bs.gross_revenue_sc::DECIMAL(13, 4)               AS gross_revenue_sc,
           bs.customer_total_price_sc::DECIMAL(13, 4)        AS customer_total_price_sc,
           bs.gross_booking_value_sc::DECIMAL(13, 4)         AS gross_booking_value_sc,
           bs.vat_on_commission_sc::DECIMAL(13, 4)           AS vat_on_commission_sc,
           bs.booking_fee_sc::DECIMAL(13, 4)                 AS booking_fee_sc,
           bs.booking_fee_net_rate_sc::DECIMAL(13, 4)        AS booking_fee_net_rate_sc,
           bs.payment_surcharge_sc::DECIMAL(13, 4)           AS payment_surcharge_sc,
           bs.payment_surcharge_net_rate_sc::DECIMAL(13, 4)  AS payment_surcharge_net_rate_sc,
           bs.commission_ex_vat_sc::DECIMAL(13, 4)           AS commission_ex_vat_sc,
           bs.insurance_commission_sc::DECIMAL(13, 4)        AS insurance_commission_sc,
           bs.flight_amount_sc::DECIMAL(13, 4)               AS flight_amount_sc,
           bs.flight_commission_sc::DECIMAL(13, 4)           AS flight_commission_sc,
           bs.credits_used_sc::DECIMAL(13, 4)                AS credits_used_sc,
           bs.total_custom_tax_sc::DECIMAL(13, 4)            AS total_custom_tax_sc,
           bs.atol_fee_sc::DECIMAL(13, 4)                    AS atol_fee_sc,
           bs.total_sell_rate_sc::DECIMAL(13, 4)             AS total_sell_rate_sc,
           bs.insurance_price_sc::DECIMAL(13, 4)             AS insurance_price_sc,
           bs.is_new_model_booking,
           bs.affiliate_user_id,
           bs.shiro_user_id,
           bs.device_platform,
           bs.booking_id,
           bs.customer_id,
           bs.currency,
           bs.sale_base_currency,
           bs.territory,
           bs.last_updated_v1,
           bs.last_updated_v2,
           bs.date_time_booked_v1,
           bs.date_time_booked_v2,
           bs.check_in_date_v1,
           bs.check_in_date_v2,
           bs.check_out_date_v1,
           bs.check_out_date_v2,
           bs.booking_type,
           bs.no_nights__o,
           bs.rooms__o,
           bs.adult_guests__o,
           bs.child_guests__o,
           bs.infant_guests__o,
           bs.vat_on_commission_cc_100,
           bs.customer_total_price_cc_100,
           bs.gross_booking_value_cc_100,
           bs.gross_booking_value_cc__o,
           bs.gross_booking_value_gbp__o,
           bs.gross_booking_value_sc__o,
           bs.commission_ex_vat_cc_100,
           bs.commission_ex_vat_sc_100,
           bs.booking_fee_cc_100,
           bs.booking_fee_net_rate_cc_100,
           bs.payment_surcharge_cc_100,
           bs.payment_surcharge_net_rate_cc_100,
           bs.insurance_commission_cc_100,
           bs.flight_amount_cc_100,
           bs.flight_commission_cc_100,
           bs.rate_to_gbp_100000,
           bs.total_custom_tax_cc_100,
           bs.atol_fee_cc_100,
           bs.total_sell_rate_cc_100,
           bs.insurance_price_cc_100,
           bs.customer_email,
           bs.sale_product,
           bs.sale_type,
           bs.booking_status,
           bs.affiliate,
           bs.affiliate_domain,
           bs.booking_class,
           bs.affiliate_id,
           bs.sale_id,
           bs.offer_id,
           bs.offer_name,
           bs.transaction_id,
           bs.bundle_id,
           bs.unique_transaction_reference,
           bs.has_flights,
           bs.supplier,
           bs.platform_name__o,
           bs.credits_used_cc_100,
           bs.insurance_provider,
           bs.payment_type,
           bs.record__o
    FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
),
     dev AS (
         SELECT 'development'                                     AS source,
                bs.no_nights,
                bs.rooms,
                bs.adult_guests,
                bs.child_guests,
                bs.infant_guests,
                bs.rate_to_gbp::DECIMAL(13, 4)                    AS rate_to_gbp,
                bs.cc_rate_to_sc::DECIMAL(13, 4)                  AS cc_rate_to_sc,
                bs.gbp_rate_to_sc::DECIMAL(13, 4)                 AS gbp_rate_to_sc,
                bs.last_updated,
                bs.date_time_booked,
                bs.booking_date,
                bs.check_in_timestamp,
                bs.check_in_date,
                bs.check_out_timestamp,
                bs.check_out_date,
                bs.booking_lead_time_days,
--        bs.flight_inbound_arrival_timestamp,
--        bs.flight_inbound_arrival_date,
--        bs.flight_outbound_departure_timestamp,
--        bs.flight_outbound_departure_date,
                bs.margin_gross_of_toms_cc::DECIMAL(13, 4)        AS margin_gross_of_toms_cc,
                bs.gross_revenue_cc::DECIMAL(13, 4)               AS gross_revenue_cc,
                bs.customer_total_price_cc::DECIMAL(13, 4)        AS customer_total_price_cc,
                bs.gross_booking_value_cc::DECIMAL(13, 4)         AS gross_booking_value_cc,
                bs.vat_on_commission_cc::DECIMAL(13, 4)           AS vat_on_commission_cc,
                bs.booking_fee_cc::DECIMAL(13, 4)                 AS booking_fee_cc,
                bs.booking_fee_net_rate_cc::DECIMAL(13, 4)        AS booking_fee_net_rate_cc,
                bs.payment_surcharge_cc::DECIMAL(13, 4)           AS payment_surcharge_cc,
                bs.payment_surcharge_net_rate_cc::DECIMAL(13, 4)  AS payment_surcharge_net_rate_cc,
                bs.commission_ex_vat_cc::DECIMAL(13, 4)           AS commission_ex_vat_cc,
                bs.insurance_commission_cc::DECIMAL(13, 4)        AS insurance_commission_cc,
                bs.flight_amount_cc::DECIMAL(13, 4)               AS flight_amount_cc,
                bs.flight_commission_cc::DECIMAL(13, 4)           AS flight_commission_cc,
                bs.credits_used_cc::DECIMAL(13, 4)                AS credits_used_cc,
                bs.total_custom_tax_cc::DECIMAL(13, 4)            AS total_custom_tax_cc,
                bs.atol_fee_cc::DECIMAL(13, 4)                    AS atol_fee_cc,
                bs.total_sell_rate_cc::DECIMAL(13, 4)             AS total_sell_rate_cc,
                bs.insurance_price_cc::DECIMAL(13, 4)             AS insurance_price_cc,
--        bs.non_cash_credits_used_cc,
--        bs.flight_only_price_cc,
--        bs.flight_vat_on_commission_cc,
                bs.margin_gross_of_toms_gbp::DECIMAL(13, 4)       AS margin_gross_of_toms_gbp,
                bs.gross_revenue_gbp::DECIMAL(13, 4)              AS gross_revenue_gbp,
                bs.customer_total_price_gbp::DECIMAL(13, 4)       AS customer_total_price_gbp,
                bs.gross_booking_value_gbp::DECIMAL(13, 4)        AS gross_booking_value_gbp,
                bs.vat_on_commission_gbp::DECIMAL(13, 4)          AS vat_on_commission_gbp,
                bs.booking_fee_gbp::DECIMAL(13, 4)                AS booking_fee_gbp,
                bs.booking_fee_net_rate_gbp::DECIMAL(13, 4)       AS booking_fee_net_rate_gbp,
                bs.payment_surcharge_gbp::DECIMAL(13, 4)          AS payment_surcharge_gbp,
                bs.payment_surcharge_net_rate_gbp::DECIMAL(13, 4) AS payment_surcharge_net_rate_gbp,
                bs.commission_ex_vat_gbp::DECIMAL(13, 4)          AS commission_ex_vat_gbp,
                bs.insurance_commission_gbp::DECIMAL(13, 4)       AS insurance_commission_gbp,
                bs.flight_amount_gbp::DECIMAL(13, 4)              AS flight_amount_gbp,
                bs.flight_commission_gbp::DECIMAL(13, 4)          AS flight_commission_gbp,
                bs.credits_used_gbp::DECIMAL(13, 4)               AS credits_used_gbp,
                bs.total_custom_tax_gbp::DECIMAL(13, 4)           AS total_custom_tax_gbp,
                bs.atol_fee_gbp::DECIMAL(13, 4)                   AS atol_fee_gbp,
                bs.total_sell_rate_gbp::DECIMAL(13, 4)            AS total_sell_rate_gbp,
                bs.insurance_price_gbp::DECIMAL(13, 4)            AS insurance_price_gbp,
--        bs.non_cash_credits_used_gbp,
--        bs.flight_only_price_gbp,
--        bs.flight_vat_on_commission_gbp,
                bs.margin_gross_of_toms_sc::DECIMAL(13, 4)        AS margin_gross_of_toms_sc,
                bs.gross_revenue_sc::DECIMAL(13, 4)               AS gross_revenue_sc,
                bs.customer_total_price_sc::DECIMAL(13, 4)        AS customer_total_price_sc,
                bs.gross_booking_value_sc::DECIMAL(13, 4)         AS gross_booking_value_sc,
                bs.vat_on_commission_sc::DECIMAL(13, 4)           AS vat_on_commission_sc,
                bs.booking_fee_sc::DECIMAL(13, 4)                 AS booking_fee_sc,
                bs.booking_fee_net_rate_sc::DECIMAL(13, 4)        AS booking_fee_net_rate_sc,
                bs.payment_surcharge_sc::DECIMAL(13, 4)           AS payment_surcharge_sc,
                bs.payment_surcharge_net_rate_sc::DECIMAL(13, 4)  AS payment_surcharge_net_rate_sc,
                bs.commission_ex_vat_sc::DECIMAL(13, 4)           AS commission_ex_vat_sc,
                bs.insurance_commission_sc::DECIMAL(13, 4)        AS insurance_commission_sc,
                bs.flight_amount_sc::DECIMAL(13, 4)               AS flight_amount_sc,
                bs.flight_commission_sc::DECIMAL(13, 4)           AS flight_commission_sc,
                bs.credits_used_sc::DECIMAL(13, 4)                AS credits_used_sc,
                bs.total_custom_tax_sc::DECIMAL(13, 4)            AS total_custom_tax_sc,
                bs.atol_fee_sc::DECIMAL(13, 4)                    AS atol_fee_sc,
                bs.total_sell_rate_sc::DECIMAL(13, 4)             AS total_sell_rate_sc,
                bs.insurance_price_sc::DECIMAL(13, 4)             AS insurance_price_sc,
--        bs.non_cash_credits_used_sc,
--        bs.flight_only_price_sc,
--        bs.flight_vat_on_commission_sc,
                bs.is_new_model_booking,
                bs.affiliate_user_id,
                bs.shiro_user_id,
                bs.device_platform,
                bs.booking_id,
                bs.customer_id,
                bs.currency,
                bs.sale_base_currency,
                bs.territory,
                bs.last_updated_v1,
                bs.last_updated_v2,
                bs.date_time_booked_v1,
                bs.date_time_booked_v2,
                bs.check_in_date_v1,
                bs.check_in_date_v2,
                bs.check_out_date_v1,
                bs.check_out_date_v2,
--        bs.flight_inbound_arrival_date_v1,
--        bs.flight_inbound_arrival_date_v2,
--        bs.flight_outbound_departure_date_v1,
--        bs.flight_outbound_departure_date_v2,
                bs.booking_type,
                bs.no_nights__o,
                bs.rooms__o,
                bs.adult_guests__o,
                bs.child_guests__o,
                bs.infant_guests__o,
                bs.vat_on_commission_cc_100,
                bs.customer_total_price_cc_100,
                bs.gross_booking_value_cc_100,
                bs.gross_booking_value_cc__o,
                bs.gross_booking_value_gbp__o,
                bs.gross_booking_value_sc__o,
                bs.commission_ex_vat_cc_100,
                bs.commission_ex_vat_sc_100,
                bs.booking_fee_cc_100,
                bs.booking_fee_net_rate_cc_100,
                bs.payment_surcharge_cc_100,
                bs.payment_surcharge_net_rate_cc_100,
                bs.insurance_commission_cc_100,
--        bs.non_cash_credits_used_cc_100,
--        bs.flight_only_price_cc_100,
--        bs.flight_only_price_sc_100,
--        bs.flight_vat_on_commission_cc_100,
                bs.flight_amount_cc_100,
                bs.flight_commission_cc_100,
                bs.rate_to_gbp_100000,
                bs.total_custom_tax_cc_100,
                bs.atol_fee_cc_100,
                bs.total_sell_rate_cc_100,
                bs.insurance_price_cc_100,
-- bs.non_cash_credits_used_cc_100,
-- bs.flight_only_price_cc_100,
-- bs.flight_only_price_sc_100,
-- bs.flight_vat_on_commission_cc_100,
                bs.customer_email,
                bs.sale_product,
                bs.sale_type,
                bs.booking_status,
                bs.affiliate,
                bs.affiliate_domain,
                bs.booking_class,
                bs.affiliate_id,
                bs.sale_id,
                bs.offer_id,
                bs.offer_name,
                bs.transaction_id,
                bs.bundle_id,
                bs.unique_transaction_reference,
                bs.has_flights,
                bs.supplier,
                bs.platform_name__o,
                bs.credits_used_cc_100,
                bs.insurance_provider,
                bs.payment_type,
--        bs.departure_airport_code,
--        bs.sale_closest_airport_code,
--        bs.vcc_reference,
--        bs.flight_carrier,
--        bs.flight_invoice_number,
                bs.record__o
         FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary bs
     ),
     dupes AS (
         SELECT *
         FROM prod
             EXCEPT
         SELECT *
         FROM dev
     ),
     stack AS (
         SELECT *
         FROM prod
         UNION ALL
         SELECT *
         FROM dev
     )
SELECT *
FROM stack
    INNER JOIN dupes ON stack.booking_id = dupes.booking_id
ORDER BY stack.booking_id
;



