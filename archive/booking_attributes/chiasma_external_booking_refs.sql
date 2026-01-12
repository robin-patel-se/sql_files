SELECT CASE
           WHEN db.source_id = 3 THEN 'TVL-' || db.booking_id
           WHEN db.source_id = 7 THEN 'AB-' || db.booking_id
           WHEN db.source_id = 8 THEN 'BX-' || db.booking_id
           END                                                                    AS booking_id,
       db.booking_id                                                              AS external_reference_id,    --Placeholder for future 3PPs
       dsta.status                                                                AS booking_status,
       ds.sale_id                                                                 AS se_sale_id,
       dc.customer_id                                                             AS customer_identifier,      --Email hashed on Chiasma, using customer_id instead
       fb.key_date_check_in                                                       AS check_in_date,
       fb.key_date_check_out                                                      AS check_out_date,
       datediff(DAY, fb.key_date_booked, check_in_date)                           AS booking_lead_time_days,
       fb.nights                                                                  AS no_nights,
       fb.rooms                                                                   AS rooms,
       fb.adults                                                                  AS adult_guests,
       fb.children                                                                AS child_guests,
       fb.infants                                                                 AS infant_guests,
       fb.key_date_booked                                                         AS booking_created_date,     --Chiasma's key_date_booked doesn't 1:1 match either of Snowflake's booking date columns
       fb.key_date_booked                                                         AS booking_completed_date,
       iff(dsta.status IS DISTINCT FROM 'Cancelled', NULL, fb.key_date_cancelled) AS booking_cancelled_date,
       fb.gross_revenue                                                           AS gross_revenue_gbp,        --Gross revenue not available in customer currency
       fb.margin_gross_of_toms                                                    AS margin_gross_of_toms_gbp, --Margin not present in customer currency
       dcu.currency_code                                                          AS customer_currency,
       --Supplier currency columns missing as data not present on Chiasma
       fb.derived_exchange_rate                                                   AS derived_exchange_rate,
       ds.territory_names                                                         AS territory,
       dp.platform_name                                                           AS device_platform,
       dpt.payment_type                                                           AS payment_type,
       ds.destination_type                                                        AS destination_type,
       dpr.product                                                                AS product_type,
       ds.country                                                                 AS posu_country,
       ds.city                                                                    AS posu_city,
       CASE
           WHEN db.source_id = 3 THEN 'Travelist'
           WHEN db.source_id = 7 THEN 'Air Berlin'
           WHEN db.source_id = 8 THEN 'BigXtra'
           ELSE 'Not Specified'
           END                                                                    AS provider_name
FROM data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_v_snapshot fb
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot db
                    ON fb.key_booking = db.key_booking
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_status_snapshot dsta
                    ON fb.key_status = dsta.key_status
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot ds
                    ON fb.key_sale = ds.key_sale
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_currencies_snapshot dcu
                    ON fb.key_currency = dcu.key_currency
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_customers_snapshot dc
                    ON fb.key_customer = dc.key_customer
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_platforms_snapshot dp
                    ON fb.key_platform = dp.key_platform
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_payment_types_snapshot dpt
                    ON fb.key_payment_type = dpt.key_payment_type
         INNER JOIN data_vault_mvp.chiasma_sql_server_snapshots.dim_products_snapshot dpr
                    ON ds.key_product = dpr.key_product
WHERE (db.source_id = 7 --Air Berlin
    OR db.source_id = 8 -- BigXtra
    OR (db.source_id = 3 AND db.transaction_id NOT LIKE 'TB%')) --Travelist

;

SELECT get_ddl('table', 'scratch.robinpatel.chiasma_external_bookings');

CREATE OR REPLACE TRANSIENT TABLE chiasma_external_bookings
(
    booking_id               VARCHAR,
    external_reference_id    VARCHAR,
    booking_status           VARCHAR,
    se_sale_id               VARCHAR,
    customer_identifier      VARCHAR,
    check_in_date            DATE,
    check_out_date           DATE,
    no_nights                NUMBER,
    rooms                    NUMBER,
    adult_guests             NUMBER,
    child_guests             NUMBER,
    infant_guests            NUMBER,
    booking_created_date     DATE,
    booking_completed_date   DATE,
    booking_cancelled_date   DATE,
    gross_revenue_gbp        FLOAT,
    margin_gross_of_toms_gbp FLOAT,
    customer_currency        VARCHAR,
    derived_exchange_rate    FLOAT,
    territory                VARCHAR,
    device_platform          VARCHAR,
    payment_type             VARCHAR,
    destination_type         VARCHAR,
    product_type             VARCHAR,
    posu_country             VARCHAR,
    posu_city                VARCHAR,
    provider_name            VARCHAR
);

SELECT DISTINCT tech_platform
FROM se.data.fact_booking fb
;


self_describing_task --include 'dv/dwh/chiasma/external_booking.py'  --method 'run' --start '2020-12-09 00:00:00' --end '2020-12-09 00:00:00'


CREATE SCHEMA data_vault_mvp_dev_robin.chiasma_sql_server_snapshots;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.fact_bookings_v_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_v_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_bookings_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_status_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_status_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_sales_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_currencies_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_currencies_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_customers_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_customers_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_platforms_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_platforms_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_payment_types_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_payment_types_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_products_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_products_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.business_units_snapshot CLONE data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.chiasma_external_bookings
WHERE chiasma_external_bookings.device_platform != 'Not Specified';


SELECT cel.booking_id,
       cel.booking_status,
       CASE
           WHEN cel.booking_status = 'Booked' THEN 'live'
           WHEN cel.booking_status = 'Cancelled' THEN 'cancelled'
           ELSE 'other'
           END                                                      AS booking_status_type,
       cel.se_sale_id,
       NULL                                                         AS shiro_user_id,
       cel.check_in_date,
       cel.check_out_date,
       cel.booking_lead_time_days,
       cel.booking_created_date,
       cel.booking_completed_date,
       cel.booking_completed_date                                   AS booking_transaction_completed_date,
       cel.gross_revenue_gbp,
       cel.gross_revenue_gbp_constant_currency,
       cel.gross_revenue_eur_constant_currency,
       NULL                                                         AS customer_total_price_gbp,
       NULL                                                         AS customer_total_price_gbp_constant_currency,
       NULL                                                         AS gross_booking_value_gbp,
       NULL                                                         AS commission_ex_vat_gbp,
       NULL                                                         AS booking_fee_net_rate_gbp,
       NULL                                                         AS payment_surcharge_net_rate_gbp,
       NULL                                                         AS insurance_commission_gbp,
       NULL                                                         AS margin_gross_of_toms_gbp,
       cel.margin_gross_of_toms_gbp,
       cel.margin_gross_of_toms_gbp_constant_currency,
       cel.margin_gross_of_toms_eur_constant_currency,
       cel.no_nights,
       cel.adult_guests,
       cel.child_guests,
       cel.infant_guests,
       cel.price_per_night,
       cel.price_per_person_per_night,
       cel.rooms,
       cel.device_platform,
       NULL                                                         AS booking_full_payment_complete,
       cel.booking_cancelled_date,
       NULL                                                         AS cancellation_reason,
       cel.territory,
       cel.posu_country,
       se.data.se_sale_travel_type(cel.territory, cel.posu_country) AS travel_type,
       cel.tech_platform

FROM data_vault_mvp_dev_robin.dwh.chiasma_external_bookings cel;

SELECT DISTINCT
       territory,
       cel.posu_country,
       se.data.se_sale_travel_type(cel.territory, cel.posu_country) AS travel_type
FROM data_vault_mvp_dev_robin.dwh.chiasma_external_bookings cel;


SELECT fb.booking_id,
       fb.booking_status,
       fb.booking_status_type,
       fb.se_sale_id,
       fb.shiro_user_id,
       fb.check_in_date,
       fb.check_out_date,
       fb.booking_lead_time_days,
       fb.booking_created_date,
       fb.booking_completed_date,
       fb.booking_transaction_completed_date,
       fb.gross_revenue_gbp,
       fb.gross_revenue_gbp_constant_currency,
       fb.gross_revenue_eur_constant_currency,
       fb.customer_total_price_gbp,
       fb.customer_total_price_gbp_constant_currency,
       fb.gross_booking_value_gbp,
       fb.commission_ex_vat_gbp,
       fb.booking_fee_net_rate_gbp,
       fb.payment_surcharge_net_rate_gbp,
       fb.insurance_commission_gbp,
       fb.margin_gross_of_toms_gbp,
       fb.margin_gross_of_toms_gbp_constant_currency,
       fb.margin_gross_of_toms_eur_constant_currency,
       fb.no_nights,
       fb.adult_guests,
       fb.child_guests,
       fb.infant_guests,
       fb.price_per_night,
       fb.price_per_person_per_night,
       fb.rooms,
       fb.device_platform,
       fb.booking_full_payment_complete,
       fb.cancellation_date,
       fb.cancellation_reason,
       fb.territory,
       fb.travel_type,
       fb.tech_platform
FROM se.data.fact_booking fb;

SELECT cel.schedule_tstamp,
       cel.run_tstamp,
       cel.operation_id,
       cel.created_at,
       cel.updated_at,
       cel.booking_id,
       cel.external_reference_id,
       cel.booking_status,
       cel.se_sale_id,
       cel.customer_identifier,
       cel.check_in_date,
       cel.check_out_date,
       cel.booking_lead_time_days,
       cel.booking_created_date,
       cel.booking_completed_date,
       cel.booking_cancelled_date,
       cel.rate_to_gbp,
       cel.customer_currency,
       cel.gross_revenue_cc,
       cel.margin_gross_of_toms_cc,
       cel.gross_revenue_gbp,
       cel.gross_revenue_gbp_constant_currency,
       cel.gross_revenue_eur_constant_currency,
       cel.margin_gross_of_toms_gbp,
       cel.margin_gross_of_toms_gbp_constant_currency,
       cel.margin_gross_of_toms_eur_constant_currency,
       cel.no_nights,
       cel.adult_guests,
       cel.child_guests,
       cel.infant_guests,
       cel.rooms,
       cel.territory,
       cel.device_platform,
       cel.payment_type,
       cel.destination_type,
       cel.product_type,
       cel.posu_country,
       cel.posu_city,
       cel.provider_name,
       cel.price_per_night,
       cel.price_per_person_per_night
FROM data_vault_mvp_dev_robin.dwh.chiasma_external_bookings cel;

self_describing_task --include 'se/data/dwh/chiasma_external_booking.py'  --method 'run' --start '2020-12-09 00:00:00' --end '2020-12-09 00:00:00'


SELECT cel.booking_id,
       cel.external_reference_id,
       cel.booking_status,
       cel.se_sale_id,
       cel.customer_identifier,
       cel.check_in_date,
       cel.check_out_date,
       cel.booking_lead_time_days,
       cel.booking_created_date,
       cel.booking_completed_date,
       cel.booking_cancelled_date,
       cel.rate_to_gbp,
       cel.customer_currency,
       cel.gross_revenue_cc,
       cel.margin_gross_of_toms_cc,
       cel.gross_revenue_gbp,
       cel.gross_revenue_gbp_constant_currency,
       cel.gross_revenue_eur_constant_currency,
       cel.margin_gross_of_toms_gbp,
       cel.margin_gross_of_toms_gbp_constant_currency,
       cel.margin_gross_of_toms_eur_constant_currency,
       cel.no_nights,
       cel.adult_guests,
       cel.child_guests,
       cel.infant_guests,
       cel.rooms,
       cel.territory,
       cel.device_platform,
       cel.payment_type,
       cel.destination_type,
       cel.product_type,
       cel.posu_country,
       cel.posu_city,
       cel.provider_name,
       cel.price_per_night,
       cel.price_per_person_per_night,
       se.data.se_sale_travel_type(cel.territory, cel.posu_country) AS travel_type
FROM data_vault_mvp_dev_robin.dwh.chiasma_external_booking cel;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.chiasma_external_booking;

self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2020-12-09 00:00:00' --end '2020-12-09 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;

SELECT DISTINCT fb.tech_platform
FROM se_dev_robin.data.fact_booking fb;

SELECT *
FROM se_dev_robin.data.fact_booking fb
WHERE fb.tech_platform LIKE 'CHIASMA%';

SELECT DISTINCT fcb.tech_platform
FROM se_dev_robin.data.fact_complete_booking fcb;

SELECT *
FROM se_dev_robin.data.chiasma_external_booking ceb
WHERE ceb.tech_platform = 'CHIASMA_TRAVELIST'

SELECT *
FROM se_dev_robin.data.fact_complete_booking fcb
WHERE fcb.booking_id LIKE 'TVL%'


SELECT *
FROM data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot;

self_describing_task --include 'dv/dwh/chiasma/external_booking.py'  --method 'run' --start '2020-12-13 00:00:00' --end '2020-12-13 00:00:00'