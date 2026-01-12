--total_adjustment = Customer total price FOR ONLY THE FLIGHT/BAG ELEMENT, how much the customer paid in totoal
--supplier currency is for the supplier amount
--adjustment currency is for the total_adjustment
SELECT client_order_reference,
       SUM(total_adjustment)                                                    AS total_flight_service_customer_price,
       SUM(supplier_amount)                                                     AS total_flight_service_supplier_cost,
       total_flight_service_customer_price - total_flight_service_supplier_cost AS flight_margin
FROM hygiene_snapshot_vault_mvp.flightservice_mysql.orders_orderchange
WHERE client_order_reference IN (
                                 'A22309-21844-7377013',
                                 'A13954-13892-7341494',
                                 'A13001-13430-7332109'
    )
GROUP BY 1
;

WITH window_functions AS (
    SELECT DISTINCT
           oo.client_order_reference,
           LAST_VALUE(oo.original_departure_datetime) IGNORE NULLS OVER (PARTITION BY oo.client_order_reference ORDER BY oo.created_at_dts ) AS original_outbound_flight_departure_timestamp,
           LAST_VALUE(oo.original_return_datetime) IGNORE NULLS OVER (PARTITION BY oo.client_order_reference ORDER BY oo.created_at_dts )    AS original_inbound_flight_arrival_timestamp,
           LAST_VALUE(oo.new_departure_datetime) IGNORE NULLS OVER (PARTITION BY oo.client_order_reference ORDER BY oo.created_at_dts )      AS outbound_flight_departure_timestamp,
           LAST_VALUE(oo.new_return_datetime) IGNORE NULLS OVER (PARTITION BY oo.client_order_reference ORDER BY oo.created_at_dts )         AS inbound_flight_arrival_timestamp,
           LAST_VALUE(oo.supplier_reference) IGNORE NULLS OVER (PARTITION BY oo.client_order_reference ORDER BY oo.created_at_dts )          AS supplier_reference,
           MAX(oo.created_at_dts) OVER (PARTITION BY oo.client_order_reference)                                                              AS flight_adjustment_last_updated
    FROM data_vault_mvp_dev_robin.dwh.flightservice__order_orderchange oo
--TODO add inner join
-- WHERE client_order_reference = 'A22309-21844-7377013'
)
SELECT oo.client_order_reference,
       wf.original_outbound_flight_departure_timestamp,
       wf.original_inbound_flight_arrival_timestamp,
       wf.outbound_flight_departure_timestamp,
       wf.inbound_flight_arrival_timestamp,
       wf.supplier_reference,
       wf.flight_adjustment_last_updated,
       LISTAGG(DISTINCT oo.component_type, ', ') WITHIN GROUP (ORDER BY oo.component_type)           AS list_of_flight_component_types,
       LISTAGG(DISTINCT oo.component_reference, ', ') WITHIN GROUP (ORDER BY oo.component_reference) AS list_of_flight_component_references,
       LISTAGG(DISTINCT oo.change_type, ', ') WITHIN GROUP (ORDER BY oo.change_type)                 AS list_of_flight_change_types,
       LISTAGG(DISTINCT oo.adjustment_reason, ', ') WITHIN GROUP (ORDER BY oo.adjustment_reason)     AS list_of_flight_adjustment_reasons,
       LISTAGG(DISTINCT oo.carriers, ', ') WITHIN GROUP (ORDER BY oo.carriers)                       AS list_of_flight_carriers,
       LISTAGG(DISTINCT oo.supplier_name, ', ') WITHIN GROUP (ORDER BY oo.supplier_name)             AS list_of_flight_supplier_names,
       LISTAGG(DISTINCT oo.supplier_currency, ', ') WITHIN GROUP (ORDER BY oo.supplier_currency)     AS list_of_flight_supplier_currencies,
       LISTAGG(DISTINCT oo.adjustment_currency, ', ') WITHIN GROUP (ORDER BY oo.adjustment_currency) AS list_of_flight_adjustment_currencies,
       SUM(total_adjustment_gbp)                                                                     AS total_flight_service_customer_price,
       SUM(supplier_amount_gbp)                                                                      AS total_flight_service_supplier_cost
FROM data_vault_mvp_dev_robin.dwh.flightservice__order_orderchange oo
    INNER JOIN window_functions wf ON oo.client_order_reference = wf.client_order_reference
--TODO add inner join
GROUP BY 1, 2, 3, 4, 5, 6, 7;



SELECT oo.id,
       oo.created_at_dts,
       oo.client_order_reference,
       oo.component_reference,
       oo.client_platform,
       oo.booking_datetime,
       oo.original_departure_datetime,
       oo.original_return_datetime,
       oo.new_departure_datetime,
       oo.new_return_datetime,
       oo.component_type,
       oo.carriers,
       oo.supplier_name,
       oo.supplier_id,
       oo.supplier_reference,
       oo.supplier_currency,
       COALESCE(oo.supplier_amount, 0)                           AS supplier_amount,
       COALESCE(sr.fx_rate, 1)                                   AS supplier_rate_to_gbp,
       COALESCE(oo.supplier_amount * supplier_rate_to_gbp, 0)    AS supplier_amount_gbp,
       oo.change_type,
       oo.change_type_2,
       oo.booking_status,
       oo.component_status,
       oo.adjustment_reference,
       oo.adjustment_reason,
       COALESCE(oo.total_adjustment, 0)                          AS total_adjustment,
       oo.adjustment_currency,
       COALESCE(ac.fx_rate, 1)                                   AS adjustment_rate_to_gbp,
       COALESCE(oo.total_adjustment * adjustment_rate_to_gbp, 0) AS total_adjustment_gbp,
       oo.vat_applicable_on_adjustment,
       oo.ticketing_fees,
       oo.order_id
FROM hygiene_snapshot_vault_mvp.flightservice_mysql.orders_orderchange oo
    -- currency exchange rate do not exist within data so using Kantox rates to convert to standardised currency
    LEFT JOIN data_vault_mvp.fx.rates sr ON oo.supplier_currency = sr.source_currency
    AND oo.created_at_dts::DATE = sr.fx_date
    AND sr.target_currency = 'GBP'
                  -- currency exchange rate do not exist within data so using Kantox rates to convert to standardised currency
    LEFT JOIN data_vault_mvp.fx.rates ac ON oo.adjustment_currency = ac.source_currency
    AND oo.created_at_dts::DATE = ac.fx_date
    AND ac.target_currency = 'GBP'
-- WHERE client_order_reference = 'A22309-21844-7377013'
;

SELECT GET_DDL('table', 'hygiene_snapshot_vault_mvp.flightservice_mysql.orders_orderchange');

CREATE OR REPLACE TABLE orders_orderchange
(
-- (lineage) metadata for the current job
    schedule_tstamp              TIMESTAMP,
    run_tstamp                   TIMESTAMP,
    operation_id                 VARCHAR,
    created_at                   TIMESTAMP,
    updated_at                   TIMESTAMP,
    id                           NUMBER,
    created_at_dts               TIMESTAMP_NTZ,
    client_order_reference       VARCHAR,
    component_reference          VARCHAR,
    client_platform              VARCHAR,
    booking_datetime             TIMESTAMP_NTZ,
    original_departure_datetime  TIMESTAMP_NTZ,
    original_return_datetime     TIMESTAMP_NTZ,
    new_departure_datetime       TIMESTAMP_NTZ,
    new_return_datetime          TIMESTAMP_NTZ,
    component_type               VARCHAR,
    carriers                     VARCHAR,
    supplier_name                VARCHAR,
    supplier_id                  NUMBER,
    supplier_reference           VARCHAR,
    supplier_currency            VARCHAR,
    supplier_amount              NUMBER,
    change_type                  VARCHAR,
    change_type_2                VARCHAR,
    booking_status               VARCHAR,
    component_status             VARCHAR,
    adjustment_reference         VARCHAR,
    adjustment_reason            VARCHAR,
    total_adjustment             NUMBER,
    adjustment_currency          VARCHAR,
    vat_applicable_on_adjustment NUMBER,
    ticketing_fees               NUMBER,
    order_id                     NUMBER,
    CONSTRAINT pk_1 PRIMARY KEY (id)
);


self_describing_task --include 'dv/dwh/flight_service/orders_orderchange.py'  --method 'run' --start '2022-01-30 00:00:00' --end '2022-01-30 00:00:00'


USE WAREHOUSE pipe_xlarge;

SELECT *
FROM hygiene_snapshot_vault_mvp.flightservice_mysql.orders_orderchange;

SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE sua.shiro_user_id = 51378313;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su
WHERE su.id = 51378313;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.date_time_booked >= CURRENT_DATE - 1;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.allocation CLONE hygiene_snapshot_vault_mvp.cms_mysql.allocation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.amendment CLONE hygiene_snapshot_vault_mvp.cms_mysql.amendment;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.booking_allocations_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.booking_allocations_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.days_before_policy CLONE hygiene_snapshot_vault_mvp.cms_mysql.days_before_policy;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.offer_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.offer_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.product_reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.product_reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_base_offer CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_base_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_exchange_rate CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_exchange_rate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_details CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_details;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.flightservice_mysql.orders_orderchange CLONE hygiene_snapshot_vault_mvp.flightservice_mysql.orders_orderchange;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation CLONE data_vault_mvp.dwh.booking_cancellation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit;

DROP SCHEMA data_vault_mvp_dev_robin.dwh;

self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2022-02-08 00:00:00' --end '2022-02-08 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking__step05__collate_fields;
SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.se_booking__step05__collate_fields');

SELECT *
FROM se.data.se_booking sb
WHERE sb.has_flights;

CREATE OR REPLACE TRANSIENT TABLE se_booking__step05__collate_fields
(
    client_order_reference                       VARCHAR,
    original_outbound_flight_departure_timestamp TIMESTAMP,
    original_inbound_flight_arrival_timestamp    TIMESTAMP,
    outbound_flight_departure_timestamp          TIMESTAMP,
    inbound_flight_arrival_timestamp             TIMESTAMP,
    supplier_reference                           VARCHAR,
    flight_adjustment_last_updated               TIMESTAMP,
    list_of_flight_component_types               VARCHAR,
    list_of_flight_component_references          VARCHAR,
    list_of_flight_change_types                  VARCHAR,
    list_of_flight_adjustment_reasons            VARCHAR,
    list_of_flight_carriers                      VARCHAR,
    list_of_flight_supplier_names                VARCHAR,
    list_of_flight_supplier_currencies           VARCHAR,
    list_of_flight_adjustment_currencies         VARCHAR,
    total_flight_service_customer_price_gbp      NUMBER,
    total_flight_service_supplier_cost_gbp       NUMBER,
    flight_commission                            NUMBER,
);

SELECT ds.sale_type,
       ds.sale_product,
       COUNT(*)
FROM se.data.dim_sale ds
WHERE ds.sale_active
GROUP BY 1, 2;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking__step04__model_flight_service_data;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking
WHERE booking_id IN (
                     'A7341494',
                     'A7377013',
                     'A5642131',
                     'A5284242',
                     'A7709196',
                     'A5233910',
                     'A7633341',
                     'A5586276',
                     'A5321341',
                     'A7332109',
                     'A5596057',
                     'A5644928',
                     'A5211540',
                     'A7487432',
                     'A5157726',
                     'A5755861',
                     'A7504565',
                     'A5334116',
                     'A7709185',
                     'A5247016',
                     'A5673893',
                     'A7559737'
    );

SELECT *
FROM hygiene_snapshot_vault_mvp.flightservice_mysql.orders_orderchange oo
WHERE oo.client_order_reference IN (
                                    'A11767-15020-5284242',
                                    'A14331-14792-5157726',
                                    'A27468-18855-7504565',
                                    'A12830-13352-7487432',
                                    'A11767-15020-5673893',
                                    'A11767-15017-5644928',
                                    'A28942-19322-7709196',
                                    'A11767-15017-5642131',
                                    'A11767-21801-5233910',
                                    'A26702-18568-7559737',
                                    'A11767-15017-5334116',
                                    'A13001-13430-5321341',
                                    'A13954-13892-7341494',
                                    'A11767-15021-5586276',
                                    'A13001-13430-7332109',
                                    'A11767-15017-5755861',
                                    'A22309-21843-7709185',
                                    'A15414-14612-5211540',
                                    'A11767-15017-5247016',
                                    'A22309-21844-7377013',
                                    'A11767-15020-5596057',
                                    'A22075-21957-7633341'
    );


self_describing_task --include 'se/data/dwh/se_booking.py'  --method 'run' --start '2022-02-09 00:00:00' --end '2022-02-09 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking__step01__list_of_booking_ids;

SELECT *
FROM se.data.se_booking sb
WHERE sb.transaction_id IN (
                            'A13001-13430-7332109',
                            'A13001-13430-7332109',
                            'A13954-13892-7341494',
                            'A13954-13892-7341494',
                            'A13001-13430-7332109',
                            'A13954-13892-7341494',
                            'A22309-21844-7377013',
                            'A22309-21844-7377013',
                            'A22309-21844-7377013',
                            'A12830-13352-7487432',
                            'A12830-13352-7487432',
                            'A27468-18855-7504565',
                            'A27468-18855-7504565',
                            'A12830-13352-7487432',
                            'A27468-18855-7504565',
                            'A26702-18568-7559737',
                            'A26702-18568-7559737',
                            'A26702-18568-7559737',
                            'A22075-21957-7633341',
                            'A22075-21957-7633341',
                            'A22075-21957-7633341',
                            'A22309-21843-7709185',
                            'A22309-21843-7709185',
                            'A28942-19322-7709196',
                            'A28942-19322-7709196',
                            'A22309-21843-7709185',
                            'A28942-19322-7709196')


SELECT * FROm data_vault_mvp_dev_robin.dwh.se_booking;


SELECT *
FROM se.data.se_booking sb
WHERE sb.transaction_id IN (
                            'A13001-13430-7332109',
                            'A13001-13430-7332109',
                            'A13954-13892-7341494',
                            'A13954-13892-7341494',
                            'A13001-13430-7332109',
                            'A13954-13892-7341494',
                            'A22309-21844-7377013',
                            'A22309-21844-7377013',
                            'A22309-21844-7377013',
                            'A12830-13352-7487432',
                            'A12830-13352-7487432',
                            'A27468-18855-7504565',
                            'A27468-18855-7504565',
                            'A12830-13352-7487432',
                            'A27468-18855-7504565',
                            'A26702-18568-7559737',
                            'A26702-18568-7559737',
                            'A26702-18568-7559737',
                            'A22075-21957-7633341',
                            'A22075-21957-7633341',
                            'A22075-21957-7633341',
                            'A22309-21843-7709185',
                            'A22309-21843-7709185',
                            'A28942-19322-7709196',
                            'A28942-19322-7709196',
                            'A22309-21843-7709185',
                            'A28942-19322-7709196')


self_describing_task --include 'se/bi/scv/customer_yearly_first_session.py'  --method 'run' --start '2022-02-20 00:00:00' --end '2022-02-20 00:00:00'