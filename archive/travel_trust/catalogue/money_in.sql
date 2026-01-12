SET date_var = CURRENT_DATE - 10;

-- MONEY IN V1
CREATE OR REPLACE VIEW collab.travel_trust.money_in COPY GRANTS AS
(
WITH flight_information AS (
    SELECT toi.order_id,
           COUNT(DISTINCT toi.order_item_id)                                                AS flight_order_items,
           LISTAGG(DISTINCT toi.partner_name, '|')                                          AS flight_partner_name,
           LISTAGG(DISTINCT ofs.provider, '|')                                              AS flight_provider,
           LISTAGG(ffs.validating_airline_id, '|') WITHIN GROUP ( ORDER BY toi.start_date ) AS flight_airline_id,
           LISTAGG(
                   REGEXP_REPLACE(toi.flight_reservation_number, ' ?[\\/|,|-] ?', '|')
               , '|') WITHIN GROUP ( ORDER BY toi.start_date )                              AS flight_pnr
    FROM se.data.tb_order_item toi
             INNER JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_flightorderitem_snapshot ofs
                        ON toi.order_item_id = ofs.orderitembase_ptr_id
             INNER JOIN data_vault_mvp.travelbird_mysql_snapshots.flights_flightproduct_snapshot ffs
                        ON ofs.flight_product_id = ffs.id
    WHERE toi.order_item_type = 'FLIGHT'
    GROUP BY 1
),
     travel_trust_bookings AS (
         --shortlist of bookings that need to be protected by trust
         --these are currently defined as any package booking that is sold in the UK
         --and includes flights
         SELECT t.order_id,
                t.booking_id,
                t.created_at_dts         AS date_booked_tstamp,
                t.transaction_id         AS booking_transaction_id,
                t.offer_id               AS booking_offer_id,
                t.se_sale_id             AS booking_se_sale_id,
                t.sold_price_currency    AS booking_sold_price_currency,
                t.sold_price_total_cc    AS booking_sold_price_total_cc,
                t.cost_price_total_cc    AS booking_cost_price_total_cc,
                t.flight_price_cc        AS booking_flight_price_cc,
                t.margin_cc              AS booking_margin_cc,
                t.is_atol_bonded_booking AS booking_is_atol_bonded_booking,
                t.booking_includes_flight,
                t.travel_date,
                t.return_date,
                fi.flight_order_items    AS booking_flight_order_items,
                fi.flight_partner_name   AS booking_flight_partner_name,
                fi.flight_provider       AS booking_flight_provider,
                fi.flight_airline_id     AS booking_flight_airline_id,
                fi.flight_pnr            AS booking_flight_pnr
         FROM se.data.tb_booking t
                  LEFT JOIN flight_information fi ON t.order_id = fi.order_id
         WHERE t.territory = 'UK'
           AND t.booking_includes_flight
     )

SELECT scob.transaction_id,
       scob.transaction_tstamp,
       scob.payment_service_provider,
       scob.payment_service_provider_transaction_type,
       scob.cashflow_direction,
       scob.cashflow_type,
       scob.transaction_amount,
       scob.transaction_currency,
       scob.orders_paymemt_classification,
       ttb.*
FROM se.finance.stripe_cash_on_booking scob
         INNER JOIN travel_trust_bookings ttb ON scob.booking_id = ttb.booking_id
WHERE scob.captured -- secured/approved money transaction --Ask finance to confirm

UNION ALL

SELECT topc.transaction_id,
       topc.transaction_tstamp,
       topc.payment_service_provider,
       topc.payment_service_provider_transaction_type,
       topc.cashflow_direction,
       topc.cashflow_type,
       topc.amount,
       topc.currency,
       topc.orders_paymemt_classification,
       ttb.*
FROM se.finance.tb_order_payment_coupon topc
         INNER JOIN travel_trust_bookings ttb ON topc.booking_id = ttb.booking_id

    );

------------------------------------------------------------------------------------------------------------------------
--MONEY IN V2

CREATE OR REPLACE VIEW collab.travel_trust.money_in COPY GRANTS AS
(
WITH travel_trust_bookings AS (
    --shortlist of bookings that need to be protected by trust
    --these are currently defined as any package booking that is sold in the UK
    --and includes flights
    SELECT t.booking_id
    FROM se.data.tb_booking t
    WHERE t.territory = 'UK'
      AND t.booking_includes_flight
)

SELECT scob.transaction_id,
       scob.transaction_tstamp,
       scob.payment_service_provider,
       scob.payment_service_provider_transaction_type,
       scob.cashflow_direction,
       scob.cashflow_type,
       scob.transaction_amount,
       scob.transaction_currency,
       scob.orders_paymemt_classification,
       scob.booking_id
FROM se.finance.stripe_cash_on_booking scob
         INNER JOIN travel_trust_bookings ttb ON scob.booking_id = ttb.booking_id
WHERE scob.captured -- secured/approved money transaction --Ask finance to confirm

UNION ALL

SELECT topc.transaction_id,
       topc.transaction_tstamp,
       topc.payment_service_provider,
       topc.payment_service_provider_transaction_type,
       topc.cashflow_direction,
       topc.cashflow_type,
       topc.amount,
       topc.currency,
       topc.orders_paymemt_classification,
       topc.booking_id
FROM se.finance.tb_order_payment_coupon topc
         INNER JOIN travel_trust_bookings ttb ON topc.booking_id = ttb.booking_id

    );


GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.money_in TO ROLE personal_role__roshnidattani;

GRANT SELECT ON ALL VIEWS IN SCHEMA collab.travel_trust TO ROLE personal_role__roshnidattani;

GRANT USAGE ON SCHEMA collab.travel_trust TO ROLE personal_role__roshnidattani;


------------------------------------------------------------------------------------------------------------------------
--find ammendment with flight and accommodation
SELECT *
FROM collab.travel_trust.money_in mi
    QUALIFY COUNT(*) OVER (PARTITION BY mi.booking_id ) > 1;


SELECT *
FROM collab.travel_trust.money_in mi
WHERE mi.booking_id = 'TB-21876575';

SELECT *
FROM se.data.tb_order_item toi
         INNER JOIN se.data.tb_booking tb ON toi.order_id = tb.order_id
WHERE toi.event_type_category = 'booking_amendment'
  AND tb.territory = 'UK'
    QUALIFY COUNT(*) OVER (PARTITION BY toi.event_id) > 1
ORDER BY event_id;

------------------------------------------------------------------------------------------------------------------------

SELECT mi.transaction_id,
       mi.transaction_tstamp,
       mi.payment_service_provider,
       mi.payment_service_provider_transaction_type,
       mi.cashflow_direction,
       mi.cashflow_type,
       mi.transaction_amount,
       mi.transaction_currency,
       mi.orders_paymemt_classification,
       mi.booking_id
FROM collab.travel_trust.money_in mi
WHERE mi.booking_id = 'TB-21905826';

------------------------------------------------------------------------------------------------------------------------
--order item query
CREATE OR REPLACE VIEW collab.travel_trust.travel_trust_booking_components COPY GRANTS AS
(
SELECT tb.booking_id,
       tb.created_at_dts         AS booking_completed_tstamp,
       tb.travel_date,
       tb.return_date,
       toi.event_created_tstamp  AS order_item_created_tstammp,
       toi.event_type_category,
       toi.order_item_id,
       toi.order_item_type,
       toi.partner_name,
       ofs.provider              AS flight_provider,
       ffs.validating_airline_id AS airline_code,
       toi.flight_reservation_number,
       toi.sold_price_currency,
       toi.sold_price_incl_vat,
       toi.cost_price_excl_vat
FROM se.data.tb_order_item toi
         INNER JOIN se.data.tb_booking tb ON toi.order_id = tb.order_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_flightorderitem_snapshot ofs
                   ON toi.order_item_id = ofs.orderitembase_ptr_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.flights_flightproduct_snapshot ffs
                   ON ofs.flight_product_id = ffs.id
    );

SELECT GET_DDL('table', 'data_vault_mvp.travelbird_mysql_snapshots.flights_flightproduct_snapshot');


GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.travel_trust_booking_components TO ROLE personal_role__roshnidattani;

SELECT *
FROM collab.travel_trust.money_in mi;
SELECT *
FROM collab.travel_trust.travel_trust_booking_components ttbc;

self_describing_task --include 'se/finance/travel_trust/travel_trust_money_in.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_order_payment_coupon CLONE data_vault_mvp.dwh.tb_order_payment_coupon;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.stripe_cash_on_booking CLONE data_vault_mvp.dwh.stripe_cash_on_booking;

SELECT *
FROM data_vault_mvp.dwh.tb_order_payment_coupon topc;

self_describing_task --include 'se/finance/travel_trust/travel_trust_booking_components.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_flightorderitem CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_flightorderitem;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.flights_flightproduct CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.flights_flightproduct;

------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------

SELECT scratch.robinpatel.travel_trust_booking(219005382)

CREATE OR REPLACE FUNCTION scratch.robinpatel.travel_trust_booking(order_id INT
                                                                  )
    RETURNS BOOLEAN
AS
$$
    SELECT IFF(order_id IN (
        SELECT tb.order_id
        FROM data_vault_mvp.dwh.tb_booking tb
        WHERE tb.territory = 'UK'
          AND tb.booking_includes_flight
    ), TRUE, FALSE) AS travel_trust_booking
$$
    self_describing_task --include 'se/finance/travel_trust/travel_trust_booking_components.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00'


SELECT tb.booking_id,
       tb.created_at_dts        AS booking_completed_tstamp,
       tb.travel_date           AS booking_travel_date,
       tb.return_date           AS booking_return_date,
       toi.event_created_tstamp AS order_item_created_tstammp,
       toi.start_date           AS order_item_start_date,
       toi.end_date             AS order_item_end_date,
       toi.event_type_category,
       toi.order_item_id,
       toi.order_item_type,
       toi.partner_name,
       foi.provider             AS flight_provider,
       ff.validating_airline_id AS airline_code,
       toi.flight_reservation_number,
       toi.sold_price_currency,
       toi.sold_price_incl_vat,
       toi.cost_price_excl_vat
FROM se_dev_robin.data.tb_order_item toi
         INNER JOIN data_vault_mvp_dev_robin.dwh.tb_booking tb ON toi.order_id = tb.order_id
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_flightorderitem foi
                   ON toi.order_item_id = foi.orderitembase_ptr_id
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.flights_flightproduct ff
                   ON foi.flight_product_id = ff.id
WHERE se.finance.travel_trust_booking(toi.order_id::INT)

self_describing_task --include 'se/finance/travel_trust/travel_trust_money_in.py'  --method 'run' --start '2021-04-27 00:00:00' --end '2021-04-27 00:00:00';

SELECT * FROM se.finance.travel_trust_booking_components ttbc;
SELECT * FROM se.finance.travel_trust_money_in ttmi ;