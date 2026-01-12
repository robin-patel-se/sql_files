WITH order_items AS (
--get components at booking level
    SELECT *
    FROM collab.travel_trust.travelbird_booking_changelog tbc
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.order_item_updated_timestamp DESC) = 1
)


SELECT *
FROM order_items
-- WHERE order_items.order_item_event_type = 'removed_orderitems'

WHERE order_items.order_id = 21872267;


DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;
DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;


--abstract the tb currency table
self_describing_task --include 'data_vault_mvp/fx/tb_rates.py'  --method 'run' --start '2021-03-07 00:00:00' --end '2021-03-07 00:00:00'

SELECT aer.run_tstamp::DATE, COUNT(*)
FROM data_vault_mvp.dwh.athena_email_reporting aer
WHERE aer.run_tstamp >= CURRENT_DATE - 14
GROUP BY 1;

CREATE SCHEMA data_vault_mvp_dev_robin.travelbird_mysql_snapshots;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.currency_currency_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.tb_booking__step01__model_data');

CREATE OR REPLACE TRANSIENT TABLE tb_booking__step01__model_data
(
    usage_date      DATE,
    source_id       NUMBER,
    source_currency VARCHAR,
    target_id       NUMBER,
    target_currency VARCHAR,
    rate            FLOAT
);


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate CLONE hygiene_snapshot_vault_mvp_mvp.travelbird_mysql.currency_exchangerateupdate;

SELECT *
FROM data_vault_mvp_dev_robin.fx.tb_rates;

self_describing_task --include 'data_vault_mvp/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-03-07 00:00:00' --end '2021-03-07 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_person_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_person_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderevent_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderproperty_snapshot AS
SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderproperty_snapshot;


SELECT *
FROM hygiene_snapshot_vault_mvp_mvp.cms_mysql.reservation r;


SELECT bs.is_flashsale
FROM hygiene_snapshot_vault_mvp_mvp.cms_mysql.base_sale bs;

self_describing_task --include 'data_vault_mvp/dwh/transactional/se_sale.py'  --method 'run' --start '2021-03-08 00:00:00' --end '2021-03-08 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active CLONE data_vault_mvp.dwh.sale_active;
ALTER TABLE data_vault_mvp_dev_robin.dwh.sale_active
    ADD COLUMN is_flashsale BOOLEAN;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
self_describing_task --include '/data_vault_mvp/dwh/transactional/sale_active_snapshot.py'  --method 'run' --start '2021-03-08 00:00:00' --end '2021-03-08 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale ss;

SELECT SPLIT_PART(offer_name, '|', 1)   AS first_offer_name, --This get the first offer shown in the offer_name
       offer_name_object:en_GB::varchar AS gb_offer_name,    --This gets the GB version of the offer name
       *
FROM se.data.se_offer_attributes

------------------------------------------------------------------------------------------------------------------------
-- tb_booking refactor


WITH most_recent_oi AS (
    SELECT *
    FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog oic
         --get the most recent version of each order item
        QUALIFY ROW_NUMBER()
                        OVER (PARTITION BY oic.order_item_id
                            ORDER BY oic.order_item_updated_tstamp DESC, oic.within_event_index DESC) = 1
)
   , removed_order_items AS (
    SELECT mro.*,
           IFF(mro.sold_price_currency = 'GBP', mro.sold_price_incl_vat,
               mro.sold_price_incl_vat_eur * cc.multiplier) AS sold_price_incl_vat_gbp_constant_currency,
           CASE
               WHEN mro.cost_price_currency = 'GBP' THEN mro.cost_price_excl_vat
               WHEN mro.sold_price_currency = 'GBP' THEN mro.cost_price_excl_vat * cost_currency_to_sold_currency_exchange_rate
               ELSE mro.cost_price_excl_vat_eur * cc.multiplier
               END                                          AS cost_price_excl_vat_gbp_constant_currency
    FROM most_recent_oi mro
             LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
            (CURRENT_DATE) >= cc.start_date AND
            (CURRENT_DATE) <= cc.end_date AND
            cc.currency = 'GBP' AND
            cc.category = 'Primary' AND
            --order items might have differing currencies, TB have a conversion to EUR,
            --so convert currency to EUR and then from EUR to GBP using constant currency
            cc.base_currency = 'EUR'
         --don't display order items that have been set as removed
    WHERE mro.order_item_change_type IS DISTINCT FROM 'removed_order_item'
)
SELECT roi.order_id,
       oo.order_status,
       oo.payment_status,
       COUNT(1)                                                                                    AS number_of_order_items,
       OBJECT_AGG(roi.order_item_id, roi.order_item_type::VARIANT)                                 AS order_item_types,
       MAX(oo.updated_at_dts)                                                                      AS updated_at_dts,
       LISTAGG(DISTINCT roi.partner_name, ', ')                                                    AS order_partners,
       OBJECT_AGG(roi.order_item_id, roi.partner_name::VARIANT)                                    AS order_item_partners,
       LISTAGG(DISTINCT roi.flight_reservation_number, ', ')                                       AS order_flight_reservation_numbers,
       OBJECT_AGG(roi.order_item_id, roi.flight_reservation_number::VARIANT)                       AS order_item_flight_reservation_numbers,
       MIN(IFF(roi.order_item_type = 'ACCOMMODATION', roi.start_date, NULL))                       AS accommodation_start_date,
       MAX(IFF(roi.order_item_type = 'ACCOMMODATION', roi.end_date, NULL))                         AS accommodation_end_date,
       MIN(roi.start_date)                                                                         AS holiday_start_date,
       MAX(roi.end_date)                                                                           AS holiday_end_date,
       ANY_VALUE(roi.sold_price_currency)                                                          AS sold_price_currency,
       SUM(roi.sold_price_incl_vat)                                                                AS total_sold_price_incl_vat_cc,
       SUM(roi.sold_price_incl_vat_eur)                                                            AS total_sold_price_incl_vat_eur,
       SUM(roi.sold_price_incl_vat_gbp)                                                            AS total_sold_price_incl_vat_gbp,
       SUM(roi.sold_price_incl_vat_gbp_constant_currency)                                          AS total_sold_price_incl_vat_gbp_constant_currency,
       OBJECT_AGG(roi.order_item_id, roi.cost_price_currency::VARIANT)                             AS cost_currencies,
       COUNT(DISTINCT roi.cost_price_currency)                                                     AS number_of_cost_currencies,

       SUM(roi.cost_price_excl_vat_sold_currency)                                                  AS total_cost_price_excl_vat_cc,
       SUM(roi.cost_price_excl_vat_eur)                                                            AS total_cost_price_excl_vat_eur,
       SUM(roi.cost_price_excl_vat_gbp)                                                            AS total_cost_price_excl_vat_gbp,
       SUM(roi.cost_price_excl_vat_gbp_constant_currency)                                          AS total_cost_price_excl_vat_gbp_constant_currency,

       SUM(IFF(roi.order_item_type = 'BOOKING_FEE', roi.sold_price_incl_vat, 0))                   AS total_booking_fee_sold_price_incl_vat_cc,
       SUM(
               IFF(roi.order_item_type = 'BOOKING_FEE', roi.sold_price_incl_vat_eur, 0))           AS total_booking_fee_sold_price_incl_vat_eur,
       SUM(
               IFF(roi.order_item_type = 'BOOKING_FEE', roi.sold_price_incl_vat_gbp, 0))           AS total_booking_fee_sold_price_incl_vat_gbp,
       SUM(IFF(roi.order_item_type = 'BOOKING_FEE', roi.sold_price_incl_vat_gbp_constant_currency,
               0))                                                                                 AS total_booking_fee_sold_price_incl_vat_gbp_constant_currency,

       SUM(IFF(roi.order_item_type = 'BOOKING_FEE',
               roi.sold_price_incl_vat - (roi.sold_price_incl_vat / (1 + roi.vat_percentage)), 0)) AS total_booking_fee_vat_cc,
       SUM(IFF(roi.order_item_type = 'BOOKING_FEE',
               roi.sold_price_incl_vat_eur - (roi.sold_price_incl_vat_eur / (1 + roi.vat_percentage)),
               0))                                                                                 AS total_booking_fee_vat_eur,
       SUM(IFF(roi.order_item_type = 'BOOKING_FEE',
               roi.sold_price_incl_vat_gbp - (roi.sold_price_incl_vat_gbp / (1 + roi.vat_percentage)),
               0))                                                                                 AS total_booking_fee_vat_gbp,
       SUM(IFF(roi.order_item_type = 'BOOKING_FEE', roi.sold_price_incl_vat_gbp_constant_currency -
                                                    (roi.sold_price_incl_vat_gbp_constant_currency / (1 + roi.vat_percentage)),
               0))                                                                                 AS total_booking_fee_vat_gbp_constant_currency,

       total_sold_price_incl_vat_cc - total_cost_price_excl_vat_cc - total_booking_fee_vat_cc      AS margin_cc,
       total_sold_price_incl_vat_eur - total_cost_price_excl_vat_eur - total_booking_fee_vat_eur   AS margin_eur,
       total_sold_price_incl_vat_gbp - total_cost_price_excl_vat_gbp - total_booking_fee_vat_gbp   AS margin_gbp,
       total_sold_price_incl_vat_gbp_constant_currency - total_cost_price_excl_vat_gbp_constant_currency -
       total_booking_fee_vat_gbp_constant_currency                                                 AS margin_gbp_constant_currency,

       SUM(IFF(roi.order_item_type = 'ATOL', roi.sold_price_incl_vat, 0))                          AS total_atol_sold_price_incl_vat,
       SUM(IFF(roi.order_item_type = 'ATOL', roi.sold_price_incl_vat_eur, 0))                      AS total_atol_sold_price_incl_vat_eur,
       SUM(IFF(roi.order_item_type = 'ATOL', roi.sold_price_incl_vat_gbp, 0))                      AS total_atol_sold_price_incl_vat_gbp,

       SUM(IFF(roi.order_item_type = 'FLIGHT', roi.sold_price_incl_vat, 0))                        AS total_flight_sold_price_incl_vat_cc,
       SUM(IFF(roi.order_item_type = 'FLIGHT', roi.sold_price_incl_vat_eur, 0))                    AS total_flight_sold_price_incl_vat_eur,
       SUM(IFF(roi.order_item_type = 'FLIGHT', roi.sold_price_incl_vat_gbp, 0))                    AS total_flight_sold_price_incl_vat_gbp,

       total_booking_fee_sold_price_incl_vat_cc - total_flight_sold_price_incl_vat_cc              AS total_non_flight_sold_price_inc_vat_cc,
       total_booking_fee_sold_price_incl_vat_eur -
       total_flight_sold_price_incl_vat_eur                                                        AS total_non_flight_sold_price_inc_vat_eur,
       total_booking_fee_sold_price_incl_vat_gbp -
       total_flight_sold_price_incl_vat_gbp                                                        AS total_non_flight_sold_price_inc_vat_gbp
FROM removed_order_items roi
         LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo ON roi.order_id = oo.id
GROUP BY 1, 2, 3
;


SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_id = 21904435;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog toic
WHERE toic.order_item_type = 'BOOKING_FEE'
  AND toic.vat_percentage > 0;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_fee_vat_cc > 0

SELECT *
FROM se_dev_robin.data.tb_order_item toic
WHERE toic.order_id = 21868056;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_id = 'TB-21903372';

SELECT *
FROM se_dev_robin.data.tb_order_item toi
WHERE toi.order_item_type = 'ATOL';



CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_person_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_person_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderevent_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderproperty_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_orderproperty_snapshot;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderproperty_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_orderproperty_snapshot;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.external_booking_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot;


SELECT COUNT(*)
FROM data_vault_mvp.dwh.tb_booking tb;
SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE tb.booking_id = 'TB-21898185';

SELECT LISTAGG(toic.flight_reservation_number, ', ')
FROM se_dev_robin.data.tb_order_item toic
WHERE toic.order_id = 21898185;

SELECT SUM(tb.sold_price_total_gbp),
       SUM(tb.cost_price_total_gbp)
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;


SELECT SUM(tb.sold_price_total_gbp),
       SUM(tb.cost_price_total_gbp)
FROM data_vault_mvp.dwh.tb_booking tb;

------------------------------------------------------------------------------------------------------------------------
--original values
SELECT oic.order_id,
       COUNT(1)                                                              AS order_creation_number_of_order_items,
       OBJECT_AGG(oic.order_item_id, oic.order_item_type::VARIANT)           AS order_creation_order_item_types,
       MIN(IFF(oic.order_item_type = 'ACCOMMODATION', oic.start_date, NULL)) AS order_creation_accommodation_start_date,
       MAX(IFF(oic.order_item_type = 'ACCOMMODATION', oic.end_date, NULL))   AS order_creation_accommodation_end_date,
       MIN(oic.start_date)                                                   AS order_creation_holiday_start_date,
       MAX(oic.end_date)                                                     AS order_creation_holiday_end_date,
       SUM(oic.sold_price_incl_vat)                                          AS order_creation_total_sold_price_incl_vat_cc,
       SUM(oic.sold_price_incl_vat_eur)                                      AS order_creation_total_sold_price_incl_vat_eur,
       SUM(oic.sold_price_incl_vat_gbp)                                      AS order_creation_total_sold_price_incl_vat_gbp,
       SUM(oic.cost_price_excl_vat_sold_currency)                            AS order_creation_total_cost_price_excl_vat_cc,
       SUM(oic.cost_price_excl_vat_eur)                                      AS order_creation_total_cost_price_excl_vat_eur,
       SUM(oic.cost_price_excl_vat_gbp)                                      AS order_creation_total_cost_price_excl_vat_gbp,
       SUM(IFF(oic.order_item_type = 'BOOKING_FEE',
               oic.sold_price_incl_vat - (oic.sold_price_incl_vat / (1 + oic.vat_percentage)),
               0))                                                           AS order_creation_total_booking_fee_vat_cc,
       SUM(IFF(oic.order_item_type = 'BOOKING_FEE',
               oic.sold_price_incl_vat_eur - (oic.sold_price_incl_vat_eur / (1 + oic.vat_percentage)),
               0))                                                           AS order_creation_total_booking_fee_vat_eur,
       SUM(IFF(oic.order_item_type = 'BOOKING_FEE',
               oic.sold_price_incl_vat_gbp - (oic.sold_price_incl_vat_gbp / (1 + oic.vat_percentage)),
               0))                                                           AS order_creation_total_booking_fee_vat_gbp,
       order_creation_total_sold_price_incl_vat_cc - order_creation_total_cost_price_excl_vat_cc -
       order_creation_total_booking_fee_vat_cc                               AS order_creation_margin_cc,
       order_creation_total_sold_price_incl_vat_eur - order_creation_total_cost_price_excl_vat_eur -
       order_creation_total_booking_fee_vat_eur                              AS order_creation_margin_eur,
       order_creation_total_sold_price_incl_vat_gbp - order_creation_total_cost_price_excl_vat_gbp -
       order_creation_total_booking_fee_vat_gbp                              AS order_creation_margin_gbp

FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog oic
WHERE oic.event_type = 'ORDER_CREATED'
GROUP BY 1;

self_describing_task --include 'se/data/dwh/tb_booking.py'  --method 'run' --start '2021-03-23 00:00:00' --end '2021-03-23 00:00:00'
self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-03-23 00:00:00' --end '2021-03-23 00:00:00'


SELECT ssa.salesforce_opportunity_id,
       ds.posu_country
FROM se.data.dim_sale ds
         INNER JOIN se.data.se_sale_tags sst ON ds.se_sale_id = sst.se_sale_id
         INNER JOIN se.data.se_sale_attributes ssa ON ds.se_sale_id = ssa.se_sale_id
WHERE LOWER(sst.tag_name) LIKE '%petfriendly%'
  AND ds.sale_active
GROUP BY 1, 2;

SELECT *
FROM se.data.scv_touched_spvs sts;
SELECT *
FROM se.data.scv_touch_basic_attributes stba;
