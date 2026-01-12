GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_booking_record TO ROLE personal_role__andypauer;


------------------------------------------------------------------------------------------------------------------------
--newsuite_booking_record

CREATE OR REPLACE VIEW collab.travel_trust.tb_netsuite_booking_record AS
(
WITH persons AS (
    SELECT op.order_id,
           COUNT(*) AS number_of_passengers
    FROM data_vault_mvp.travelbird_mysql_snapshots.orders_person_snapshot op
    GROUP BY 1
),
     flatten_events AS (
         --flatten creation events
         SELECT oo.order_id,
                oo.event_type,
                oo.created_at_dts,
                coi_elements.value                     AS created_order_items,
                coi_elements.value:start_date::VARCHAR AS start_date,
                coi_elements.value:end_date::VARCHAR   AS end_date

         FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
              LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements

         WHERE oo.event_type IN ('ORDER_CONFIRMED', 'ORDER_CREATED')
     ),
     aggregate_events AS (
         --aggregate creation events to order level
         SELECT fe.order_id,
                MIN(IFF(event_type = 'ORDER_CONFIRMED', fe.created_at_dts, NULL)) AS booking_confirmation_date,
                MIN(IFF(event_type = 'ORDER_CREATED', fe.start_date, NULL))       AS start_date,
                MAX(IFF(event_type = 'ORDER_CREATED', fe.end_date, NULL))         AS end_date
         FROM flatten_events fe
         GROUP BY 1
     )

SELECT oo.id                  AS booking_id,
       oof.product_line,
       oo.created_at_dts      AS booking_created_date,
       ae.booking_confirmation_date AS booking_completed_date,
       ae.start_date AS holiday_start_date,
       ae.end_date AS holiday_end_date,
       oo.price_total,
       cc.code                AS currency,
       p.number_of_passengers,
       se.data.posa_territory_from_tb_site_id(oo.site_id)                AS territory,
       oof.external_reference AS cms_sale_id

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_order_snapshot oo
         LEFT JOIN persons p ON oo.id = p.order_id -- need to aggregate
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.common_sitesettings_snapshot cs ON oo.site_id = cs.site_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot cc ON cs.site_currency_id = cc.id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.offers_offer_snapshot oof ON oo.offer_id = oof.id
         LEFT JOIN aggregate_events ae ON oo.id = ae.order_id
    )
;

GRANT SELECT ON VIEW  collab.travel_trust.netsuite_booking_record TO ROLE personal_role__kirstengrieve;


SELECT oo.order_id,
       oo.event_type,
       oo.created_at_dts,
       PARSE_JSON(oo.event_data) AS event_data,
       PARSE_JSON(oo.event_data):removed_orderitems

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo
WHERE oo.event_type IN ('ORDER_CONFIRMED', 'ORDER_CREATED')

WITH flatten_events AS (
    SELECT oo.order_id,
           oo.event_type,
           oo.created_at_dts,
--        PARSE_JSON(oo.event_data):created_orderitems AS created_order_items,
           coi_elements.value                     AS created_order_items,
           coi_elements.value:start_date::VARCHAR AS start_date,
           coi_elements.value:end_date::VARCHAR   AS end_date

    FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
         LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements

    WHERE oo.event_type IN ('ORDER_CONFIRMED', 'ORDER_CREATED')
)
SELECT fe.order_id,
       MIN(IFF(event_type = 'ORDER_CONFIRMED', fe.created_at_dts, NULL)) AS booking_confirmation_date,
       MIN(IFF(event_type = 'ORDER_CREATED', fe.start_date, NULL))       AS start_date,
       MAX(IFF(event_type = 'ORDER_CREATED', fe.end_date, NULL))         AS end_date
FROM flatten_events fe
GROUP BY 1
;

