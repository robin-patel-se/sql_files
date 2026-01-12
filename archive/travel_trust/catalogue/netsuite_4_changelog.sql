CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderevent_snapshot CLONE data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot;

--step 1: booking level dimensions, this will regenerate with every run to compute up to date booking metrics
WITH flatten_events AS (
    --flatten creation events
    SELECT oo.order_id,
           oo.event_type,
           oo.created_at_dts,
           coi_elements.value                     AS created_order_items,
           coi_elements.value:start_date::VARCHAR AS start_date,
           coi_elements.value:end_date::VARCHAR   AS end_date
    FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
         LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements
    WHERE oo.event_type = 'ORDER_CREATED'
),
     aggregate_events AS (
         --aggregate creation events to order level
         SELECT fe.order_id,
                MIN(fe.start_date) AS start_date,
                MAX(fe.end_date)   AS end_date
         FROM flatten_events fe
         GROUP BY 1
     )
SELECT oo.id                      AS original_booking_reference,
       oo.created_at_dts          AS original_booking_date,
       o.product_line,
       ae.start_date              AS original_departure_date,
       ae.end_date                AS original_return_date,
       IFF(oo.order_status IN ('CANCELLED',
                               'REPLACED',
                               'CLOSED')
           , 'Cancelled', 'Live') AS booking_status

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_order_snapshot oo
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.offers_offer_snapshot o ON oo.offer_id = o.id
         LEFT JOIN aggregate_events ae ON oo.id = ae.order_id
;

USE WAREHOUSE pipe_xlarge;
--change log ddl
CREATE OR REPLACE TABLE collab.travel_trust.travelbird_booking_changelog
(
    changelog_id                                 VARCHAR PRIMARY KEY NOT NULL,
    order_id                                     NUMBER,
    event_id                                     INT,
    event_type                                   VARCHAR,
    event_type_category                          VARCHAR,
    event_created_tstamp                         TIMESTAMP,
    event_data                                   VARIANT,
    order_adjustment_type                        VARCHAR,
    order_item_event_type                        VARCHAR, --event type from within the event_data
    order_item_event                             VARIANT,
    order_item_change_type                       VARCHAR, --type of change, order item creations can at and after booking creation
    order_item_id                                NUMBER,
    order_item_created_timestamp                 TIMESTAMP,
    order_item_updated_timestamp                 TIMESTAMP,
    sold_price_incl_vat                          DECIMAL(13, 4),
    sold_price_currency                          VARCHAR,
    sold_price_to_eur_exchange_rate              DECIMAL(13, 4),
    vat_percentage                               NUMBER,
    cost_price_excl_vat                          DECIMAL(13, 4),
    cost_price_currency                          VARCHAR,
    cost_currency_to_sold_currency_exchange_rate DECIMAL(13, 4),
    start_date                                   DATE,
    end_date                                     DATE,
    order_item_type                              VARCHAR,
    order_item_type_id                           VARCHAR,
    main_order_item_type                         VARCHAR,
    main_order_item_type_id                      VARCHAR,
    partner_id                                   NUMBER,
    partner_name                                 VARCHAR,
    is_cancellable_with_partner                  BOOLEAN,
    allocation_board_id                          INT,
    allocation_unit_id                           INT,
    supplier_reference                           VARCHAR,
    flight_reservation_number                    VARCHAR,
    within_event_index                           INT      --within each event object there are instance where there are multiple events for the same object
) CLUSTER BY (order_id, event_created_tstamp);

--step 2: flatten order creation,
INSERT INTO collab.travel_trust.travelbird_booking_changelog
SELECT oo.id || '-' || oo.event_type || '-' || coi_elements.value:id::VARCHAR || '-created_orderitems' AS changelog_id,
       oo.order_id,
       oo.id                                                                                           AS event_id,
       oo.event_type,
       'booking_creation'                                                                              AS event_type_category,
       oo.created_at_dts                                                                               AS event_created_tstamp,
       PARSE_JSON(oo.event_data)                                                                       AS event_data,
       PARSE_JSON(oo.event_data):adjustment_reason::VARCHAR                                            AS adjustment_reason,
       'created_orderitems'                                                                            AS order_adjustment_type,
       coi_elements.value                                                                              AS order_item_event,
       'created_order_item_at_booking_creation'                                                        AS order_item_change_type,
       coi_elements.value:id::INT                                                                      AS order_item_id,
       coi_elements.value:creation_datetime::TIMESTAMP                                                 AS order_item_created_timestamp,
       oo.created_at_dts                                                                               AS order_item_updated_timestamp,
       coi_elements.value:sold_price_incl_vat::DECIMAL(13, 4)                                          AS sold_price_incl_vat,
       coi_elements.value:sold_price_currency::VARCHAR                                                 AS sold_price_currency,
       coi_elements.value:sold_price_to_eur_exchange_rate::DECIMAL(13, 4)                              AS sold_price_to_eur_exchange_rate,
       coi_elements.value:vat_percentage::DECIMAL(13, 4)                                               AS vat_percentage,
       coi_elements.value:cost_price_excl_vat::DECIMAL(13, 4)                                          AS cost_price_excl_vat,
       coi_elements.value:cost_price_currency::VARCHAR                                                 AS cost_price_currency,
       coi_elements.value:cost_currency_to_sold_currency_exchange_rate::DECIMAL(13, 4)                 AS cost_currency_to_sold_currency_exchange_rate,
       coi_elements.value:start_date::DATE                                                             AS start_date,
       coi_elements.value:end_date::DATE                                                               AS end_date,
       coi_elements.value:order_item_type::VARCHAR                                                     AS order_item_type,
       coi_elements.value:order_item_type_id::VARCHAR                                                  AS order_item_type_id,
       coi_elements.value:main_order_item_type::VARCHAR                                                AS main_order_item_type,
       coi_elements.value:main_order_item_type_id::VARCHAR                                             AS main_order_item_type_id,
       coi_elements.value:partner_id::INT                                                              AS partner_id,
       coi_elements.value:partner_name::VARCHAR                                                        AS partner_name,
       coi_elements.value:is_cancellable_with_partner::BOOLEAN                                         AS is_cancellable_with_partner,
       coi_elements.value:allocation_board_id::INT                                                     AS allocation_board_id,
       coi_elements.value:allocation_unit_id::INT                                                      AS allocation_unit_id,
       coi_elements.value:supplier_reference::VARCHAR                                                  AS supplier_reference,
       coi_elements.value:flight_reservation_number::VARCHAR                                           AS flight_reservation_number,
       coi_elements.index                                                                              AS within_event_index
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
     LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements
WHERE oo.event_type = 'ORDER_CREATED'
  AND order_item_event IS NOT NULL
-- AND oo.created_at_dts >= current_date -1
;

--step 3: flatten order amendments with new created components
INSERT INTO collab.travel_trust.travelbird_booking_changelog
SELECT oo.id || '-' || oo.event_type || '-' || coi_elements.value:id::VARCHAR || '-created_orderitems' AS changelog_id,
       oo.order_id,
       oo.id                                                                                           AS event_id,
       oo.event_type,
       'booking_amendment'                                                                             AS event_type_category,
       oo.created_at_dts                                                                               AS event_created_tstamp,
       PARSE_JSON(oo.event_data)                                                                       AS event_data,
       'created_orderitems'                                                                            AS order_adjustment_type,
       PARSE_JSON(oo.event_data):adjustment_reason::VARCHAR                                            AS adjustment_reason,
       coi_elements.value                                                                              AS order_item_event,
       'created_order_item_after_booking_creation'                                                     AS order_item_change_type,
       coi_elements.value:id::INT                                                                      AS order_item_id,
       coi_elements.value:creation_datetime::TIMESTAMP                                                 AS order_item_created_timestamp,
       oo.created_at_dts                                                                               AS order_item_updated_timestamp,
       coi_elements.value:sold_price_incl_vat::DECIMAL(13, 4)                                          AS sold_price_incl_vat,
       coi_elements.value:sold_price_currency::VARCHAR                                                 AS sold_price_currency,
       coi_elements.value:sold_price_to_eur_exchange_rate::DECIMAL(13, 4)                              AS sold_price_to_eur_exchange_rate,
       coi_elements.value:vat_percentage::DECIMAL(13, 4)                                               AS vat_percentage,
       coi_elements.value:cost_price_excl_vat::DECIMAL(13, 4)                                          AS cost_price_excl_vat,
       coi_elements.value:cost_price_currency::VARCHAR                                                 AS cost_price_currency,
       coi_elements.value:cost_currency_to_sold_currency_exchange_rate::DECIMAL(13, 4)                 AS cost_currency_to_sold_currency_exchange_rate,
       coi_elements.value:start_date::DATE                                                             AS start_date,
       coi_elements.value:end_date::DATE                                                               AS end_date,
       coi_elements.value:order_item_type::VARCHAR                                                     AS order_item_type,
       coi_elements.value:order_item_type_id::VARCHAR                                                  AS order_item_type_id,
       coi_elements.value:main_order_item_type::VARCHAR                                                AS main_order_item_type,
       coi_elements.value:main_order_item_type_id::VARCHAR                                             AS main_order_item_type_id,
       coi_elements.value:partner_id::INT                                                              AS partner_id,
       coi_elements.value:partner_name::VARCHAR                                                        AS partner_name,
       coi_elements.value:is_cancellable_with_partner::BOOLEAN                                         AS is_cancellable_with_partner,
       coi_elements.value:allocation_board_id::INT                                                     AS allocation_board_id,
       coi_elements.value:allocation_unit_id::INT                                                      AS allocation_unit_id,
       coi_elements.value:supplier_reference::VARCHAR                                                  AS supplier_reference,
       coi_elements.value:flight_reservation_number::VARCHAR                                           AS flight_reservation_number,
       coi_elements.index                                                                              AS within_event_index
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
     LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements
WHERE oo.event_type IS DISTINCT FROM 'ORDER_CREATED'
  AND order_item_event IS NOT NULL
-- AND oo.created_at_dts >= current_date -1
;

--step 4: flatten order amendments
INSERT INTO collab.travel_trust.travelbird_booking_changelog
SELECT oo.id || '-' || oo.event_type || '-' || uoi_elements.value:id::VARCHAR || '-updated_orderitems' AS changelog_id,
       oo.order_id,
       oo.id                                                                                           AS event_id,
       oo.event_type,
       'booking_amendment'                                                                             AS event_type_category,
       oo.created_at_dts                                                                               AS event_created_tstamp,
       PARSE_JSON(oo.event_data)                                                                       AS event_data,
       PARSE_JSON(oo.event_data):adjustment_reason::VARCHAR                                            AS adjustment_reason,
       'updated_orderitems'                                                                            AS order_item_event_type,
       uoi_elements.value                                                                              AS order_item_event,
       'updated_order_item'                                                                            AS order_item_change_type,
       uoi_elements.value:id::INT                                                                      AS order_item_id,
       uoi_elements.value:creation_datetime::TIMESTAMP                                                 AS order_item_created_timestamp,
       oo.created_at_dts                                                                               AS order_item_updated_timestamp,
       uoi_elements.value:sold_price_incl_vat::DECIMAL(13, 4)                                          AS sold_price_incl_vat,
       uoi_elements.value:sold_price_currency::VARCHAR                                                 AS sold_price_currency,
       uoi_elements.value:sold_price_to_eur_exchange_rate::DECIMAL(13, 4)                              AS sold_price_to_eur_exchange_rate,
       uoi_elements.value:vat_percentage::DECIMAL(13, 4)                                               AS vat_percentage,
       uoi_elements.value:cost_price_excl_vat::DECIMAL(13, 4)                                          AS cost_price_excl_vat,
       uoi_elements.value:cost_price_currency::VARCHAR                                                 AS cost_price_currency,
       uoi_elements.value:cost_currency_to_sold_currency_exchange_rate::DECIMAL(13, 4)                 AS cost_currency_to_sold_currency_exchange_rate,
       uoi_elements.value:start_date::DATE                                                             AS start_date,
       uoi_elements.value:end_date::DATE                                                               AS end_date,
       uoi_elements.value:order_item_type::VARCHAR                                                     AS order_item_type,
       uoi_elements.value:order_item_type_id::VARCHAR                                                  AS order_item_type_id,
       uoi_elements.value:main_order_item_type::VARCHAR                                                AS main_order_item_type,
       uoi_elements.value:main_order_item_type_id::VARCHAR                                             AS main_order_item_type_id,
       uoi_elements.value:partner_id::INT                                                              AS partner_id,
       uoi_elements.value:partner_name::VARCHAR                                                        AS partner_name,
       uoi_elements.value:is_cancellable_with_partner::BOOLEAN                                         AS is_cancellable_with_partner,
       uoi_elements.value:allocation_board_id::INT                                                     AS allocation_board_id,
       uoi_elements.value:allocation_unit_id::INT                                                      AS allocation_unit_id,
       uoi_elements.value:supplier_reference::VARCHAR                                                  AS supplier_reference,
       uoi_elements.value:flight_reservation_number::VARCHAR                                           AS flight_reservation_number,
       uoi_elements.index                                                                              AS within_event_index
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
     LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):updated_orderitems, OUTER => TRUE) uoi_elements
WHERE oo.event_type IS DISTINCT FROM 'ORDER_CREATED'
  AND order_item_event IS NOT NULL
-- AND oo.created_at_dts >= current_date -1
;

--step 5: flatten cancelled events
INSERT INTO collab.travel_trust.travelbird_booking_changelog
WITH removed_order_items AS (
    --list of existing order items that we've received removed events for.
    SELECT oo.order_id,
           oo.id                                                   AS event_id,
           oo.event_type,
           oo.created_at_dts                                       AS event_created_tstamp,
           PARSE_JSON(oo.event_data)                               AS event_data,
           PARSE_JSON(oo.event_data):adjustment_reason::VARCHAR    AS adjustment_reason,
           roi_elements.value                                      AS removed_order_item,
           roi_elements.value:id::INT                              AS order_item_id,
           roi_elements.value:is_cancellable_with_partner::BOOLEAN AS is_cancellable_with_partner,
           roi_elements.value:order_item_type_id::INT              AS order_item_type_id,
           roi_elements.value:order_item_type::VARCHAR             AS order_item_type,
           roi_elements.index                                      AS within_event_index
    FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
         LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):removed_orderitems, OUTER => TRUE) roi_elements
    WHERE oo.event_type IS DISTINCT FROM 'ORDER_CREATED'
      AND removed_order_item IS NOT NULL
    -- AND oo.created_at_dts >= current_date -1
)
--get the latest version of a order item prior to removal
--0 financials and update categorisation
SELECT ro.event_id || '-' || ro.event_type || '-' || ro.order_item_id || '-removed_orderitems' AS changelog_id,
       cl.order_id,
       cl.event_id,
       ro.event_type,
       'booking_amendment'                                                                     AS event_type_category,
       ro.event_created_tstamp,
       cl.event_data,
       ro.adjustment_reason,
       'removed_orderitems'                                                                    AS order_item_event_type,
       cl.order_item_event,
       'removed_order_item'                                                                    AS order_item_change_type,
       cl.order_item_id,
       cl.order_item_created_timestamp,
       ro.event_created_tstamp                                                                 AS order_item_updated_timestamp,
       0                                                                                       AS sold_price_incl_vat,
       cl.sold_price_currency,
       0                                                                                       AS sold_price_to_eur_exchange_rate,
       0                                                                                       AS vat_percentage,
       0                                                                                       AS cost_price_excl_vat,
       cl.cost_price_currency,
       0                                                                                       AS cost_currency_to_sold_currency_exchange_rate,
       cl.start_date,
       cl.end_date,
       ro.order_item_type,
       ro.order_item_type_id,
       cl.main_order_item_type,
       cl.main_order_item_type_id,
       cl.partner_id,
       cl.partner_name,
       ro.is_cancellable_with_partner,
       cl.allocation_board_id,
       cl.allocation_unit_id,
       cl.supplier_reference,
       cl.flight_reservation_number,
       ro.within_event_index
FROM collab.travel_trust.travelbird_booking_changelog cl
         INNER JOIN removed_order_items ro ON cl.order_item_id = ro.order_item_id
    --to ensure its based on events that happened prior to the removal
    AND cl.event_created_tstamp < ro.event_created_tstamp
--get latest version of the component
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cl.order_item_id ORDER BY cl.event_created_tstamp) = 1
;

-- DELETE
-- FROM collab.travel_trust.travelbird_booking_changelog tbc
-- WHERE tbc.order_item_change_type = 'removed_order_item';

SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc;

GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.travelbird_booking_changelog TO ROLE personal_role__ailiemcderment;



SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21872662
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.event_created_tstamp DESC) = 1;


SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21872662;



SELECT *
FROM collab.travel_trust.travelbird_booking_changelog cl
WHERE cl.order_id = 21904132
ORDER BY event_created_tstamp;

SELECT *
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_refundrequest_snapshot ors
WHERE ors.order_id = 21903469;

SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_item_change_type = 'removed_order_item';

SELECT *,
       COUNT(*) OVER (PARTITION BY travelbird_booking_changelog.order_id) AS events
FROM collab.travel_trust.travelbird_booking_changelog
    QUALIFY events = 10
;

SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21890258
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.event_created_tstamp DESC) = 1;


SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21901176
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.event_created_tstamp DESC) = 1;

SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21866980
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.event_created_tstamp DESC) = 1;


SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21872662;

SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21872662
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.event_created_tstamp DESC) = 1;


SELECT *
FROM collab.travel_trust.travelbird_booking_changelog tbc
WHERE tbc.order_id = 21872662;

SELECT *
FROM collab.travel_trust.travelbird_booking_changelog
WHERE travelbird_booking_changelog.order_id = 21872662;


------------------------------------------------------------------------------------------------------------------------
WITH snap AS (
    SELECT *
    FROM collab.travel_trust.travelbird_booking_changelog tbc
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.event_created_tstamp DESC) = 1
),
     agg_items AS (
         SELECT s.order_id
              , s.sold_price_currency
              , SUM(s.sold_price_incl_vat) AS sold_price_incl_vat
         FROM snap s
         GROUP BY 1
                , 2
     )

SELECT ai.order_id,
       ai.sold_price_currency,
       ai.sold_price_incl_vat,
       oo.price_total,
       oo.price_total - ai.sold_price_incl_vat  AS diff,
       diff / NULLIF(ai.sold_price_incl_vat, 0) AS diff_perc
FROM agg_items ai
         LEFT JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo ON ai.order_id = oo.id
ORDER BY ABS(diff_perc) DESC
;

SELECT id,
       oo.price_total
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order oo;

------------------------------------------------------------------------------------------------------------------------
DROP TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog;
self_describing_task --include 'dv/dwh/transactional/tb_order_item_changelog.py'  --method 'run' --start '2019-04-01 00:00:00' --end '2019-04-01 00:00:00'


MERGE INTO data_vault_mvp_dev_robin.dwh.tb_order_item_changelog AS target
    USING data_vault_mvp_dev_robin.dwh.tb_order_item_changelog__step01__model_order_creation_item AS batch
    ON target.changelog_id = batch.changelog_id
    WHEN MATCHED
        THEN UPDATE SET
        -- (lineage) metadata for the current job
        target.schedule_tstamp = '2021-03-07 03:00:00',
        target.run_tstamp = '2021-03-09 13:43:55',
        target.operation_id =
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/transactional/tb_order_item_changelog.py__20210307T030000__daily_at_03h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.changelog_id,
        target.order_id,
        target.event_id,
        target.event_type,
        target.event_type_category,
        target.event_created_tstamp,
        target.event_data,
        target.order_adjustment_type,
        target.order_item_event_type,
        target.order_item_event,
        target.order_item_change_type,
        target.order_item_id,
        target.order_item_created_timestamp,
        target.order_item_updated_timestamp,
        target.sold_price_incl_vat,
        target.sold_price_currency,
        target.sold_price_to_eur_exchange_rate,
        target.vat_percentage,
        target.cost_price_excl_vat,
        target.cost_price_currency,
        target.cost_currency_to_sold_currency_exchange_rate,
        target.start_date,
        target.end_date,
        target.order_item_type,
        target.order_item_type_id,
        target.main_order_item_type,
        target.main_order_item_type_id,
        target.partner_id,
        target.partner_name,
        target.is_cancellable_with_partner,
        target.allocation_board_id,
        target.allocation_unit_id,
        target.supplier_reference,
        target.flight_reservation_number
    ]

DROP TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog tbc
WHERE tbc.order_id = 21882389
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tbc.order_item_id ORDER BY tbc.order_item_updated_timestamp DESC) = 1;;


SELECT MIN(oo.created_at_dts)
FROM data_vault_mvp_dev_robin.travelbird_mysql_snapshots.orders_orderevent_snapshot oo;

SELECT *
FROM data_vault_mvp.fx.tb_rates tr;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog tbc
    QUALIFY COUNT(*) OVER (PARTITION BY tbc.changelog_id) > 1
ORDER BY changelog_id;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog toic
WHERE toic.order_id = 21882389
  AND toic.event_id = 35090;

SELECT id,
       PARSE_JSON(oos.event_data)
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oos
WHERE id = 36386;



SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_order_item_changelog toic
--     QUALIFY COUNT(*) OVER (PARTITION BY tbc.changelog_id) > 1
WHERE toic.order_id = 21882389;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.tb_rates CLONE data_vault_mvp.fx.tb_rates;

self_describing_task --include 'se/data/dwh/tb_order_item_changelog.py'  --method 'run' --start '2021-03-10 00:00:00' --end '2021-03-10 00:00:00'
self_describing_task --include 'se/data/dwh/tb_order_item.py'  --method 'run' --start '2021-03-10 00:00:00' --end '2021-03-10 00:00:00'

SELECT *
FROM se_dev_robin.data.tb_order_item_changelog
WHERE order_id = 21882389;
SELECT *
FROM se_dev_robin.data.tb_order_item
WHERE order_id = 21882389;
SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_id = 'TB-21882389'

SELECT order_id,
       SUM(cost_price_excl_vat)

FROM se_dev_robin.data.tb_order_item
WHERE order_id = 21882389
GROUP BY 1;

WITH order_item AS (
    SELECT order_id,
           SUM(tb_order_item.cost_price_excl_vat_sold_currency) AS order_item_cost_price,
           SUM(tb_order_item.sold_price_incl_vat)               AS order_item_sold_price
    FROM se_dev_robin.data.tb_order_item
    GROUP BY 1
)
SELECT oi.order_id,
       oi.order_item_cost_price::DECIMAL(13, 2) AS oi_cost_price,
       tb.cost_price_total_cc::DECIMAL(13, 2)   AS tb_cost_price,
       oi.order_item_sold_price                 AS oi_sold_price,
       tb.sold_price_total_cc                   AS tb_sold_price,
       oo.price_total                           AS oo_sold_price
FROM order_item oi
         LEFT JOIN se.data.tb_booking tb ON 'TB-' || oi.order_id = tb.booking_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_order_snapshot oo ON oi.order_id = oo.id;


SELECT *
FROM se_dev_robin.data.tb_order_item toi
WHERE toi.order_id = 21882389;
SELECT *
FROM se_dev_robin.data.tb_order_item_changelog toic
WHERE toic.order_id = 21882389;

SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_id = 'TB-' || 21871417 airflow backfill --start_date '2019-04-01 03:00:00' --end_date '2019-04-01 03:00:00' --task_regex '.*' dwh__transactional__tb_order_item_changelog__daily_at_03h00


DROP TABLE data_vault_mvp_dev_robin.dwh.tb_order_item_changelog;

self_describing_task --include 'dv/dwh/transactional/tb_order_item_changelog.py'  --method 'run' --start '2019-04-01 00:00:00' --end '2019-04-01 00:00:00'

SELECT *
FROM se.data.tb_booking tb;

self_describing_task --include 'se/data/dwh/tb_order_item_changelog.py'  --method 'run' --start '2021-03-10 00:00:00' --end '2021-03-10 00:00:00'
self_describing_task --include 'se/data/dwh/tb_order_item.py'  --method 'run' --start '2021-03-10 00:00:00' --end '2021-03-10 00:00:00'

SELECT *
FROM hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency;

SELECT *
FROM se_dev_robin.data.tb_order_item toi;

DROP TABLE data_vault_mvp.dwh.tb_order_item_changelog;
airflow backfill --start_date '2019-04-01 03:00:00' --end_date '2019-04-01 03:00:00' --task_regex '.*' dwh__transactional__tb_order_item_changelog__daily_at_03h00
airflow clear --start_date '2019-04-01 03:00:00' --end_date '2019-04-01 03:00:00' --task_regex '.*' dwh__transactional__tb_order_item_changelog__daily_at_03h00
airflow backfill --start_date '2021-03-23 03:00:00' --end_date '2021-03-23 03:00:00' --task_regex '.*' transform__dv__tb_fx__rates__daily_at_03h00

self_describing_task --include 'dv/fx/tb_rates.py'  --method 'run' --start '2021-03-23 00:00:00' --end '2021-03-23 00:00:00'

SELECT *
FROM se.data.tb_order_item toi
WHERE toi.order_id = 21898185;

SELECT oos.order_id,
       tb.booking_id,
       tb.transaction_id,
       PARSE_JSON(oos.event_data)
FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oos
         LEFT JOIN se.data.tb_booking tb ON oos.order_id = tb.order_id;



CREATE OR REPLACE TRANSIENT TABLE se.bi.global_deal_potential_pit COPY GRANTS AS (
    SELECT view_date,
           salesforce_opportunity_id,
           date,
           SUM(room_remaining_potential_margin_gbp) AS deal_remaining_potential_margin_gbp,
           SUM(room_total_potential_margin_gbp)     AS deal_total_potential_margin_gbp,
           SUM(room_sell_through)                   AS deal_total_sold_margin_gbp
    FROM (
             SELECT view_date,
                    salesforce_opportunity_id,
                    room_type_code,
                    date,
                    MAX(room_offer_remaining_potential_margin_gbp) AS room_remaining_potential_margin_gbp,
                    MAX(room_offer_total_potential_margin_gbp)     AS room_total_potential_margin_gbp,
                    SUM(room_offer_sell_through)                   AS room_sell_through
             FROM (
                      SELECT rr.view_date,
                             s.salesforce_opportunity_id,
                             hso.offer_id,
                             rr.room_type_code,
                             rr.date,
                             rr.available * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
                             CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_remaining_potential_margin_gbp,
                             (rr.available + rr.booked_any_offer) * rr.rate *
                             (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
                             CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_total_potential_margin_gbp,
                             rr.booked_this_offer * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
                             CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_sell_through
                      FROM se.data.se_sale_attributes s
                               INNER JOIN se.data.se_hotel_sale_offer hso ON hso.sale_id = s.se_sale_id
                               INNER JOIN se.data.se_cms_mari_link mari ON hso.offer_id = mari.offer_id
                               INNER JOIN se.data.se_offer o ON hso.offer_id = o.id
                               INNER JOIN se.data.se_hotel_offer_rooms_and_rates_snapshot rr
                                          ON mari.hotel_code = rr.hotel_code AND hso.offer_id = rr.offer_id
                               LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency fx ON
                              rr.currency = fx.base_currency AND
                              CURRENT_DATE >= fx.start_date AND
                              CURRENT_DATE <= fx.end_date AND
                              fx.currency = 'GBP' AND
                              fx.category = 'Primary'
                      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
                  ) t1
             GROUP BY 1, 2, 3, 4
         ) t2
    GROUP BY 1, 2, 3
);



SELECT rr.view_date,
       s.salesforce_opportunity_id,
       hso.offer_id,
       rr.room_type_code,
       rr.date,
       rr.available * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
       CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_remaining_potential_margin_gbp,
       (rr.available + rr.booked_any_offer) * rr.rate *
       (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
       CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_total_potential_margin_gbp,
       rr.booked_this_offer * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
       CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_sell_through
FROM se.data.se_sale_attributes s
         INNER JOIN se.data.se_hotel_sale_offer hso ON hso.sale_id = s.se_sale_id
         INNER JOIN se.data.se_cms_mari_link mari ON hso.offer_id = mari.offer_id
         INNER JOIN se.data.se_offer o ON hso.offer_id = o.id
         INNER JOIN se.data.se_hotel_offer_rooms_and_rates_snapshot rr
                    ON mari.hotel_code = rr.hotel_code AND hso.offer_id = rr.offer_id
         LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency fx ON
        rr.currency = fx.base_currency AND
        CURRENT_DATE >= fx.start_date AND
        CURRENT_DATE <= fx.end_date AND
        fx.currency = 'GBP' AND
        fx.category = 'Primary';



SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos;

SELECT *
FROM data_vault_mvp.dwh.tb_order_item_changelog toic;


CREATE OR REPLACE TABLE data_vault_mvp.dwh.stripe_chargeback__step01__model_data AS
SELECT d.id                           AS transaction_id,
       d.created                      AS transaction_tstamp,
       'stripe'                       AS payment_service_provider,
       'disputes'                     AS payment_service_provider_transaction_type,
       'money out'                    AS cashflow_direction,
       'customer psp dispute'         AS cashflow_type,
       d.amount / 100::DECIMAL(13, 4) AS transaction_amount,
       UPPER(d.currency)              AS transaction_currency,
       d.status                       AS transaction_dispute_status,
       d.balance_transaction,
       d.balance_transactions,
       d.charge                       AS transaction_charge_id,
       c.tb_order_id,
       'TB-' || c.tb_order_id         AS booking_id d.evidence, d.evidence_details,
       d.is_charge_refundable,
       d.livemode,
       d.payment_intent,
       d.reason,
       d.record__o
FROM hygiene_snapshot_vault_mvp.stripe.disputes d
         LEFT JOIN hygiene_snapshot_vault_mvp.stripe.charges c ON d.charge = c.id;


self_describing_task --include 'dv/finance/stripe/stripe_chargeback.py'  --method 'run' --start '2021-04-06 00:00:00' --end '2021-04-06 00:00:00'