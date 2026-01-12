USE WAREHOUSE pipe_medium;
--module 2
--aggregation up to hotel inventory level

WITH agg_inventory_item AS (
    SELECT inventory_id,
           COUNT(*)                                               AS no_total_rooms,
           SUM(CASE WHEN state = 'AVAILABLE' THEN 1 ELSE 0 END)   AS no_available_rooms,
           SUM(CASE WHEN state = 'RESERVED' THEN 1 ELSE 0 END)    AS no_booked_rooms,
           SUM(CASE WHEN state = 'BLACKED_OUT' THEN 1 ELSE 0 END) AS no_blackedout_rooms
    FROM data_vault_mvp_dev_robin.mari_snapshots.inventory_item_snapshot
    GROUP BY inventory_id
)

SELECT h.id              AS hotel_id,
       h.name            AS hotel_name,
       h.code            AS hotel_code,
       rt.id             AS room_type_id,
       rt.name           AS room_type_name,
       rt.code           AS room_type_code,
       inv.date          AS inventory_date,
       dayname(inv.date) AS inventory_day,
       ii.no_total_rooms,
       ii.no_available_rooms,
       ii.no_booked_rooms,
       ii.no_blackedout_rooms
FROM data_vault_mvp_dev_robin.mari_snapshots.inventory_snapshot inv
         INNER JOIN agg_inventory_item ii ON inv.id = ii.inventory_id
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.room_type_snapshot rt ON inv.room_type_id = rt.id
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
ORDER BY h.id, inv.date ASC;
;

SELECT *
FROM raw_vault_mvp.mari.inventory_item
    QUALIFY ROW_NUMBER() OVER (PARTITION BY inventory_id ORDER BY loaded_at DESC) = 1;


------------------------------------------------------------------------------------------------------------------------
--snapshot
CREATE SCHEMA raw_vault_mvp_dev_robin.mari;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.inventory_item CLONE raw_vault_mvp.mari.inventory_item;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.inventory CLONE raw_vault_mvp.mari.inventory;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.room_type CLONE raw_vault_mvp.mari.room_type;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.hotel CLONE raw_vault_mvp.mari.hotel;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.rate_plan CLONE raw_vault_mvp.mari.rate_plan;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.rate CLONE raw_vault_mvp.mari.rate;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.cash_to_settle_rate CLONE raw_vault_mvp.mari.cash_to_settle_rate;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.reservation CLONE raw_vault_mvp.mari.reservation;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.reservation_room_confirmation CLONE raw_vault_mvp.mari.reservation_room_confirmation;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.reservation_tax CLONE raw_vault_mvp.mari.reservation_tax;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.reservation_supplement_charges CLONE raw_vault_mvp.mari.reservation_supplement_charges;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.mari.reservation_rate_plan CLONE raw_vault_mvp.mari.reservation_rate_plan;

self_describing_task --include 'se/data/create_se_data_objects.py'  --method 'run' --start '2020-05-19 07:00:00' --end '2020-05-19 07:00:00'

SELECT *
FROM se_dev_robin.data.se_hotel_room_availability;
SELECT *
FROM data_vault_mvp.dwh.se_offer;


