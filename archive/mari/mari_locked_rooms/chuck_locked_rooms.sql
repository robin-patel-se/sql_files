SELECT state, count(*)
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot iis
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------
--how many rooms are in a locked state vs available vs booked vs closed out at hotel level
--at inventory level
--where locked is greater than 0


WITH agg_inventory_item AS (
    SELECT inventory_id,
           COUNT(*)                                                         AS no_total_rooms,
           SUM(CASE WHEN state = 'AVAILABLE' THEN 1 ELSE 0 END)             AS no_available_rooms,
           SUM(CASE WHEN state IN ('RESERVED', 'LOCKED') THEN 1 ELSE 0 END) AS no_booked_rooms,
           SUM(CASE WHEN state = 'BLACKED_OUT' THEN 1 ELSE 0 END)           AS no_closedout_rooms,
           SUM(CASE WHEN state = 'LOCKED' THEN 1 ELSE 0 END)                AS no_locked_rooms
    FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot
    GROUP BY inventory_id
)

SELECT h.id              AS mari_hotel_id,
       ch.id             AS cms_hotel_id,
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
       ii.no_closedout_rooms,
       ii.no_locked_rooms
FROM data_vault_mvp.mari_snapshots.inventory_snapshot inv
         INNER JOIN agg_inventory_item ii ON inv.id = ii.inventory_id
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON inv.room_type_id = rt.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot ch ON h.code = ch.hotel_code
WHERE ii.no_locked_rooms > 0
ORDER BY mari_hotel_id, inventory_date;
;

