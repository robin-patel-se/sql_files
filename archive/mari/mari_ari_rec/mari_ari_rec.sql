--raw vault file
SELECT *
FROM raw_vault_mvp.ari.hotel_inventory
WHERE record['hotel_code']::VARCHAR = '001w000001DVHY1';
--hygiene
SELECT *
FROM hygiene_vault_mvp.ari.hotel_inventory
WHERE hotel_code = '001w000001DVHY1';

--raw vault flattened
SELECT record,
       record['hotel_code']::VARCHAR           AS hotel_code,
       record['hotel_name']::VARCHAR           AS hotel_name,
       rooms.value['code']::VARCHAR            AS room_code,
       rooms.value['name']::VARCHAR            AS room_name,
       inventory.value['date']::VARCHAR        AS inventory_date,
       inventory.value['total']::VARCHAR       AS inventory_total,
       inventory.value['available']::VARCHAR   AS inventory_available,
       inventory.value['reserved']::VARCHAR    AS inventory_reserved,
       inventory.value['blacked_out']::VARCHAR AS inventory_blacked_out,
       inventory.value
FROM raw_vault_mvp.ari.hotel_inventory,
     LATERAL FLATTEN(INPUT => record['rooms'], OUTER => TRUE) rooms,
     LATERAL FLATTEN(INPUT => rooms.value['inventory'], OUTER => TRUE) inventory
WHERE record['hotel_code']::VARCHAR = '001w000001DVHY1';

--ari data
SELECT hi.hotel_code,
       hi.hotel_name,
       hi.room_name,
       hi.inventory_date,
       hi.inventory_total,
       hi.inventory_available,
       hi.inventory_reserved,
       hi.inventory_blacked_out
FROM hygiene_vault_mvp.ari.hotel_inventory hi
WHERE hotel_code = '001w000001Kgx2u'
  AND inventory_date >= CURRENT_DATE
    QUALIFY ROW_NUMBER()
                    OVER (PARTITION BY hotel_code, room_code, inventory_date
                        ORDER BY row_loaded_at DESC, updated_at DESC) = 1
ORDER BY inventory_date, room_name;

--mari data model
SELECT shra.hotel_code,
       shra.hotel_name,
       shra.room_type_name AS room_name,
       shra.inventory_date,
       shra.no_total_rooms,
       shra.no_available_rooms,
       shra.no_booked_rooms,
       shra.no_closedout_rooms
FROM se.data.se_hotel_room_availability shra
WHERE hotel_code = '001w000001Kgx2u'
  AND shra.inventory_date >= CURRENT_DATE
ORDER BY inventory_date, room_type_name;

------------------------------------------------------------------------------------------------------------------------
--check instances where there was a difference between ari and mari data and mari data reconciled with the live portal

--check inventory items for fistral on the 2nd September 2020 in dwh mari data
SELECT i.date  AS inventory_date,
       h.code,
       h.name  AS hotel_name,
       rt.name AS room_type_name,
       ii.id,
       ii.date_created,
       ii.last_updated,
       ii.reservation_id,
       ii.inventory_id,
       ii.state
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON i.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
WHERE h.code = '001w000001DVHY1'
  AND i.date = '2020-09-02'
  AND rt.name = 'Sea View Room';


--check inventory items for fistral on the 2nd September 2020 in ari hygiene data
SELECT hi.hotel_code,
       hi.hotel_name,
       hi.room_name,
       hi.inventory_date,
       hi.inventory_total,
       hi.inventory_available,
       hi.inventory_reserved,
       hi.inventory_blacked_out,
       hi.o_record,
       hi.schedule_tstamp,
       hi.run_tstamp,
       hi.operation_id,
       hi.created_at,
       hi.updated_at,
       hi.row_dataset_name,
       hi.row_dataset_source,
       hi.row_loaded_at,
       hi.row_schedule_tstamp,
       hi.row_run_tstamp,
       hi.row_filename,
       hi.row_file_row_number
FROM hygiene_vault_mvp.ari.hotel_inventory hi
WHERE hi.hotel_code = '001w000001DVHY1'
  AND hi.inventory_date = '2020-09-02'
  AND hi.room_name = 'Sea View Room'
ORDER BY row_loaded_at DESC, updated_at DESC;

SELECT *
FROM raw_vault_mvp.ari.hotel_inventory hi
WHERE hi.record['hotel_code']::VARCHAR = '001w000001DVHY1'
;


------------------------------------------------------------------------------------------------------------------------
--check instances where there was a difference between ari and mari data and ari data reconciled with the live portal

--check inventory items for carbis on the 17th September 2020 in dwh mari data
SELECT i.date  AS inventory_date,
       h.code,
       h.name  AS hotel_name,
       rt.name AS room_type_name,
       ii.id,
       ii.date_created,
       ii.last_updated,
       ii.reservation_id,
       ii.inventory_id,
       ii.state
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON i.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
WHERE h.code = '001w000001DVHS5'
  AND i.date = '2020-09-17'
  AND rt.name = 'Junior Suite';

--check inventory items for carbis on the 17th September 2020 in dwh ari data

SELECT hi.hotel_code,
       hi.hotel_name,
       hi.room_name,
       hi.inventory_date,
       hi.inventory_total,
       hi.inventory_available,
       hi.inventory_reserved,
       hi.inventory_blacked_out,
       hi.o_record,
       hi.schedule_tstamp,
       hi.run_tstamp,
       hi.operation_id,
       hi.created_at,
       hi.updated_at,
       hi.row_dataset_name,
       hi.row_dataset_source,
       hi.row_loaded_at,
       hi.row_schedule_tstamp,
       hi.row_run_tstamp,
       hi.row_filename,
       hi.row_file_row_number
FROM hygiene_vault_mvp.ari.hotel_inventory hi
WHERE hi.hotel_code = '001w000001DVHS5'
  AND hi.inventory_date = '2020-09-17'
  AND hi.room_name = 'Junior Suite'
ORDER BY row_loaded_at DESC, updated_at DESC;

SELECT *
FROM data_vault_mvp.dwh.user_attributes ua
WHERE ua.country IS NULL;


SELECT *
FROM se.data.se_booking ssa SAMPLE (100 rows)


--check inventory items for carbis on the 4th October 2020 in dwh ari data

SELECT hi.hotel_code,
       hi.hotel_name,
       hi.room_name,
       hi.inventory_date,
       hi.inventory_total,
       hi.inventory_available,
       hi.inventory_reserved,
       hi.inventory_blacked_out,
       hi.o_record,
       hi.schedule_tstamp,
       hi.run_tstamp,
       hi.operation_id,
       hi.created_at,
       hi.updated_at,
       hi.row_dataset_name,
       hi.row_dataset_source,
       hi.row_loaded_at,
       hi.row_schedule_tstamp,
       hi.row_run_tstamp,
       hi.row_filename,
       hi.row_file_row_number
FROM hygiene_vault_mvp.ari.hotel_inventory hi
WHERE hi.hotel_code = '001w000001DVHS5'
  AND hi.inventory_date = '2020-10-04'
  AND hi.room_name = 'Superior Room'
ORDER BY row_loaded_at DESC, updated_at DESC;


SELECT i.date  AS inventory_date,
       h.code,
       h.name  AS hotel_name,
       rt.name AS room_type_name,
       ii.id,
       ii.date_created,
       ii.last_updated,
       ii.reservation_id,
       ii.inventory_id,
       ii.state
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON i.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
WHERE h.code = '001w000001DVHS5'
  AND i.date = '2020-10-04'
  AND rt.name = 'Superior Room';

------------------------------------------------------------------------------------------------------------------------
--check carbis bay hotel junior suite for the 18th October 2020

SELECT hi.hotel_code,
       hi.hotel_name,
       hi.room_name,
       hi.inventory_date,
       hi.inventory_total,
       hi.inventory_available,
       hi.inventory_reserved,
       hi.inventory_blacked_out,
       hi.o_record,
       hi.schedule_tstamp,
       hi.run_tstamp,
       hi.operation_id,
       hi.created_at,
       hi.updated_at,
       hi.row_dataset_name,
       hi.row_dataset_source,
       hi.row_loaded_at,
       hi.row_schedule_tstamp,
       hi.row_run_tstamp,
       hi.row_filename,
       hi.row_file_row_number
FROM hygiene_vault_mvp.ari.hotel_inventory hi
WHERE hi.hotel_code = '001w000001DVHS5'
  AND hi.inventory_date = '2020-10-18'
  AND hi.room_name = 'Junior Suite'
ORDER BY row_loaded_at DESC, updated_at DESC;

SELECT i.date  AS inventory_date,
       h.code,
       h.name  AS hotel_name,
       rt.name AS room_type_name,
       ii.id,
       ii.date_created,
       ii.last_updated,
       ii.reservation_id,
       ii.inventory_id,
       ii.state
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON i.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
WHERE h.code = '001w000001DVHS5'
  AND i.date = '2020-10-18'
  AND rt.name = 'Junior Suite';

SELECT *
FROM raw_vault_mvp.mari.inventory_item ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
WHERE ii.id = 112247
  AND i.date = '2020-10-18';


--get list of top 5 hotels
SELECT ssa.se_sale_id,
       ssa.hotel_code,
       ssa.company_name,
       count(DISTINCT fcb.booking_id) AS bookings
FROM se.data.se_sale_attributes ssa
         LEFT JOIN se.data.fact_complete_booking fcb ON ssa.se_sale_id = fcb.sale_id
WHERE ssa.sale_active
  AND ssa.product_configuration = 'Hotel'
GROUP BY 1, 2, 3
ORDER BY 4 DESC;




------------------------------------------------------------------------------------------------------------------------
--Check mari inventory items for Hampton by Hilton CC
SELECT i.date  AS inventory_date,
       h.code,
       h.name  AS hotel_name,
       rt.name AS room_type_name,
       ii.id,
       ii.date_created,
       ii.last_updated,
       ii.reservation_id,
       ii.inventory_id,
       ii.state
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON i.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
WHERE h.code = '001w000001kDthr'
  AND i.date = '2020-11-27';

--Check ari inventory items for Hampton by Hilton CC
SELECT hi.hotel_code,
       hi.hotel_name,
       hi.room_name,
       hi.inventory_date,
       hi.inventory_total,
       hi.inventory_available,
       hi.inventory_reserved,
       hi.inventory_blacked_out,
       hi.o_record,
       hi.schedule_tstamp,
       hi.run_tstamp,
       hi.operation_id,
       hi.created_at,
       hi.updated_at,
       hi.row_dataset_name,
       hi.row_dataset_source,
       hi.row_loaded_at,
       hi.row_schedule_tstamp,
       hi.row_run_tstamp,
       hi.row_filename,
       hi.row_file_row_number
FROM hygiene_vault_mvp.ari.hotel_inventory hi
WHERE hi.hotel_code = '001w000001kDthr'
  AND hi.inventory_date = '2020-11-27'
ORDER BY row_loaded_at DESC, updated_at DESC;

SELECT distinct shrar.hotel_name, shrar.hotel_code FROM se.data.se_hotel_rooms_and_rates shrar WHERE LOWER(shrar.hotel_name) LIKE '%bird%';
