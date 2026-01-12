SELECT DISTINCT rs.res_id
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot iis
         LEFT JOIN data_vault_mvp.mari_snapshots.reservation_snapshot rs ON iis.reservation_id = rs.id
WHERE state = 'LOCKED';


SELECT MAX(loaded_at)
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot;


------------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT rs.res_id
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot iis
         LEFT JOIN data_vault_mvp.mari_snapshots.reservation_snapshot rs ON iis.reservation_id = rs.id
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON iis.inventory_id = i.id
WHERE state = 'LOCKED'
  AND rs.res_id;


SELECT *
FROM se.data.se_hotel_room_availability shra
WHERE shra.hotel_name = 'Mill End Hotel'
  AND shra.room_type_name = 'Junior Suite';


SELECT hs.name  AS hotel_name,
       rts.name AS room_type_name,
       i.date   AS inventory_date,
       iis.id,
       iis.date_created,
       iis.last_updated,
       iis.reservation_id,
       iis.inventory_id,
       iis.state,
       iis.extract_metadata
FROM data_vault_mvp.mari_snapshots.hotel_snapshot hs
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON hs.id = rts.hotel_id
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON rts.id = i.room_type_id
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_item_snapshot iis ON i.id = iis.inventory_id
WHERE hs.name = 'Mill End Hotel'
  AND rts.name = 'Junior Suite'
  AND i.date >= '2021-02-12'
  AND i.date <= '2021-02-14';

SELECT *
FROM raw_vault_mvp.mari.inventory_item ii
WHERE ii.inventory_id IN ('50702',
                          '50704',
                          '50703'
    )


SELECT hs.name  AS hotel_name,
       rts.name AS room_type_name,
       i.date   AS inventory_date,
       iis.id,
       iis.date_created,
       iis.last_updated,
       iis.reservation_id,
       iis.inventory_id,
       iis.state,
       iis.extract_metadata
FROM data_vault_mvp.mari_snapshots.hotel_snapshot hs
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON hs.id = rts.hotel_id
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON rts.id = i.room_type_id
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_item_snapshot iis ON i.id = iis.inventory_id
WHERE hs.code = '001w000001DVHQ8'
  AND rts.name = 'Classic Double'
  AND i.date >= '2020-08-10'
  AND i.date <= '2020-08-13';


SELECT *
FROM se.data.se_hotel_room_availability shra
WHERE shra.hotel_code = '001w000001DVHQ8'
  AND shra.room_type_name = 'Classic Double';

--check against ari data

SELECT *
FROM hygiene_vault_mvp.ari.hotel_inventory
WHERE hotel_code = '001w000001DVHQ8'
  AND room_code = 'CD'
  AND row_loaded_at::DATE = '2020-07-21'
ORDER BY inventory_date DESC;

SELECT *
FROM hygiene_vault_mvp.ari.hotel_inventory
WHERE row_loaded_at = (
    SELECT MAX(row_loaded_at)
    FROM hygiene_vault_mvp.ari.hotel_inventory
)
  AND hotel_code = '001w000001DVHQ8'
  AND room_code = 'CD'
  AND inventory_date >= '2020-08-10'
  AND inventory_date <= '2020-08-13';

------------------------------------------------------------------------------------------------------------------------