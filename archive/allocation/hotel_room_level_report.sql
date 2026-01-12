--hotel room level
SELECT shra.mari_hotel_id,                                                                                  --currently mari id
--        shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
       shra.room_type_id,
       shra.room_type_name,
--        shra.inventory_date,
--        shra.inventory_day,
       SUM(shra.no_total_rooms)                                                             AS number_total_rooms,
       SUM(shra.no_booked_rooms)                                                            AS number_booked_rooms,
       SUM(shra.no_closedout_rooms)                                                         AS number_blackout_rooms,
       SUM(shra.no_available_rooms)                                                         AS number_available_rooms,
       SUM(CASE WHEN shra.no_available_rooms > 0 THEN 1 END)                                AS number_available_days,
       ROUND(number_available_rooms / number_available_days, 1)                             AS avg_available_rooms,

       number_booked_rooms / NULLIF(number_available_days, 0)                               AS higher_perc, --don't understand this logic
       number_available_rooms / number_total_rooms                                          AS avails_remaining_perc,
       SUM(CASE WHEN shra.inventory_day IN ('Fri', 'Sat') THEN shra.no_total_rooms END)     AS total_weekend_rooms,
       SUM(CASE WHEN shra.inventory_day IN ('Fri', 'Sat') THEN shra.no_available_rooms END) AS total_availalble_weekend_rooms
FROM se.data.se_hotel_room_availability shra
WHERE shra.mari_hotel_id = 1628
-- shra.hotel_code = '001w000001DVHxf'
GROUP BY 1, 2, 3, 4, 5;

SELECT *
FROM se.data.se_hotel_room_availability shra
WHERE shra.mari_hotel_id = 1628;


SELECT hs.company_id FROM data_vault_mvp.cms_mysql_snapshots.hotel_snapshot hs;