--hotel level report by day
WITH booking_numbers AS (
    --aggregate up to hotel level
    SELECT h.hotel_code,
           COUNT(DISTINCT sb.booking_id)    AS bookings,
           SUM(sb.margin_gross_of_toms_gbp) AS margin,
           SUM(sb.no_nights)                AS room_nights

    FROM data_vault_mvp.dwh.se_booking sb
             --join to sale to get hotel only sales
             LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                       ON sb.offer_id = 'A' || bop.base_offer_products_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON bop.product_id = p.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
    WHERE sb.booking_status = 'COMPLETE'
      AND LEFT(sb.booking_id, 1) = 'A'       -- new data model bookings ;
      AND ss.product_configuration = 'Hotel' --hotel only
--   AND h.hotel_code = '001w000001DVHS5' --carbis bay
    GROUP BY 1
)

SELECT shra.cms_hotel_id,
       shra.mari_hotel_id,                                                      --currently mari id
--        shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
--        shra.inventory_date,
--        shra.inventory_day,
       b.bookings,
       b.margin,
       SUM(shra.no_total_rooms)                                 AS number_total_rooms,
       SUM(shra.no_booked_rooms)                                AS number_booked_rooms,

       SUM(shra.no_available_rooms)                             AS number_available_rooms,
       SUM(CASE WHEN shra.no_available_rooms > 0 THEN 1 END)    AS number_available_days,
       ROUND(number_available_rooms / number_available_days, 1) AS avg_available_rooms,

       number_booked_rooms / NULLIF(number_available_days, 0)   AS higher_perc, --don't understand this logic
       number_available_rooms / number_total_rooms              AS avails_remaining_perc,
       SUM(CASE
               WHEN shra.inventory_day IN ('Fri', 'Sat')
                   THEN shra.no_total_rooms END)                AS total_weekend_rooms,
       SUM(CASE
               WHEN shra.inventory_day IN ('Fri', 'Sat')
                   THEN shra.no_available_rooms END)            AS total_availalble_weekend_rooms
FROM se.data.se_hotel_room_availability shra
         LEFT JOIN booking_numbers b ON shra.hotel_code = b.hotel_code
-- WHERE shra.mari_hotel_id = 1628
GROUP BY 1, 2, 3, 4, 5, 6;

------------------------------------------------------------------------------------------------------------------------
SELECT *
FROM se.data.se_hotel_room_availability shra;

--hotel level report
WITH booking_numbers AS (
    --aggregate up to hotel level
    SELECT h.hotel_code,
           COUNT(DISTINCT sb.booking_id)    AS bookings,
           SUM(sb.margin_gross_of_toms_gbp) AS margin,
           SUM(sb.no_nights)                AS room_nights

    FROM data_vault_mvp.dwh.se_booking sb
             --join to sale to get hotel only sales
             LEFT JOIN data_vault_mvp.dwh.se_sale ss ON sb.sale_id = ss.se_sale_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                       ON sb.offer_id = 'A' || bop.base_offer_products_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON bop.product_id = p.id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
    WHERE sb.booking_status = 'COMPLETE'
      AND LEFT(sb.booking_id, 1) = 'A' -- new data model bookings ;
      AND ss.product_configuration = 'Hotel'
--   AND h.hotel_code = '001w000001DVHS5' --carbis bay
    GROUP BY 1
)

SELECT shra.cms_hotel_id,
       shra.mari_hotel_id,                                                      --currently mari id
--        shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
--        shra.inventory_date,
--        shra.inventory_day,
       b.bookings,
       b.margin,
       SUM(shra.no_total_rooms)                                 AS number_total_rooms,
       SUM(shra.no_booked_rooms)                                AS number_booked_rooms,

       SUM(shra.no_available_rooms)                             AS number_available_rooms,
       SUM(CASE WHEN shra.no_available_rooms > 0 THEN 1 END)    AS number_available_days,
       ROUND(number_available_rooms / number_available_days, 1) AS avg_available_rooms,

       number_booked_rooms / NULLIF(number_available_days, 0)   AS higher_perc, --don't understand this logic
       number_available_rooms / number_total_rooms              AS avails_remaining_perc,
       SUM(CASE
               WHEN shra.inventory_day IN ('Fri', 'Sat')
                   THEN shra.no_total_rooms END)                AS total_weekend_rooms,
       SUM(CASE
               WHEN shra.inventory_day IN ('Fri', 'Sat')
                   THEN shra.no_available_rooms END)            AS total_availalble_weekend_rooms
FROM se.data.se_hotel_room_availability shra
         LEFT JOIN booking_numbers b ON shra.hotel_code = b.hotel_code
-- WHERE shra.mari_hotel_id = 1628
GROUP BY 1, 2, 3, 4, 5, 6;

------------------------------------------------------------------------------------------------------------------------

--hotel availability by month
SELECT shra.mari_hotel_id, --currently mari id
       shra.hotel_name,
       shra.hotel_code,
       TO_CHAR(DATE_TRUNC(MONTH, shra.inventory_date), 'YYYY-MM') AS month,
       SUM(shra.no_available_rooms) / SUM(shra.no_total_rooms)    AS availability_perc
FROM se.data.se_hotel_room_availability shra
WHERE shra.mari_hotel_id = 1628
GROUP BY 1, 2, 3, 4;


--hotel availability by week
SELECT shra.mari_hotel_id, --currently mari id
       shra.hotel_name,
       shra.hotel_code,
       DATE_TRUNC(WEEK, shra.inventory_date) AS week_start,
       SUM(shra.no_available_rooms) / SUM(shra.no_total_rooms)    AS availability_perc
FROM se.data.se_hotel_room_availability shra
WHERE shra.mari_hotel_id = 1628
GROUP BY 1, 2, 3, 4;


--availablity by month
SELECT *
FROM (
    -- hotel by month availability percentages
    SELECT shra.mari_hotel_id, --currently mari id
           shra.hotel_name,
           shra.hotel_code,
           TO_CHAR(DATE_TRUNC(MONTH, shra.inventory_date), 'YYYY-MM') AS month,
           SUM(shra.no_available_rooms) / SUM(shra.no_total_rooms)    AS availability_perc
    FROM se.data.se_hotel_room_availability shra
--     WHERE shra.mari_hotel_id = 1628
--   AND shra.inventory_date BETWEEN '2020-08-01'
--     AND '2020-08-31'
    GROUP BY 1, 2, 3, 4
)
    PIVOT (sum(availability_perc) FOR month IN (
        '2020-08',
        '2020-09',
        '2020-10',
        '2020-11',
        '2020-12',
        '2021-01',
        '2021-02',
        '2021-03'
        ))

SELECT * FROM data_vault_mvp.engagement_stg.user_snapshot us;
