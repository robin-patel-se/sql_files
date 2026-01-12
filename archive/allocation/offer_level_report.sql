------------------------------------------------------------------------------------------------------------------------

--offer level

WITH room_allocation AS (
    --aggregate inventory up to room
    SELECT shra.room_type_id,
           shra.room_type_name,
           shra.room_type_code,
           shra.mari_hotel_id,
           shra.cms_hotel_id,
           shra.hotel_name,
           shra.hotel_code,
           shra.hotel_code ||
           ':' || rp.code ||
           ':' || rp.rack_code          AS hotel_rate_rack_code,
           sum(shra.no_total_rooms)     AS number_total_rooms,
           sum(shra.no_closedout_rooms) AS number_closedout_rooms,
           sum(shra.no_booked_rooms)    AS number_booked_rooms,
           sum(shra.no_available_rooms) AS number_available_rooms
    FROM se.data.se_hotel_room_availability shra
             LEFT JOIN data_vault_mvp.mari_snapshots.rate_plan_snapshot rp ON shra.room_type_id = rp.room_type_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
   , offer_bookings AS (
    SELECT o.se_offer_id,
           o.offer_name,
           o.offer_name_object,
           o.offer_active,
           o.product_id,
           o.provider_name,
           o.board_type,
           o.internal_note,
           cml.hotel_code,
           cml.rate_code,
           cml.rack_rate_code,
           cml.hotel_rate_rack_code,
           count(*)                        AS bookings,
           sum(b.margin_gross_of_toms_gbp) AS margin,
           sum(b.gross_booking_value_gbp)  AS booking_value,
           SUM(b.no_nights)                AS no_nights
    FROM data_vault_mvp.dwh.se_offer o
             --only want new data model hotel only offers, these are the only ones in the se_offer table at this point in time
             LEFT JOIN data_vault_mvp.dwh.se_booking b ON b.offer_id = o.se_offer_id
             LEFT JOIN se.data.se_cms_mari_link cml ON o.base_offer_id = cml.offer_id
         --only for offers that are currently active
    WHERE o.offer_active = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)
-- for offers that are currently active combine mari allocation data to.
SELECT ra.hotel_name,
       ra.hotel_code,
       ra.mari_hotel_id,
       ra.cms_hotel_id,
       ob.se_offer_id,
       ob.offer_name,
       ob.offer_name_object,
       ob.offer_active,
       ob.product_id,
       ob.provider_name,
       ob.board_type,
       ob.internal_note,
       ob.bookings,
       ob.margin,
       ob.booking_value,
       ob.no_nights,
       ra.room_type_id,
       ra.room_type_name,
       ra.hotel_rate_rack_code,
       ra.number_total_rooms,
       ra.number_closedout_rooms,
       ra.number_booked_rooms,
       ra.number_available_rooms
FROM offer_bookings ob
         INNER JOIN room_allocation ra ON ob.hotel_rate_rack_code = ra.hotel_rate_rack_code
-- WHERE ob.hotel_code = '001w000001DVHS5' -- carbis bay
;

