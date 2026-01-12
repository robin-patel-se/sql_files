WITH rates AS (
    SELECT hs.name                             AS hotel_name,
           hs.id                               AS mari_hotel_id,
           srr.date,
           srr.room_type_id,
           srr.rate,
           srr.rack_rate,
           1- (srr.rate / NULLIF(srr.rack_rate, 0))  AS discount_rate
    FROM se.data.se_room_rates srr
             LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON srr.room_type_id = rts.id
             LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
)
SELECT shra.mari_hotel_id,
       shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
       shra.room_type_id,
       shra.room_type_name,
       shra.room_type_code,
       shra.inventory_date,
       shra.inventory_day,
       shra.no_total_rooms,
       shra.no_available_rooms,
       shra.no_booked_rooms,
       shra.no_closedout_rooms,
       r.rate,
       r.rack_rate,
       r.discount_rate
FROM se.data.se_hotel_room_availability shra
         LEFT JOIN rates r ON shra.mari_hotel_id = r.mari_hotel_id
              AND shra.inventory_date = r.date
              AND shra.room_type_id = r.room_type_id
WHERE LOWER(shra.hotel_name) LIKE '%carbis%';

SELECT ssa.se_sale_id,
       ssa.base_sale_id,
       ssa.sale_id,
       ssa.salesforce_opportunity_id,
       ssa.sale_name,
       ssa.sale_name_object,
       ssa.sale_active,
       ssa.has_flights_available,
       ssa.sale_product,
       ssa.sale_type,
       ssa.product_type,
       ssa.product_configuration,
       ssa.product_line,
       ssa.data_model,
       ssa.commission,
       ssa.commission_type,
       ssa.contractor_id,
       cs.name AS contractor_name,
       ssa.date_created,
       ssa.destination_type,
       ssa.start_date,
       ssa.end_date,
       ssa.hotel_id,
       ssa.base_currency,
       ssa.company_id,
       ssa.company_name,
       ssa.hotel_code,
       ssa.latitude,
       ssa.longitude,
       ssa.location_info_id,
       ssa.posa_territory,
       ssa.posa_country,
       ssa.posa_currency,
       ssa.posu_division,
       ssa.posu_country,
       ssa.posu_city,

       srtrar.room_type_id,
       rts.name as room_type_name,
       srtrar.hotel_code,
       srtrar.hotel_name,
       srtrar.rate_date,
       srtrar.rate_currency,
       srtrar.rt_lead_rate,
       srtrar.rt_top_discount_percentage,
       srtrar.rt_no_rates,
       srtrar.rt_no_total_rooms,
       srtrar.rt_no_available_rooms,
       srtrar.rt_available_lead_rate,
       srtrar.rt_available_lead_rate_rooms
FROM se.data.se_room_type_rooms_and_rates srtrar
INNER JOIN data_vault_mvp.dwh.se_sale ssa ON srtrar.hotel_code = ssa.hotel_code AND ssa.sale_active
LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot cs ON ssa.contractor_id = cs.id
LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON srtrar.room_type_id = rts.id
WHERE ssa.hotel_code = '001w000001DVHS5'
ORDER BY srtrar.rate_date;




