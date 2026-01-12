WITH bookings AS (
    --bookings by sale id
    SELECT f.sale_id                       AS se_sale_id,
           count(*)                        AS trx,
           sum(f.margin_gross_of_toms_gbp) AS margin
    FROM se.data.fact_complete_booking f
    GROUP BY 1
),
     spvs AS (
         --spvs by sale id
         SELECT sp.se_sale_id,
                count(*) AS spvs
         FROM se.data.scv_touched_spvs sp
         GROUP BY 1
     )
        ,
     sale_territory_level AS (
         SELECT ss.se_sale_id,
                ss.salesforce_opportunity_id,
                ss.sale_name,
                ss.sale_name_object,
                ss.sale_active,
                ss.hotel_code,
                b.trx,
                b.margin,
                s.spvs
         FROM se.data.se_sale_attributes ss
                  LEFT JOIN bookings b ON ss.se_sale_id = b.se_sale_id
                  LEFT JOIN spvs s ON ss.se_sale_id = s.se_sale_id
--          WHERE ss.sale_active
     ),
     allocation_data AS (
         SELECT shra.hotel_code,
                shra.hotel_name,
                SUM(shra.no_total_rooms)     AS total_rooms,
                SUM(shra.no_closedout_rooms) AS total_blacked_out_rooms,
                SUM(shra.no_booked_rooms)    AS total_booked_rooms,
                SUM(shra.no_available_rooms) AS total_available_rooms
         FROM se.data.se_hotel_room_availability shra
         GROUP BY 1, 2
     )


SELECT ss.posu_city,

       LISTAGG(DISTINCT ss.salesforce_opportunity_id, ', ') AS global_sale_ids,
       count(DISTINCT ss.salesforce_opportunity_id)         AS global_sales,
       LISTAGG(DISTINCT ss.se_sale_id, ', ')                AS territory_sale_ids,
       count(DISTINCT ss.se_sale_id)                        AS sales,
       SUM(a.total_rooms)                                   AS total_rooms,
       SUM(a.total_booked_rooms)                            AS total_booked_rooms,
       SUM(a.total_available_rooms)                         AS total_available_rooms,
       MIN(ss.start_date)                                   AS global_sale_start_date,
       MAX(ss.end_date)                                     AS global_sale_end_date,
       COALESCE(SUM(stl.trx), 0)                            AS trx,
       COALESCE(SUM(stl.margin), 0)                         AS margin,
       COALESCE(SUM(stl.spvs), 0)                           AS spvs

FROM se.data.se_sale_attributes ss
         LEFT JOIN sale_territory_level stl ON ss.se_sale_id = stl.se_sale_id
         LEFT JOIN allocation_data a ON ss.hotel_code = a.hotel_code
WHERE ss.sale_active
GROUP BY 1
