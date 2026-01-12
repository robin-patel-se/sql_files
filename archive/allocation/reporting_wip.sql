--hotel room availability
SELECT *
FROM se_dev_robin.data.se_hotel_room_availability
ORDER BY hotel_code, room_type_name, inventory_date;

------------------------------------------------------------------------------------------------------------------------
--code for niro management deck, found to be unnecessary
--aggregated up to global sale id level
WITH list_of_sales AS (
    --list of sales we want to include
    SELECT sa.se_sale_id
    FROM se.data.se_sale_attributes sa
    WHERE sa.sale_active = 1
      AND sa.data_model = 'New Data Model'
      AND sa.product_configuration = 'Hotel'
      AND DATEDIFF(DAY, sa.start_date, current_date) > 7
)
   , bookings_by_week AS (
    SELECT f.sale_id                                  AS se_sale_id,
           date_trunc(WEEK, f.booking_completed_date) AS booking_week,
           count(*)                                   AS bookings,
           sum(f.margin_gross_of_toms_gbp)            AS margin_gbp
    FROM se.data.fact_complete_booking f
    WHERE f.sale_id IN (
        SELECT se_sale_id
        FROM list_of_sales
    )
    GROUP BY 1, 2
)
   , spvs_by_week AS (
    SELECT sp.se_sale_id,
           date_trunc(WEEK, sp.event_tstamp) AS spv_week,
           count(*)                          AS sales_page_views
    FROM se.data.scv_touched_spvs sp
    WHERE sp.se_sale_id IN (
        SELECT se_sale_id
        FROM list_of_sales
    )
    GROUP BY 1, 2
)
   , week_metrics AS (
    --aggregate up to global sale id by week
    SELECT sa.sale_name,
           sa.sale_active,
           sa.salesforce_opportunity_id   AS global_sale_id,
           s.spv_week                     AS week,
           SUM(s.sales_page_views)        AS spvs,
           COALESCE(SUM(b.bookings), 0)   AS trx,
           COALESCE(SUM(b.margin_gbp), 0) AS margin,
           (trx / spvs) * 100             AS cvr,
           ROUND((margin / spvs), 2)      AS gpv
--            ROW_NUMBER() OVER (PARTITION BY global_sale_id ORDER BY cvr DESC) AS cvr_rank, --to get best cvr
--            ROW_NUMBER() OVER (PARTITION BY global_sale_id ORDER BY gpv DESC) AS gpv_rank  --to get best gpv
    FROM se.data.se_sale_attributes sa
             LEFT JOIN spvs_by_week s ON sa.se_sale_id = s.se_sale_id
             LEFT JOIN bookings_by_week b ON sa.se_sale_id = b.se_sale_id AND s.spv_week = b.booking_week
    WHERE sa.se_sale_id IN (
        SELECT se_sale_id
        FROM list_of_sales
    )
          --aggregate up to global sale id
    GROUP BY 1, 2, 3, 4
)
   , best_cvr AS (
    --filter to each sale's best week cvr
    SELECT global_sale_id,
           ROUND(cvr, 2) AS best_cvr
    FROM week_metrics
        QUALIFY ROW_NUMBER() OVER (PARTITION BY global_sale_id ORDER BY cvr DESC) = 1
)
   , best_gpv AS (
    --filter to each sale's best week gpv
    SELECT global_sale_id,
           ROUND(gpv, 2) AS best_gpv
    FROM week_metrics
        QUALIFY ROW_NUMBER() OVER (PARTITION BY global_sale_id ORDER BY gpv DESC) = 1
)
   , min_sale_start_date AS (
    SELECT sa.salesforce_opportunity_id AS global_sale_id,
           --get minimum start date across all sales with same global id
           MIN(sa.start_date)           AS start_date
    FROM se.data.se_sale_attributes sa
    WHERE sa.se_sale_id IN (
        SELECT se_sale_id
        FROM list_of_sales
    )
    GROUP BY 1
)

SELECT wm.sale_name,
       wm.sale_active,
       ms.start_date,
       DATEDIFF(DAY, ms.start_date, current_date) AS days_live,
       wm.week,
       wm.spvs,
       wm.trx,
       wm.margin,
       bc.best_cvr,
       ROUND(wm.cvr, 2)                           AS cvr_now,
       bg.best_gpv,
       ROUND(wm.gpv, 2)                           AS gpv_now
FROM week_metrics wm
         LEFT JOIN min_sale_start_date ms ON wm.global_sale_id = ms.global_sale_id
         LEFT JOIN best_cvr bc ON wm.global_sale_id = bc.global_sale_id
         LEFT JOIN best_gpv bg ON wm.global_sale_id = bg.global_sale_id
WHERE wm.week = dateadd(WEEK, -1, date_trunc(WEEK, current_date)) --last week
;

------------------------------------------------------------------------------------------------------------------------
--query for niro on weekend avail

WITH agg_rooms_to_hotel AS (
    --aggregate room counts up to hotel by day
    SELECT shra.cms_hotel_id,
           shra.hotel_name,
           shra.hotel_code,
           shra.inventory_date,
           shra.inventory_day,
           SUM(shra.no_total_rooms)     AS no_total_rooms,
           SUM(shra.no_available_rooms) AS no_available_rooms
    FROM se.data.se_hotel_room_availability shra
    GROUP BY 1, 2, 3, 4, 5
)

SELECT hbd.cms_hotel_id,
       hbd.hotel_name,
       hbd.hotel_code,

       SUM(CASE WHEN hbd.inventory_day IN ('Fri', 'Sat') THEN 1 END)                      AS total_weekend_days,
       SUM(CASE
               WHEN hbd.inventory_day IN ('Fri', 'Sat') AND hbd.no_available_rooms > 0
                   THEN 1 END)                                                            AS available_weekend_days,
       SUM(CASE WHEN hbd.inventory_day IN ('Fri', 'Sat') THEN hbd.no_total_rooms END)     AS total_weekend_rooms_nights,
       SUM(CASE WHEN hbd.inventory_day IN ('Fri', 'Sat') THEN hbd.no_available_rooms END) AS available_weekend_rooms_nights
FROM agg_rooms_to_hotel hbd
GROUP BY 1, 2, 3;


------------------------------------------------------------------------------------------------------------------------
--sale level report


------------------------------------------------------------------------------------------------------------------------
--demonstation attaching offer to hotel inventory
SELECT r.*,
       p.hotel_id,
       p.id,
       bop.base_offer_products_id,
       bop.product_id,
       o.offer_name_object['en_GB']::VARCHAR AS offer_name,
       o.se_offer_id,
       o.board_type,
       o.offer_active,
       o.provider_name,
       o.internal_note

FROM se_dev_robin.data.se_hotel_room_availability r
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON p.hotel_id = r.cms_hotel_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop ON p.id = bop.product_id
         LEFT JOIN se_dev_robin.data.se_offer_attributes o ON bop.base_offer_products_id = o.se_offer_id
;


SELECT s.se_sale_id,
       hso.*,
       o.*,
       h.*,
       hs.*
FROM data_vault_mvp_dev_robin.dwh.se_sale s
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso ON s.se_sale_id = 'A' || hso.hotel_sale_id
         LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offer o ON o.base_offer_id = hso.hotel_offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                   ON o.base_offer_id = bop.base_offer_products_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON bop.product_id = p.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON h.hotel_code = hs.code
WHERE s.sale_active;

SELECT *
FROM raw_vault_mvp.mari.hotel h
WHERE h.code = '001w000001DVHS5';

SELECT *
FROM data_vault_mvp.mari_snapshots.hotel_snapshot hs
WHERE hs.code = '001w000001DVHS5';


SELECT o.se_offer_id,
       o.offer_name_object['en_GB']::VARCHAR AS offer_name,
       o.board_type,
       o.offer_active,
       o.provider_name,
       o.internal_note,

       r.hotel_code,
       r.hotel_name,

       r.inventory_date,
       r.inventory_day,
       r.no_total_rooms,
       r.no_available_rooms,
       r.no_booked_rooms,
       r.no_closedout_rooms
FROM se_dev_robin.data.se_offer_attributes o
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                   ON bop.base_offer_products_id = o.se_offer_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON p.id = bop.product_id
         LEFT JOIN se_dev_robin.data.se_hotel_room_availability r ON p.hotel_id = r.cms_hotel_id
ORDER BY r.hotel_code, r.room_type_code, r.inventory_date, o.se_offer_id;



SELECT s.se_sale_id,
       s.sale_name,
       s.sale_type,
       s.sale_product,
       hso.offer_id
FROM se_dev_robin.data.se_sale_attributes s
         LEFT JOIN se_dev_robin.data.se_hotel_sale_offer hso ON s.se_sale_id = hso.sale_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop
                   ON s.default_hotel_offer_id = bop.base_offer_products_id
--new data model hotel only sales
WHERE s.data_model = 'New Data Model'
  AND s.product_configuration = 'Hotel'
ORDER BY s.se_sale_id, hso.offer_id;


--reservation list
SELECT b.booking_id,
       b.booking_status,
       b.check_in_date,
       b.check_out_date,
       b.margin_gross_of_toms_gbp,

       s.se_sale_id,
       s.sale_name,
       s.product_configuration,
       s.product_type,
       s.product_line,

       o.se_offer_id,
       o.offer_name,
       o.offer_name_object['en_GB']::VARCHAR AS offer_name,
       o.board_type,
       o.offer_active,
       o.provider_name,
       o.internal_note

FROM data_vault_mvp.dwh.se_booking b
         LEFT JOIN se_dev_robin.data.se_sale_attributes s ON b.sale_id = s.se_sale_id
         LEFT JOIN se_dev_robin.data.se_offer_attributes o ON b.offer_id = 'A' || o.se_offer_id
WHERE LEFT(booking_id, 1) = 'A'     -- ndm sales
  AND b.booking_status = 'COMPLETE' --complete bookings only
  AND b.sale_type = 'HOTEL';
--hotel only bookings

--offer level report
SELECT s.se_sale_id,
       s.sale_name,
       s.start_date::DATE                                     AS start_date,
       s.end_date::DATE                                       AS end_date,
       s.posa_territory,
       s.salesforce_opportunity_id,

       'A' || o.se_offer_id                                   AS se_offer_id,
       o.offer_name_object['en_GB']::VARCHAR                  AS offer_name,
       o.board_type,
       o.offer_active,
       o.provider_name,
       o.internal_note,

       COUNT(DISTINCT b.booking_id)                           AS bookings,
       COALESCE(SUM(b.no_nights), 0)                          AS room_nights,
       COALESCE(ROUND(SUM(b.margin_gross_of_toms_gbp), 2), 0) AS margin

FROM se_dev_robin.data.se_sale_attributes s
         LEFT JOIN se_dev_robin.data.se_hotel_sale_offer hso ON s.se_sale_id = hso.sale_id
         LEFT JOIN se_dev_robin.data.se_offer_attributes o ON hso.offer_id = o.se_offer_id
         LEFT JOIN data_vault_mvp.dwh.se_booking b
                   ON s.se_sale_id = b.sale_id
                       AND 'A' || o.se_offer_id = b.offer_id
                       AND b.booking_status = 'COMPLETE' --complete bookings only
WHERE s.data_model = 'New Data Model'
  AND s.product_configuration = 'Hotel'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
ORDER BY se_sale_id, se_offer_id;


------------------------------------------------------------------------------------------------------------------------
--Next steps:
--cm to rank dimensions measures
--dp to discover new dimensions measures
--one 1 and 2 done, weigh up effort to get new dimensions measures
--dp how to surface the data
--dp create views in snowflake that cm can temporarily download
--dp make snowflake account for maxime


CREATE OR REPLACE SCHEMA collab.aoho_reporting;



CREATE OR REPLACE VIEW collab.aoho_reporting.hotel_room_allocation_by_date COPY GRANTS AS
(
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
       shra.no_closedout_rooms
FROM se.data.se_hotel_room_availability shra
    );

CREATE OR REPLACE VIEW collab.aoho_reporting.hotel_room_allocation COPY GRANTS AS
(
SELECT shra.mari_hotel_id,                                                                                  --currently mari id
       shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
       shra.room_type_id,
       shra.room_type_name,
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
-- WHERE shra.mari_hotel_id = 1628
-- shra.hotel_code = '001w000001DVHxf'
GROUP BY 1, 2, 3, 4, 5, 6
    );

CREATE OR REPLACE VIEW collab.aoho_reporting.hotel_allocation COPY GRANTS AS
(
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
       shra.hotel_name,
       shra.hotel_code,
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
GROUP BY 1, 2, 3, 4, 5, 6
    );
CREATE OR REPLACE VIEW collab.aoho_reporting.hotel_allocation_by_month COPY GRANTS AS
(
SELECT shra.mari_hotel_id, --currently mari id
       shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
       TO_CHAR(DATE_TRUNC(MONTH, shra.inventory_date), 'YYYY-MM') AS month,
       SUM(shra.no_available_rooms)                               AS total_available_rooms,
       SUM(shra.no_total_rooms)                                   AS total_total_rooms,
       SUM(shra.no_available_rooms) / SUM(shra.no_total_rooms)    AS availability_perc
FROM se.data.se_hotel_room_availability shra
GROUP BY 1, 2, 3, 4, 5
    );


CREATE OR REPLACE VIEW collab.aoho_reporting.hotel_allocation_by_week COPY GRANTS AS
(
SELECT shra.mari_hotel_id, --currently mari id
       shra.cms_hotel_id,
       shra.hotel_name,
       shra.hotel_code,
       DATE_TRUNC(WEEK, shra.inventory_date)                   AS week_start,
       SUM(shra.no_available_rooms)                            AS total_available_rooms,
       SUM(shra.no_total_rooms)                                AS total_total_rooms,
       SUM(shra.no_available_rooms) / SUM(shra.no_total_rooms) AS availability_perc
FROM se.data.se_hotel_room_availability shra
GROUP BY 1, 2, 3, 4, 5
    );

CREATE OR REPLACE VIEW collab.aoho_reporting.offer_performance COPY GRANTS AS
(
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
    );

CREATE OR REPLACE VIEW collab.aoho_reporting.territory_sale_performance COPY GRANTS AS
(
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
SELECT ss.se_sale_id                AS territory_sale_id,
       ss.salesforce_opportunity_id AS global_sale_id,
       ss.sale_name,
       ss.sale_name_object,
       ss.sale_active,
       ss.posa_territory,
       ss.hotel_code,
       hs.name                      AS hotel_name,
       COALESCE(b.trx, 0)           AS trx,
       COALESCE(b.margin, 0)        AS margin,
       COALESCE(s.spvs, 0)          AS spvs
FROM se.data.se_sale_attributes ss
         LEFT JOIN bookings b ON ss.se_sale_id = b.se_sale_id
         LEFT JOIN spvs s ON ss.se_sale_id = s.se_sale_id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON ss.hotel_code = hs.code
WHERE ss.sale_active
ORDER BY ss.salesforce_opportunity_id
    );


CREATE OR REPLACE VIEW collab.aoho_reporting.global_sale_performance COPY GRANTS AS
(
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


SELECT ss.salesforce_opportunity_id              AS global_sale_id,
       ss.sale_name,
       ss.sale_name_object,
       ss.sale_active,
       a.hotel_name,
       ss.hotel_code,
       a.total_rooms,
       a.total_booked_rooms,
       a.total_available_rooms,
       LISTAGG(DISTINCT ss.se_sale_id, ', ')     AS territory_sale_ids,
       LISTAGG(DISTINCT ss.posa_territory, ', ') AS sale_territories,
       COUNT(DISTINCT ss.se_sale_id)             AS sales,
       MIN(ss.start_date)                        AS global_sale_start_date,
       MAX(ss.end_date)                          AS global_sale_end_date,
       COALESCE(SUM(stl.trx), 0)                 AS trx,
       COALESCE(SUM(stl.margin), 0)              AS margin,
       COALESCE(SUM(stl.spvs), 0)                AS spvs

FROM se.data.se_sale_attributes ss
         LEFT JOIN sale_territory_level stl ON ss.se_sale_id = stl.se_sale_id
         LEFT JOIN allocation_data a ON ss.hotel_code = a.hotel_code

--add allocation date
WHERE ss.sale_active
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    );


GRANT USAGE ON SCHEMA collab.aoho_reporting TO ROLE personal_role__maximedecocq;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.aoho_reporting TO ROLE personal_role__maximedecocq;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.aoho_reporting TO ROLE personal_role__maximedecocq;
GRANT USAGE ON SCHEMA collab.aoho_reporting TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.aoho_reporting TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.aoho_reporting TO ROLE personal_role__samanthamandeldallal;
GRANT USAGE ON SCHEMA collab.aoho_reporting TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.aoho_reporting TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.aoho_reporting TO ROLE personal_role__niroshanbalakumar;
GRANT USAGE ON SCHEMA collab.aoho_reporting TO ROLE personal_role__judygarber;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.aoho_reporting TO ROLE personal_role__judygarber;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.aoho_reporting TO ROLE personal_role__judygarber;
GRANT USAGE ON SCHEMA collab.aoho_reporting TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.aoho_reporting TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.aoho_reporting TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.aoho_reporting TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON ALL TABLES IN SCHEMA collab.aoho_reporting TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.aoho_reporting TO ROLE personal_role__kirstengrieve;


SELECT *
FROM collab.aoho_reporting.hotel_room_allocation_by_date;
SELECT *
FROM collab.aoho_reporting.hotel_room_allocation;
SELECT *
FROM collab.aoho_reporting.hotel_allocation;
SELECT *
FROM collab.aoho_reporting.hotel_allocation_by_week;
SELECT *
FROM collab.aoho_reporting.hotel_allocation_by_month;
SELECT *
FROM collab.aoho_reporting.offer_performance;
SELECT *
FROM collab.aoho_reporting.territory_sale_performance;
SELECT *
FROM collab.aoho_reporting.global_sale_performance;

SELECT ss.schedule_tstamp,
       ss.run_tstamp,
       ss.operation_id,
       ss.created_at,
       ss.updated_at,
       ss.se_sale_id,
       ss.base_sale_id,
       ss.sale_id,
       ss.salesforce_opportunity_id,
       ss.sale_name,
       ss.sale_name_object,
       ss.sale_active,
       ss.class,
       ss.has_flights_available,
       ss.default_preferred_airport_code,
       ss.type,
       ss.hotel_chain_link,
       ss.closest_airport_code,
       ss.is_team20package,
       ss.sale_able_to_sell_flights,
       ss.sale_product,
       ss.sale_type,
       ss.product_type,
       ss.product_configuration,
       ss.product_line,
       ss.data_model,
       ss.hotel_location_info_id,
       ss.active,
       ss.default_hotel_offer_id,
       ss.commission,
       ss.commission_type,
       ss.contractor_id,
       ss.date_created,
       ss.destination_type,
       ss.start_date,
       ss.end_date,
       ss.hotel_id,
       ss.base_currency,
       ss.city_district_id,
       ss.company_id,
       ss.hotel_code,
       ss.latitude,
       ss.longitude,
       ss.location_info_id,
       ss.posa_territory,
       ss.posa_country,
       ss.posa_currency,
       ss.posu_division,
       ss.posu_country,
       ss.posu_city,
       cs.dataset_name,
       cs.dataset_source,
       cs.schedule_interval,
       cs.schedule_tstamp,
       cs.run_tstamp,
       cs.loaded_at,
       cs.filename,
       cs.file_row_number,
       cs.id,
       cs.version,
       cs.name,
       cs.email,
       cs.salesforce_user_id,
       cs.region,
       cs.extract_metadata
FROM data_vault_mvp.dwh.se_sale ss
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.contractor_snapshot cs ON ss.contractor_id = cs.id
WHERE ss.contractor_id IS NULL;


airflow backfill --start_date '2020-07-03 00:00:00' --end_date '2020-07-08 00:00:00' --task_regex '.*' -m dwh__master_tb_booking_list__daily_at_03h00



