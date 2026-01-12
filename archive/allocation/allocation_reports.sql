-- allocations by global sale by day
-- allocations by global sale by room type
-- allocations by territory sale (by day)
-- 4th Report = aggregated - global sale ID

-- allocations by global sale by day
WITH allocation_data AS (
    SELECT shra.hotel_code,
           shra.hotel_name,
           shra.inventory_date,
           shra.inventory_day,
           SUM(shra.no_total_rooms)     AS total_rooms,
           SUM(shra.no_closedout_rooms) AS total_closedout_rooms,
           SUM(shra.no_booked_rooms)    AS total_booked_rooms,
           SUM(shra.no_available_rooms) AS total_available_rooms
    FROM se.data.se_hotel_room_availability shra
    GROUP BY 1, 2, 3, 4
),
     global_sale AS (
         --aggregate territory sales up to global sale level
         SELECT ss.salesforce_opportunity_id              AS global_sale_id,
                ss.sale_name,
                ss.sale_name_object,
                ss.sale_active,
                ss.hotel_code,
                LISTAGG(DISTINCT ss.se_sale_id, ', ')     AS territory_sale_ids,
                LISTAGG(DISTINCT ss.posa_territory, ', ') AS sale_territories,
                COUNT(DISTINCT ss.se_sale_id)             AS sales,
                MIN(ss.start_date)                        AS global_sale_start_date,
                MAX(ss.end_date)                          AS global_sale_end_date
         FROM se.data.se_sale_attributes ss
         WHERE ss.data_model = 'New Data Model'
           AND ss.product_configuration = 'Hotel'
         GROUP BY 1, 2, 3, 4, 5
     )
SELECT gs.global_sale_id,
       gs.sale_name,
       gs.sale_name_object,
       gs.sale_active,
       gs.hotel_code,
       gs.territory_sale_ids,
       gs.sale_territories,
       gs.sales,
       gs.global_sale_start_date,
       gs.global_sale_end_date,
       a.hotel_name,
       a.hotel_code,
       a.inventory_date,
       a.inventory_day,
       a.total_rooms,
       a.total_available_rooms,
       a.total_booked_rooms,
       a.total_closedout_rooms
FROM global_sale gs
         INNER JOIN allocation_data a
                    ON gs.hotel_code = a.hotel_code
                        AND a.inventory_date >= current_date
ORDER BY gs.hotel_code, a.inventory_date
;
WITH rates_by_hotel AS (
    SELECT hs.id    AS mari_hotel_id,
           hs.code  AS hotel_code,
           srr.date AS rate_date,
           srr.rate,
           srr.rack_rate
    FROM se.data.se_room_rates srr
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON srr.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    WHERE srr.rate > 0
),
     agg_day_rate AS (
         --create aggregate for day rates as can't nest aggregates for percent rates at lead rate
         SELECT rbh.mari_hotel_id,
                rbh.hotel_code,
                rbh.rate_date,
                MIN(rbh.rate)                                              AS lead_rate_day,
                MAX(rbh.rack_rate)                                         AS max_rack_rate,
                MAX((rbh.rack_rate - rbh.rate) / NULLIF(rbh.rack_rate, 0)) AS top_discount_percentage_day
         FROM rates_by_hotel rbh
         GROUP BY 1, 2, 3
     ),
     hotel_day_availability AS (
         --find out if a hotel is available on a date,
         SELECT shra.mari_hotel_id,
                shra.hotel_code,
                shra.inventory_date,
                SUM(shra.no_available_rooms)                AS no_available_rooms,
                MAX(IFF(shra.no_available_rooms > 0, 1, 0)) AS hotel_available
         FROM se.data.se_hotel_room_availability shra
         GROUP BY 1, 2, 3
     )


SELECT rbh.mari_hotel_id,
       rbh.hotel_code,
       rbh.rate_date           AS date,
--        rbh.rate,
--        rbh.rack_rate,
       adr.lead_rate_day,
       adr.top_discount_percentage_day,
       hda.hotel_available = 1 AS hotel_available,
       no_available_rooms
--        count(*)                                                AS no_of_rates,
--        SUM(IFF(rbh.rate = adr.lead_rate_day, 1, 0)) / COUNT(*) AS percent_rates_at_lead_rate_day
FROM rates_by_hotel rbh
         LEFT JOIN hotel_day_availability hda ON rbh.hotel_code = hda.hotel_code AND rbh.rate_date = hda.inventory_date
         LEFT JOIN agg_day_rate adr ON rbh.hotel_code = adr.hotel_code AND rbh.rate_date = adr.rate_date
ORDER BY rbh.hotel_code, rbh.rate_date;

SELECT *
FROM se.data.se_room_rates srr;



WITH allocations_by_room_type_by_day AS (
    --aggregate alocation inventory to room so we can combine with rate
    SELECT shra.room_type_id,
           shra.inventory_date,
           SUM(shra.no_total_rooms)     AS no_total_rooms,
           SUM(shra.no_available_rooms) AS no_available_rooms,
           SUM(shra.no_booked_rooms)    AS no_booked_rooms,
           SUM(shra.no_closedout_rooms) AS no_closedout_rooms
    FROM se.data.se_hotel_room_availability shra
    GROUP BY 1, 2
),
     room_lead_rate_available AS (
         --create lead room rate if avail due to not being able to nest aggregates
         SELECT srr.room_type_id,
                srr.date,
                MIN(IFF(artd.no_available_rooms > 0, rate, NULL)) AS lead_room_rate_day_available

         FROM se.data.se_room_rates srr
                  INNER JOIN allocations_by_room_type_by_day artd
                             ON srr.room_type_id = artd.room_type_id AND srr.date = artd.inventory_date
         WHERE srr.date >= CURRENT_DATE
         GROUP BY 1, 2
     ),
     room_rate_allocation AS (
         --room by date level combination of rate information with allocation
         SELECT srr.room_type_id,
                srr.date                                                   AS rate_date,
                srr.currency,
                artd.no_total_rooms,
                artd.no_available_rooms,
                IFF(artd.no_available_rooms > 0, TRUE, FALSE)              AS room_available,
                MIN(rlra.lead_room_rate_day_available)                     AS lead_room_rate_day_available,
                SUM(IFF(artd.no_available_rooms > 0
                            AND rlra.lead_room_rate_day_available = rate,
                        artd.no_available_rooms, 0))                       AS lead_rate_rooms_day_available,
                count(*)                                                   AS no_rates,
                MIN(rate)                                                  AS lead_room_rate_day,
                MAX((srr.rack_rate - srr.rate) / NULLIF(srr.rack_rate, 0)) AS top_room_discount_percentage_day
         FROM se.data.se_room_rates srr
                  INNER JOIN allocations_by_room_type_by_day artd
                             ON srr.room_type_id = artd.room_type_id AND srr.date = artd.inventory_date
                  LEFT JOIN room_lead_rate_available rlra ON srr.room_type_id = rlra.room_type_id AND srr.date = rlra.date
         WHERE srr.date >= CURRENT_DATE
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     hotel_by_day_lead_rate AS (
         --aggregate rates up to hotel by date for percent allocations calculation
         --cannot nest aggregations

         SELECT hs.code                     AS hotel_code,
                rra.currency,
                rra.rate_date               AS date,
                MIN(rra.lead_room_rate_day) AS hotel_lead_rate_day
         FROM room_rate_allocation rra
                  INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rra.room_type_id = rts.id
                  INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         GROUP BY 1, 2, 3

     )

SELECT hs.code                                                                                AS hotel_code,
       hs.name                                                                                AS hotel_name,
       rra.rate_date                                                                          AS date,
       sc.day_name,
       rra.currency,
       SUM(rra.no_total_rooms)                                                                AS no_total_rooms,
       SUM(rra.no_available_rooms)                                                            AS no_available_rooms,
       MAX(rra.room_available)                                                                AS hotel_available, --has any rooms with some availablity

       SUM(rra.no_rates)                                                                      AS no_rates,
       MIN(rra.lead_room_rate_day)                                                            AS lead_rate_day,
       MIN(rra.lead_room_rate_day_available)                                                  AS lead_rate_day_available,
       MIN(rra.lead_rate_rooms_day_available)                                                 AS lead_rate_rooms_day_available,
       --add rooms available at available lead rate
       MAX(rra.top_room_discount_percentage_day)                                              AS top_discount_percentage_day,
       SUM(IFF(rra.lead_room_rate_day = hdlr.hotel_lead_rate_day, rra.no_available_rooms, 0)) AS no_available_rooms_at_lead_rate,
       SUM(IFF(rra.lead_room_rate_day = hdlr.hotel_lead_rate_day, rra.no_available_rooms, 0)) /
       SUM(rra.no_total_rooms)                                                                AS percent_allocations_at_lead_rate

FROM room_rate_allocation rra
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rra.rate_date = hdlr.date
         LEFT JOIN se.data.se_calendar sc ON rra.rate_date = sc.date_value
WHERE LOWER(hs.name) LIKE '%carbis%'
GROUP BY 1, 2, 3, 4, 5
ORDER BY hotel_code, date;

------------------------------------------------------------------------------------------------------------------------
--room type allocation data
WITH allocations_by_room_type_by_day AS (
    --aggregate alocation inventory to room so we can combine with rate
    SELECT shra.room_type_id,
           shra.inventory_date,
           SUM(shra.no_total_rooms)     AS no_total_rooms,
           SUM(shra.no_available_rooms) AS no_available_rooms,
           SUM(shra.no_booked_rooms)    AS no_booked_rooms,
           SUM(shra.no_closedout_rooms) AS no_closedout_rooms
    FROM se.data.se_hotel_room_availability shra
    GROUP BY 1, 2
),
     rates_by_room_type_by_day AS (
         SELECT srr.room_type_id,
                srr.date                                                   AS rate_date,
                srr.currency,
                COUNT(*)                                                   AS rt_no_rates,
                MIN(srr.rate)                                              AS rt_lead_rate,
                MAX((srr.rack_rate - srr.rate) / NULLIF(srr.rack_rate, 0)) AS rt_top_discount_percentage
         FROM se.data.se_room_rates srr
         GROUP BY 1, 2, 3
     )
SELECT rrtd.room_type_id,
       hs.code                                                                        AS hotel_code,
       hs.name                                                                        AS hotel_name,
       rrtd.rate_date,
       rrtd.currency                                                                  AS rate_currency,
       rrtd.rt_lead_rate,
       rrtd.rt_top_discount_percentage,
       rrtd.rt_no_rates,
       artd.no_total_rooms                                                            AS rt_no_total_rooms,
       artd.no_available_rooms                                                        AS rt_no_available_rooms,
       IFF(artd.no_available_rooms > 0, rrtd.rt_lead_rate, NULL)                      AS rt_available_lead_rate,
       IFF(rrtd.rt_lead_rate = rt_available_lead_rate, artd.no_available_rooms, NULL) AS rt_available_lead_rate_rooms
FROM rates_by_room_type_by_day rrtd
         INNER JOIN allocations_by_room_type_by_day artd
                    ON rrtd.room_type_id = artd.room_type_id
                        AND rrtd.rate_date = artd.inventory_date
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rrtd.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id;


SELECT *
FROM se.data.se_room_type_rooms_and_rates srtrar;


-- allocations by global sale by day
CREATE OR REPLACE VIEW collab.allocation_reporting.global_sale_rooms_and_rates COPY GRANTS AS
WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations

    SELECT hs.code                AS hotel_code,
           rtra.rate_currency,
           rtra.rate_date         AS date,
           MIN(rtra.rt_lead_rate) AS hotel_lead_rate
    FROM se.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    GROUP BY 1, 2, 3
),
     global_sale_att AS (
         SELECT ssa.salesforce_opportunity_id              AS global_sale_id,
                ssa.hotel_code,
                LISTAGG(DISTINCT ssa.posa_territory, ', ') AS global_territories,
                COUNT(*)                                   AS global_territory_sales,
                MIN(ssa.start_date)::DATE                  AS global_start_date,
                MAX(ssa.end_date)::DATE                    AS global_end_date
         FROM se.data.se_sale_attributes ssa
         WHERE ssa.data_model = 'New Data Model'
           AND ssa.product_configuration = 'Hotel'
         GROUP BY 1, 2
     )
SELECT hs.code                                        AS hotel_code,
       hs.name                                        AS hotel_name,
       gsa.global_sale_id,
       gsa.global_territories,
       gsa.global_territory_sales,
       gsa.global_start_date,
       gsa.global_end_date,
       rtra.rate_date                                 AS date,
       sc.day_name,
       rtra.rate_currency,
       SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
       SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
       SUM(rtra.rt_no_rates)                          AS no_rates,
       MIN(rtra.rt_lead_rate)                         AS lead_rate,
       MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,

       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate,
               rtra.rt_available_lead_rate_rooms, 0)) AS lead_rate_rooms,
       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate, rtra.rt_no_available_rooms, 0)) /
       SUM(rtra.rt_no_total_rooms)                    AS percent_allocations_at_lead_rate,

       MIN(rtra.rt_available_lead_rate)               AS available_lead_rate,
       MIN(rtra.rt_available_lead_rate_rooms)         AS available_lead_rate_rooms

FROM se.data.se_room_type_rooms_and_rates rtra --switch with
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
         LEFT JOIN global_sale_att gsa ON hs.code = gsa.hotel_code
-- WHERE LOWER(hs.name) LIKE '%carbis%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ORDER BY hotel_code, date;


self_describing_task --include 'se/data/se_room_type_rooms_and_rates'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
SELECT *
FROM se_dev_robin.data.se_room_type_rooms_and_rates srtrar;

-- allocations by global sale by room type
CREATE OR REPLACE VIEW collab.allocation_reporting.room_types_by_rooms_and_rates COPY GRANTS AS
WITH global_sale_att AS (
    SELECT ssa.salesforce_opportunity_id              AS global_sale_id,
           ssa.hotel_code,
           LISTAGG(DISTINCT ssa.posa_territory, ', ') AS global_territories,
           COUNT(*)                                   AS global_territory_sales,
           MIN(ssa.start_date)::DATE                  AS global_start_date,
           MAX(ssa.end_date)::DATE                    AS global_end_date
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.data_model = 'New Data Model'
      AND ssa.product_configuration = 'Hotel'
    GROUP BY 1, 2
)
SELECT hs.code        AS hotel_code,
       hs.name        AS hotel_name,
       gsa.global_sale_id,
       gsa.global_territories,
       gsa.global_territory_sales,
       gsa.global_start_date,
       gsa.global_end_date,
       rtra.rate_date AS date,
       rtra.room_type_id,
       r.name         AS room_name,
       sc.day_name,
       sc.month_name,
       rtra.rate_currency,
       rtra.rt_no_total_rooms,
       rtra.rt_no_available_rooms,
       rtra.rt_no_rates,
       rtra.rt_lead_rate,
       rtra.rt_top_discount_percentage
FROM se.data.se_room_type_rooms_and_rates rtra
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
                    ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
         LEFT JOIN se.data.se_sale_attributes s ON hs.code = s.hotel_code
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot r ON rtra.room_type_id = r.id
         LEFT JOIN global_sale_att gsa ON hs.code = gsa.hotel_code
-- WHERE LOWER(hs.name) LIKE '%carbis%'
ORDER BY hotel_code, date;

-- allocations by territory sale (by day)
CREATE OR REPLACE VIEW collab.allocation_reporting.territory_sale_by_rooms_and_rates COPY GRANTS AS
WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations

    SELECT hs.code                AS hotel_code,
           rtra.rate_currency,
           rtra.rate_date         AS date,
           MIN(rtra.rt_lead_rate) AS hotel_lead_rate
    FROM se.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    GROUP BY 1, 2, 3
)

SELECT hs.code                                        AS hotel_code,
       hs.name                                        AS hotel_name,
       s.salesforce_opportunity_id                    AS global_sale_id,
       s.se_sale_id,
       s.sale_name,
       s.posa_territory,
       s.start_date,
       s.end_date,
       rtra.rate_date                                 AS date,
       sc.day_name,
       rtra.rate_currency,
       SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
       SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
       SUM(rtra.rt_no_rates)                          AS no_rates,
       MIN(rtra.rt_lead_rate)                         AS lead_rate,
       MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,

       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate,
               rtra.rt_available_lead_rate_rooms, 0)) AS lead_rate_rooms,
       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate, rtra.rt_no_available_rooms, 0)) /
       SUM(rtra.rt_no_total_rooms)                    AS percent_allocations_at_lead_rate,

       MIN(rtra.rt_available_lead_rate)               AS available_lead_rate,
       MIN(rtra.rt_available_lead_rate_rooms)         AS available_lead_rate_rooms
FROM se.data.se_room_type_rooms_and_rates rtra
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
                    ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
         LEFT JOIN se.data.se_sale_attributes s ON hs.code = s.hotel_code
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot r ON rtra.room_type_id = r.id
-- WHERE LOWER(hs.name) LIKE '%carbis%'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
ORDER BY hotel_code, date
;

self_describing_task --include 'se/data/se_room_type_rooms_and_rates'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.se_room_type_rooms_and_rates srtrar;

SELECT *
FROM se.data.se_room_rates srr;


SELECT *
FROM collab.allocation_reporting.global_sale_rooms_and_rates gsrar
WHERE gsrar.global_sale_id = '0061r00001FKK5a';

SELECT *
FROM collab.allocation_reporting.territory_sale_by_rooms_and_rates tsbrar
WHERE tsbrar.global_sale_id = '0061r00001FKK5a';

SELECT *
FROM collab.allocation_reporting.room_types_by_rooms_and_rates rtbrar;
WHERE tsbrar.global_sale_id = '0061r00001FKK5a';

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.salesforce_opportunity_id = '0061r00001FKK5a';


GRANT SELECT ON ALL VIEWS IN SCHEMA collab.allocation_reporting TO ROLE personal_role__gianniraftis;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.allocation_reporting TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.allocation_reporting TO ROLE personal_role__judygarber;
GRANT SELECT ON ALL VIEWS IN SCHEMA collab.allocation_reporting TO ROLE personal_role__maximedecocq;
GRANT USAGE ON SCHEMA collab.allocation_reporting TO ROLE personal_role__kirstengrieve;
GRANT USAGE ON SCHEMA collab.allocation_reporting TO ROLE personal_role__gianniraftis;
GRANT USAGE ON SCHEMA collab.allocation_reporting TO ROLE personal_role__judygarber;
GRANT USAGE ON SCHEMA collab.allocation_reporting TO ROLE personal_role__maximedecocq;


--user segmentation

SELECT *
FROM se.data.user_segmentation us
WHERE us.created_at::DATE = CURRENT_DATE;
DELETE
FROM se.data.user_segmentation us
WHERE us.created_at::DATE = CURRENT_DATE;

SELECT *
FROM se.data.active_user_base aub
WHERE aub.created_at::DATE = CURRENT_DATE;


------------------------------------------------------------------------------------------------------------------------
--create a hotel level view
WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations
    SELECT hs.code                AS hotel_code,
           rtra.rate_currency,
           rtra.rate_date         AS date,
           MIN(rtra.rt_lead_rate) AS hotel_lead_rate
    FROM se.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    GROUP BY 1, 2, 3
)
SELECT hs.code                                        AS hotel_code,
       hs.name                                        AS hotel_name,
       rtra.rate_date                                 AS date,
       sc.day_name,
       rtra.rate_currency,
       SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
       SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
       SUM(rtra.rt_no_rates)                          AS no_rates,
       MIN(rtra.rt_lead_rate)                         AS lead_rate,
       MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,

       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate,
               rtra.rt_available_lead_rate_rooms, 0)) AS lead_rate_rooms,
       SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate, rtra.rt_no_available_rooms, 0)) /
       SUM(rtra.rt_no_total_rooms)                    AS percent_rooms_at_lead_rate,

       MIN(rtra.rt_available_lead_rate)               AS available_lead_rate,
       MIN(rtra.rt_available_lead_rate_rooms)         AS available_lead_rate_rooms

FROM se.data.se_room_type_rooms_and_rates rtra
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
GROUP BY 1, 2, 3, 4, 5
ORDER BY hotel_code, date;

self_describing_task --include 'se/data/se_hotel_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE VIEW se_dev_robin.data.se_calendar AS
SELECT *
FROM se.data.se_calendar sc;

SELECT *
FROM se_dev_robin.data.se_hotel_rooms_and_rates;

------------------------------------------------------------------------------------------------------------------------
SELECT * FROM se.data.se_room_type_rooms_and_rates srtrar;

SELECT so.schedule_tstamp,
       so.run_tstamp,
       so.operation_id,
       so.created_at,
       so.updated_at,
       so.se_offer_id,
       so.base_offer_id,
       so.offer_name,
       so.offer_name_object,
       so.offer_active
FROM data_vault_mvp.dwh.se_offer so;

SELECT * FROM data_vault_mvp.dwh.master_se_booking_list msbl WHERE msbl.ratepay_amount IS NOT NULL;

sELECT * FROM se.data_pii.master_all_booking_list mabl;

SELECT * FROM se.data.se_hotel_rooms_and_rates;

