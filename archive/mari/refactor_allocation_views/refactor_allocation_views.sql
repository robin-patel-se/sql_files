-- collab.allocation_reporting.global_sale_rooms_and_rates -- done
-- collab.allocation_reporting.room_types_by_rooms_and_rates -- done
-- collab.allocation_reporting.territory_sale_by_rooms_and_rates -- done
--
-- collab.dach.date_availability --done
-- collab.dach.weekend_avails --done

------------------------------------------------------------------------------------------------------------------------
-- collab.allocation_reporting.global_sale_rooms_and_rates
-- CREATE OR REPLACE VIEW collab.allocation_reporting.global_sale_rooms_and_rates COPY GRANTS AS
-- WITH hotel_by_day_lead_rate AS (
--     --aggregate rates up to hotel by date for percent allocations calculation
--     --cannot nest aggregations
--     SELECT hs.code                   AS hotel_code,
--            rtra.rate_currency,
--            rtra.rate_date            AS date,
--            MIN(rtra.rt_lead_rate_rc) AS hotel_lead_rate_rc
--     FROM se.data.se_room_type_rooms_and_rates rtra
--              INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
--              INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
--     GROUP BY 1, 2, 3
--
-- ),
--      global_sale_att AS (
--          SELECT ssa.salesforce_opportunity_id              AS global_sale_id,
--                 ssa.hotel_code,
--                 LISTAGG(DISTINCT ssa.posa_territory, ', ') AS global_territories,
--                 COUNT(*)                                   AS global_territory_sales,
--                 MIN(ssa.start_date)::DATE                  AS global_start_date,
--                 MAX(ssa.end_date)::DATE                    AS global_end_date
--          FROM se.data.se_sale_attributes ssa
--          WHERE ssa.data_model = 'New Data Model'
--            AND ssa.product_configuration = 'Hotel'
--          GROUP BY 1, 2
--      )
-- SELECT hs.code                                        AS hotel_code,
--        hs.name                                        AS hotel_name,
--        gsa.global_sale_id,
--        gsa.global_territories,
--        gsa.global_territory_sales,
--        gsa.global_start_date,
--        gsa.global_end_date,
--        rtra.rate_date                                 AS date,
--        sc.day_name,
--        rtra.rate_currency,
--        SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
--        SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
--        SUM(rtra.rt_no_rates)                          AS no_rates,
--        MIN(rtra.rt_lead_rate_rc)                      AS lead_rate,
--        MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,
--        SUM(IFF(rtra.rt_lead_rate_rc = hdlr.hotel_lead_rate_rc,
--                rtra.rt_available_lead_rate_rooms, 0)) AS lead_rate_rooms,
--        SUM(IFF(rtra.rt_lead_rate_rc = hdlr.hotel_lead_rate_rc, rtra.rt_no_available_rooms, 0)) /
--        SUM(rtra.rt_no_total_rooms)                    AS percent_allocations_at_lead_rate,
--        MIN(rtra.rt_available_lead_rate_rc)            AS available_lead_rate,
--        MIN(rtra.rt_available_lead_rate_rooms)         AS available_lead_rate_rooms
-- FROM se.data.se_room_type_rooms_and_rates rtra --switch with
--          INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
--          INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
--          LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
--          LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
--          LEFT JOIN global_sale_att gsa ON hs.code = gsa.hotel_code
-- -- WHERE LOWER(hs.name) LIKE '%carbis%'
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
-- ORDER BY hotel_code, date;


CREATE OR REPLACE VIEW collab.allocation_reporting.global_sale_rooms_and_rates COPY GRANTS AS
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
SELECT gsa.global_sale_id,
       gsa.hotel_code,
       gsa.global_territories,
       gsa.global_territory_sales,
       gsa.global_start_date,
       gsa.global_end_date,
       shrar.hotel_name,
       shrar.date,
       shrar.day_name,
       shrar.no_total_rooms,
       shrar.no_available_rooms,
       shrar.no_booked_rooms,
       shrar.no_closedout_rooms,
       shrar.no_rates,
       shrar.rate_currency,
       shrar.avail_weighted_discount_percentage,
       shrar.average_discount_percentage,
       shrar.top_discount_percentage,
       shrar.avail_weighted_rack_rate_gbp,
       shrar.avail_weighted_rack_rate_eur,
       shrar.avail_weighted_rack_rate_rc,
       shrar.lead_rate_room_type_name,
       shrar.lead_rate_plan_name,
       shrar.lead_rate_plan_code,
       shrar.lead_rate_gbp,
       shrar.lead_rate_eur,
       shrar.lead_rate_rc,
       shrar.lead_rate_rooms,
       shrar.percent_rooms_at_lead_rate,
       shrar.available_lead_rate_room_type_name,
       shrar.available_lead_rate_plan_name,
       shrar.available_lead_rate_plan_code,
       shrar.available_lead_rate_gbp,
       shrar.available_lead_rate_eur,
       shrar.available_lead_rate_rc,
       shrar.available_lead_rate_rooms
FROM se.data.se_hotel_rooms_and_rates shrar
         INNER JOIN global_sale_att gsa ON shrar.hotel_code = gsa.hotel_code

ORDER BY gsa.hotel_code, shrar.date;

------------------------------------------------------------------------------------------------------------------------

-- collab.allocation_reporting.room_types_by_rooms_and_rates

-- CREATE OR REPLACE VIEW collab.allocation_reporting.room_types_by_rooms_and_rates COPY GRANTS AS
-- WITH global_sale_att AS (
--     SELECT ssa.salesforce_opportunity_id              AS global_sale_id,
--            ssa.hotel_code,
--            LISTAGG(DISTINCT ssa.posa_territory, ', ') AS global_territories,
--            COUNT(*)                                   AS global_territory_sales,
--            MIN(ssa.start_date)::DATE                  AS global_start_date,
--            MAX(ssa.end_date)::DATE                    AS global_end_date
--     FROM se.data.se_sale_attributes ssa
--     WHERE ssa.data_model = 'New Data Model'
--       AND ssa.product_configuration = 'Hotel'
--     GROUP BY 1, 2
-- )
-- SELECT hs.code        AS hotel_code,
--        hs.name        AS hotel_name,
--        gsa.global_sale_id,
--        gsa.global_territories,
--        gsa.global_territory_sales,
--        gsa.global_start_date,
--        gsa.global_end_date,
--        rtra.rate_date AS date,
--        rtra.room_type_id,
--        r.name         AS room_name,
--        sc.day_name,
--        rtra.rate_currency,
--        rtra.rt_no_total_rooms,
--        rtra.rt_no_available_rooms,
--        rtra.rt_no_rates,
--        rtra.rt_lead_rate,
--        rtra.rt_top_discount_percentage
-- FROM se.data.se_room_type_rooms_and_rates rtra
--          INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
--                     ON rtra.room_type_id = rts.id
--          INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
--          LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
--          LEFT JOIN se.data.se_sale_attributes s ON hs.code = s.hotel_code
--          LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot r ON rtra.room_type_id = r.id
--          LEFT JOIN global_sale_att gsa ON hs.code = gsa.hotel_code
-- -- WHERE LOWER(hs.name) LIKE '%carbis%'
-- ORDER BY hotel_code, date;

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
SELECT gsa.global_sale_id,
       gsa.global_territories,
       gsa.global_territory_sales,
       gsa.global_start_date,
       gsa.global_end_date,
       srtrar.*
FROM se.data.se_room_type_rooms_and_rates srtrar
         INNER JOIN global_sale_att gsa ON srtrar.hotel_code = gsa.hotel_code
ORDER BY srtrar.hotel_code, srtrar.rate_date;

------------------------------------------------------------------------------------------------------------------------

-- collab.allocation_reporting.territory_sale_by_rooms_and_rates

-- CREATE OR REPLACE VIEW collab.allocation_reporting.territory_sale_by_rooms_and_rates COPY GRANTS AS
-- WITH hotel_by_day_lead_rate AS (
--     --aggregate rates up to hotel by date for percent allocations calculation
--     --cannot nest aggregations
--
--     SELECT hs.code                AS hotel_code,
--            rtra.rate_currency,
--            rtra.rate_date         AS date,
--            MIN(rtra.rt_lead_rate) AS hotel_lead_rate
--     FROM se.data.se_room_type_rooms_and_rates rtra
--              INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
--              INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
--     GROUP BY 1, 2, 3
-- )
--
-- SELECT hs.code                                        AS hotel_code,
--        hs.name                                        AS hotel_name,
--        s.salesforce_opportunity_id                    AS global_sale_id,
--        s.se_sale_id,
--        s.sale_name,
--        s.posa_territory,
--        s.start_date,
--        s.end_date,
--        rtra.rate_date                                 AS date,
--        sc.day_name,
--        rtra.rate_currency,
--        SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
--        SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
--        SUM(rtra.rt_no_rates)                          AS no_rates,
--        MIN(rtra.rt_lead_rate)                         AS lead_rate,
--        MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,
--
--        SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate,
--                rtra.rt_available_lead_rate_rooms, 0)) AS lead_rate_rooms,
--        SUM(IFF(rtra.rt_lead_rate = hdlr.hotel_lead_rate, rtra.rt_no_available_rooms, 0)) /
--        SUM(rtra.rt_no_total_rooms)                    AS percent_allocations_at_lead_rate,
--
--        MIN(rtra.rt_available_lead_rate)               AS available_lead_rate,
--        MIN(rtra.rt_available_lead_rate_rooms)         AS available_lead_rate_rooms
-- FROM se.data.se_room_type_rooms_and_rates rtra
--          INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
--                     ON rtra.room_type_id = rts.id
--          INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
--          LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
--          LEFT JOIN se.data.se_calendar sc ON rtra.rate_date = sc.date_value
--          LEFT JOIN se.data.se_sale_attributes s ON hs.code = s.hotel_code
--          LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot r ON rtra.room_type_id = r.id
-- -- WHERE LOWER(hs.name) LIKE '%carbis%'
-- GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
-- ORDER BY hotel_code, date;


CREATE OR REPLACE VIEW collab.allocation_reporting.territory_sale_by_rooms_and_rates COPY GRANTS AS
WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations

    SELECT hs.code                    AS hotel_code,
           rtra.rate_currency,
           rtra.rate_date             AS date,
           MIN(rtra.rt_lead_rate_gbp) AS hotel_lead_rate
    FROM se.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    GROUP BY 1, 2, 3
)

SELECT rtra.hotel_code,
       rtra.hotel_name,
       s.salesforce_opportunity_id               AS global_sale_id,
       s.se_sale_id,
       s.sale_name,
       s.posa_territory,
       s.start_date,
       s.end_date,
       rtra.rate_date                            AS date,
       rtra.rate_currency,

       rtra.rt_no_total_rooms                    AS no_total_rooms,
       rtra.rt_no_available_rooms                AS no_available_rooms,
       rtra.rt_no_rates                          AS no_rates,
       rtra.rt_lead_rate_gbp                     AS lead_rate_gbp,
       rtra.rt_lead_rate_eur                     AS lead_rate_eur,
       rtra.rt_lead_rate_rc                      AS lead_rate_rc,
       rtra.rt_top_discount_percentage           AS top_discount_percentage,

       IFF(rtra.rt_lead_rate_gbp = hdlr.hotel_lead_rate,
           rtra.rt_available_lead_rate_rooms, 0) AS lead_rate_rooms,
       IFF(rtra.rt_lead_rate_gbp = hdlr.hotel_lead_rate, rtra.rt_no_available_rooms, 0) /
       rtra.rt_no_total_rooms                    AS percent_allocations_at_lead_rate,

       rtra.rt_available_lead_rate_gbp           AS available_lead_rate_gbp,
       rtra.rt_available_lead_rate_eur           AS available_lead_rate_eur,
       rtra.rt_available_lead_rate_rc            AS available_lead_rate_rc,
       rtra.rt_available_lead_rate_rooms         AS available_lead_rate_rooms
FROM se.data.se_room_type_rooms_and_rates rtra
         LEFT JOIN hotel_by_day_lead_rate hdlr ON rtra.hotel_code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se.data.se_sale_attributes s ON rtra.hotel_code = s.hotel_code
ORDER BY hotel_code, date;



------------------------------------------------------------------------------------------------------------------------
-- collab.dach.date_availability

SELECT get_ddl('table', 'collab.dach.date_availability');


CREATE OR REPLACE VIEW date_availability
    COPY GRANTS
AS
SELECT DISTINCT
       global_sale_id,
       global_start_date,
       global_end_date,
       hotel_name,
       hotel_code,
       date,
       no_total_rooms,
       no_available_rooms,
       day_name
FROM collab.allocation_reporting.global_sale_rooms_and_rates;

------------------------------------------------------------------------------------------------------------------------
--collab.dach.weekend_avails

SELECT get_ddl('table', 'collab.dach.weekend_avails');


CREATE OR REPLACE VIEW collab.dach.weekend_avails
    COPY GRANTS
AS
WITH weekend_availability
         AS (
        SELECT *,
               IFF(day_name IN ('Fri', 'Sat'), 'WEEKEND', 'WEEKDAY') AS type
        FROM collab.dach.date_availability
-- WHERE DAY_NAME IN ('Fri', 'Sat')
    )
SELECT global_sale_id,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', CURRENT_DATE) THEN no_available_rooms
               ELSE 0 END)                    AS current_week,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', DATEADD(WEEK, 1, CURRENT_DATE))
                   THEN no_available_rooms
               ELSE 0 END)                    AS week_1,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', DATEADD(WEEK, 2, CURRENT_DATE))
                   THEN no_available_rooms
               ELSE 0 END)                    AS week_2,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', DATEADD(WEEK, 3, CURRENT_DATE))
                   THEN no_available_rooms
               ELSE 0 END)                    AS week_3,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', DATEADD(WEEK, 4, CURRENT_DATE))
                   THEN no_available_rooms
               ELSE 0 END)                    AS week_4,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', DATEADD(WEEK, 5, CURRENT_DATE))
                   THEN no_available_rooms
               ELSE 0 END)                    AS week_5,
       SUM(CASE
               WHEN type = 'WEEKEND' AND DATE_TRUNC('WEEK', date) = DATE_TRUNC('WEEK', DATEADD(WEEK, 6, CURRENT_DATE))
                   THEN no_available_rooms
               ELSE 0 END)                    AS week_6,
       CASE WHEN current_week > 0 THEN 1 ELSE 0 END +
       CASE WHEN week_1 > 0 THEN 1 ELSE 0 END +
       CASE WHEN week_2 > 0 THEN 1 ELSE 0 END +
       CASE WHEN week_3 > 0 THEN 1 ELSE 0 END +
       CASE WHEN week_4 > 0 THEN 1 ELSE 0 END +
       CASE WHEN week_5 > 0 THEN 1 ELSE 0 END +
       CASE WHEN week_6 > 0 THEN 1 ELSE 0 END AS no_weekends_available
FROM weekend_availability
WHERE date >= CURRENT_DATE - 1
  AND DATE_TRUNC('WEEK', date) <= DATEADD(WEEK, 6, CURRENT_DATE)
GROUP BY 1;

SELECT get_ddl('table', 'collab.dach.date_availability');



SELECT stba.touch_id,
       stba.attributed_user_id,
       stba.stitched_identity_type,
       stba.touch_logged_in,
       stba.touch_start_tstamp,
       stba.touch_end_tstamp,
       stba.touch_duration_seconds,
       stba.touch_hostname_territory,
       stba.touch_experience,
       stba.touch_landing_page,
       stba.touch_landing_pagepath,
       stba.touch_hostname,
       stba.touch_exit_pagepath,
       stba.touch_referrer_url,
       stba.touch_event_count,
       stba.touch_has_booking,
       stba.user_ipaddress,
       stba.geo_country,
       stba.geo_city,
       stba.geo_zipcode,
       stba.geo_latitude,
       stba.geo_longitude,
       stba.geo_region_name,
       stba.useragent,
       stba.br_name,
       stba.br_family,
       stba.os_name,
       stba.os_family,
       stba.os_manufacturer,
       stba.dvce_screenwidth,
       stba.dvce_screenheight
FROM se.data_pii.scv_touch_basic_attributes stba;



