--Elizabeth

SELECT us.booker_segment,
       ua.original_affiliate_territory,
       ROUND(COUNT(*) / (
           SELECT COUNT(*)
           FROM se.data.user_segmentation
           WHERE date = current_date - 1
       ) * 100, 2) AS percentage
FROM se.data.user_segmentation us
         LEFT JOIN se.data.se_user_attributes AS ua
                   ON us.shiro_user_id = ua.shiro_user_id
WHERE date = current_date - 1
GROUP BY 1, 2;

--user
WITH user_totals AS (
    SELECT us.booker_segment,
           ua.original_affiliate_territory,
           count(*) AS users
    FROM se.data.user_segmentation us
             LEFT JOIN se.data.se_user_attributes AS ua
                       ON us.shiro_user_id = ua.shiro_user_id
    WHERE us.date = current_date - 1
    GROUP BY 1, 2
)
SELECT ut.booker_segment,
       ut.original_affiliate_territory,
       ut.users,
       SUM(ut.users) OVER ()                                             AS total_users,
       (ut.users / total_users) * 100                                    AS percentage_total_users,
       SUM(ut.users) OVER (PARTITION BY ut.original_affiliate_territory) AS territory_users,
       (ut.users / territory_users) * 100                                AS percentage_territory_users
FROM user_totals ut;

---------------------------------------------------------------------------------------------------------

--Santana

SELECT dim.sale_name,
       sum(seb.margin_gross_of_toms_cc) AS margin
FROM se.data.dim_sale AS dim
         JOIN se.data.se_booking AS seb
              ON dim.se_sale_id = seb.sale_id
WHERE dim.sale_product = 'Hotel'
  AND dim.sale_start_date >= '2020-05-01'
  AND timestampdiff(DAY, seb.booking_completed_timestamp, dim.sale_start_date) < 7
GROUP BY 1;



SELECT dim.sale_name,
       sum(seb.margin_gross_of_toms_cc) AS margin,
       (CASE WHEN timestampdiff(DAY, seb.booking_completed_timestamp, dim.sale_start_date) < 7 THEN sum(margin) ELSE 0 END)
FROM se.data.dim_sale AS dim
         JOIN se.data.se_booking AS seb
              ON dim.se_sale_id = seb.sale_id
WHERE dim.sale_product = 'Hotel'
  AND dim.sale_start_date >= '2020-05-01'
GROUP BY 1;


SELECT ds.se_sale_id,
       ds.sale_start_date,
       FLOOR(DATEDIFF(DAY, ds.sale_start_date, sc.date_value) / 7) + 1 AS week,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency)             AS margin
FROM se.data.dim_sale ds
         LEFT JOIN se.data.se_calendar sc ON ds.sale_start_date::DATE <= sc.date_value
         LEFT JOIN se.data.fact_complete_booking fcb
                   ON fcb.booking_completed_date::DATE = sc.date_value
                       AND fcb.sale_id = ds.se_sale_id
WHERE ds.sale_product = 'Hotel'
  AND ds.sale_start_date >= '2020-05-01'
  AND sc.date_value <= CURRENT_DATE
  AND ds.se_sale_id = 'A10630'
GROUP BY 1, 2, 3;



SELECT ds.se_sale_id,
       ds.sale_start_date,
       sc.date_value,
       FLOOR(DATEDIFF(DAY, ds.sale_start_date, sc.date_value) / 7) + 1 AS week,
       SUM(fcb.margin_gross_of_toms_gbp_constant_currency)             AS margin
FROM se.data.dim_sale ds
         LEFT JOIN se.data.se_calendar sc ON ds.sale_start_date::DATE <= sc.date_value
         LEFT JOIN se.data.fact_complete_booking fcb
                   ON fcb.booking_completed_date::DATE = sc.date_value
                       AND fcb.sale_id = ds.se_sale_id
WHERE ds.sale_product = 'Hotel'
  AND ds.sale_start_date >= '2020-05-01'
  AND sc.date_value <= CURRENT_DATE
  AND ds.se_sale_id = 'A10630'
GROUP BY 1, 2, 3, 4;

------------------------------------------------------------------------------------------------------------------------
--KJ

-- add Posa territory
-- average_number available_rooms per all days regardless of availability
-- unique available days (days with minimum one room)

--se_hotel_rooms_and_rates
--join se.data.se_sale_attributes on hotel_code

SELECT shra.hotel_name,
       CASE
           WHEN shra.inventory_date >= CURRENT_DATE() AND shra.inventory_date <= DATEADD(DAY, +21, CURRENT_DATE())
               THEN '0_3_weeks'
           WHEN shra.inventory_date >= DATEADD(DAY, +22, CURRENT_DATE()) AND
                shra.inventory_date <= DATEADD(DAY, +42, CURRENT_DATE()) THEN '3_6_weeks'
           WHEN shra.inventory_date >= DATEADD(DAY, +43, CURRENT_DATE()) AND
                shra.inventory_date <= DATEADD(DAY, +64, CURRENT_DATE()) THEN '6_9_weeks'
           WHEN shra.inventory_date >= DATEADD(DAY, +65, CURRENT_DATE()) THEN 'more_than_9_weeks'
           ELSE '0' END                                         AS window,

       SUM(shra.no_total_rooms)                                 AS number_total_rooms,
       SUM(shra.no_booked_rooms)                                AS number_booked_rooms,
       SUM(shra.no_available_rooms)                             AS number_available_rooms,
       SUM(CASE WHEN shra.no_available_rooms > 0 THEN 1 END)    AS number_available_days,
       ROUND(number_available_rooms / number_available_days, 1) AS avg_available_rooms


FROM se.data.se_hotel_room_availability shra
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON shra.mari_hotel_id = h.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON h.id = p.hotel_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_product_snapshot bop ON bop.product_id = p.id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.offer_snapshot bo ON bo.id = bop.base_offer_products_id
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.allocation_snapshot a ON a.offer_id = bop.base_offer_products_id
         LEFT JOIN data_vault_mvp.dwh.se_sale ss ON ss.se_sale_id = 'A' || bo.sale_id
         LEFT JOIN se.data.se_sale_attributes ssa ON ssa.se_sale_id = ss.se_sale_id


GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
WITH avail AS (
    SELECT shrar.hotel_name,
           shrar.hotel_code,
           CASE
               WHEN shrar.date >= DATEADD(DAY, +65, CURRENT_DATE()) THEN 'more_than_9_weeks'
               WHEN shrar.date >= DATEADD(DAY, +43, CURRENT_DATE()) THEN '6_9_weeks'
               WHEN shrar.date >= DATEADD(DAY, +22, CURRENT_DATE()) THEN '3_6_weeks'
               WHEN shrar.date >= CURRENT_DATE() THEN '0_3_weeks'
               ELSE '0' END              AS window,
           SUM(shrar.no_available_rooms) AS no_available_rooms,
           SUM(shrar.no_total_rooms)     AS total_rooms,
           count(1)                      AS days
    FROM se.data.se_hotel_rooms_and_rates shrar
    GROUP BY 1, 2, 3
)
SELECT a.hotel_name,
       a.hotel_code,
       ssa.posa_territory,
       a.window,
       a.no_available_rooms,
       a.total_rooms,
       a.days
FROM avail a
         LEFT JOIN se.data.se_sale_attributes ssa ON a.hotel_code = ssa.hotel_code;

------------------------------------------------------------------------------------------------------------------------
SELECT ssa.se_sale_id,
       fcb.check_in_date,
       count(*)
FROM se.data.fact_complete_booking fcb
         LEFT JOIN se.data.se_sale_attributes ssa ON fcb.sale_id = ssa.se_sale_id
WHERE ssa.sale_active
GROUP BY 1, 2
ORDER BY 1, 2;
------------------------------------------------------------------------------------------------------------------------

-- monthly active users in the app for the last 3 months
-- value for each month

SELECT date_trunc(MONTH, stba.touch_start_tstamp)   AS month,
       count(DISTINCT stba.attributed_user_id_hash) AS users,
       count(*)                                     AS sessions
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_experience = 'native app'
  AND stba.touch_start_tstamp >= '2020-07-01'
  AND stba.touch_start_tstamp < CURRENT_DATE
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------

WITH avail AS
         (
             SELECT shrar.hotel_name,
                    shrar.hotel_code,
                    CASE
                        WHEN shrar.date >= DATEADD(DAY, +65, CURRENT_DATE()) THEN 'more_than_9_weeks'
                        WHEN shrar.date >= DATEADD(DAY, +43, CURRENT_DATE()) THEN '6_9_weeks'
                        WHEN shrar.date >= DATEADD(DAY, +22, CURRENT_DATE()) THEN '3_6_weeks'
                        WHEN shrar.date >= CURRENT_DATE() THEN '0_3_weeks'
                        ELSE '0' END                                       AS window,

                    SUM(shrar.no_available_rooms)                          AS number_available_rooms,
                    SUM(shrar.no_booked_rooms)                             AS number_booked_rooms,
                    SUM(CASE WHEN shrar.no_available_rooms > 0 THEN 1 END) AS number_available_days,
--ROUND(number_available_rooms / number_available_days, 1) AS avg_available_rooms,
                    SUM(shrar.no_total_rooms)                              AS total_rooms,
                    count(1)                                               AS days,
                    ROUND(number_available_rooms / days, 1)                AS avg_available_rooms_in_window
             FROM se.data.se_hotel_rooms_and_rates shrar
             GROUP BY 1, 2, 3
         ),


     bookings AS (
         SELECT ssa.se_sale_id,
                CASE
                    WHEN fcb.check_in_date >= DATEADD(DAY, +65, CURRENT_DATE()) THEN 'more_than_9_weeks'
                    WHEN fcb.check_in_date >= DATEADD(DAY, +43, CURRENT_DATE()) THEN '6_9_weeks'
                    WHEN fcb.check_in_date >= DATEADD(DAY, +22, CURRENT_DATE()) THEN '3_6_weeks'
                    WHEN fcb.check_in_date >= CURRENT_DATE() THEN '0_3_weeks'
                    ELSE '0' END AS window,
                ssa.posa_territory,

                count(*)         AS bookings
         FROM se.data.se_sale_attributes ssa
                  LEFT JOIN se.data.fact_complete_booking fcb ON fcb.sale_id = ssa.se_sale_id
         WHERE fcb.booking_completed_date >= '2020-09-01'
           AND fcb.booking_completed_date <= '2020-09-30'
           AND ssa.sale_active
         GROUP BY 1, 2, 3
     )


SELECT a.hotel_name,
       a.hotel_code,
       ssa.posa_territory,
       a.window,
       a.number_available_rooms,
       a.avg_available_rooms_in_window,
       a.number_available_days,
       a.total_rooms,
       sum(b.bookings)

FROM avail a
         LEFT JOIN se.data.se_sale_attributes ssa ON a.hotel_code = ssa.hotel_code
         LEFT JOIN bookings b ON b.se_sale_id = ssa.se_sale_id AND a.window = b.window

GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


SELECT * FROM se.data.se_sale_attributes ssa WHERE ssa.hotel_code = '001w000001DVHS5'