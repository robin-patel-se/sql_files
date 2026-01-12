--confirmed that a room can't have two rates with different currencies for the same date
SELECT srr.room_type_id,
       srr.date AS rate_date,
       srr.currency,
       COUNT(*) AS rt_no_rates
FROM se_dev_robin.data.se_room_rates srr
GROUP BY 1, 2, 3
HAVING count(DISTINCT srr.currency) > 1;
--no rows

------------------------------------------------------------------------------------------------------------------------

SELECT h.code                                                                AS hotel_code,
       h.name                                                                AS hotel_name,
       rp.room_type_id                                                       AS room_type_id,
       rp.id                                                                 AS rate_plan_id,
       rp.name                                                               AS rate_plan_name,
       rp.code                                                               AS rate_plan_code,
       rp.rack_code                                                          AS rate_plan_rack_code,
       rp.code || ':' || rp.rack_code                                        AS rate_plan_code_rack_code, -- this is the field to join to the SE CMS
       rp.free_children,
       rp.free_infants,
       rp.cts_commission                                                     AS cash_to_settle_commission,
       r.id                                                                  AS rate_id,
       r.date                                                                AS date,

       --rates converted to gbp
       IFF(rp.currency = 'GBP', r.rate, r.rate * gbpr.fx_rate)               AS rate_gbp,
       IFF(rp.currency = 'GBP', r.rack_rate, r.rack_rate * gbpr.fx_rate)     AS rack_rate_gbp,
       IFF(rp.currency = 'GBP', r.single_rate, r.single_rate * gbpr.fx_rate) AS single_rate_gbp,
       IFF(rp.currency = 'GBP', r.child_rate, r.child_rate * gbpr.fx_rate)   AS child_rate_gbp,
       IFF(rp.currency = 'GBP', r.infant_rate, r.infant_rate * gbpr.fx_rate) AS infant_rate_gbp,
       IFF(rp.currency = 'GBP', 1, gbpr.fx_rate)                             AS rc_to_gbp,

       --rates converted to eur
       IFF(rp.currency = 'EUR', r.rate, r.rate * eurr.fx_rate)               AS rate_eur,
       IFF(rp.currency = 'EUR', r.rack_rate, r.rack_rate * eurr.fx_rate)     AS rack_rate_eur,
       IFF(rp.currency = 'EUR', r.single_rate, r.single_rate * eurr.fx_rate) AS single_rate_eur,
       IFF(rp.currency = 'EUR', r.child_rate, r.child_rate * eurr.fx_rate)   AS child_rate_eur,
       IFF(rp.currency = 'EUR', r.infant_rate, r.infant_rate * eurr.fx_rate) AS infant_rate_eur,
       IFF(rp.currency = 'EUR', 1, eurr.fx_rate)                             AS rc_to_eur,

       --rate currency, the currency the rate is loaded in
       rp.currency,
       r.rate                                                                AS rate_rc,
       r.rack_rate                                                           AS rack_rate_rc,
       r.single_rate                                                         AS single_rate_rc,
       r.child_rate                                                          AS child_rate_rc,
       r.infant_rate                                                         AS infant_rate_rc,

       r.min_los                                                             AS min_length_of_stay,
       r.max_los                                                             AS max_length_of_stay,
       c.id                                                                  AS cash_to_settle_rate_id,
       c.cts_rate,
       c.cts_single_rate,
       c.cts_infant_rate,
       c.cts_child_rate
FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rp
         --only include rate plans that have a rate associated to them
         INNER JOIN data_vault_mvp.mari_snapshots.rate_snapshot r ON r.rate_plan_id = rp.id
         LEFT JOIN data_vault_mvp.mari_snapshots.cash_to_settle_rate_snapshot c ON r.id = c.rate_id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON rp.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
         LEFT JOIN se_dev_robin.data.fx_rates gbpr
                   ON rp.currency = gbpr.source_currency
                       AND gbpr.target_currency = 'GBP'
                       AND gbpr.fx_date = CURRENT_DATE
         LEFT JOIN se_dev_robin.data.fx_rates eurr
                   ON rp.currency = eurr.source_currency
                       AND eurr.target_currency = 'EUR'
                       AND eurr.fx_date = CURRENT_DATE
;

SELECT *
FROM se_dev_robin.data.fx_rates eurr
WHERE eurr.fx_date = CURRENT_DATE
  AND eurr.target_currency = 'GBP';

SELECT *
FROM se_dev_robin.data.se_room_rates srr;

------------------------------------------------------------------------------------------------------------------------
self_describing_task --include 'se/data/se_room_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


------------------------------------------------------------------------------------------------------------------------
WITH allocations_by_room_type_by_day AS (
    --aggregate allocation inventory to room so we can combine with rate
    SELECT shra.room_type_id,
           shra.inventory_date,
           SUM(shra.no_total_rooms)     AS no_total_rooms,
           SUM(shra.no_available_rooms) AS no_available_rooms,
           SUM(shra.no_booked_rooms)    AS no_booked_rooms,
           SUM(shra.no_closedout_rooms) AS no_closedout_rooms
    FROM se_dev_robin.data.se_hotel_room_availability shra
    GROUP BY 1, 2
),
     rates_by_room_type_by_day AS (
         SELECT srr.room_type_id,
                srr.date                             AS rate_date,
                --currency should be distinct, but putting listagg incase multiple
                --currencies come in for some reason
                LISTAGG(DISTINCT srr.currency, ', ') AS currency,
                COUNT(*)                             AS rt_no_rates,
                MIN(srr.rate_gbp)                    AS rt_lead_rate_gbp,
                MIN(srr.rate_eur)                    AS rt_lead_rate_eur,
                MIN(srr.rate_rc)                     AS rt_lead_rate_rc,
                MAX((srr.rack_rate_rc - srr.rate_rc) /
                    NULLIF(srr.rack_rate_rc, 0))     AS rt_top_discount_percentage
         FROM se_dev_robin.data.se_room_rates srr
              --remove 0 rates, CMs 0 out rates when certain offers have been closed out
              --but another parallel offer exists that shares the same allocation
         WHERE srr.rate_rc > 0
         GROUP BY 1, 2
     )
SELECT rrtd.room_type_id,
       rts.name                                                      AS room_type_name,
       hs.code                                                       AS hotel_code,
       hs.name                                                       AS hotel_name,
       rrtd.rate_date,
       rrtd.currency                                                 AS rate_currency,
       rrtd.rt_lead_rate_gbp,
       rrtd.rt_lead_rate_eur,
       rrtd.rt_lead_rate_rc,
       rrtd.rt_top_discount_percentage,
       rrtd.rt_no_rates,
       artd.no_total_rooms                                           AS rt_no_total_rooms,
       artd.no_available_rooms                                       AS rt_no_available_rooms,
       artd.no_booked_rooms                                          AS rt_no_booked_rooms,
       artd.no_closedout_rooms                                       AS rt_no_closedout_rooms,
       IFF(artd.no_available_rooms > 0, rrtd.rt_lead_rate_gbp, NULL) AS rt_available_lead_rate_gbp,
       IFF(artd.no_available_rooms > 0, rrtd.rt_lead_rate_eur, NULL) AS rt_available_lead_rate_eur,
       IFF(artd.no_available_rooms > 0, rrtd.rt_lead_rate_rc, NULL)  AS rt_available_lead_rate_rc,
       IFF(rrtd.rt_lead_rate_gbp = rt_available_lead_rate_gbp,
           artd.no_available_rooms, NULL)                            AS rt_available_lead_rate_rooms
FROM rates_by_room_type_by_day rrtd
         INNER JOIN allocations_by_room_type_by_day artd
                    ON rrtd.room_type_id = artd.room_type_id
                        AND rrtd.rate_date = artd.inventory_date
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.room_type_snapshot rts ON rrtd.room_type_id = rts.id
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id


SELECT *
FROM se_dev_robin.data.se_room_rates srr
WHERE LOWER(srr.hotel_name) LIKE '%carbis%';

------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'se/data/se_room_type_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT *
FROM se_dev_robin.data.se_room_type_rooms_and_rates;

------------------------------------------------------------------------------------------------------------------------

WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations
    SELECT hs.code                              AS hotel_code,
           rtra.rate_date                       AS date,
           MIN(rtra.rt_lead_rate_gbp)           AS hotel_lead_rate_gbp,
           MIN(rtra.rt_lead_rate_eur)           AS hotel_lead_rate_eur,
           MIN(rtra.rt_lead_rate_rc)            AS hotel_lead_rate_rc,
           MIN(rtra.rt_available_lead_rate_gbp) AS hotel_available_lead_rate_gbp,
           MIN(rtra.rt_available_lead_rate_eur) AS hotel_available_lead_rate_eur,
           MIN(rtra.rt_available_lead_rate_rc)  AS hotel_available_lead_rate_rc
    FROM se_dev_robin.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
                        ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    GROUP BY 1, 2
)
SELECT hs.code                                        AS hotel_code,
       hs.name                                        AS hotel_name,
       rtra.rate_date                                 AS date,
       sc.day_name,
       LISTAGG(DISTINCT rtra.rate_currency, ', ')     AS rate_currency,
       SUM(rtra.rt_no_total_rooms)                    AS no_total_rooms,
       SUM(rtra.rt_no_available_rooms)                AS no_available_rooms,
       SUM(rtra.rt_no_booked_rooms)                   AS no_booked_rooms,
       SUM(rtra.rt_no_closedout_rooms)                AS no_closedout_rooms,
       SUM(rtra.rt_no_rates)                          AS no_rates,
       MIN(rtra.rt_lead_rate_gbp)                     AS lead_rate_gbp,
       MIN(rtra.rt_lead_rate_eur)                     AS lead_rate_eur,
       MIN(rtra.rt_lead_rate_rc)                      AS lead_rate_rc,
       MAX(rtra.rt_top_discount_percentage)           AS top_discount_percentage,

       SUM(IFF(rtra.rt_lead_rate_gbp = hdlr.hotel_lead_rate_gbp,
               rtra.rt_available_lead_rate_rooms, 0)) AS lead_rate_rooms,
       SUM(IFF(rtra.rt_lead_rate_gbp = hdlr.hotel_lead_rate_gbp, rtra.rt_no_available_rooms, 0)) /
       SUM(rtra.rt_no_total_rooms)                    AS percent_rooms_at_lead_rate,

       MIN(rtra.rt_available_lead_rate_gbp)           AS available_lead_rate_gbp,
       MIN(rtra.rt_available_lead_rate_eur)           AS available_lead_rate_eur,
       MIN(rtra.rt_available_lead_rate_rc)            AS available_lead_rate_rc,
       SUM(IFF(rtra.rt_available_lead_rate_gbp = hdlr.hotel_available_lead_rate_gbp,
               rtra.rt_available_lead_rate_rooms, 0)) AS available_lead_rate_rooms

FROM se_dev_robin.data.se_room_type_rooms_and_rates rtra
         INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts
                    ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se_dev_robin.data.se_calendar sc ON rtra.rate_date = sc.date_value
GROUP BY 1, 2, 3, 4
ORDER BY hotel_code, date;

self_describing_task --include 'se/data/se_hotel_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT *
FROM se_dev_robin.data.se_hotel_rooms_and_rates shrar
WHERE shrar.hotel_code = '001w000001DVHS5';

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.mari_snapshots CLONE data_vault_mvp.mari_snapshots;



WITH hotel_by_day_lead_rate AS (
    --aggregate rates up to hotel by date for percent allocations calculation
    --cannot nest aggregations
    SELECT hs.code                              AS hotel_code,
           rtra.rate_date                       AS date,
           MIN(rtra.rt_lead_rate_gbp)           AS hotel_lead_rate_gbp,
           MIN(rtra.rt_lead_rate_eur)           AS hotel_lead_rate_eur,
           MIN(rtra.rt_lead_rate_rc)            AS hotel_lead_rate_rc,
           MIN(rtra.rt_available_lead_rate_gbp) AS hotel_available_lead_rate_gbp,
           MIN(rtra.rt_available_lead_rate_eur) AS hotel_available_lead_rate_eur,
           MIN(rtra.rt_available_lead_rate_rc)  AS hotel_available_lead_rate_rc
    FROM se_dev_robin.data.se_room_type_rooms_and_rates rtra
             INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
             INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    GROUP BY 1, 2
)
SELECT DISTINCT
       rtra.hotel_code,
       hs.name                                                     AS hotel_name,
       rtra.rate_date,
       sc.day_name,

       SUM(rtra.rt_no_total_rooms)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS no_total_rooms,
       SUM(rtra.rt_no_available_rooms)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS no_available_rooms,
       SUM(rtra.rt_no_booked_rooms)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS no_booked_rooms,
       SUM(rtra.rt_no_closedout_rooms)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS no_closedout_rooms,
       SUM(rtra.rt_no_rates)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS no_rates,
       LISTAGG(DISTINCT rtra.rate_currency, ', ')
               OVER (PARTITION BY rtra.hotel_code, rtra.rate_date) AS rate_currency,

       --lead rate data
       FIRST_VALUE(rtra.room_type_name)
                   OVER (PARTITION BY rtra.hotel_code, rtra.rate_date
                       ORDER BY rtra.rt_lead_rate_rc)              AS lead_rate_room_type_name,
       FIRST_VALUE(rtra.lead_rate_plan_name)
                   OVER (PARTITION BY rtra.hotel_code, rtra.rate_date
                       ORDER BY rtra.rt_lead_rate_rc)              AS lead_rate_plan_name,
       FIRST_VALUE(rtra.lead_rate_plan_code)
                   OVER (PARTITION BY rtra.hotel_code, rtra.rate_date
                       ORDER BY rtra.rt_lead_rate_rc)              AS lead_rate_plan_code,
       MIN(rtra.rt_lead_rate_gbp)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS lead_rate_gbp,
       MIN(rtra.rt_lead_rate_eur)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS lead_rate_eur,
       MIN(rtra.rt_lead_rate_rc)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS lead_rate_rc,
       MAX(rtra.rt_top_discount_percentage)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS top_discount_percentage,
       SUM(IFF(rtra.rt_lead_rate_gbp = hdlr.hotel_lead_rate_gbp,
               rtra.rt_available_lead_rate_rooms,
               0))
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS lead_rate_rooms,
       SUM(IFF(rtra.rt_lead_rate_gbp = hdlr.hotel_lead_rate_gbp, rtra.rt_no_available_rooms, 0))
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date) /
       no_total_rooms                                              AS percent_rooms_at_lead_rate,

       --available rate data
       FIRST_VALUE(IFF(rtra.rt_no_available_rooms > 0, rtra.room_type_name, NULL))
                   IGNORE NULLS OVER (PARTITION BY rtra.hotel_code, rtra.rate_date
                       ORDER BY rtra.rt_lead_rate_rc)              AS available_lead_rate_room_type_name,
       FIRST_VALUE(IFF(rtra.rt_no_available_rooms > 0, rtra.lead_rate_plan_name, NULL))
                   IGNORE NULLS OVER (PARTITION BY rtra.hotel_code, rtra.rate_date
                       ORDER BY rtra.rt_lead_rate_rc)              AS available_lead_rate_plan_name,
       FIRST_VALUE(IFF(rtra.rt_no_available_rooms > 0, rtra.lead_rate_plan_code, NULL))
                   IGNORE NULLS OVER (PARTITION BY rtra.hotel_code, rtra.rate_date
                       ORDER BY rtra.rt_lead_rate_rc)              AS available_lead_rate_plan_code,
       MIN(rtra.rt_available_lead_rate_gbp)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS available_lead_rate_gbp,
       MIN(rtra.rt_available_lead_rate_eur)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS available_lead_rate_eur,
       MIN(rtra.rt_available_lead_rate_rc)
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS available_lead_rate_rc,
       SUM(IFF(rtra.rt_available_lead_rate_gbp = hdlr.hotel_available_lead_rate_gbp,
               rtra.rt_available_lead_rate_rooms,
               0))
           OVER (PARTITION BY rtra.hotel_code, rtra.rate_date)     AS available_lead_rate_rooms
FROM se_dev_robin.data.se_room_type_rooms_and_rates rtra
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.room_type_snapshot rts ON rtra.room_type_id = rts.id
         INNER JOIN data_vault_mvp_dev_robin.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
         LEFT JOIN hotel_by_day_lead_rate hdlr ON hs.code = hdlr.hotel_code AND rtra.rate_date = hdlr.date
         LEFT JOIN se_dev_robin.data.se_calendar sc ON rtra.rate_date = sc.date_value;



SELECT *
FROM se_dev_robin.data.se_hotel_rooms_and_rates shrar
WHERE shrar.lead_rate_plan_name != shrar.available_lead_rate_plan_name;

SELECT *
FROM se_dev_robin.data.se_hotel_rooms_and_rates shrar
WHERE shrar.hotel_code = '001w000001DVHS5' ;

SELECT *
FROM se.data.se_hotel_rooms_and_rates shrar
WHERE shrar.hotel_code = '001w000001DVHS5';


SELECT i.date  AS inventory_date,
       h.code,
       h.name  AS hotel_name,
       rt.name AS room_type_name,
       ii.id,
       ii.date_created,
       ii.last_updated,
       ii.reservation_id,
       ii.inventory_id,
       ii.state
FROM data_vault_mvp.mari_snapshots.inventory_item_snapshot ii
         LEFT JOIN data_vault_mvp.mari_snapshots.inventory_snapshot i ON ii.inventory_id = i.id
         LEFT JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rt ON i.room_type_id = rt.id
         LEFT JOIN data_vault_mvp.mari_snapshots.hotel_snapshot h ON rt.hotel_id = h.id
WHERE h.code = '0011r00001lek7o'
  AND i.date = '2020-08-04';
