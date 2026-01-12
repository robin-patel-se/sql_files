USE WAREHOUSE pipe_large;

SELECT bs.saleid,
       bs.transactionid,
       bs.adults::INT                       AS adults,
       bs.children::INT                     AS children,
       bs.infants::INT                      AS infants,
       bs.pax                               AS pax,
       bs.customeremail,
       TO_DATE(bs.datebooked, 'dd/mm/yyyy') AS datebooked,
       TO_DATE(bs.checkin, 'dd/mm/yyyy')    AS checkin,
       TO_DATE(bs.checkout, 'dd/mm/yyyy')   AS checkout,
       bs.nonights::INT                     AS nonights,
       bs.rooms::INT                        AS rooms,
       bs.territory,
       bs.totalsellrate::FLOAT              AS totalsellrate,
       bs.commissionexvat::FLOAT            AS commissionexvat,
       bs.company,
       bs.supplier,
       bs.country,
       bs.division,
       bs.city,
       bs.saledimension,
       bs.flightbuyrate::FLOAT              AS flightbuyrate,
       bs.dynamicflightbooked,
       bs.carrier,
       bs.arrivalairport,
       bs.departureairportcode,
       ltv.lifetime_bookings,
       ltv.lifetime_margin,
       segm.bookings_less_13m,
       segm.bookings_more_13m,
       segm.booker_segment
FROM (
         SELECT *
         FROM raw_vault_mvp.cms_reports.booking_summary QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
     ) bs

         LEFT JOIN (
    SELECT *
    FROM raw_vault_mvp.cms_reports.cancellations QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
) cnx ON bs.transactionid = cnx.transactionid

         LEFT JOIN (SELECT * FROM data_vault_mvp.dwh.se_booking WHERE booking_status = 'REFUNDED') seb
                   ON seb.transaction_id = bs.transactionid

         LEFT JOIN (
    SELECT shiro_user_id::VARCHAR        AS shiro_user_id,
           COUNT(1)                      AS lifetime_bookings,
           SUM(margin_gross_of_toms_gbp) AS lifetime_margin
    FROM data_vault_mvp.dwh.se_booking
    WHERE booking_status = 'COMPLETE'
    GROUP BY 1
) AS ltv ON bs.customerid = ltv.shiro_user_id

         LEFT JOIN (
    SELECT shiro_user_id::VARCHAR AS shiro_user_id,
           bookings_less_13m,
           bookings_more_13m,
           booker_segment
    FROM se.data.user_segmentation
    WHERE date = DATEADD(DAY, -1, CURRENT_DATE)
) AS segm ON bs.customerid = segm.shiro_user_id

WHERE cnx.transactionid IS NULL
  AND seb.transaction_id IS NULL
;

------------------------------------------------------------------------------------------------------------------------
--first refactor
WITH bs_dedupe AS (
    SELECT *
    FROM raw_vault_mvp.cms_reports.booking_summary
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
),
     canx_dedupe AS (
         SELECT *
         FROM raw_vault_mvp.cms_reports.cancellations
             QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
     ),
     ltv AS (
         SELECT shiro_user_id::VARCHAR        AS shiro_user_id,
                COUNT(1)                      AS lifetime_bookings,
                SUM(margin_gross_of_toms_gbp) AS lifetime_margin
         FROM data_vault_mvp.dwh.se_booking
         WHERE booking_status = 'COMPLETE'
         GROUP BY 1
     )
SELECT bs.saleid,
       bs.transactionid,
       bs.adults::INT                                                                AS adults,
       bs.children::INT                                                              AS children,
       bs.infants::INT                                                               AS infants,
       bs.pax                                                                        AS pax,
       bs.customeremail,
       TO_DATE(bs.datebooked, 'dd/mm/yyyy')                                          AS datebooked,
       TO_DATE(bs.checkin, 'dd/mm/yyyy')                                             AS checkin,
       TO_DATE(bs.checkout, 'dd/mm/yyyy')                                            AS checkout,
       ba.check_in_date::DATE                                                        AS check_in_date_new,
       ba.check_out_date::DATE                                                       AS check_out_date_new,
       bs.nonights::INT                                                              AS nonights,
       bs.rooms::INT                                                                 AS rooms,
       bs.territory,
       bs.totalsellrate::FLOAT                                                       AS totalsellrate,
       bs.commissionexvat::FLOAT                                                     AS commissionexvat,
       bs.company,
       bs.supplier,
       bs.country,
       bs.division,
       bs.city,
       bs.saledimension,
       bs.flightbuyrate::FLOAT                                                       AS flightbuyrate,
       bs.dynamicflightbooked,
       bs.carrier,
       bs.arrivalairport,
       bs.departureairportcode,
       ltv.lifetime_bookings,
       ltv.lifetime_margin,
       segm.bookings_less_13m,
       segm.bookings_more_13m,
       segm.booker_segment,
       COALESCE(bs.bundleid, REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')) AS booking_id
FROM bs_dedupe AS bs
         LEFT JOIN ltv AS ltv ON bs.customerid = ltv.shiro_user_id
         LEFT JOIN canx_dedupe AS cnx ON bs.transactionid = cnx.transactionid
         LEFT JOIN data_vault_mvp.dwh.se_booking seb
                   ON seb.transaction_id = bs.transactionid AND seb.booking_status = 'REFUNDED'
         LEFT JOIN se.data.user_segmentation AS segm
                   ON bs.customerid = segm.shiro_user_id::VARCHAR
                       AND date = DATEADD(DAY, -1, CURRENT_DATE)
         LEFT JOIN se.data.se_booking_adjustment ba
                   ON ba.booking_id = COALESCE(bs.bundleid, REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e'))
WHERE cnx.transactionid IS NULL
  AND seb.transaction_id IS NULL
-- AND segm.booker_segment = 'Prospect'
;

------------------------------------------------------------------------------------------------------------------------

WITH bs_dedupe AS (
    SELECT bs.*,
           --make a dwh_rec version of the booking id
           CASE
               WHEN LEFT(bs.transactionid, 1) = 'A'
                   THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
               ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END AS dwh_booking_id
    FROM raw_vault_mvp.cms_reports.booking_summary bs
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
),
     canx_dedupe AS (
         SELECT *
         FROM raw_vault_mvp.cms_reports.cancellations
             QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
     ),
     ltv AS (
         SELECT shiro_user_id::VARCHAR        AS shiro_user_id,
                COUNT(1)                      AS lifetime_bookings,
                SUM(margin_gross_of_toms_gbp) AS lifetime_margin
         FROM data_vault_mvp.dwh.se_booking
         WHERE booking_status = 'COMPLETE'
         GROUP BY 1
     )

SELECT bs.saleid,
       bs.transactionid,
       bs.adults::INT                       AS adults,
       bs.children::INT                     AS children,
       bs.infants::INT                      AS infants,
       bs.pax                               AS pax,
       bs.customeremail,
       TO_DATE(bs.datebooked, 'dd/mm/yyyy') AS datebooked,
       TO_DATE(bs.checkin, 'dd/mm/yyyy')    AS checkin,
       TO_DATE(bs.checkout, 'dd/mm/yyyy')   AS checkout,
       ba.check_in_date::DATE               AS check_in_date_new,
       ba.check_out_date::DATE              AS check_out_date_new,
       bs.nonights::INT                     AS nonights,
       bs.rooms::INT                        AS rooms,
       bs.territory,
       bs.totalsellrate::FLOAT              AS totalsellrate,
       bs.commissionexvat::FLOAT            AS commissionexvat,
       bs.company,
       bs.supplier,
       bs.country,
       bs.division,
       bs.city,
       bs.saledimension,
       bs.flightbuyrate::FLOAT              AS flightbuyrate,
       bs.dynamicflightbooked,
       bs.carrier,
       bs.arrivalairport,
       bs.departureairportcode,
       ltv.lifetime_bookings,
       ROUND(ltv.lifetime_margin, 2)        AS lifetime_margin,
       segm.bookings_less_13m,
       segm.bookings_more_13m,
       segm.booker_segment,
       bs.dwh_booking_id                    AS booking_id

FROM bs_dedupe AS bs
         -- bring in dwh_rec data to enrich the customer id, as the customer id
         -- can be updated but won't be reflected in the cms booking summary
         LEFT JOIN data_vault_mvp.dwh.se_booking dwhb ON bs.dwh_booking_id = dwhb.booking_id
         LEFT JOIN ltv AS ltv ON dwhb.shiro_user_id = ltv.shiro_user_id
         LEFT JOIN canx_dedupe AS cnx ON bs.transactionid = cnx.transactionid
         LEFT JOIN data_vault_mvp.dwh.se_booking bref
                   ON bref.transaction_id = bs.transactionid AND bref.booking_status = 'REFUNDED'
         LEFT JOIN se.data.user_segmentation AS segm
                   ON dwhb.shiro_user_id = segm.shiro_user_id::VARCHAR
                       AND date = DATEADD(DAY, -1, CURRENT_DATE)
    --adjustments can only happen on old model
         LEFT JOIN se.data.se_booking_adjustment ba
                   ON ba.booking_id = COALESCE(bs.bundleid, REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e'))
WHERE cnx.transactionid IS NULL
  AND bref.transaction_id IS NULL
;

------------------------------------------------------------------------------------------------------------------------

WITH bs_dedupe AS (
    SELECT *
    FROM raw_vault_mvp.cms_reports.booking_summary
        QUALIFY ROW_NUMBER() OVER (PARTITION BY transactionid ORDER BY filename DESC) = 1
),
     booking_id AS (
         SELECT CASE
                    WHEN LEFT(bs.transactionid, 1) = 'A'
                        THEN 'A' || REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e')
                    ELSE REGEXP_SUBSTR(bs.transactionid, '-.*-(.*)', 1, 1, 'e') END AS booking_id,
                bs.customerid
         FROM bs_dedupe AS bs
     )

SELECT b.booking_id,
       b.customerid,
       dwh.booking_id,
       dwh.shiro_user_id
FROM booking_id b
         LEFT JOIN data_vault_mvp.dwh.se_booking dwh ON b.booking_id = dwh.booking_id
WHERE dwh.booking_id IS NULL
-- WHERE b.customerid != dwh_rec.shiro_user_id::VARCHAR
;

SELECT booking_id,
       shiro_user_id,
       booking_status
FROM data_vault_mvp.dwh.se_booking
WHERE booking_id IN ('45552713',
                     '47023131',
                     '47305572',
                     '47354572',
                     '48018963',
                     '50594900'
    );

SELECT id,
       user_id,
       status,
       last_updated,
       dataset_source,
       schedule_tstamp,
       run_tstamp,
       loaded_at,
       filename,
       file_row_number
FROM raw_vault_mvp.cms_mysql.booking
WHERE id IN ('45552713',
             '47354572',
             '50594900',
             '48018963',
             '47305572',
             '47023131'
    )
ORDER BY id,;

SELECT id,
       user_id,
       status,
       last_updated
FROM hygiene_snapshot_vault_mvp.cms_mysql.booking
WHERE id IN ('45552713',
             '47354572',
             '50594900',
             '48018963',
             '47305572',
             '47023131'
    );



SELECT booking_id
FROM se.data.se_booking_adjustment
WHERE LEFT(booking_id, 1) = 'A';


SELECT booking_id,
       count(*)
FROM se.data.se_booking_adjustment
GROUP BY 1
HAVING COUNT(*) > 1;

SELECT *
FROM hygiene_vault_mvp.cms_mysql.booking
    QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY last_updated DESC, status ASC) = 1

SELECT schedule_tstamp,
       run_tstamp,
       operation_id,
       created_at,
       updated_at,
       row_dataset_name,
       row_dataset_source,
       row_loaded_at,
       row_schedule_tstamp,
       row_run_tstamp,
       row_filename,
       row_file_row_number,
       booking_id,
       user_id,
       date_created,
       last_updated,
       version,
       status
FROM hygiene_vault_mvp.cms_mysql.booking
WHERE booking_id IN (
                     '45552713',
                     '47354572',
                     '50594900',
                     '48018963',
                     '47305572',
                     '47023131'
    )

SELECT

--file row number
152147

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.cms_mysql.booking CLONE hygiene_vault_mvp.cms_mysql.booking;

--before change
SELECT user_id,
       last_updated,
       row_file_row_number,
       booking_id,
       version,
       status,
       updated_at
FROM hygiene_vault_mvp.cms_mysql.booking
WHERE booking_id IN ('45552713',
                     '47354572',
                     '50594900',
                     '48018963',
                     '47305572',
                     '47023131'
    )
ORDER BY booking_id, last_updated DESC;

--after change
SELECT user_id,
       last_updated,
       row_file_row_number,
       booking_id,
       version,
       status,
       updated_at
FROM hygiene_vault_mvp_dev_robin.cms_mysql.booking
WHERE booking_id IN ('45552713',
                     '47354572',
                     '50594900',
                     '48018963',
                     '47305572',
                     '47023131'
    )
ORDER BY booking_id, last_updated DESC;

UPDATE hygiene_vault_mvp.cms_mysql.booking AS target
SET target.last_updated = '2019-04-29 20:01:33', -- 2019-04-29 20:00:33
    target.updated_at   = '2020-04-22 16:20:00'
WHERE target.row_file_row_number = 152147
  AND target.booking_id = '45552713'
  AND version = 5;

UPDATE hygiene_vault_mvp.cms_mysql.booking AS target
SET target.last_updated = '2019-06-12 17:01:50', -- 2019-06-12 17:00:50
    target.updated_at   = '2020-04-22 16:20:00'
WHERE target.row_file_row_number = 51938
  AND target.booking_id = '47023131'
  AND version = 10;

UPDATE hygiene_vault_mvp.cms_mysql.booking AS target
SET target.last_updated = '2019-06-20 20:01:14', -- 2019-06-20 20:00:14
    target.updated_at   = '2020-04-22 16:20:00'
WHERE target.row_file_row_number = 58869
  AND target.booking_id = '47305572'
  AND version = 9;

UPDATE hygiene_vault_mvp.cms_mysql.booking AS target
SET target.last_updated = '2019-06-22 16:01:28', -- 2019-06-22 16:00:28
    target.updated_at   = '2020-04-22 16:20:00'
WHERE target.row_file_row_number = 32479
  AND target.booking_id = '47354572'
  AND version = 6;

UPDATE hygiene_vault_mvp.cms_mysql.booking AS target
SET target.last_updated = '2019-07-10 18:01:36', -- 2019-07-10 18:00:36
    target.updated_at   = '2020-04-22 16:20:00'
WHERE target.row_file_row_number = 57728
  AND target.booking_id = '48018963'
  AND version = 7;

UPDATE hygiene_vault_mvp.cms_mysql.booking AS target
SET target.last_updated = '2019-10-04 21:01:35', -- 2019-10-04 21:00:35
    target.updated_at   = '2020-04-22 16:20:00'
WHERE target.row_file_row_number = 41745
  AND target.booking_id = '50594900'
  AND version = 7;

------------------------------------------------------------------------------------------------------------------------
--adjust the booker segment for these users

--check when these bookings happened
SELECT booking_id, booking_completed_date, shiro_user_id, booking_status
FROM data_vault_mvp.dwh.se_booking
WHERE booking_id IN ('45552713',
                     '47354572',
                     '50594900',
                     '48018963',
                     '47305572',
                     '47023131'
    );

CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation CLONE se.data.user_segmentation;
CREATE OR REPLACE TABLE se_dev_robin.data.user_segmentation_bkup CLONE se.data.user_segmentation;

SELECT DISTINCT booker_segment
FROM se.data.user_segmentation;

--34529112
SELECT date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation
WHERE shiro_user_id = 34529112
  AND date >= '2019-07-10' -- booking date
ORDER BY date;

UPDATE se_dev_robin.data.user_segmentation target
SET target.bookings_less_13m = 1,
    target.booker_segment    = 'Single'
WHERE shiro_user_id = 34529112
  AND date >= '2019-07-11';

--58602466
SELECT date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation
WHERE shiro_user_id = 58602466
  AND date >= '2019-06-20'
ORDER BY date;

UPDATE se_dev_robin.data.user_segmentation target
SET target.bookings_less_13m = 1,
    target.booker_segment    = 'Single'
WHERE shiro_user_id = 58602466
  AND date >= '2019-06-21';

--58255867
SELECT date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation
WHERE shiro_user_id = 58255867
  AND date >= '2019-04-29'
ORDER BY date;

UPDATE se_dev_robin.data.user_segmentation target
SET target.bookings_less_13m = 1,
    target.booker_segment    = 'Single'
WHERE shiro_user_id = 58255867
  AND date >= '2019-04-30';

--11569412
SELECT date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation
WHERE shiro_user_id = 11569412
  AND date >= '2019-06-12'
ORDER BY date;

UPDATE se_dev_robin.data.user_segmentation target
SET target.bookings_less_13m = 1,
    target.booker_segment    = 'Single'
WHERE shiro_user_id = 11569412
  AND date >= '2019-06-13';

--65784960
SELECT date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation
WHERE shiro_user_id = 65784960
  AND date >= '2019-06-22'
ORDER BY date;

UPDATE se_dev_robin.data.user_segmentation target
SET target.bookings_less_13m = 1,
    target.booker_segment    = 'Single'
WHERE shiro_user_id = 65784960
  AND date >= '2019-06-23';

--22558263
SELECT date,
       shiro_user_id,
       bookings_less_13m,
       bookings_more_13m,
       booker_segment,
       subscription_type,
       opt_in_status
FROM se_dev_robin.data.user_segmentation
WHERE shiro_user_id = 22558263
  AND date >= '2019-10-04'
ORDER BY date;

UPDATE se_dev_robin.data.user_segmentation target
SET target.bookings_less_13m = 1,
    target.booker_segment    = 'Single'
WHERE shiro_user_id = 22558263
  AND date >= '2019-10-05';

--run on prod
CREATE OR REPLACE TABLE se.data.user_segmentation CLONE se_dev_robin.data.user_segmentation;

DROP TABLE se_dev_robin.data.user_segmentation;

SELECT sale_type, has_flights
FROM data_vault_mvp.dwh.se_booking;

SELECT transaction_id,
       record__o['saleDimension']::VARCHAR as sale_dimension,
       record__o['hasFlights']::VARCHAR as has_flights
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

SELECT booking_status, count(*) FROM data_vault_mvp.dwh.se_booking GROUP BY 1;

SELECT DISTINCT status FROM hygiene_vault_mvp.cms_mysql.booking;
SELECT DISTINCT status FROM hygiene_vault_mvp.cms_mysql.reservation;