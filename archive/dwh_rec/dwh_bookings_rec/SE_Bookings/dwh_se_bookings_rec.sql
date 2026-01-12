USE WAREHOUSE PIPE_LARGE;
--hygiene
SELECT BOOKING_ID,
       DATE_TIME_BOOKED::DATE as booking_date,
       BOOKING_STATUS,
       MARGIN_GROSS_OF_TOMS_GBP
FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MONGODB.BOOKING_SUMMARY
WHERE BOOKING_DATE >= '2020-02-28'
ORDER BY BOOKING_DATE, BOOKING_ID;

--dwh_rec
SELECT BOOKING_ID,
       BOOKING_COMPLETED_DATE,
       BOOKING_STATUS,
       MARGIN_GROSS_OF_TOMS_GBP
FROM DATA_VAULT_MVP.DWH.SE_BOOKING
WHERE BOOKING_COMPLETED_DATE >= '2020-02-28'
ORDER BY BOOKING_COMPLETED_DATE, BOOKING_ID;


------------------------------------------------------------------------------------------------------------------------
WITH grain AS (
    SELECT DATE_TIME_BOOKED::DATE as booking_date
    FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MONGODB.BOOKING_SUMMARY
    WHERE DATE_TIME_BOOKED >= '2020-02-28'
    GROUP BY 1

    UNION

    SELECT BOOKING_COMPLETED_DATE as booking_date
    FROM DATA_VAULT_MVP.DWH.SE_BOOKING
    WHERE BOOKING_COMPLETED_DATE >= '2020-02-28'
    GROUP BY 1
),
     hygiene_booking_summary as (
         SELECT DATE_TIME_BOOKED::DATE        as booking_date,
                COUNT(distinct BOOKING_ID)    as bookings,
--                 BOOKING_STATUS,
                SUM(MARGIN_GROSS_OF_TOMS_GBP) as margin_gross_of_toms
         FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MONGODB.BOOKING_SUMMARY
         WHERE BOOKING_DATE >= '2020-02-28'
         GROUP BY 1
     ),
     dwh as (
         SELECT BOOKING_COMPLETED_DATE,
                COUNT(distinct BOOKING_ID)    as bookings,

--                 BOOKING_STATUS,
                SUM(MARGIN_GROSS_OF_TOMS_GBP) as margin_gross_of_toms
         FROM DATA_VAULT_MVP.DWH.SE_BOOKING
         WHERE BOOKING_COMPLETED_DATE >= '2020-02-28'
         GROUP BY 1
     )


SELECT g.booking_date,
       bs.bookings                   hygiene_bookings,
       bs.margin_gross_of_toms::INT  hygiene_margin,
       dwh.bookings                  dwh_bookings,
       dwh.margin_gross_of_toms::INT dwh_margin
FROM grain g
         LEFT JOIN hygiene_booking_summary bs ON g.booking_date = bs.booking_date
         LEFT JOIN dwh ON g.booking_date = dwh.BOOKING_COMPLETED_DATE
;

CREATE SCHEMA IF NOT EXISTS COLLAB.DWH_REC;

GRANT USAGE ON SCHEMA COLLAB.DWH_REC TO ROLE PERSONAL_ROLE__CARMENMARDIROS;
GRANT SELECT ON FUTURE TABLES IN SCHEMA COLLAB.DWH_REC TO ROLE PERSONAL_ROLE__CARMENMARDIROS;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA COLLAB.DWH_REC TO ROLE PERSONAL_ROLE__CARMENMARDIROS;

--query ran in cube
/* SELECT DISTINCT
 DB.transaction_id
,DB.booking_id
,DS.status AS booking_status
,FB.key_date_booked AS booking_date
,FB.commission_ex_vat
,FB.booking_fee_net_rate
,FB.insurance_net_rate
,FB.insurance_net_rate_constant_curremcy
,FB.payment_surcharge_net_rate
,FB.margin
,FB.margin_constant_currency
,FB.margin_gross_of_toms
,FB.margin_gross_of_toms_constant_currency
,FB.margin_local_currency
,DC.customer_id
,DSA.sale_id
,OS.source_name
,BU.business_name AS territory
FROM dbo.dim_bookings DB
INNER JOIN dbo.dim_status DS ON DB.key_status = DS.key_status
INNER JOIN dbo.fact_bookings_v FB ON DB.key_booking = FB.key_booking
INNER JOIN dbo.business_units BU ON FB.key_current_business_unit_id = BU.business_unit_id
INNER JOIN dbo.dim_customers DC ON FB.key_customer = DC.key_customer
INNER JOIN dbo.dim_sales DSA ON FB.key_sale = DSA.key_sale
INNER JOIN dbo.original_sources OS ON FB.key_source = OS.source_id
WHERE fb.key_date_booked >= '2020-02-28'*/

CREATE OR REPLACE TABLE COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS
(
    transaction_id                         VARCHAR,
    booking_id                             VARCHAR,
    status                                 VARCHAR,
    key_date_booked                        DATE,
    commission_ex_vat                      FLOAT,
    booking_fee_net_rate                   FLOAT,
    insurance_net_rate                     FLOAT,
    insurance_net_rate_constant_currency   FLOAT,
    payment_surcharge_net_rate             FLOAT,
    margin                                 FLOAT,
    margin_constant_currency               FLOAT,
    margin_gross_of_toms                   FLOAT,
    margin_gross_of_toms_constant_currency FLOAT,
    margin_local_currency                  FLOAT,
    customer_id                            VARCHAR,
    sale_id                                VARCHAR,
    source_name                            VARCHAR,
    provider_name                          VARCHAR,
    business_name                          VARCHAR
);
USE SCHEMA COLLAB.MUSE_DATA_MODELLING;

put file:///Users/robin/sqls/dwh_bookings_rec/SE_Bookings/cube_bookings.csv @%CUBE_BOOKINGS;

copy into COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS
    file_format = (
        type = csv
            field_delimiter = ','
            skip_header = 1
            field_optionally_enclosed_by = '\"'
            record_delimiter = '\\n'
        );

SELECT *
FROM COLLAB.DWH_REC.CUBE_BOOKINGS
WHERE KEY_DATE_BOOKED >= '2020-02-28'
  AND STATUS = 'Booked'
  AND LEFT(BOOKING_ID, 3) != 'TB-'
  AND LEFT(BOOKING_ID, 1) != 'R';



SELECT KEY_DATE_BOOKED,
       COUNT(distinct BOOKING_ID)     as bookings,
       SUM(MARGIN_GROSS_OF_TOMS)::INT as margin_gross_of_toms
FROM COLLAB.DWH_REC.CUBE_BOOKINGS
WHERE KEY_DATE_BOOKED >= '2020-02-28'
  AND STATUS = 'Booked'
  AND LEFT(BOOKING_ID, 3) != 'TB-' --travelbird
  AND LEFT(BOOKING_ID, 1) != 'R'   --travelist
  AND PROVIDER_NAME = 'Secret Escapes'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--daily comparison
WITH grain AS (
    SELECT DATE_TIME_BOOKED::DATE as booking_date
    FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MONGODB.BOOKING_SUMMARY
    WHERE DATE_TIME_BOOKED >= '2020-02-28'
    GROUP BY 1

    UNION

    SELECT BOOKING_COMPLETED_DATE as booking_date
    FROM DATA_VAULT_MVP.DWH.SE_BOOKING
    WHERE BOOKING_COMPLETED_DATE >= '2020-02-28'
    GROUP BY 1
),
     hygiene_booking_summary as (
         SELECT DATE_TIME_BOOKED::DATE        as booking_date,
                COUNT(distinct BOOKING_ID)    as bookings,
--                 BOOKING_STATUS,
                SUM(MARGIN_GROSS_OF_TOMS_GBP) as margin_gross_of_toms
         FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MONGODB.BOOKING_SUMMARY
         WHERE BOOKING_DATE >= '2020-02-28'
         GROUP BY 1
     ),
     dwh as (
         SELECT BOOKING_COMPLETED_DATE,
                COUNT(distinct BOOKING_ID)                     as bookings,
                COUNT(DISTINCT CASE
                                   WHEN BOOKING_STATUS IN ('COMPLETE', 'HOLD_BOOKED')
                                       THEN BOOKING_ID END)    as complete_bookings,
--                 BOOKING_STATUS,
                SUM(MARGIN_GROSS_OF_TOMS_GBP)                  as margin_gross_of_toms,
                SUM(DISTINCT
                    CASE
                        WHEN BOOKING_STATUS IN ('COMPLETE', 'HOLD_BOOKED')
                            THEN MARGIN_GROSS_OF_TOMS_GBP END) as complete_margin_gross_of_toms
         FROM DATA_VAULT_MVP.DWH.SE_BOOKING
         WHERE BOOKING_COMPLETED_DATE >= '2020-02-28'
         GROUP BY 1
     ),
     cube as (
         SELECT KEY_DATE_BOOKED,
                COUNT(distinct BOOKING_ID)                                          as bookings,
                SUM(MARGIN_GROSS_OF_TOMS)::INT                                      as margin_gross_of_toms,
                SUM(CASE WHEN STATUS = 'Booked' THEN MARGIN_GROSS_OF_TOMS END)::INT as complete_margin_gross_of_toms
         FROM COLLAB.DWH_REC.CUBE_BOOKINGS
         WHERE KEY_DATE_BOOKED >= '2020-02-28'
           AND LEFT(BOOKING_ID, 3) != 'TB-' --travelbird
           AND LEFT(BOOKING_ID, 1) != 'R'   --travelist
           AND PROVIDER_NAME = 'Secret Escapes'
         GROUP BY 1
     )


SELECT g.booking_date,
       bs.bookings                                       hygiene_bookings,
       dwh.bookings                                      dwh_bookings,

       dwh.complete_bookings                             complete_complete_bookings,
       c.bookings                                        cube_bookings,

       bs.margin_gross_of_toms::INT                      hygiene_margin,
       dwh.margin_gross_of_toms::INT                     dwh_margin,
       dwh.complete_margin_gross_of_toms::INT            dwh_complete_margin,
       c.margin_gross_of_toms                            cube_margin,
       c.complete_margin_gross_of_toms                as complete_cube_margin,

       dwh_margin / cube_margin - 1                   as dwh_margin_over_cube_margin,
       dwh_complete_margin / complete_cube_margin - 1 as dwh_c_margin_over_cube_c_margin

FROM grain g
         LEFT JOIN hygiene_booking_summary bs ON g.booking_date = bs.booking_date
         LEFT JOIN dwh ON g.booking_date = dwh.BOOKING_COMPLETED_DATE
         LEFT JOIN cube c ON g.booking_date = c.KEY_DATE_BOOKED
ORDER BY g.booking_date
;


------------------------------------------------------------------------------------------------------------------------
--booking comparison

WITH cube as (
    SELECT case
               when LEFT(transaction_id, 1) = 'A' and
                    source_name IN ('Intuitive Package Provider', 'connected - synxis', 'Secret Escapes Provider')
                   then 'A' || booking_id -- prefix new data model bookings
               else booking_id
               end              as booking_id,
           key_date_booked,
           status,
           margin_gross_of_toms as margin_gross_of_toms,
           provider_name,
           business_name,
           source_name,
           transaction_id
    FROM COLLAB.DWH_REC.CUBE_BOOKINGS
    WHERE KEY_DATE_BOOKED >= '2020-02-28'
      AND coalesce(source_name, '') != 'Travelbird'
      and coalesce(source_name, '') != 'WRD'
      and coalesce(source_name, '') != 'TVLflash'
      and coalesce(source_name, '') != 'Air Berlin'
      and coalesce(source_name, '') != 'BigXtra'
      and coalesce(source_name, '') != 'Secret Escapes Poland'
      and coalesce(source_name, '') != 'Third_party'
      and coalesce(provider_name, '') != 'Travelbird'
),
     dwh as (
         SELECT booking_id,
                booking_completed_date,
                booking_status,
                margin_gross_of_toms_gbp as margin_gross_of_toms
         FROM DATA_VAULT_MVP.DWH.SE_BOOKING
         WHERE booking_completed_date >= '2020-02-28'
           AND booking_status IN ('COMPLETE', 'HOLD_BOOKED', 'REFUNDED')
           AND booking_type != 'HOLD'
     ),
     grain AS (
         SELECT booking_id
         FROM dwh
         GROUP BY 1

         UNION

         SELECT booking_id
         FROM cube
         GROUP BY 1
     )

select g.booking_id,
       case when dwh.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END  AS in_dwh,
       case when cube.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END AS in_cube,
       dwh.booking_completed_date                                                    AS dwh_booked_date,
       dwh.booking_status                                                            AS dwh_status,
       cube.key_date_booked                                                          AS cube_booked_date,
       cube.status                                                                   AS cube_status,
       cube.provider_name,
       cube.business_name,
       cube.source_name,
       dwh.margin_gross_of_toms                                                         dwh_margin,
       cube.margin_gross_of_toms                                                        cube_margin
FROM grain g
         LEFT JOIN dwh ON g.booking_id = dwh.booking_id
         LEFT JOIN cube ON g.booking_id = cube.booking_id
-- WHERE in_cube = 'does_not_exist' AND dwh_rec.BOOKING_COMPLETED_DATE < CURRENT_DATE()--<-- In dwh_rec not in cube
WHERE in_dwh = 'does_not_exist' --<-- In cube not in dwh_rec

create or replace table COLLAB.DWH_REC.booking_reconciliation as (
    WITH cube as (
        SELECT case
                   when LEFT(transaction_id, 1) = 'A' and
                        source_name IN ('Intuitive Package Provider', 'connected - synxis', 'Secret Escapes Provider')
                       then 'A' || booking_id -- prefix new data model bookings
                   else booking_id
                   end              as booking_id,
               key_date_booked,
               status,
               margin_gross_of_toms as margin_gross_of_toms,
               provider_name,
               business_name,
               source_name,
               transaction_id
        FROM COLLAB.DWH_REC.CUBE_BOOKINGS
        WHERE KEY_DATE_BOOKED >= '2020-02-28'
          AND coalesce(provider_name, '') != 'Travelbird'
          and coalesce(provider_name, '') != 'WRD'
          and coalesce(provider_name, '') != 'TVLflash'
          and coalesce(source_name, '') != 'Air Berlin'
          and coalesce(source_name, '') != 'BigXtra'
          and coalesce(source_name, '') != 'Secret Escapes Poland'
          and coalesce(source_name, '') != 'Third_party'
          and coalesce(provider_name, '') != 'Travelbird'
    ),
         dwh as (
             SELECT booking_id,
                    booking_completed_date,
                    booking_status,
                    margin_gross_of_toms_gbp as margin_gross_of_toms
             FROM DATA_VAULT_MVP.DWH.SE_BOOKING
             WHERE booking_completed_date >= '2020-02-28'
               AND booking_status IN ('COMPLETE', 'HOLD_BOOKED', 'REFUNDED')
               AND booking_type != 'HOLD'
         ),
         grain AS (
             SELECT booking_id
             FROM dwh
             GROUP BY 1

             UNION

             SELECT booking_id
             FROM cube
             GROUP BY 1
         )

    select g.booking_id,
           case when dwh.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END  AS in_dwh,
           case when cube.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END AS in_cube,
           dwh.booking_completed_date                                                    AS dwh_booked_date,
           dwh.booking_status                                                            AS dwh_status,
           cube.key_date_booked                                                          AS cube_booked_date,
           cube.status                                                                   AS cube_status,
           cube.provider_name,
           cube.business_name,
           cube.source_name,
           dwh.margin_gross_of_toms                                                         dwh_margin,
           cube.margin_gross_of_toms                                                        cube_margin
    FROM grain g
             LEFT JOIN dwh ON g.booking_id = dwh.booking_id
             LEFT JOIN cube ON g.booking_id = cube.booking_id
-- WHERE in_cube = 'does_not_exist' AND dwh_rec.BOOKING_COMPLETED_DATE < CURRENT_DATE()--<-- In dwh_rec not in cube
--     WHERE in_dwh = 'does_not_exist' --<-- In cube not in dwh_rec
);
------------------------------------------------------------------------------------------------------------------------
--margin

SELECT SUM(dwh_margin),
       SUM(cube_margin),
       SUM(diff)
FROM (
         SELECT booking_id,
                dwh_margin,
                cube_margin,
                dwh_margin - cube_margin AS diff
         FROM COLLAB.DWH_REC.booking_reconciliation

         WHERE in_cube = 'exists'
           AND in_dwh = 'exists'
     )
;
------------------------------------------------------------------------------------------------------------------------
--status
SELECT DISTINCT booking_id,
                cube_status,
                dwh_status
FROM COLLAB.DWH_REC.booking_reconciliation
WHERE in_cube = 'exists'
  AND in_dwh = 'exists'
  AND NOT (cube_status = 'Booked' AND dwh_status = 'COMPLETE');

-- '54444511',
-- '54354686',
-- '54364612',
-- '54444387',
-- '54155031',
-- '54364606',
-- '53599446',
-- 'A1209273',
-- 'A1247708',
-- 'A1247702',


SELECT *
FROM COLLAB.DWH_REC.CUBE_BOOKINGS
WHERE BOOKING_ID IN ('A1247702',
                     'A1247708',
                     '54444511',
                     '54444387');

SELECT *
FROM HYGIENE_VAULT_MVP.CMS_MYSQL.BOOKING
WHERE BOOKING_ID IN (
                     '54444511',
                     '54444387'
    );


SELECT *
FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MYSQL.BOOKING
WHERE BOOKING_ID IN (
                     '54444511',
                     '54444387'
    );

SELECT *
FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MYSQL.RESERVATION
WHERE BOOKING_ID IN ('A1247702',
                     'A1247708');

SELECT BOOKING_ID, BOOKING_STATUS
FROM HYGIENE_SNAPSHOT_VAULT_MVP.CMS_MONGODB.BOOKING_SUMMARY
WHERE BOOKING_ID IN ('A1247702',
                     'A1247708',
                     '54444511',
                     '54444387'
    );


SELECT BOOKING_ID, BOOKING_STATUS, LAST_UPDATED
FROM DATA_VAULT_MVP.DWH.SE_BOOKING
WHERE BOOKING_ID IN ('A1247702',
                     'A1247708',
                     '54444511',
                     '54444387');

GRANT SELECT ON TABLE COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS TO ROLE PERSONAL_ROLE__ANDYPAUER;
SELECT * FROM COLLAB.MUSE_DATA_MODELLING.CUBE_BOOKINGS;