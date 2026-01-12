USE WAREHOUSE PIPE_LARGE;
--dwh_rec
SELECT *
FROM DATA_VAULT_MVP.DWH.TB_BOOKING
WHERE COMPLETE_DATE >= '2020-02-28'
  AND payment_status NOT IN ('NEW', 'FINISHED');

--cube
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
  AND (PROVIDER_NAME = 'Travelbird'
    OR (source_name = 'Secret Escapes Poland' AND business_name = 'Poland' AND provider_name = 'Travelbird'));

SELECT COUNT(*)
FROM COLLAB.DWH_REC.CUBE_BOOKINGS
WHERE KEY_DATE_BOOKED >= '2020-02-28'
  AND PROVIDER_NAME = 'Travelbird';

WITH dwh as (
    SELECT 'TB-' || ID AS booking_id,
           COMPLETE_DATE,
           PAYMENT_STATUS,
           MARGIN_GBP
    FROM DATA_VAULT_MVP.DWH.TB_BOOKING
    WHERE COMPLETE_DATE >= '2020-02-28'
      AND payment_status NOT IN ('NEW', 'FINISHED')
)
   , cube as (
    SELECT booking_id,
           key_date_booked,
           status,
           margin_gross_of_toms as margin_gross_of_toms,
           provider_name,
           business_name,
           source_name,
           transaction_id

    FROM COLLAB.DWH_REC.CUBE_BOOKINGS
    WHERE KEY_DATE_BOOKED >= '2020-02-28'
      AND provider_name = 'Travelbird'
)
   , grain as (
    SELECT BOOKING_ID
    FROM dwh
    GROUP BY 1
    UNION
    SELECT BOOKING_ID
    FROM cube
    GROUP BY 1
)
SELECT g.booking_id,
       case when d.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END AS in_dwh,
       case when c.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END AS in_cube,
       d.PAYMENT_STATUS                                                           AS dwh_status,
       c.STATUS                                                                   AS cube_status,
       d.COMPLETE_DATE::DATE                                                      AS dwh_booked_date,
       c.KEY_DATE_BOOKED                                                          AS cube_booked_date
FROM grain g
         LEFT JOIN cube c ON g.booking_id = c.booking_id
         LEFT JOIN dwh d ON g.booking_id = d.booking_id
;

SELECT *
FROM DATA_VAULT_MVP.DWH.TB_BOOKING
WHERE ID = '21895147';

SELECT ID, PAYMENT_STATUS, COMPLETE_DATE
FROM DATA_VAULT_MVP.DWH.TB_BOOKING
WHERE ID = '21895147';

    SELECT booking_id,
           key_date_booked,
           status,
           margin_gross_of_toms as margin_gross_of_toms,
           provider_name,
           business_name,
           source_name,
           transaction_id

    FROM COLLAB.DWH_REC.CUBE_BOOKINGS
;

    SELECT 'TB-' || ID AS booking_id,
           COMPLETE_DATE,
           PAYMENT_STATUS,
           MARGIN_GBP
    FROM DATA_VAULT_MVP.DWH.TB_BOOKING
    WHERE COMPLETE_DATE >= '2020-02-28'
      AND payment_status NOT IN ('NEW', 'FINISHED');

