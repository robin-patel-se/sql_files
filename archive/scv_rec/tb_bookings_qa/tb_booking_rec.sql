USE WAREHOUSE PIPE_LARGE;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE SCHEMA COLLAB.TB_QA;

--bookings
CREATE or replace table COLLAB.TB_QA.TB_BOOKING_EVENTS AS (
    SELECT regexp_substr(CONTEXTS_COM_SECRETESCAPES_BOOKING_CONTEXT_1[0]['id']::VARCHAR, '-(.*)', 1, 1,
                         'e')::INT                                                            AS booking_id,
           COLLECTOR_TSTAMP                                                                   as date,
           CONTEXTS_COM_SECRETESCAPES_PRODUCT_DISPLAY_CONTEXT_1[0]['posa_territory']::VARCHAR AS territory

    FROM SNOWPLOW.ATOMIC.EVENTS
    WHERE UNSTRUCT_EVENT_COM_SECRETESCAPES_BOOKING_UPDATE_EVENT_1['sub_category']::VARCHAR = 'booking confirmed'
      AND COLLECTOR_TSTAMP::DATE >= '2020-01-01'
      AND EVENT_NAME = 'booking_update_event'
      AND V_TRACKER LIKE 'py-%'
)
;

------------------------------------------------------------------------------------------------------------------------
--tb bookings

CREATE OR REPLACE TABLE COLLAB.TB_QA.TB_BOOKING_TRANSACTIONS
(
    booking_id INT,
    date       timestamp,
    territory  varchar
);
USE SCHEMA COLLAB.TB_QA;

put file:///Users/robin/sqls/tb_bookings_qa/TB_bookings.csv @%TB_BOOKING_TRANSACTIONS;

/*SELECT id as booking_id,
       created_at_dts as date,
       case
           WHEN site_id = 44 then 'BE'
           WHEN site_id = 43 then 'DK'
           WHEN site_id = 42 then 'NL'
           WHEN site_id = 2 then 'TB-BE_NL'
           WHEN site_id = 4 then 'TB-BE_FR'
           WHEN site_id = 3 then 'DE'
           WHEN site_id = 23 then 'DK'
           WHEN site_id = 1 then 'TB-NL'
           WHEN site_id = 46 then 'TL'
           WHEN site_id = 47 then 'UK'
           WHEN site_id = 45 then 'DE'
           WHEN NULL then 'undefined'
           else 'unknown'
           END as territory
FROM orders_order
WHERE cast(created_at_dts as date) >= '2020-01-01'
  AND payment_status NOT IN ('NEW', 'FINISHED')
group by 1
;*/

copy into COLLAB.TB_QA.TB_BOOKING_TRANSACTIONS
    file_format = (
        type = csv
            field_delimiter = ','
            skip_header = 1
            field_optionally_enclosed_by = '\"'
            record_delimiter = '\\n'
        );
--transactions
select *
from COLLAB.TB_QA.TB_BOOKING_TRANSACTIONS
ORDER BY BOOKING_ID;
--events
select *
from COLLAB.TB_QA.TB_BOOKING_EVENTS
ORDER BY BOOKING_ID;

use schema COLLAB.TB_QA;

CREATE OR REPLACE TABLE TB_QA_BOOKING_ID_RECONCILIATION AS (
    WITH grain AS (
        SELECT booking_id
        FROM TB_BOOKING_TRANSACTIONS

        UNION

        SELECT booking_id
        FROM TB_BOOKING_EVENTS
    )


    SELECT g.booking_id,
           CASE WHEN e.BOOKING_ID IS NOT NULL THEN 'in_events' else 'not_in_events' end as events_status,
           CASE
               WHEN t.BOOKING_ID IS NOT NULL THEN 'in_transactions'
               else 'not_in_transactions' end                                           as transcations_status,
           e.date                                                                       as events_date,
           e.territory                                                                  as events_territory,
           t.date                                                                       as transaction_date,
           t.territory                                                                  as transactions_territory

    FROM grain g
             LEFT JOIN TB_BOOKING_EVENTS e on g.BOOKING_ID = e.BOOKING_ID
             LEFT JOIN TB_BOOKING_TRANSACTIONS t on g.BOOKING_ID = t.BOOKING_ID
)
;
GRANT USAGE ON SCHEMA COLLAB.TB_QA TO ROLE PERSONAL_ROLE__ADAMJONES;
GRANT SELECT ON TABLE COLLAB.TB_QA.TB_QA_BOOKING_ID_RECONCILIATION TO ROLE PERSONAL_ROLE__ADAMJONES;
GRANT USAGE ON SCHEMA COLLAB.TB_QA TO ROLE PERSONAL_ROLE__GIANNIRAFTIS;
GRANT SELECT ON TABLE COLLAB.TB_QA.TB_QA_BOOKING_ID_RECONCILIATION TO ROLE PERSONAL_ROLE__GIANNIRAFTIS;
GRANT USAGE ON SCHEMA COLLAB.TB_QA TO ROLE PERSONAL_ROLE__KIRSTENGRIEVE;
GRANT SELECT ON TABLE COLLAB.TB_QA.TB_QA_BOOKING_ID_RECONCILIATION TO ROLE PERSONAL_ROLE__KIRSTENGRIEVE;
GRANT USAGE ON SCHEMA COLLAB.TB_QA TO ROLE PERSONAL_ROLE__CARMENMARDIROS;
GRANT SELECT ON TABLE COLLAB.TB_QA.TB_QA_BOOKING_ID_RECONCILIATION TO ROLE PERSONAL_ROLE__CARMENMARDIROS;

SELECT *
FROM TB_QA_BOOKING_ID_RECONCILIATION;

SELECT COUNT(CASE WHEN events_status = 'not_in_events' THEN 1 END)             AS bookings_not_in_events,
       COUNT(CASE WHEN transcations_status = 'not_in_transactions' THEN 1 END) AS bookings_not_in_transactions
FROM TB_QA_BOOKING_ID_RECONCILIATION;

SELECT booking_id,
       events_status,
       transcations_status,
       transaction_date,
       transactions_territory
FROM TB_QA_BOOKING_ID_RECONCILIATION
WHERE events_status = 'not_in_events';

SELECT booking_id,
       events_status,
       transcations_status,
       events_date,
       events_territory

FROM TB_QA_BOOKING_ID_RECONCILIATION
WHERE transcations_status = 'not_in_transactions';

SELECT COUNT(*) FROM TB_QA_BOOKING_ID_RECONCILIATION
WHERE events_date IS NOT NULL;

SELECT COUNT(*) FROM TB_QA_BOOKING_ID_RECONCILIATION
WHERE transaction_date IS NOT NULL;