SELECT event_tstamp, touch_experience, count(booking_id)
FROM (SELECT to_date(e.event_tstamp) AS event_tstamp,
             CASE
                 WHEN a.touch_experience = 'native app' THEN 'native app'
                 WHEN a.touch_experience IN ('web', 'tablet web', 'mobile web') THEN 'core'
                 WHEN a.touch_experience = 'not specified' THEN 'not specified'
                 END                 AS touch_experience,
             e.booking_id
      FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions e
               INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                          ON a.touch_id = e.touch_id
      WHERE to_date(event_tstamp) >= CURRENT_DATE - 30
        AND event_category = 'transaction'
      GROUP BY 1, 2, 3)
GROUP BY 1, 2
ORDER BY event_tstamp DESC;

------------------------------------------------------------------------------------------------------------------------
--found instances where we are

SELECT e.event_tstamp,
       e.event_hash,
       CASE
           WHEN a.touch_experience = 'native app' THEN 'native app'
           WHEN a.touch_experience IN ('web', 'tablet web', 'mobile web') THEN 'core'
           WHEN a.touch_experience = 'not specified' THEN 'not specified'
           END AS touch_experience,
       e.booking_id
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                    ON a.touch_id = e.touch_id
WHERE to_date(event_tstamp) >= CURRENT_DATE - 30
  AND event_category = 'transaction'
GROUP BY 1, 2, 3, 4;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
WHERE event_tstamp >= CURRENT_DATE - 30

SELECT LEFT(NULL, 1) != 'A',
       LEFT('A1234', 1) != 'A';

SELECT REGEXP_REPLACE('AA12344', 'AA', 'A')

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions target
SET target.booking_id = REGEXP_REPLACE(target.booking_id, 'AA', 'A');

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;

------------------------------------------------------------------------------------------------------------------------

SELECT e.*,
       CASE
           WHEN a.touch_experience = 'native app' THEN 'native app'
           WHEN a.touch_experience IN ('web', 'tablet web', 'mobile web') THEN 'core'
           WHEN a.touch_experience = 'not specified' THEN 'not specified'
           END AS touch_experience

FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes a
                    ON a.touch_id = e.touch_id
WHERE to_date(event_tstamp) >= CURRENT_DATE - 30
  AND event_category = 'transaction';

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE e.event_tstamp::DATE = '2020-05-20'
  AND e.booking_id IS NOT NULL
  AND e.device_platform = 'not specified';


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification t ON e.event_hash = t.event_hash
WHERE e.event_tstamp::DATE = '2020-05-20'
  AND e.unique_browser_id = 'e5917da6-ed18-446a-9841-8ce75e2089e6';

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_utm_referrer_marker
WHERE event_hash IN (
                     '0c84547c4d757326a46924f7a0c370d72229ed182c4081041b575fc3394d6ef4',
                     '13950e9079373cb9dccd3224ee78dd7fb672aae5c050f930f21b9d861bc64cd9',
                     '33d264f3c6c731ed106dfd2df6d6c530986cd4539c33d826e85bfe1da0d3627a',
                     '12d0982b5020c7f5e4c039580a7f625b60b9d79d900196a1ac5417b283d989c2',
                     '17b89c74f091805477f984361f41b68a0ba8b9bcab0ec6e4ed0fcb1a05e0a91d'
    )

SELECT event_tstamp,
       event_hash,
       is_server_side_event,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR,
       contexts_com_secretescapes_environment_context_1[0]['device_platform']::VARCHAR
FROM hygiene_vault_mvp.snowplow.event_stream e
WHERE contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
      'transaction complete'
  AND e.event_tstamp::DATE >= '2020-04-01'
  AND e.booking_id IS NOT NULL
  AND v_tracker LIKE 'java-%';

SELECT *
FROM data_vault_mvp.dwh.se_booking;


DROP TABLE data_vault_mvp_dev_robin.dwh.se_booking;

SELECT event_tstamp,
       event_name,
       booking_id,
       is_server_side_event,
       unique_browser_id,
       v_tracker,
       contexts_com_secretescapes_content_context_1,
       contexts_com_secretescapes_environment_context_1,
       contexts_com_secretescapes_environment_context_1[0]['device_platform']
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp >= '2020-04-01'
  AND contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
      'transaction complete';

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp >= '2020-03-01'
  AND contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
      'transaction complete'
  AND event_name = 'booking_update_event';



SELECT event_tstamp,
       event_name,
       booking_id,
       is_server_side_event,
       unique_browser_id,
       v_tracker,
       contexts_com_secretescapes_content_context_1,
       contexts_com_secretescapes_environment_context_1,
       contexts_com_secretescapes_environment_context_1[0]['device_platform']
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE event_tstamp >= '2020-04-01'
  AND unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
      'booking confirmed';



SELECT MIN(updated_at)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification; --2020-02-28 17:06:45.849000000

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE booking_id IN ('A1279780', '1279780');

SELECT *
FROM data_vault_mvp.dwh.se_booking
WHERE booking_id = 'A1279776';

SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream__step03__replicate_event_data
WHERE booking_id = 'A1279776';
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream__step02__missing_bookings
WHERE booking_id = 'A1279776';

------------------------------------------------------------------------------------------------------------------------
--to update hygiene to include insemination of events that aren't
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
self_describing_task --include 'dv/dwh/events/00_artificial_transaction_insert/artificial_transaction_insert_se'  --method 'run' --start '2020-02-28 03:00:00' --end '2020-02-28 03:00:00'

--get events that have been added
SELECT created_at, count(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE created_at >= CURRENT_DATE
GROUP BY 1;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream_bkup CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
USE WAREHOUSE pipe_xlarge;
DELETE
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE created_at < '2020-05-26 10:09:08.139000000';

SELECT COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE event_hash IN (SELECT event_hash FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream);

SELECT COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream;

--use the created at based on updated data (run on production)
INSERT INTO hygiene_vault_mvp.snowplow.event_stream
SELECT *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE created_at >= '2020-05-26 10:09:08.139000000' --use date of newly inserted rows from merge
;

--reprocess touched transactions
DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;
CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg CLONE data_vault_mvp.single_customer_view_stg;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream; --with new artifical inseminated bookings in there
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;

self_describing_task --include 'dv/dwh/events/07_events_of_interest/02_module_touched_transactions'  --method 'run' --start '2020-02-28 03:00:00' --end '2020-02-28 03:00:00'

CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;

SELECT event_tstamp::DATE, count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
WHERE event_tstamp > '2020-01-01'
GROUP BY 1;
SELECT event_tstamp::DATE, count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
WHERE event_tstamp > '2020-01-01'
GROUP BY 1;


--edited
SELECT booking_id, event_subcategory
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
WHERE event_tstamp::DATE = '2020-01-01';
--old
SELECT booking_id, event_subcategory
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
WHERE event_tstamp::DATE = '2020-01-01';
