USE WAREHOUSE pipe_xlarge;
USE SCHEMA data_vault_mvp.single_customer_view_stg;

SELECT t.event_hash,
       t.event_tstamp,
       CASE
           WHEN t.booking_id LIKE '%-%' THEN
               'TB-' || REGEXP_SUBSTR(t.booking_id, '-(.*)', 1, 1, 'e')
           ELSE t.booking_id
           END AS new_booking_id
FROM module_touched_transactions t
WHERE t.event_tstamp::DATE >= '2020-02-28';

--dwh_rec tb bookings
SELECT *
FROM data_vault_mvp.dwh.tb_booking
WHERE payment_status NOT IN ('NEW', 'FINISHED')
  AND complete_date >= '2020-02-28';

--dwh_rec se bookings
SELECT *
FROM data_vault_mvp.dwh.se_booking
WHERE booking_completed_date >= '2020-02-28'
  AND booking_status IN ('COMPLETE', 'REFUNDED')
  AND booking_type != 'HOLD';

------------------------------------------------------------------------------------------------------------------------

GRANT USAGE ON SCHEMA collab.dwh_rec TO ROLE personal_role__andypauer;
GRANT SELECT ON TABLE collab.dwh_rec.single_customer_view_transactions TO ROLE personal_role__andypauer;

CREATE OR REPLACE TABLE collab.dwh_rec.single_customer_view_transactions AS (
    WITH dwh_bookings AS (
        SELECT complete_date,
               'TB-' || id  AS booking_id,
               'Travelbird' AS platform
        FROM data_vault_mvp.dwh.tb_booking
        WHERE payment_status NOT IN ('NEW', 'FINISHED') -- bookings that would have ever been confirmed/complete
          AND created_at_dts >= '2020-02-28'
          AND created_at_dts < '2020-03-18'

        UNION

        SELECT booking_completed_date,
               booking_id,
               'Secret Escapes' AS platform
        FROM data_vault_mvp.dwh.se_booking
        WHERE booking_completed_date >= '2020-02-28'
          AND booking_completed_date < '2020-03-18'
          AND booking_status IN ('COMPLETE', 'REFUNDED', 'HOLD_BOOKED') -- bookings that would have ever been confirmed/complete
    ),

         single_customer_view_bookings AS (
             SELECT DISTINCT event_tstamp,
                             booking_id,
                             CASE
                                 WHEN booking_id LIKE 'TB-%' THEN 'Travelbird'
                                 ELSE 'Secret Escapes' END AS platform
             FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions t
             WHERE t.event_tstamp::DATE >= '2020-02-28'
               AND t.event_tstamp::DATE < '2020-03-18'
         ),
         grain AS (
             SELECT booking_id
             FROM dwh_bookings

             UNION

             SELECT booking_id
             FROM single_customer_view_bookings
         )

    SELECT g.booking_id,
           CASE WHEN d.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END AS in_dwh,
           CASE WHEN s.booking_id IS NOT NULL THEN 'exists' ELSE 'does_not_exist' END AS in_scv,
           d.complete_date                                                            AS dwh_date,
           s.event_tstamp                                                             AS scv_date,
           d.platform                                                                 AS dwh_platform,
           s.platform                                                                 AS scv_platform

    FROM grain g
             LEFT JOIN dwh_bookings d ON g.booking_id = d.booking_id
             LEFT JOIN single_customer_view_bookings s ON g.booking_id = s.booking_id
)
;

--aggregated
SELECT COUNT(*),
       SUM(CASE WHEN in_dwh = 'exists' AND in_scv = 'exists' THEN 1 END)         AS in_both,
       SUM(CASE WHEN in_dwh = 'exists' AND in_scv = 'does_not_exist' THEN 1 END) AS only_dwh,
       SUM(CASE WHEN in_dwh = 'does_not_exist' AND in_scv = 'exists' THEN 1 END) AS only_scv
FROM collab.dwh_rec.single_customer_view_transactions;

SELECT *
FROM collab.dwh_rec.single_customer_view_transactions;

--bookings in dwh_rec that aren't in single customer view
SELECT *
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'exists'
  AND in_scv = 'does_not_exist';

--bookings in single customer view that aren't in dwh_rec
SELECT *
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'does_not_exist'
  AND in_scv = 'exists';

------------------------------------------------------------------------------------------------------------------------
--checking bookings that exist in DWH that don't in SCV

--are they in the touched transactions table at all?

SELECT dwh_platform,
       COUNT(*)
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'exists'
  AND in_scv = 'does_not_exist'
GROUP BY 1;

SELECT DISTINCT event_tstamp,
                booking_id
FROM module_touched_transactions t
WHERE booking_id IN (SELECT booking_id
                     FROM collab.dwh_rec.single_customer_view_transactions
                     WHERE in_dwh = 'exists'
                       AND in_scv = 'does_not_exist'
);
-- no

--are they in the hygiene output?
SELECT event_hash,
       se_user_id,
       unique_browser_id,
       cookie_id,
       session_userid,
       event_tstamp,
       booking_id,
       event,
       event_name,
       is_robot_spider_event,
       is_server_side_event,
       contexts_com_secretescapes_booking_context_1[0]['id']::VARCHAR                   AS booking_id,
       v_tracker,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR AS unstruct_sub_category,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'booking confirmed'                                                              AS bc,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR         AS content_sub_category,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'transaction complete'                                                           AS tc
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE event_tstamp >= '2020-01-01'
  AND booking_id IN (SELECT t.booking_id
                     FROM collab.dwh_rec.single_customer_view_transactions t
                     WHERE in_dwh = 'exists'
                       AND in_scv = 'does_not_exist')
  AND is_server_side_event = TRUE
  AND (contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'transaction complete'
    OR
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'booking confirmed')
;
--yes

--are they in raw snowplow
SELECT user_id,
       contexts_com_secretescapes_user_context_1[0]['unique_browser_id']::VARCHAR       AS ubid,
       domain_userid,
       contexts_com_snowplowanalytics_snowplow_client_session_1[0]['userId']::VARCHAR   AS session_id,
       derived_tstamp,
       collector_tstamp,
       event,
       event_name,
       COALESCE(ti_orderid,
                contexts_com_secretescapes_booking_context_1[0]['id']::VARCHAR)         AS booking_id,
       v_tracker,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR AS unstruct_sub_category,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'booking confirmed'                                                              AS bc,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR         AS content_sub_category,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'transaction complete'                                                           AS tc
FROM snowplow.atomic.events
WHERE derived_tstamp >= '2020-01-01'
  AND COALESCE(ti_orderid,
               contexts_com_secretescapes_booking_context_1[0]['id']::VARCHAR)
    IN (SELECT t.booking_id
        FROM collab.dwh_rec.single_customer_view_transactions t
        WHERE in_dwh = 'exists'
          AND in_scv = 'does_not_exist')
  AND (v_tracker LIKE 'java-%');
--yes, but no conf page view or conf booking update events


--definition of confirmed booking event is inconsistent. TB don't send a confirmation pageview event, SE do. So for TB
--we will use the booking update event.

SELECT contexts_com_secretescapes_booking_context_1[0]['id']::VARCHAR                   AS booking_id,
       event_tstamp,
       event_name,
       v_tracker,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR AS unstruct_sub_category,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'booking confirmed'                                                              AS bc,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR         AS content_sub_category,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'transaction complete'                                                           AS tc
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE (v_tracker LIKE 'java-%' OR v_tracker LIKE 'py-%')
  AND collector_tstamp >= '2020-02-28'
  AND (contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'transaction complete'
    OR
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
       'booking confirmed');


--adjustment to module touched transactions

--transcation events
SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       CASE
           --TB send booking ids with their own internal prefix, removing it and putting TB
           WHEN v_tracker LIKE 'py-%' THEN
               'TB-' || REGEXP_SUBSTR(booking_id, '-(.*)', 1, 1, 'e')
           ELSE booking_id
           END                                AS booking_id,
       'transaction'                          AS event_category,
       CASE
           WHEN v_tracker LIKE 'py-%' THEN 'tb platform transaction'
           ELSE 'se platform transaction' END AS event_subcategory

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE t.updated_at >= TIMESTAMPADD('day', -1, '2020-03-04 00:00:00'::TIMESTAMP)

  AND (
        ( -- client side transactions
                e.collector_tstamp < '2020-02-28 00:00:00'
                AND e.event_name IN ('transaction_item', 'transaction')
                AND e.ti_orderid IS NOT NULL
            )
        OR
        ( -- server side transactions
                ( -- SE, we are using booking confirmation page view events due to latency of update events not always able to be fired at time of the session
                        e.collector_tstamp >= '2020-02-28 00:00:00'
                        AND v_tracker LIKE 'java-%' --SE
                        AND
                        contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
                        'transaction complete'
                    )
                OR
                ( -- TB
                        e.collector_tstamp >= '2020-02-28 00:00:00'
                        AND v_tracker LIKE 'py-%' --TB
                        AND
                        unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
                        'booking confirmed'
                    )
            )
    );


CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
self_describing_task --include 'dv/dwh_rec/events/07_events_of_interest/02_module_touched_transactions'  --method 'run' --start '2020-02-29 00:00:00' --end '2020-02-29 00:00:00'

------------------------------------------------------------------------------------------------------------------------
--checking after changes the bookings that are in the dwh_rec that aren't in scv
SELECT *
FROM collab.dwh_rec.single_customer_view_transactions;

--bookings in dwh_rec that aren't in single customer view
SELECT *
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'exists'
  AND in_scv = 'does_not_exist'
  AND dwh_platform = 'Travelbird';

--check if they exist at all in the SCV
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
WHERE booking_id IN (
    SELECT single_customer_view_transactions.booking_id
    FROM collab.dwh_rec.single_customer_view_transactions
    WHERE in_dwh = 'exists'
      AND in_scv = 'does_not_exist'
      AND dwh_platform = 'Secret Escapes'
);

--checking if any events exist in the event stream with this booking id
SELECT booking_id,
       unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR AS up_e_sub_category,
       contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR         AS cont_sub_category,
       event_tstamp,
       event_name,
       event_hash,
       v_tracker,
       is_robot_spider_event
FROM hygiene_vault_mvp.snowplow.event_stream
WHERE booking_id IN (SELECT booking_id
                     FROM collab.dwh_rec.single_customer_view_transactions
                     WHERE in_dwh = 'exists'
                       AND in_scv = 'does_not_exist'
                       AND dwh_platform = 'Secret Escapes');
;


------------------------------------------------------------------------------------------------------------------------
--checking bookings that are in SCV that aren't in DWH

SELECT *
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'does_not_exist'
  AND in_scv = 'exists'
  AND scv_platform = 'Secret Escapes';

--SE

SELECT *
FROM data_vault_mvp.dwh.se_booking
WHERE booking_id IN (SELECT booking_id
                     FROM collab.dwh_rec.single_customer_view_transactions
                     WHERE in_dwh = 'does_not_exist'
                       AND in_scv = 'exists'
                       AND scv_platform = 'Secret Escapes'
);


--TB
SELECT COUNT(*)
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'does_not_exist'
  AND in_scv = 'exists'
  AND scv_platform = 'Travelbird';


SELECT *
FROM data_vault_mvp.dwh.tb_booking
WHERE 'TB-' || id IN (SELECT booking_id
                      FROM collab.dwh_rec.single_customer_view_transactions
                      WHERE in_dwh = 'does_not_exist'
                        AND in_scv = 'exists'
                        AND scv_platform = 'Travelbird');

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
WHERE event_tstamp >= '2020-02-28';

SELECT event_tstamp::DATE,
       COUNT(*)
FROM module_touched_transactions
GROUP BY 1
ORDER BY 1;


SELECT MIN(updated_at)
FROM data_vault_mvp.single_customer_view_stg.module_touchification;

USE WAREHOUSE pipe_xlarge;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions;

SELECT event_tstamp::DATE,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_transactions
GROUP BY 1
ORDER BY 1;

------------------------------------------------------------------------------------------------------------------------
WITH page_view_conf AS (
    SELECT booking_id
    FROM module_touched_transactions
    WHERE event_subcategory = 'se platform transaction'
      AND event_tstamp::DATE >= '2020-02-28')

SELECT e.booking_id,
       sum(CASE
               WHEN e.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR =
                    'initial booking object created' THEN 1 END) AS no_of_initial_ob

FROM hygiene_vault_mvp.snowplow.event_stream e
         INNER JOIN page_view_conf p ON p.booking_id = e.booking_id
GROUP BY 1
ORDER BY 2;

------------------------------------------------------------------------------------------------------------------------

SELECT dwh_date,
       count(*)
FROM collab.dwh_rec.single_customer_view_transactions
WHERE in_dwh = 'exists'
  AND in_scv = 'does_not_exist'
GROUP BY 1;

SELECT * from data_vault_mvp.single_customer_view_stg.module_touched_transactions WHERE booking_id = '54646283';
SELECT * FROM hygiene_vault_mvp.snowplow.event_stream WHERE collector_tstamp > '2020-03-16' AND booking_id = '54646283';


