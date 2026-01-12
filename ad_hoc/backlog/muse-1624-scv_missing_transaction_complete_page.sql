SELECT stt.event_subcategory,
       COUNT(*)
FROM se.data.scv_touched_transactions stt
WHERE stt.event_tstamp >= CURRENT_DATE - 30
GROUP BY 1
;

SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.event_tstamp >= CURRENT_DATE - 30
  AND stt.event_subcategory = 'backfill_booking';

USE WAREHOUSE pipe_2xlarge;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '1e1ce1456c04e5a5b394f1ab5fb8878345a0e3174a12831429ee877fa2bbdab2'
  AND ssel.event_tstamp::DATE >= '2021-12-18';

-- jen's booking id A7331924

SELECT *
FROM se.data.scv_touched_transactions stt
WHERE stt.booking_id = 'A7331924';

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '81f7707462b44d9a9839f3afad7c4aa7e73cc51d143de10f1c4a784b1f5a4b36'
  AND ssel.event_tstamp::DATE >= '2021-12-18';

--we have sessions where the transaction event arrives late so we artificially inseminate these transactions to ensure they show a converted session
-- in order to see what sessions have an artificially inseminated booking and no other booking event we need to first filter for sessions with artificially inseminated bookings
-- then limit those sessions to only ones that have only the artificially inseminated transaction in them

--list of sessions with artificially inseminated transactions
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.sessions_with_only_art_ins_transactions AS (
    WITH list_of_sessions_with_as_bookings AS (
        SELECT DISTINCT ssel.touch_id
        FROM se.data_pii.scv_session_events_link ssel
            INNER JOIN se.data_pii.scv_event_stream e ON ssel.event_hash = e.event_hash
        WHERE e.event_tstamp >= CURRENT_DATE - 8
          AND e.useragent = 'data_team_artificial_insemination_transactions'
    ),
         all_trans_for_as_sessions AS (
--check transaction events against sessions
             SELECT IFF(e.useragent = 'data_team_artificial_insemination_transactions', 'af_trans', 'trans') AS trans_type,
                    ssel.touch_id,
                    ssel.event_hash,
                    ssel.event_tstamp,
                    e.event_name,
                    e.v_tracker,
                    e.collector_tstamp,
                    e.ti_orderid,
                    e.useragent
             FROM se.data_pii.scv_session_events_link ssel
                 INNER JOIN list_of_sessions_with_as_bookings ls ON ssel.touch_id = ls.touch_id
                 INNER JOIN se.data_pii.scv_event_stream e ON ssel.event_hash = e.event_hash
             WHERE --filter for transaction events, lifted from dv/dwh/events/07_events_of_interest/02_module_touched_transactions.py
                   (
                           ( -- client side transactions
                                       e.collector_tstamp < '2020-02-28 00:00:00'
                                   AND e.event_name IN ('transaction_item', 'transaction')
                                   AND e.ti_orderid IS NOT NULL
                               )
                           OR
                           ( -- server side transactions
                                   ( -- SE, we are using booking confirmation page view events due to latency of
                                       --update events not always able to be fired at time of the session
                                               e.collector_tstamp >= '2020-02-28 00:00:00'
                                           AND e.event_name = 'page_view'
                                           AND e.v_tracker LIKE 'java-%' --SE
                                           AND
                                               e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR IS NOT DISTINCT FROM
                                               'transaction complete'
                                       )
                                   OR
                                   ( -- TB
                                               e.collector_tstamp >= '2020-02-28 00:00:00'
                                           AND e.event_name = 'booking_update_event'
                                           AND e.v_tracker LIKE 'py-%' --TB
                                           AND
                                               e.unstruct_event_com_secretescapes_booking_update_event_1['sub_category']::VARCHAR IS NOT DISTINCT FROM
                                               'booking confirmed'
                                       )
                               )
                           OR
                           ( -- transaction events for transactions that weren't tracked.
                               e.useragent = 'data_team_artificial_insemination_transactions'
                               )
                       )
         ),
         agg_to_session AS (
             SELECT ats.touch_id,
                    COUNT(DISTINCT ats.trans_type) AS count_transaction_types
             FROM all_trans_for_as_sessions ats
             GROUP BY 1
         )

--list of sessions that only have an artificial insemination transaction event
    SELECT *
    FROM agg_to_session
    WHERE agg_to_session.count_transaction_types = 1
)
-- to check how many sessions only had an artificial insemination event
-- SELECT COUNT(*)
-- FROM agg_to_session
-- WHERE agg_to_session.count_transaction_types = 1
;

-- over last 7 days there were 1,756 sessions that required an artificial insemination transaction
-- over last 7 days there were 15,494 sessions that converted
-- 11% of converted sessions were due to artificial insemination and had no accompanying transaction event

--check how many converted sessions over the last 7 days
SELECT COUNT(DISTINCT touch_id)
FROM se.data.scv_touched_transactions stt
WHERE stt.event_tstamp >= CURRENT_DATE - 8;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '8865bbf7216108f8b2d804add0a14cacf12ecbf3184b54449b14b464e28a7451'
  AND ssel.event_tstamp::DATE >= CURRENT_DATE - 8;


SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.attributed_user_id = '13715843'
  AND ssel.stitched_identity_type = 'se_user_id'
  AND ssel.event_tstamp::DATE >= CURRENT_DATE - 8;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.booking_id = 'A7336170'
  AND ses.event_tstamp >= CURRENT_DATE - 8

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.is_server_side_event
  AND ses.event_tstamp >= CURRENT_DATE
  AND ses.unstruct_event_com_secretescapes_booking_update_event_1 IS NOT NULL;

------------------------------------------------------------------------------------------------------------------------
-- check for all bookings if a subsequent confirmation page event occurs after
USE WAREHOUSE pipe_xlarge;

SELECT TRY_TO_NUMBER(stba.attributed_user_id) AS user_id,
       stt.touch_id,
       stt.booking_id
FROM se.data.scv_touched_transactions stt
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE stt.event_tstamp >= CURRENT_DATE - 8
  AND stba.stitched_identity_type = 'se_user_id';

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '6879c983900f72eaa215790755f525114795faeda2e79696d487fe55121f1d04'
  AND ses.event_tstamp >= CURRENT_DATE - 8;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '540f30793bc95e57040f56904540c4dbd20ab0ddf07e4c7f902ccf1ef014f4d1'
  AND ses.event_tstamp >= CURRENT_DATE - 8;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '0c2a3ac16cfe229a9c95bbf5fbf1c320143113faee569d73fd0cd4f2075d5216'
  AND ses.event_tstamp >= CURRENT_DATE - 8;

SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = 'b15d675325edbb3e2f040aa624868b3c58fff8968658b6b8c79905b2ff264615'
  AND ses.event_tstamp >= CURRENT_DATE - 8;

--/sale/book%
--page_view

SELECT ses.event_tstamp,
       ssel.touch_id,
       ses.se_user_id,
       ses.se_sale_id,
       ses.booking_id,
       ses.useragent,
       ses.page_url,
       ses.page_urlpath,
       ses.unstruct_event_com_secretescapes_booking_update_event_1,
       PARSE_URL(ses.page_url, 1)['parameters']
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ses.event_tstamp >= CURRENT_DATE - 8
  AND ses.page_urlpath LIKE '/sale/book%'
  AND ses.event_name = 'page_view'
  AND ses.booking_id IS NOT NULL;


--107K confirmation pages without booking id
SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ses.event_tstamp >= CURRENT_DATE - 8
  AND ses.page_urlpath LIKE '/sale/book%'
  AND ses.event_name = 'page_view'
  AND ses.booking_id IS NULL


WITH confirmation_pages AS (
    SELECT ses.event_tstamp,
           ssel.touch_id,
           ses.se_user_id,
           ses.se_sale_id,
           ses.booking_id,
           ses.useragent,
           ses.page_url,
           ses.page_urlpath,
           ses.unstruct_event_com_secretescapes_booking_update_event_1,
           PARSE_URL(ses.page_url, 1)['parameters']
    FROM se.data_pii.scv_session_events_link ssel
        INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
    WHERE ses.event_tstamp >= CURRENT_DATE - 8
      AND (
                ses.page_urlpath LIKE '/sale/book%'
            OR
                ses.page_urlpath = '/payment/directSuccess/reservation'
        )
      AND ses.event_name = 'page_view'
      AND ses.booking_id IS NOT NULL
      AND LEFT(ses.booking_id, 3) IS DISTINCT FROM 'TB-'
),
     agg_conf_page_to_booking AS (
         SELECT cp.booking_id
         FROM confirmation_pages cp
         GROUP BY 1
     ),
     model_transactions AS (
         SELECT TRY_TO_NUMBER(stba.attributed_user_id)                                                   AS user_id,
                stt.touch_id,
                stt.booking_id,
                IFF(ag.booking_id IS NOT NULL, 'confirmation_page_fired', 'confirmation_page_not_fired') AS confirmation_page_status
         FROM se.data.scv_touched_transactions stt
             INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
             LEFT JOIN  agg_conf_page_to_booking ag ON stt.booking_id = ag.booking_id
         WHERE stt.event_tstamp >= CURRENT_DATE - 8
           AND stba.stitched_identity_type = 'se_user_id'
           AND LEFT(stt.booking_id, 3) IS DISTINCT FROM 'TB-'
     )
SELECT mt.confirmation_page_status,
       COUNT(*)
FROM model_transactions mt
GROUP BY 1

-- SELECT *
-- FROM model_transactions
-- WHERE model_transactions.confirmation_page_status = 'confirmation_page_not_fired'
;

USE WAREHOUSE pipe_2xlarge;

-- CONFIRMATION_PAGE_STATUS	COUNT(*)
-- confirmation_page_not_fired 289 2%
-- confirmation_page_fired 13,455 98%
-- 13,744

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_user_id = '38380937'
  AND ses.event_tstamp >= CURRENT_DATE - 8;

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_user_id = '34933551'
  AND ses.event_tstamp >= CURRENT_DATE - 8;

------------------------------------------------------------------------------------------------------------------------
WITH transactions AS (
    SELECT stba.attributed_user_id AS user_id,
           stt.touch_id,
           stt.event_tstamp,
           stt.booking_id
    FROM se.data.scv_touched_transactions stt
        INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
    WHERE stt.event_tstamp >= CURRENT_DATE - 8
      AND stba.stitched_identity_type = 'se_user_id'
      AND LEFT(stt.booking_id, 3) IS DISTINCT FROM 'TB-'
),
     confirmation_pages AS (
         SELECT ses.event_tstamp,
                ssel.touch_id,
                ses.se_user_id,
                ses.se_sale_id,
                ses.booking_id,
                ses.useragent,
                ses.page_url,
                ses.page_urlpath,
                ses.unstruct_event_com_secretescapes_booking_update_event_1
         FROM se.data_pii.scv_session_events_link ssel
             INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
                            -- conf pages after booking tstamp
             INNER JOIN transactions t ON ses.booking_id = t.booking_id AND ssel.event_tstamp > t.event_tstamp
         WHERE ses.event_tstamp >= CURRENT_DATE - 8
           AND (
                     ses.page_urlpath LIKE '/sale/book%'
                 OR
                     ses.page_urlpath = '/payment/directSuccess/reservation'
             )
           AND ses.event_name = 'page_view'
           AND ses.booking_id IS NOT NULL
           AND LEFT(ses.booking_id, 3) IS DISTINCT FROM 'TB-'
     ),
     agg_conf_page_to_booking AS (
         SELECT cp.booking_id
         FROM confirmation_pages cp
         GROUP BY 1
     ),
     model_transactions AS (
         SELECT TRY_TO_NUMBER(t.user_id)                                                                 AS user_id,
                t.touch_id,
                t.booking_id,
                IFF(ag.booking_id IS NOT NULL, 'confirmation_page_fired', 'confirmation_page_not_fired') AS confirmation_page_status
         FROM transactions t
             LEFT JOIN agg_conf_page_to_booking ag ON t.booking_id = ag.booking_id
     )
SELECT mt.confirmation_page_status,
       COUNT(*)
FROM model_transactions mt
GROUP BY 1
--
-- SELECT *
-- FROM model_transactions
-- WHERE model_transactions.confirmation_page_status = 'confirmation_page_not_fired'

;
USE WAREHOUSE pipe_xlarge;