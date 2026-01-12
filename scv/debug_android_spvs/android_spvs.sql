--checked original dashboard, spvs to session numbers for android native app are far too low
--checked demand model in tableau and the same issue was shown
--investigate scv datasets to see if the issue still persists

USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------
--create a query that replicates view of spvs and sessions shown in tableau dashboard
SELECT DATE_TRUNC('month', stba.touch_start_tstamp) AS month,
       COUNT(DISTINCT sts.event_hash)               AS spvs,
       COUNT(DISTINCT stba.touch_id)                AS sessions
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp >= '2021-01-01'
  AND stba.touch_experience = 'native app android'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1;

--numbers of sessions to spvs appear far too low. there appear to be in the range of 200K sessions within a month but less than 10K spvs

------------------------------------------------------------------------------------------------------------------------
--identify a user that has multiple sessions on the android app to use for investigation
SELECT stba.attributed_user_id,
       COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2021-08-01'
  AND stba.touch_experience = 'native app android'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1
HAVING COUNT(*) > 5;

--chosen to use active android app user 53714139 who has had multiple sessions on the android app in aug 2021
------------------------------------------------------------------------------------------------------------------------

--have a quick glance at the users sessions to see if they are a viable candidate to help debug
SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2021-08-01'
  AND stba.touch_experience = 'native app android'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.attributed_user_id = '53714139';
--looks like this is a good candidate with lots of data
--touch_id a7db1d0d4eaf7f7fbd7f541a47c8b2623d19e121992b9487b7b247865152816e has a lot of events associated to it in sessionisation


------------------------------------------------------------------------------------------------------------------------
--use session link to see events associated to a high event session,
SELECT ses.*
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = 'a7db1d0d4eaf7f7fbd7f541a47c8b2623d19e121992b9487b7b247865152816e'
  AND ses.event_tstamp::DATE = '2021-08-02';

--all events associated to this session are set as native app android
--cross referenced code base to see how we identify app spvs, dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py
/*
( -- new world native app event data
        e.collector_tstamp >= '2020-02-28 00:00:00'
        AND
        e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
)
 */

SELECT ses.se_sale_id,
       ses.contexts_com_secretescapes_screen_context_1,
       ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR   AS screen_context_screen_name,
       ses.contexts_com_secretescapes_content_context_1,
       ses.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR AS content_context_sub_category,
       ses.contexts_com_secretescapes_sale_page_context_1,
       ses.contexts_com_secretescapes_secret_escapes_sale_context_1
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = 'a7db1d0d4eaf7f7fbd7f541a47c8b2623d19e121992b9487b7b247865152816e'
  AND ses.event_tstamp::DATE = '2021-08-02';
--found that contexts_com_secretescapes_content_context_1 has not been populated for what appears to be spv pages, based off of contexts_com_secretescapes_screen_context_1 information
--contexts_com_secretescapes_secret_escapes_sale_context_1 has also not been populated, which is where we get the se_sale_id from


------------------------------------------------------------------------------------------------------------------------
--going to look at an android ios user for comparison metrics
SELECT stba.attributed_user_id,
       COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2021-08-01'
  AND stba.touch_experience = 'native app ios'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1
HAVING COUNT(*) > 5;

--chosen 16846616 as user who's performed a lot of app ios sessions

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2021-08-01'
  AND stba.touch_experience = 'native app ios'
  AND stba.stitched_identity_type = 'se_user_id'
  AND stba.attributed_user_id = '16846616';

--chosen this session which has a lot of events associated to it b19f6c5c5f1bc8370949b372bfbb133fd49820f876feaffc955f1c6d28d03837

SELECT ssel.*
FROM se.data_pii.scv_session_events_link ssel
WHERE ssel.touch_id = 'b19f6c5c5f1bc8370949b372bfbb133fd49820f876feaffc955f1c6d28d03837'
--all events occur on the 2021-08-14


SELECT ses.se_sale_id,
       ses.contexts_com_secretescapes_screen_context_1,
       ses.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR   AS screen_context_screen_name,
       ses.contexts_com_secretescapes_content_context_1,
       ses.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR AS content_context_sub_category,
       ses.contexts_com_secretescapes_sale_page_context_1,
       ses.contexts_com_secretescapes_secret_escapes_sale_context_1
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = 'b19f6c5c5f1bc8370949b372bfbb133fd49820f876feaffc955f1c6d28d03837'
  AND ses.event_tstamp::DATE = '2021-08-14';

--can see that the sub category and the sale id are populated for this ios session

------------------------------------------------------------------------------------------------------------------------
SELECT DATE_TRUNC('month', stba.touch_start_tstamp)                                            AS month,
       COUNT(DISTINCT stba.touch_id)                                                           AS total_sessions,
       COUNT(DISTINCT IFF(stba.touch_experience = 'native app android', stba.touch_id, NULL))  AS native_android_sessions,
       COUNT(DISTINCT IFF(stba.touch_experience = 'native app ios', stba.touch_id, NULL))      AS native_ios_sessions,
       native_android_sessions / total_sessions AS android_perc,
       COUNT(DISTINCT sts.event_hash)                                                          AS total_spvs,
       COUNT(DISTINCT IFF(stba.touch_experience = 'native app android', sts.event_hash, NULL)) AS native_android_spvs,
       COUNT(DISTINCT IFF(stba.touch_experience = 'native app ios', sts.event_hash, NULL))     AS native_ios_spvs
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp >= '2021-01-01'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1;

SELECT * FROM hygiene_snapshot_vault_mvp.cms_mysql.offer o;

SELECT * FROM se.data.se_hotel_sale_offer shso

------------------------------------------------------------------------------------------------------------------------
--new fix
SELECT sts.event_tstamp::date AS date,
       COUNT(*)               AS spvs
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE sts.event_tstamp >= '2021-09-20'
  AND stba.touch_experience = 'native app android'
  AND stba.stitched_identity_type = 'se_user_id'
GROUP BY 1;

--historic data
SELECT es.event_tstamp::DATE AS date,
       COUNT(*)              AS spvs
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE >= '2021-09-20'
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%'
GROUP BY 1;

USE WAREHOUSE pipe_xlarge;

-- https://docs.google.com/spreadsheets/d/1cqGczVXEoqOXio5AtlBqohxKj2eHmtd0sKt5ak4ImTk/edit#gid=0


SELECT * FROM data_vault_mvp_dev_robin.dwh.iterable__product__model_data;


