/***********************************************************************************************
Potential sale page views not in scv_touched_spvs
***********************************************************************************************/
USE WAREHOUSE pipe_xlarge;
-- 1.
-- Here is the SQL Query from Apoorva raised in the Thursday DATA SME session
-- Question: Why 700,000+ sessions with zero SPV's
-- Question: We have what look to be sale page views - that don't appear in scv_touched_spvs
WITH spv_table AS (
    SELECT DISTINCT
           stba.touch_id                  AS session_id, -- 700K
           COUNT(DISTINCT sts.event_hash) AS num_spvs    -- zero
    FROM se.data.scv_touch_basic_attributes stba
        LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
    WHERE stba.touch_start_tstamp:: date >= CURRENT_DATE - 30
      AND stba.touch_experience = 'mobile web'
      AND stba.touch_landing_pagepath LIKE '%sale-hotel'
    GROUP BY 1
)
SELECT st.num_spvs,                              -- zero
       COUNT(DISTINCT st.session_id) AS sessions -- 700K
FROM spv_table AS st
GROUP BY st.num_spvs
ORDER BY sessions DESC
;

-- 2.
-- Here is the example given:
-- The URL indicates its a sale page
-- https://www.secretescapes.de/sonnenverwohnter-luxus-auf-kreta-kostenfrei-stornierbar-creta-palace-rethymno-kreta-griechenland/sale-hotel
SELECT *
FROM se.data_pii.scv_touch_basic_attributes
WHERE touch_id = 'ebf37afc30e8554a84ddc799c5ce92e2b77407e20ca2b15b42c61cddbc537846'
  AND touch_start_tstamp::DATE BETWEEN '2021-07-19' AND '2021-07-24'
;

-- 3.
-- But it does not appear in touched spvs
SELECT *
FROM se.data.scv_touched_spvs
WHERE touch_id = 'ebf37afc30e8554a84ddc799c5ce92e2b77407e20ca2b15b42c61cddbc537846'
  AND event_tstamp::DATE BETWEEN '2021-07-19' AND '2021-07-24'
;


/*
-- All columns on the stream
SELECT
    *
FROM hygiene_vault_mvp.snowplow.event_stream   AS ses
LEFT JOIN se.data_pii.scv_session_events_link ssel
ON ses.event_hash = ssel.event_hash AND ssel.stitched_identity_type = 'se_user_id'
WHERE ses.EVENT_HASH IN ('ebf37afc30e8554a84ddc799c5ce92e2b77407e20ca2b15b42c61cddbc537846', '514176712deaf81db8578ed10a52a33bba6044d8679be9765a009f88aa48f8a2')
AND  ses.RUN_TSTAMP::DATE BETWEEN '2021-07-19' AND '2021-07-24'
;
*/

-- 4.
-- Given that it looks to be an SPV (via the URL), tried to investigate why it's not appearing in touched
-- Investigated this module: -- ./dv/dwh/events/07_events_of_interest/01_module_touched_spvs.py
-- Can see these pathways:

-- i) merge_new_data_app (does not follow this path)
-- a) It does not follow this path as the device platform is "mobile web" - so does not match: e.device_platform IN ('native app ios', 'native app android') line:270

-- ii) merge_new_data_web (it follows this pathway, but drops out see b.)
-- a) In here it follows the server-side tracking pathway as it meets this condition:  "e.collector_tstamp >= '2020-02-28 00:00:00'"  line:188
-- b) However, because this touches value is NULL: e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR it does not meet this condition: e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
-- b) Also e.is_server_side_event = FALSE (it needs to be TRUE) -- Explicitly, for this record the server side event is FALSE

-- iii) merge_new_data_webredirect
-- a) It does not follow this pathway as se_category is NULL (needs to be: e.se_category = 'web redirect click') line:378


------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = 'ebf37afc30e8554a84ddc799c5ce92e2b77407e20ca2b15b42c61cddbc537846'
  AND ses.event_tstamp::DATE = '2021-07-21';


--find sessions that have a landing page that appear to be a spv but the session shows no spvs
SELECT *
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON stba.touch_id = sts.touch_id
WHERE stba.touch_start_tstamp:: date >= CURRENT_DATE - 30
  AND stba.touch_experience = 'mobile web'
  AND stba.touch_landing_pagepath LIKE '%sale-hotel'
  AND sts.event_hash IS NULL;


--investigate logged in session that shows spv landing page pattern but shows no spvs
SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '8ce3ac35a481bf3f762e756074574ec957a50255acde680616240fa4c28c1115'
  AND ses.event_tstamp::DATE = '2021-07-29';

--found no unique browser id populated for event


SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.attributed_user_id = '17340051'
  AND ses.event_tstamp::DATE BETWEEN '2021-07-26' AND '2021-08-01';


-- found clientside event that looks like an spv for a logged in user
-- this event has no unique browser id
-- this event also has no corresponding server side event for this user


------------------------------------------------------------------------------------------------------------------------
--investigating another user

--investigate logged in session that shows spv landing page pattern but shows no spvs
SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.touch_id = '96e5bbaafc382b17bdf2c9eead746d6a3c89a35bdb244f2acd29a021bb16cb72'
  AND ses.event_tstamp::DATE = '2021-08-03';

--found no unique browser id populated for event


SELECT *
FROM se.data_pii.scv_session_events_link ssel
    INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash
WHERE ssel.attributed_user_id = '70928897'
  AND ses.event_tstamp::DATE BETWEEN '2021-08-01' AND '2021-08-05';

--checking  hygiene for other events that aren't sessionised

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream ses
WHERE ses.user_id = '70928897'
  AND ses.event_tstamp::DATE BETWEEN '2021-08-01' AND '2021-08-05';

SELECT COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp::DATE = CURRENT_DATE - 1
  AND stba.touch_experience = 'native app android'

------------------------------------------------------------------------------------------------------------------------

--need to quantify how many spvs we are missing giving that we've now identified that server side spvs do not refire when people press back button due to caching
--look at UK and DE traffic for 1 day and then 30 days on domains www.secretecapes.com and www.secretescapes.de
--only look at web spvs


--current UK spv figures using server side tracking yesterday
SELECT COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
  AND stmc.touch_affiliate_territory = 'UK'
  AND sts.event_category = 'page views'--remove non web spvs
  AND stmc.touch_hostname = 'www.secretescapes.com'
;
--173,928

--current UK spv figures using server side tracking last 30 days
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.server_side_spvs AS (
    SELECT sts.event_tstamp::DATE AS date,
           'server_side'          AS source,
           COUNT(*)               AS spvs
    FROM se.data.scv_touched_spvs sts
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    WHERE sts.event_tstamp::DATE >= CURRENT_DATE - 30
      AND stmc.touch_affiliate_territory = 'UK'
      AND sts.event_category = 'page views'--remove non web spvs
      AND stmc.touch_hostname = 'www.secretescapes.com'
    GROUP BY 1, 2
)
;
--5,220,455

--identify UK spv events from client side tracking, utilise logic from codebase to calculate spvs prior to server side tracking going live
USE WAREHOUSE pipe_2xlarge;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.client_side_spvs AS (
    SELECT e.event_tstamp::DATE AS date,
           'client_side'        AS source,
           COUNT(*)             AS spvs
    FROM data_vault_mvp.single_customer_view_stg.module_touchification t
        INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON t.touch_id = mtmc.touch_id
    WHERE t.event_tstamp::DATE >= CURRENT_DATE - 30
      AND mtmc.touch_affiliate_territory = 'UK'
      AND e.event_name = 'page_view'
      AND mtmc.touch_hostname = 'www.secretescapes.com'
      AND e.se_sale_id IS NOT NULL
      AND e.device_platform NOT IN ('native app ios', 'native app android') --explicitly remove native app (as app offer pages appear like web SPVs)
      AND (--line in sand between client side and server side tracking
        (--client side tracking, prior implementation/validation
                (
                            e.page_urlpath LIKE '%/sale'
                        OR
                            e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                    )
                AND e.is_server_side_event = FALSE -- exclude non validated ss events
            ))
    GROUP BY 1, 2
)
;
--157,757


-------------------------------------------------------------------------------------------------------------------------
--duplicate discovery based on member spvs

--work out top level numbers
--server side top numbers
SELECT COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
  AND stmc.touch_affiliate_territory = 'UK'
  AND sts.event_category = 'page views'--remove non web spvs
  AND stmc.touch_hostname = 'www.secretescapes.com';
--207,981

--client side top numbers
SELECT COUNT(*) AS spvs
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON t.touch_id = mtmc.touch_id
WHERE t.event_tstamp::DATE >= CURRENT_DATE - 1
  AND mtmc.touch_affiliate_territory = 'UK'
  AND e.event_name = 'page_view'
  AND mtmc.touch_hostname = 'www.secretescapes.com'
  AND e.se_sale_id IS NOT NULL
  AND e.device_platform NOT IN ('native app ios', 'native app android') --explicitly remove native app (as app offer pages appear like web SPVs)
  AND (--line in sand between client side and server side tracking
    (--client side tracking, prior implementation/validation
            (
                        e.page_urlpath LIKE '%/sale'
                    OR
                        e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                )
            AND e.is_server_side_event = FALSE -- exclude non validated ss events
        ));
--194,490

--work out member spv numbers
--server side member numbers
SELECT COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type = 'se_user_id'
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
  AND stmc.touch_affiliate_territory = 'UK'
  AND sts.event_category = 'page views'--remove non web spvs
  AND stmc.touch_hostname = 'www.secretescapes.com';
--175,916

--client side member numbers
SELECT COUNT(*) AS spvs
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON t.touch_id = mtmc.touch_id
WHERE t.event_tstamp::DATE >= CURRENT_DATE - 1
  AND mtmc.touch_affiliate_territory = 'UK'
  AND e.event_name = 'page_view'
  AND mtmc.touch_hostname = 'www.secretescapes.com'
  AND e.se_sale_id IS NOT NULL
  AND e.device_platform NOT IN ('native app ios', 'native app android') --explicitly remove native app (as app offer pages appear like web SPVs)
  AND (--line in sand between client side and server side tracking
    (--client side tracking, prior implementation/validation
            (
                        e.page_urlpath LIKE '%/sale'
                    OR
                        e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                )
            AND e.is_server_side_event = FALSE -- exclude non validated ss events
        ))
  AND t.stitched_identity_type = 'se_user_id';
--172,620


--looks like when we filter to non member spvs these match.
--investigating non member spvs
--server side non member numbers
SELECT COUNT(*)
FROM se.data.scv_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
    INNER JOIN se.data.scv_touch_basic_attributes stba ON sts.touch_id = stba.touch_id AND stba.stitched_identity_type IS DISTINCT FROM 'se_user_id'
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
  AND stmc.touch_affiliate_territory = 'UK'
  AND sts.event_category = 'page views'--remove non web spvs
  AND stmc.touch_hostname = 'www.secretescapes.com';
--32,065

--client side non member numbers
SELECT COUNT(*) AS spvs
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON t.touch_id = mtmc.touch_id
WHERE t.event_tstamp::DATE >= CURRENT_DATE - 1
  AND mtmc.touch_affiliate_territory = 'UK'
  AND e.event_name = 'page_view'
  AND mtmc.touch_hostname = 'www.secretescapes.com'
  AND e.se_sale_id IS NOT NULL
  AND e.device_platform NOT IN ('native app ios', 'native app android') --explicitly remove native app (as app offer pages appear like web SPVs)
  AND (--line in sand between client side and server side tracking
    (--client side tracking, prior implementation/validation
            (
                        e.page_urlpath LIKE '%/sale'
                    OR
                        e.page_urlpath LIKE '%/sale-%' --eg. /sale-ihp, /sale-hotel for connected sales
                )
            AND e.is_server_side_event = FALSE -- exclude non validated ss events
        ))
  AND t.stitched_identity_type IS DISTINCT FROM 'se_user_id';
--21,870
