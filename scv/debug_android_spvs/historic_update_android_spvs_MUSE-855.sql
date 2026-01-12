USE WAREHOUSE pipe_xlarge;

SELECT es.event_hash,
       es.event_tstamp,
       es.v_tracker,
       es.page_url,
       es.se_sale_id,
       es.contexts_com_secretescapes_screen_context_1,
       es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_context_screen_name,
       es.contexts_com_secretescapes_screen_context_1[0]['screen_id']::VARCHAR   AS screen_context_screen_id,
       SPLIT_PART(es.contexts_com_secretescapes_screen_context_1[0]['screen_id']::VARCHAR, ' page ', -1)                        AS se_sale_id,
       es.contexts_com_secretescapes_content_context_1,
       *
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE = CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%';

--android yesterday
SELECT COUNT(DISTINCT stba.touch_id)  AS sessions,
       COUNT(DISTINCT sts.event_hash) AS spvs,
       spvs / sessions                AS ratio
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON sts.touch_id = stba.touch_id
WHERE stba.touch_experience = 'native app android'
  AND stba.touch_start_tstamp::DATE = CURRENT_DATE - 1;
--current ratio at 0.05

--ios yesterday
SELECT COUNT(DISTINCT stba.touch_id)  AS sessions,
       COUNT(DISTINCT sts.event_hash) AS spvs,
       spvs / sessions                AS ratio
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON sts.touch_id = stba.touch_id
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp::DATE = CURRENT_DATE - 1;
--current ratio at 1.8


/*
    current resolve would add 90K spvs to android, unfortunately that would inflate the ratio to 9.0 which looks far to high
    android sessions: 10298
    SPVs after fix: 53780
*/

------------------------------------------------------------------------------------------------------------------------
--to update history

SELECT es.event_hash,
       es.event_tstamp,
       es.v_tracker,
       es.page_url,
       es.se_sale_id,
       es.contexts_com_secretescapes_screen_context_1,
       es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR AS screen_context_screen_name,
       es.contexts_com_secretescapes_screen_context_1[0]['screen_id']::VARCHAR   AS screen_context_screen_id,
       SPLIT_PART(screen_context_screen_id, ' page ', -1)                        AS se_sale_id,
       es.contexts_com_secretescapes_content_context_1
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = es.event_hash
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE = CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%';



SELECT COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE = CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%';
--additional 54K spvs, would increase android spv/session ratio to 5

------------------------------------------------------------------------------------------------------------------------
--expanding out comparison times to entire month to check validity of 5 spvs per session

--android last 30 days
SELECT COUNT(DISTINCT stba.touch_id)  AS sessions,
       COUNT(DISTINCT sts.event_hash) AS spvs,
       spvs / sessions                AS ratio
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON sts.touch_id = stba.touch_id
WHERE stba.touch_experience = 'native app android'
  AND stba.touch_start_tstamp::DATE >= CURRENT_DATE - 30;
--current ratio at 0.03

--ios last 30 days
SELECT COUNT(DISTINCT stba.touch_id)  AS sessions,
       COUNT(DISTINCT sts.event_hash) AS spvs,
       spvs / sessions                AS ratio
FROM se.data.scv_touch_basic_attributes stba
    LEFT JOIN se.data.scv_touched_spvs sts ON sts.touch_id = stba.touch_id
WHERE stba.touch_experience = 'native app ios'
  AND stba.touch_start_tstamp::DATE >= CURRENT_DATE - 30
--current ratio at 2.0


--check additional spvs
SELECT COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE >= CURRENT_DATE - 30
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%';
--additional android spvs 1747421
--would change the android spv ratio to 5.2


------------------------------------------------------------------------------------------------------------------------
--double check there aren't other details that can mean android spvs via this method need to be further filters

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE = CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%';


USE WAREHOUSE pipe_xlarge;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON es.event_hash = mt.event_hash
WHERE es.contexts_com_secretescapes_screen_context_1 IS NOT NULL
  AND es.event_tstamp::DATE = CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_screen_context_1[0]['screen_name']::VARCHAR = 'sale page'
  AND es.v_tracker LIKE 'andr-%';

