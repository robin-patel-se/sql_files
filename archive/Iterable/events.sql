USE WAREHOUSE pipe_xlarge;
--event field schema

SELECT ua.email,
       mt.attributed_user_id AS user_id,
       'sale page view'      AS page_categorisation, -- placeholder when we start to send more event types, can be swapped with new tracking fields
       es.device_platform,
       ds.tech_platform,                             -- need to correct platform to come from event before we send more event types
       mts.se_sale_id,
       --site_event
       mts.page_url,
       mts.event_tstamp

FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON mts.touch_id = mt.touch_id
    AND mt.stitched_identity_type = 'se_user_id' -- user only spvs
    INNER JOIN data_vault_mvp.dwh.user_attributes ua ON mt.attributed_user_id = ua.shiro_user_id::VARCHAR
    INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash
    INNER JOIN data_vault_mvp.dwh.dim_sale ds ON mts.se_sale_id = ds.se_sale_id
WHERE mts.event_tstamp::DATE >= CURRENT_DATE - 1;

------------------------------------------------------------------------------------------------------------------------


SELECT CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP::timestamp_tz,
       TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP),
       TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS TZH:TZM');


ALTER SESSION SET TIMESTAMP_TZ_OUTPUT_FORMAT = 'YYYY-MM-DD HH24:MI:SS TZH:TZM';
CREATE OR REPLACE TABLE scratch.robinpatel.timestamp_test AS (

    SELECT CURRENT_TIMESTAMP AS CURRENT_TIME,
           CURRENT_TIMESTAMP::TIMESTAMP_TZ(0) AS current_time_tz

);
SELECT *
FROM scratch.robinpatel.timestamp_test tt;

SELECT GET_DDL('table', 'scratch.robinpatel.timestamp_test');



SELECT event_name,
       page_urlpath,
       contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::date = '2021-09-21'
  AND e.contexts_com_secretescapes_user_context_1 IS NOT NULL
  AND e.event_name IN ('page_view', 'screen_view')
ORDER BY collector_tstamp DESC;


self_describing_task --include 'dv/dwh/iterable/event.py'  --method 'run' --start '2021-09-21 00:00:00' --end '2021-09-21 00:00:00'

CREATE OR REPLACE VIEW HYGIENE_VAULT_MVP_DEV_ROBIN.SNOWPLOW.EVENT_STREAM AS SELECT * FROM hygiene_vault_mvp.snowplow.event_stream es;
CREATE OR REPLACE VIEW DATA_VAULT_MVP_DEV_ROBIN.DWH.DIM_SALE AS SELECT * FROM DATA_VAULT_MVP.DWH.DIM_SALE;

SELECT * FROM data_vault_mvp_dev_robin.dwh.iterable__event WHERE event_tstamp::DATE >= current_date