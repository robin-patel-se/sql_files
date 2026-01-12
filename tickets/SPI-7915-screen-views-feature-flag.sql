USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.feature_toggle
	CLONE latest_vault.cms_mysql.feature_toggle
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
;


self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.04_events_of_interest.05_module_touched_feature_flags.py' \
    --method 'run' \
    --start '2025-10-16 00:00:00' \
    --end '2025-10-16 00:00:00';



SELECT
	touchification.touch_id,
	touchification.event_index_within_touch,
	touchification.event_hash,
	event_stream.event_tstamp,
	event_stream.se_user_id IS NOT NULL AS is_logged_in,
	event_stream.contexts_com_secretescapes_user_state_context_1
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification touchification
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream event_stream
	ON event_stream.event_hash = touchification.event_hash
	AND event_stream.v_tracker LIKE ANY ('andr%', 'ios%')
	AND event_stream.event_name = 'screen_view'
-- 	AND event_stream.contexts_com_secretescapes_user_state_context_1[0]['feature_flags'] IS NOT NULL
WHERE touchification.updated_at >= TIMESTAMPADD('day', - 1, '2025-10-14 02:00:00' :: TIMESTAMP)

-- 6.2M -- filtering out not null feature flags and only pageview
-- 6.2M -- filtering out not null feature flags


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.
	module_touched_feature_flags_clone__model_data

SELECT
	event_stream.event_tstamp,
	event_stream.device_platform,
	event_stream.event_name,
	event_stream.v_tracker,
	event_stream.contexts_com_secretescapes_screen_context_1,
	event_stream.contexts_com_secretescapes_user_state_context_1,
	event_stream.contexts_com_secretescapes_user_state_context_1[0]['featureFlags']::ARRAY,
	event_stream.contexts_com_snowplowanalytics_mobile_application_1,
	event_stream.contexts_com_snowplowanalytics_mobile_application_1[0]['version']::VARCHAR AS version,
	*
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream event_stream
WHERE event_stream.device_platform = 'native app ios'
  AND event_stream.event_name = 'screen_view'
  AND event_stream.event_tstamp >= CURRENT_DATE - 10
  AND ARRAY_SIZE(event_stream.contexts_com_secretescapes_user_state_context_1[0]['featureFlags']::ARRAY) > 0



SELECT
	event_stream.event_tstamp::DATE AS date,
	COUNT(*)
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream event_stream
WHERE event_stream.device_platform = 'native app ios'
  AND event_stream.event_name = 'screen_view'
  AND event_stream.event_tstamp >= CURRENT_DATE - 10
  AND ARRAY_SIZE(event_stream.contexts_com_secretescapes_user_state_context_1[0]['featureFlags']::ARRAY) > 0
GROUP BY 1
;


SELECT
	tracker_type,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags_clone__step04__union_ff_data
GROUP BY 1
;



SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags_clone__step02__model_web_ss_data

;

USE WAREHOUSE pipe_xlarge
;

SELECT
	touchification.touch_id,
	touchification.event_index_within_touch,
	touchification.event_hash,
	event_stream.event_tstamp,
	event_stream.se_user_id IS NOT NULL AS is_logged_in,
	event_stream.contexts_com_secretescapes_user_state_context_1
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification touchification
INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream event_stream
	ON event_stream.event_hash = touchification.event_hash
	AND ARRAY_SIZE(event_stream.contexts_com_secretescapes_user_state_context_1[0]['featureFlags']::ARRAY) > 0
	AND event_stream.event_tstamp::DATE >= '2025-10-01'
	AND event_stream.event_name = 'screen_view'
WHERE
	-- date of go live, to avoid backfilling weirdness
	touchification.updated_at >= TIMESTAMPADD('day', -1, '2025-10-14 02:00:00'::TIMESTAMP)


-- post deps
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_feature_flags
	ADD COLUMN num_occurences_app NUMBER
;
-- Backfill to 13th of october


USE ROLE pipelinerunner
;

ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touched_feature_flags
	ADD COLUMN num_occurences_app NUMBER
;

SELECT
	mtff.touch_start_tstamp::DATE,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_feature_flags mtff
WHERE mtff.touch_start_tstamp >= CURRENT_DATE - 5
GROUP BY 1
;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_feature_flags mtff
WHERE mtff.num_occurences_app > 1