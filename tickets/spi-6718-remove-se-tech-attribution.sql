USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

DROP TABLE IF EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.07_touch_attribution.01_module_touch_attribution.py' \
    --method 'run' \
    --start '2018-01-01 00:00:00' \
    --end '2018-01-01 00:00:00'

SELECT
	attribution_model,
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
GROUP BY 1
;

SELECT
	attribution_model,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution
GROUP BY 1
;



------------------------------------------------------------------------------------------------------------------------

-- post deps
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20241119 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

DROP TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

-- rerun attribution step in scv from 2018

SELECT
	attribution_model,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution_20241119
GROUP BY 1
;

SELECT
	attribution_model,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution
GROUP BY 1
;


USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE VIEW scratch.krystynajohnson.scv_touch_attribution_20241119 AS
SELECT
	touch_id,
	attributed_touch_id,
	attribution_model,
	attributed_weight
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;



USE WAREHOUSE pipe_xlarge
;
-- production last non direct
SELECT
	YEAR(stba.touch_start_tstamp) AS year,
	stmc.touch_mkt_channel,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sta.attributed_touch_id = stmc.touch_id AND sta.attribution_model = 'last non direct'
GROUP BY 1, 2
;

-- development last non direct
SELECT
	YEAR(stba.touch_start_tstamp) AS year,
	stmc.touch_mkt_channel,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution sta
			   ON stba.touch_id = sta.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sta.attributed_touch_id = stmc.touch_id AND sta.attribution_model = 'last non direct'
GROUP BY 1, 2
;


-- production last paid
SELECT
	YEAR(stba.touch_start_tstamp) AS year,
	stmc.touch_mkt_channel,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_attribution sta ON stba.touch_id = sta.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sta.attributed_touch_id = stmc.touch_id AND sta.attribution_model = 'last paid'
GROUP BY 1, 2
;

-- development last paid
SELECT
	YEAR(stba.touch_start_tstamp) AS year,
	stmc.touch_mkt_channel,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution sta
			   ON stba.touch_id = sta.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc
			   ON sta.attributed_touch_id = stmc.touch_id AND sta.attribution_model = 'last paid'
GROUP BY 1, 2
;

------------------------------------------------------------------------------------------------------------------------
-- investigating KJ question on how some transactions are linking back to a different territory via attribution
USE WAREHOUSE pipe_xlarge
;

SELECT *
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.scv_touch_attribution sta
			   ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE stt.booking_id = 'A21077339'
;


-- looking at events of transaction session
SELECT *
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= '2024-11-03'
WHERE ssel.touch_id = '624d4ac2284a0cefcc62eda27e25b7df4fba09a2dca5e90f67416bcc1a0418a2'
  AND ssel.event_tstamp > '2024-11-03'


-- looking at details of attributed session
SELECT *
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_id = '0ea07ba04c9fb7dd85a631a069f7917d80b5183708433b9f1325596db2a10a77'


-- looking at events of attributed session
SELECT *
FROM se.data_pii.scv_session_events_link ssel
	INNER JOIN se.data_pii.scv_event_stream ses ON ssel.event_hash = ses.event_hash AND ses.event_tstamp >= '2024-10-27'
WHERE ssel.touch_id = '0ea07ba04c9fb7dd85a631a069f7917d80b5183708433b9f1325596db2a10a77'
  AND ssel.event_tstamp > '2024-10-27'
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
WHERE stba.attributed_user_id = '12699068'

-- found this first example was due to a session being misappropriated a territory due to the campaign name in an
-- iterable push campaign

-- looking at another example: A21114080

SELECT *
FROM se.data.scv_touched_transactions stt
	INNER JOIN se.data.scv_touch_attribution sta
			   ON stt.touch_id = sta.touch_id AND sta.attribution_model = 'last non direct'
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
	INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON stt.touch_id = stba.touch_id
WHERE stt.booking_id = 'A21114080'
;

SELECT *
FROM se.data_pii.scv_touch_basic_attributes stba
INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.attributed_user_id = '75279314'


------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20241125 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_attribution_temp_20241125;

SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution;
SELECT * FROm data_vault_mvp.single_customer_view_stg.module_touch_attribution_20241125;


SELECT * FROM se.data.scv_touch_attribution sta;