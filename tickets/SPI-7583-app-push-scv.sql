-- scv without app push
-- make branch in dbt with source of new table

-- module=/biapp/task_catalogue/dv/dwh/scv/06_touch_channelling/02_module_touch_marketing_channel.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate
	CLONE latest_vault.cms_mysql.affiliate
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_app_installs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_app_installs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_utm_referrer
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_utm_referrer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

--
-- -- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.06_touch_channelling.02_module_touch_marketing_channel.py' \
    --method 'run' \
    --start '2022-11-01 00:00:00' \
    --end '2022-11-01 00:00:00'
*/

-- prod

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
;

-- 473,516,921

-- dev
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
;

-- 473,516,921

-- prod
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
GROUP BY 1
ORDER BY 1
;

-- dev
SELECT
	mtmc.touch_mkt_channel,
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_no_app_push_delete_on_20250901 mtmc
GROUP BY 1
ORDER BY 1
;

-- module=/biapp/task_catalogue/dv/dwh/scv/07_touch_attribution/01_module_touch_attribution.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
-- 	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

-- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.scv.07_touch_attribution.01_module_touch_attribution.py' \
    --method 'run' \
    --start '2022-11-01 00:00:00' \
    --end '2022-11-01 00:00:00'

 */


-- prod

SELECT
	COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
;

-- 947,033,842

-- dev
SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
;

-- 947,033,842

------------------------------------------------------------------------------------------------------------------------
-- module=/biapp/task_catalogue/dv/dwh/transactional/touch_attributes_augmented.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
-- CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS dbt_dev_robin.bi_customer_insight
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_segments_historical_daily
	CLONE dbt.bi_customer_insight.ci_rfv_segments_historical_daily
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_customer_base_historical_daily
	CLONE dbt.bi_customer_insight.ci_rfv_customer_base_historical_daily
;

DROP TABLE data_vault_mvp_dev_robin.dwh.touch_attributes_augmented
;

-- -- optional statement to create the module target table --
-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.touch_attributes_augmented
-- CLONE data_vault_mvp.dwh.touch_attributes_augmented;

/*
self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.transactional.touch_attributes_augmented.py' \
    --method 'run' \
    --start '2022-11-01 00:00:00' \
    --end '2022-11-01 00:00:00'
 */



USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_no_app_push_delete_on_20250901 CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_no_app_push_delete_on_20250901 CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.touch_attributes_augmented_no_app_push_delete_on_20250901 CLONE data_vault_mvp_dev_robin.dwh.touch_attributes_augmented
;

USE ROLE personal_role__robinpatel
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_no_app_push_delete_on_20250901
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution_no_app_push_delete_on_20250901
;

WITH
	touch_basic_attributes AS (
		SELECT
			scv_touch_basic_attributes.touch_start_tstamp::DATE AS date,
			COUNT(*)                                            AS sessions,
		FROM se.data.scv_touch_basic_attributes
		WHERE scv_touch_basic_attributes.stitched_identity_type = 'se_user_id'
		  AND scv_touch_basic_attributes.touch_se_brand = 'SE Brand'
		GROUP BY ALL
	)
		,
	augmented AS (
		SELECT
			touch_attributes_augmented.touch_start_tstamp::DATE AS date,
			COUNT(*)                                            AS sessions,
		FROM se.data.touch_attributes_augmented
		GROUP BY ALL
	)
SELECT
	touch_basic_attributes.date,
	touch_basic_attributes.sessions AS tba_sessions,
	augmented.sessions              AS tbaa_sessions
FROM touch_basic_attributes
LEFT JOIN augmented
			  ON touch_basic_attributes.date = augmented.date
WHERE touch_basic_attributes.date >= '2025-01-01';


SELECT * FROm se.data.touch_attributes_augmented taa WHERE chan